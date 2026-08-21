#!/usr/bin/env python3

"""
Football Match Data Pipeline
Stage: NORMALIZE

Responsibilities:
    - Read raw JSON / Football.TXT files
    - Convert source-specific records into canonical match records
    - Preserve source provenance
    - Write normalized/<source_id>__*.json

This stage does NOT:
    - fetch remote data
    - deduplicate matches
    - perform final validation
    - publish matches.json
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ============================================================================
# PATHS
# ============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DIR = PROJECT_ROOT / "raw"
NORMALIZED_DIR = PROJECT_ROOT / "normalized"


# ============================================================================
# CONSTANTS
# ============================================================================

NORMALIZED_SCHEMA_VERSION = "1.0"

MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


# ============================================================================
# BASIC UTILITIES
# ============================================================================

def utc_now() -> str:

    return datetime.now(
        timezone.utc
    ).isoformat()


def clean_string(
    value: Any,
) -> str | None:

    if value is None:
        return None

    value = str(value).strip()

    if not value:
        return None

    value = re.sub(
        r"\s+",
        " ",
        value,
    )

    return value


def normalize_team_name(
    value: Any,
) -> str | None:

    return clean_string(value)


def normalize_team_id(
    value: Any,
) -> str | None:

    return clean_string(value)


# ============================================================================
# STATUS
# ============================================================================

def normalize_status(
    value: Any,
) -> str:

    if value is None:
        return "UNKNOWN"

    raw = str(
        value
    ).strip().upper()

    mapping = {

        "SCHEDULED": "SCHEDULED",
        "UPCOMING": "SCHEDULED",
        "NOT_STARTED": "SCHEDULED",

        "LIVE": "LIVE",
        "IN_PLAY": "LIVE",
        "PLAYING": "LIVE",

        "FINISHED": "FINISHED",
        "FT": "FINISHED",
        "FULL_TIME": "FINISHED",
        "COMPLETED": "FINISHED",

        "POSTPONED": "POSTPONED",

        "CANCELLED": "CANCELLED",
        "CANCELED": "CANCELLED",

        "SUSPENDED": "SUSPENDED",
    }

    return mapping.get(
        raw,
        "UNKNOWN",
    )


# ============================================================================
# DATETIME
# ============================================================================

def normalize_datetime(
    value: Any,
) -> str | None:

    value = clean_string(value)

    if value is None:
        return None

    value = value.replace(
        " UTC",
        "+00:00",
    )

    value = value.replace(
        "Z",
        "+00:00",
    )

    try:

        parsed = datetime.fromisoformat(
            value
        )

        return parsed.isoformat()

    except ValueError:
        pass

    formats = (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%d.%m.%Y",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
    )

    for fmt in formats:

        try:

            parsed = datetime.strptime(
                value,
                fmt,
            )

            return parsed.replace(
                tzinfo=timezone.utc
            ).isoformat()

        except ValueError:
            continue

    return None


# ============================================================================
# TEAM EXTRACTION
# ============================================================================

def extract_team(
    value: Any,
) -> dict[str, Any]:

    if isinstance(value, dict):

        team_id = (
            value.get("id")
            or value.get("teamId")
            or value.get("team_id")
            or value.get("uid")
        )

        team_name = (
            value.get("name")
            or value.get("teamName")
            or value.get("team")
            or value.get("club")
        )

    else:

        team_id = None
        team_name = value

    return {
        "id": normalize_team_id(
            team_id
        ),
        "name": normalize_team_name(
            team_name
        ),
    }


# ============================================================================
# MATCH ID
# ============================================================================

def normalize_match_id(
    record: dict[str, Any],
    source_id: str,
    fallback: str | None = None,
) -> str:

    source_match_id = (
        record.get("id")
        or record.get("matchId")
        or record.get("match_id")
        or record.get("eventId")
        or record.get("event_id")
        or fallback
    )

    source_match_id = clean_string(
        source_match_id
    )

    if source_match_id:

        return (
            f"{source_id}:"
            f"{source_match_id}"
        )

    # Deterministic fallback.
    home = clean_string(
        record.get("homeTeam")
        or record.get("team1")
        or record.get("home")
    ) or "unknown-home"

    away = clean_string(
        record.get("awayTeam")
        or record.get("team2")
        or record.get("away")
    ) or "unknown-away"

    date = clean_string(
        record.get("date")
        or record.get("scheduledAt")
    ) or "unknown-date"

    return (
        f"{source_id}:"
        f"{date}:"
        f"{home}:"
        f"{away}"
    )


# ============================================================================
# JSON MATCH NORMALIZATION
# ============================================================================

def normalize_json_match(
    record: dict[str, Any],
    source_id: str,
) -> dict[str, Any] | None:

    home_value = (
        record.get("homeTeam")
        or record.get("home_team")
        or record.get("home")
        or record.get("team1")
        or record.get("localTeam")
    )

    away_value = (
        record.get("awayTeam")
        or record.get("away_team")
        or record.get("away")
        or record.get("team2")
        or record.get("visitorTeam")
    )

    home = extract_team(
        home_value
    )

    away = extract_team(
        away_value
    )

    date_value = (
        record.get("scheduledAt")
        or record.get("scheduled_at")
        or record.get("date")
        or record.get("datetime")
        or record.get("dateTime")
        or record.get("startTime")
        or record.get("start_time")
    )

    scheduled_at = normalize_datetime(
        date_value
    )

    competition_value = record.get(
        "competition"
    )

    if isinstance(
        competition_value,
        dict,
    ):

        competition_id = (
            competition_value.get("id")
            or competition_value.get(
                "competitionId"
            )
        )

        competition_name = (
            competition_value.get("name")
            or competition_value.get(
                "competitionName"
            )
        )

    else:

        competition_id = (
            record.get("competitionId")
            or record.get("competition_id")
        )

        competition_name = (
            record.get("competitionName")
            or record.get("league")
            or record.get("leagueName")
            or competition_value
        )

    status = normalize_status(
        record.get("status")
        or record.get("state")
        or record.get("matchStatus")
    )

    # OpenFootball JSON uses team1/team2/date.
    # Therefore a valid match can have no explicit ID.
    if (
        home["name"] is None
        or away["name"] is None
        or scheduled_at is None
    ):
        return None

    match_id = normalize_match_id(
        record,
        source_id,
    )

    return {
        "id": match_id,

        "competition": {
            "id": normalize_team_id(
                competition_id
            ),
            "name": clean_string(
                competition_name
            ),
        },

        "homeTeam": home,

        "awayTeam": away,

        "scheduledAt": scheduled_at,

        "status": status,

        "provenance": {
            "sourceId": source_id,
            "format": "json",
        },
    }


# ============================================================================
# JSON RECORD DISCOVERY
# ============================================================================

def discover_json_records(
    data: Any,
) -> list[dict[str, Any]]:

    records: list[
        dict[str, Any]
    ] = []

    if isinstance(
        data,
        list,
    ):

        for item in data:

            if isinstance(
                item,
                dict,
            ):

                records.append(item)

        return records

    if not isinstance(
        data,
        dict,
    ):
        return records

    collection_keys = (
        "matches",
        "fixtures",
        "events",
        "games",
        "results",
    )

    found_collection = False

    for key in collection_keys:

        value = data.get(key)

        if not isinstance(
            value,
            list,
        ):
            continue

        found_collection = True

        for item in value:

            if isinstance(
                item,
                dict,
            ):
                records.append(item)

    if not found_collection:

        for value in data.values():

            if isinstance(
                value,
                (dict, list),
            ):

                records.extend(
                    discover_json_records(
                        value
                    )
                )

    return records


# ============================================================================
# FOOTBALL.TXT PARSER
# ============================================================================

def extract_season_year(
    path: Path,
) -> int | None:

    matches = re.findall(
        r"20\d{2}",
        str(path),
    )

    if not matches:
        return None

    return int(
        matches[-1]
    )


def parse_football_date(
    line: str,
    current_year: int,
) -> tuple[int, int, int] | None:

    """
    Parse common Football.TXT date lines.

    Examples:

        Sun Aug 23 2026
        Sun Aug 23
        Aug 23 2026
        Aug 23
    """

    clean = line.strip()

    patterns = (

        r"^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)"
        r"\s+"
        r"([A-Za-z]+)"
        r"\s+"
        r"(\d{1,2})"
        r"(?:\s+(\d{4}))?",

        r"^([A-Za-z]+)"
        r"\s+"
        r"(\d{1,2})"
        r"(?:\s+(\d{4}))?",
    )

    for pattern in patterns:

        match = re.match(
            pattern,
            clean,
            re.IGNORECASE,
        )

        if not match:
            continue

        month_name = (
            match.group(1)
            .lower()
        )

        day = int(
            match.group(2)
        )

        year_value = match.group(3)

        year = (
            int(year_value)
            if year_value
            else current_year
        )

        month = MONTHS.get(
            month_name
        )

        if month is None:
            continue

        return (
            year,
            month,
            day,
        )

    return None


def parse_football_time(
    line: str,
) -> tuple[int, int] | None:

    match = re.search(
        r"(?<!\d)"
        r"(\d{1,2}):(\d{2})"
        r"(?!\d)",
        line,
    )

    if not match:
        return None

    hour = int(
        match.group(1)
    )

    minute = int(
        match.group(2)
    )

    if hour > 23 or minute > 59:
        return None

    return (
        hour,
        minute,
    )


def strip_score(
    team: str,
) -> str:

    """
    Remove Football.TXT score suffixes.

    Examples:

        Bayern  3-0  Dortmund
        Argentina 3-3 France [aet; 4-2 on pens]
    """

    team = re.sub(
        r"\s+\d+\s*[-–]\s*\d+.*$",
        "",
        team,
    )

    return team.strip()


def parse_match_line(
    line: str,
) -> tuple[str, str, int | None, int | None] | None:

    """
    Parse a Football.TXT match line.

    Common examples:

        Udinese Calcio v Como 1907

        Bayern München 2-0 VfL Wolfsburg

        Argentina v France

    We deliberately support 'v' / 'vs' as the primary separator.
    """

    clean = line.strip()

    if not clean:
        return None

    # Ignore headings/comments/metadata.
    if clean.startswith(
        ("#", "=", "▪", "»", "|")
    ):
        return None

    # Remove venue.
    clean = re.split(
        r"\s+@\s+",
        clean,
        maxsplit=1,
    )[0].strip()

    # Remove leading time.
    clean = re.sub(
        r"^\d{1,2}:\d{2}\s+",
        "",
        clean,
    )

    # ---------------------------------------------------------------
    # Separator: v / vs
    # ---------------------------------------------------------------

    separator = re.search(
        r"\s+(?:v|vs\.?)\s+",
        clean,
        re.IGNORECASE,
    )

    if separator:

        home = clean[
            :separator.start()
        ].strip()

        away = clean[
            separator.end():
        ].strip()

        away = re.sub(
            r"\s+\[.*$",
            "",
            away,
        )

        if home and away:

            return (
                home,
                away,
                None,
                None,
            )

    # ---------------------------------------------------------------
    # Separator: score
    # ---------------------------------------------------------------

    score = re.search(
        r"\s+(\d+)\s*[-–]\s*(\d+)\s+",
        clean,
    )

    if score:

        home = clean[
            :score.start()
        ].strip()

        away = clean[
            score.end():
        ].strip()

        away = re.sub(
            r"\s+\[.*$",
            "",
            away,
        )

        if home and away:

            return (
                home,
                away,
                int(score.group(1)),
                int(score.group(2)),
            )

    return None


def parse_football_txt(
    text: str,
    source_id: str,
    source_path: Path,
) -> list[dict[str, Any]]:

    lines = text.splitlines()

    matches: list[
        dict[str, Any]
    ] = []

    current_date: tuple[
        int,
        int,
        int,
    ] | None = None

    current_year = (
        extract_season_year(
            source_path
        )
        or datetime.now(
            timezone.utc
        ).year
    )

    competition_name: str | None = None

    for line_number, raw_line in enumerate(
        lines,
        start=1,
    ):

        line = raw_line.strip()

        if not line:
            continue

        # ---------------------------------------------------------------
        # Competition title
        # ---------------------------------------------------------------

        if line.startswith("="):

            title = line[1:].strip()

            title = re.sub(
                r"\s+#.*$",
                "",
                title,
            ).strip()

            if title:
                competition_name = title

            continue

        # ---------------------------------------------------------------
        # Comments / metadata
        # ---------------------------------------------------------------

        if line.startswith("#"):
            continue

        # ---------------------------------------------------------------
        # Date
        # ---------------------------------------------------------------

        parsed_date = parse_football_date(
            line,
            current_year,
        )

        if parsed_date:

            current_date = parsed_date
            continue

        # ---------------------------------------------------------------
        # Match
        # ---------------------------------------------------------------

        parsed_match = parse_match_line(
            line
        )

        if not parsed_match:
            continue

        home, away, home_score, away_score = (
            parsed_match
        )

        if current_date is None:
            # Cannot safely invent a date.
            continue

        year, month, day = current_date

        match_time = parse_football_time(
            line
        )

        if match_time:

            hour, minute = match_time

        else:

            hour = 0
            minute = 0

        scheduled_at = datetime(
            year=year,
            month=month,
            day=day,
            hour=hour,
            minute=minute,
            tzinfo=timezone.utc,
        ).isoformat()

        fallback_id = (
            f"{source_path.stem}:"
            f"{line_number}"
        )

        record = {
            "id": fallback_id,

            "competition": {
                "id": None,
                "name": competition_name,
            },

            "homeTeam": {
                "id": None,
                "name": home,
            },

            "awayTeam": {
                "id": None,
                "name": away,
            },

            "scheduledAt": scheduled_at,

            "status": (
                "FINISHED"
                if home_score is not None
                else "SCHEDULED"
            ),

            "provenance": {
                "sourceId": source_id,
                "format": "football.txt",
                "sourceFile": str(
                    source_path
                ),
                "sourceLine": line_number,
            },
        }

        matches.append(
            record
        )

    return matches


# ============================================================================
# RAW FILE DETECTION
# ============================================================================

def detect_format(
    path: Path,
    body: bytes,
) -> str:

    extension = path.name.lower()

    if extension.endswith(
        ".json.raw"
    ):
        return "json"

    if extension.endswith(
        ".txt.raw"
    ):
        return "football.txt"

    # Fallback: inspect content.
    try:

        decoded = body.decode(
            "utf-8"
        ).lstrip()

    except UnicodeDecodeError:

        return "unknown"

    if decoded.startswith(
        ("{", "[")
    ):

        return "json"

    return "football.txt"


# ============================================================================
# FILE LOADING
# ============================================================================

def load_raw_file(
    path: Path,
) -> tuple[str, Any]:

    body = path.read_bytes()

    data_format = detect_format(
        path,
        body,
    )

    if data_format == "json":

        try:

            data = json.loads(
                body.decode(
                    "utf-8"
                )
            )

        except json.JSONDecodeError as error:

            raise ValueError(
                f"JSON_PARSE_FAILED: {error}"
            ) from error

        return (
            "json",
            data,
        )

    if data_format == "football.txt":

        return (
            "football.txt",
            body.decode(
                "utf-8",
                errors="replace",
            ),
        )

    raise ValueError(
        "Unsupported raw data format."
    )


# ============================================================================
# SOURCE ID
# ============================================================================

def source_id_from_path(
    path: Path,
) -> str:

    relative = path.relative_to(
        RAW_DIR
    )

    parts = relative.parts

    if len(parts) >= 2:
        return parts[0]

    filename = path.name

    if filename.endswith(
        ".raw"
    ):
        filename = filename[:-4]

    return filename.split(
        "__",
        maxsplit=1,
    )[0]


# ============================================================================
# NORMALIZE ONE FILE
# ============================================================================

def normalize_file(
    raw_path: Path,
) -> tuple[
    str,
    list[dict[str, Any]],
]:

    source_id = source_id_from_path(
        raw_path
    )

    print()
    print(
        f"[NORMALIZE] "
        f"{raw_path.relative_to(PROJECT_ROOT)}"
    )

    try:

        data_format, data = (
            load_raw_file(
                raw_path
            )
        )

    except Exception as error:

        print(
            f"[NORMALIZE][PARSE_FAILED] "
            f"{raw_path}: {error}",
            file=sys.stderr,
        )

        return (
            source_id,
            [],
        )

    normalized: list[
        dict[str, Any]
    ] = []

    if data_format == "json":

        raw_records = (
            discover_json_records(
                data
            )
        )

        for record in raw_records:

            match = normalize_json_match(
                record,
                source_id,
            )

            if match:

                normalized.append(
                    match
                )

    elif data_format == "football.txt":

        normalized = (
            parse_football_txt(
                data,
                source_id,
                raw_path,
            )
        )

    print(
        f"[NORMALIZE] "
        f"format={data_format} "
        f"normalized={len(normalized)}"
    )

    return (
        source_id,
        normalized,
    )


# ============================================================================
# SAVE
# ============================================================================

def save_normalized(
    source_id: str,
    source_file: Path,
    matches: list[dict[str, Any]],
) -> Path:

    NORMALIZED_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    source_stem = re.sub(
        r"[^A-Za-z0-9_.-]",
        "_",
        source_file.stem,
    )

    output_path = (
        NORMALIZED_DIR
        / f"{source_id}__"
        f"{source_stem}.json"
    )

    payload = {
        "schemaVersion":
            NORMALIZED_SCHEMA_VERSION,

        "sourceId":
            source_id,

        "sourceFile":
            str(
                source_file.relative_to(
                    PROJECT_ROOT
                )
            ),

        "normalizedAt":
            utc_now(),

        "matchCount":
            len(matches),

        "matches":
            matches,
    }

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            payload,
            file,
            ensure_ascii=False,
            indent=2,
        )

        file.write("\n")

    return output_path


# ============================================================================
# MAIN
# ============================================================================

def run() -> int:

    print("=" * 72)
    print(
        "Football Match Data Pipeline - NORMALIZE"
    )
    print("=" * 72)

    if not RAW_DIR.exists():

        print(
            "[NORMALIZE][FATAL] "
            "raw/ directory does not exist.",
            file=sys.stderr,
        )

        return 1

    raw_files = sorted(
        RAW_DIR.rglob(
            "*.raw"
        )
    )

    if not raw_files:

        print(
            "[NORMALIZE][FATAL] "
            "No raw files found.",
            file=sys.stderr,
        )

        return 1

    total_normalized = 0
    successful_files = 0
    failed_files = 0

    for raw_file in raw_files:

        source_id, matches = (
            normalize_file(
                raw_file
            )
        )

        if matches:

            save_normalized(
                source_id,
                raw_file,
                matches,
            )

            total_normalized += len(
                matches
            )

            successful_files += 1

        else:

            failed_files += 1

    print()
    print("-" * 72)
    print("NORMALIZATION SUMMARY")
    print("-" * 72)

    print(
        f"Raw files            : "
        f"{len(raw_files)}"
    )

    print(
        f"Successful files     : "
        f"{successful_files}"
    )

    print(
        f"Files with no matches: "
        f"{failed_files}"
    )

    print(
        f"Normalized matches   : "
        f"{total_normalized}"
    )

    print("-" * 72)

    # Zero normalized records is a real pipeline failure.
    # We must not allow an empty dataset to reach publication.

    if total_normalized == 0:

        print(
            "[NORMALIZE][FAIL] "
            "Zero matches were normalized.",
            file=sys.stderr,
        )

        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(run())
