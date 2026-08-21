#!/usr/bin/env python3

"""
Football Match Data Pipeline
Stage: NORMALIZE

Responsibilities:
    - Read raw JSON / Football.TXT files
    - Convert source-specific records into canonical match records
    - Preserve source provenance
    - Write normalized/<source_id>__*.json

Supported formats:
    - JSON
    - Football.TXT

This stage does NOT:
    - fetch remote data
    - deduplicate matches
    - perform final validation
    - publish matches.json
"""

from __future__ import annotations

import calendar
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
# TIME
# ============================================================================

def utc_now() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


# ============================================================================
# BASIC STRING UTILITIES
# ============================================================================

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
# DATETIME NORMALIZATION
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

    if value.endswith("Z"):
        value = (
            value[:-1]
            + "+00:00"
        )

    # ------------------------------------------------------------------------
    # ISO-8601
    # ------------------------------------------------------------------------

    try:

        parsed = datetime.fromisoformat(
            value
        )

        if parsed.tzinfo is None:
            parsed = parsed.replace(
                tzinfo=timezone.utc
            )

        return parsed.isoformat()

    except ValueError:
        pass

    # ------------------------------------------------------------------------
    # Common date formats
    # ------------------------------------------------------------------------

    formats = (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%d.%m.%Y",

        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",

        "%Y/%m/%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
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

    if isinstance(
        value,
        dict,
    ):

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

    # A match must have both teams and a valid date.
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
# FOOTBALL.TXT DATE PARSING
# ============================================================================

def parse_football_date(
    line: str,
    current_year: int,
) -> tuple[int, int, int] | None:
    """
    Parse common Football.TXT date lines safely.

    Examples:
        Sun Aug 23 2026
        Sun Aug 23
        Aug 23 2026
        Aug 23

    Invalid calendar dates are rejected instead of raising an exception.
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

        try:

            day = int(
                match.group(2)
            )

        except ValueError:

            return None

        year_value = match.group(3)

        try:

            year = (
                int(year_value)
                if year_value
                else current_year
            )

        except ValueError:

            return None

        month = MONTHS.get(
            month_name
        )

        if month is None:
            return None

        # --------------------------------------------------------------------
        # CRITICAL SAFETY CHECK
        #
        # Never allow an invalid calendar date to reach datetime().
        # --------------------------------------------------------------------

        try:

            max_day = calendar.monthrange(
                year,
                month,
            )[1]

        except (
            ValueError,
            OverflowError,
        ):

            return None

        if day < 1 or day > max_day:

            print(
                "[NORMALIZE][WARNING] "
                f"Invalid calendar date ignored: "
                f"{year}-{month:02d}-{day:02d} "
                f"from line: {line!r}",
                file=sys.stderr,
            )

            return None

        return (
            year,
            month,
            day,
        )

    return None


def looks_like_date_line(
    line: str,
) -> bool:

    """
    Determine whether a line resembles a date line.

    This prevents an invalid date line from accidentally being parsed
    as a team-v-team match.
    """

    patterns = (

        r"^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)"
        r"\s+[A-Za-z]+\s+\d{1,2}"
        r"(?:\s+\d{4})?",

        r"^[A-Za-z]+\s+\d{1,2}"
        r"(?:\s+\d{4})?",
    )

    return any(
        re.match(
            pattern,
            line,
            re.IGNORECASE,
        )
        for pattern in patterns
    )


# ============================================================================
# FOOTBALL.TXT TIME PARSING
# ============================================================================

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

    try:

        hour = int(
            match.group(1)
        )

        minute = int(
            match.group(2)
        )

    except ValueError:

        return None

    if hour > 23 or minute > 59:
        return None

    return (
        hour,
        minute,
    )


# ============================================================================
# FOOTBALL.TXT MATCH PARSING
# ============================================================================

def strip_score(
    team: str,
) -> str:

    return re.sub(
        r"\s+\d+\s*[-–]\s*\d+.*$",
        "",
        team,
    ).strip()


def parse_match_line(
    line: str,
) -> tuple[
    str,
    str,
    int | None,
    int | None,
] | None:
    """
    Parse common Football.TXT match lines.

    Supported examples:

        Udinese Calcio v Como 1907

        Bayern München 2-0 VfL Wolfsburg

        Argentina v France

        Argentina 3-3 France
    """

    clean = line.strip()

    if not clean:
        return None

    # Ignore comments/headings.
    if clean.startswith(
        (
            "#",
            "=",
            "▪",
            "»",
            "|",
        )
    ):
        return None

    # Remove venue suffix.
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

    # Remove common bullet prefix.
    clean = re.sub(
        r"^[•*]\s*",
        "",
        clean,
    ).strip()

    # ------------------------------------------------------------------------
    # Separator: v / vs
    # ------------------------------------------------------------------------

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

        # Remove annotations.
        away = re.sub(
            r"\s+\[.*$",
            "",
            away,
        ).strip()

        if home and away:

            return (
                strip_score(home),
                strip_score(away),
                None,
                None,
            )

    # ------------------------------------------------------------------------
    # Separator: score
    # ------------------------------------------------------------------------

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
        ).strip()

        if home and away:

            return (
                strip_score(home),
                strip_score(away),
                int(score.group(1)),
                int(score.group(2)),
            )

    return None


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

        # --------------------------------------------------------------------
        # Competition title
        # --------------------------------------------------------------------

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

        # --------------------------------------------------------------------
        # Comments / metadata
        # --------------------------------------------------------------------

        if line.startswith("#"):
            continue

        # --------------------------------------------------------------------
        # Date
        # --------------------------------------------------------------------

        parsed_date = parse_football_date(
            line,
            current_year,
        )

        if parsed_date:

            current_date = parsed_date
            continue

        # --------------------------------------------------------------------
        # CRITICAL:
        #
        # If the line looks like a date but was invalid, do not allow it
        # to fall through into the match parser.
        # --------------------------------------------------------------------

        if looks_like_date_line(
            line
        ):

            continue

        # --------------------------------------------------------------------
        # Match
        # --------------------------------------------------------------------

        parsed_match = parse_match_line(
            line
        )

        if not parsed_match:
            continue

        (
            home,
            away,
            home_score,
            away_score,
        ) = parsed_match

        # --------------------------------------------------------------------
        # No valid date = do not invent one.
        # --------------------------------------------------------------------

        if current_date is None:

            print(
                "[NORMALIZE][WARNING] "
                "Match ignored because no valid "
                f"date context exists: "
                f"{line!r}; "
                f"source={source_path}; "
                f"line={line_number}",
                file=sys.stderr,
            )

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

        # --------------------------------------------------------------------
        # Defensive datetime creation.
        # --------------------------------------------------------------------

        try:

            scheduled_at = datetime(
                year=year,
                month=month,
                day=day,
                hour=hour,
                minute=minute,
                tzinfo=timezone.utc,
            ).isoformat()

        except (
            ValueError,
            OverflowError,
        ) as error:

            print(
                "[NORMALIZE][WARNING] "
                f"Invalid match datetime ignored: "
                f"{year}-{month:02d}-{day:02d} "
                f"{hour:02d}:{minute:02d}; "
                f"source={source_path}; "
                f"line={line_number}; "
                f"error={error}",
                file=sys.stderr,
            )

            continue

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
                if (
                    home_score is not None
                    and away_score is not None
                )
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

        # --------------------------------------------------------------------
        # Preserve score when available.
        # --------------------------------------------------------------------

        if (
            home_score is not None
            and away_score is not None
        ):

            record["score"] = {
                "home": home_score,
                "away": away_score,
            }

        matches.append(
            record
        )

    return matches


# ============================================================================
# RAW FORMAT DETECTION
# ============================================================================

def detect_format(
    path: Path,
    body: bytes,
) -> str:

    filename = path.name.lower()

    if filename.endswith(
        ".json.raw"
    ):
        return "json"

    if filename.endswith(
        ".txt.raw"
    ):
        return "football.txt"

    try:

        decoded = body.decode(
            "utf-8"
        ).lstrip()

    except UnicodeDecodeError:

        return "unknown"

    if decoded.startswith(
        (
            "{",
            "[",
        )
    ):

        return "json"

    return "football.txt"


# ============================================================================
# RAW FILE LOADING
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

        except (
            json.JSONDecodeError,
            UnicodeDecodeError,
        ) as error:

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
        "[NORMALIZE] "
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
            "[NORMALIZE][PARSE_FAILED] "
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
        "[NORMALIZE] "
        f"format={data_format} "
        f"normalized={len(normalized)}"
    )

    return (
        source_id,
        normalized,
    )


# ============================================================================
# SAVE NORMALIZED FILE
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

    empty_files = 0

    failed_files = 0

    source_counts: dict[
        str,
        int,
    ] = {}

    for raw_file in raw_files:

        try:

            source_id, matches = (
                normalize_file(
                    raw_file
                )
            )

        except Exception as error:

            # ---------------------------------------------------------------
            # Last-resort file isolation.
            #
            # A single malformed source file must never crash normalization
            # of all other sources.
            # ---------------------------------------------------------------

            failed_files += 1

            print(
                "[NORMALIZE][FILE_FAILED] "
                f"{raw_file}: {error}",
                file=sys.stderr,
            )

            continue

        if matches:

            try:

                save_normalized(
                    source_id,
                    raw_file,
                    matches,
                )

            except Exception as error:

                failed_files += 1

                print(
                    "[NORMALIZE][SAVE_FAILED] "
                    f"{raw_file}: {error}",
                    file=sys.stderr,
                )

                continue

            total_normalized += len(
                matches
            )

            source_counts[
                source_id
            ] = (
                source_counts.get(
                    source_id,
                    0,
                )
                + len(matches)
            )

            successful_files += 1

        else:

            empty_files += 1

    # =========================================================================
    # SUMMARY
    # =========================================================================

    print()
    print("-" * 72)
    print(
        "NORMALIZATION SUMMARY"
    )
    print("-" * 72)

    print(
        f"Raw files             : "
        f"{len(raw_files)}"
    )

    print(
        f"Successful files      : "
        f"{successful_files}"
    )

    print(
        f"Empty/no-match files  : "
        f"{empty_files}"
    )

    print(
        f"Failed files          : "
        f"{failed_files}"
    )

    print(
        f"Normalized matches    : "
        f"{total_normalized}"
    )

    if source_counts:

        print()
        print(
            "MATCHES BY SOURCE"
        )

        for source_id in sorted(
            source_counts
        ):

            print(
                f"  {source_id}: "
                f"{source_counts[source_id]}"
            )

    print("-" * 72)

    # =========================================================================
    # PIPELINE SAFETY
    # =========================================================================

    # Never allow an empty normalized dataset to continue toward publication.

    if total_normalized == 0:

        print(
            "[NORMALIZE][FAIL] "
            "Zero matches were normalized.",
            file=sys.stderr,
        )

        return 1

    # File-level failures are reported but do not destroy valid matches
    # produced by other files. The validation stage will decide whether
    # the resulting dataset is acceptable for publication.

    if failed_files > 0:

        print(
            "[NORMALIZE][WARNING] "
            f"{failed_files} raw file(s) failed "
            "and were isolated."
        )

    print(
        "[NORMALIZE][PASS] "
        f"{total_normalized} match(es) normalized."
    )

    return 0


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    raise SystemExit(
        run()
    )
