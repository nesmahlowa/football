#!/usr/bin/env python3

"""
Football Match Data Pipeline
Stage: NORMALIZE

Responsibility:
    Convert raw source records into a common canonical match structure.

Pipeline:

    source.json
        |
        v
    fetch.py
        |
        v
    raw/
        |
        v
    normalize.py
        |
        v
    normalized/
        |
        v
    deduplicate.py
        |
        v
    validate.py
        |
        v
    build_matches.py
        |
        v
    data/matches.json

IMPORTANT:
    This module does NOT:
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


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DIR = PROJECT_ROOT / "raw"
NORMALIZED_DIR = PROJECT_ROOT / "normalized"


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NORMALIZED_SCHEMA_VERSION = "1.0"


ALLOWED_STATUSES = {
    "SCHEDULED",
    "LIVE",
    "FINISHED",
    "POSTPONED",
    "CANCELLED",
    "SUSPENDED",
    "UNKNOWN",
}


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_string(value: Any) -> str | None:
    """
    Convert a value to a normalized non-empty string.
    """

    if value is None:
        return None

    value = str(value).strip()

    if not value:
        return None

    return value


def normalize_status(value: Any) -> str:
    """
    Convert source-specific status values into the canonical status model.
    """

    if value is None:
        return "UNKNOWN"

    raw = str(value).strip().upper()

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

    return mapping.get(raw, "UNKNOWN")


def normalize_datetime(value: Any) -> str | None:
    """
    Normalize common ISO-8601 datetime representations.

    No timezone is invented when the source does not provide one.
    """

    value = clean_string(value)

    if value is None:
        return None

    # Already ISO-like.
    try:
        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        return parsed.isoformat()

    except ValueError:
        pass

    # Common football-data date format.
    formats = (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%d.%m.%Y",
    )

    for fmt in formats:

        try:
            parsed = datetime.strptime(value, fmt)

            return parsed.replace(
                tzinfo=timezone.utc
            ).isoformat()

        except ValueError:
            continue

    return None


def normalize_team_id(value: Any) -> str | None:
    value = clean_string(value)

    if value is None:
        return None

    return value


def normalize_team_name(value: Any) -> str | None:
    value = clean_string(value)

    if value is None:
        return None

    # Normalize repeated whitespace.
    value = re.sub(r"\s+", " ", value)

    return value


# ---------------------------------------------------------------------------
# Team extraction
# ---------------------------------------------------------------------------

def extract_team(
    value: Any,
    fallback_id: Any = None,
) -> dict[str, Any]:

    if isinstance(value, dict):

        team_id = (
            value.get("id")
            or value.get("teamId")
            or value.get("team_id")
            or value.get("uid")
            or fallback_id
        )

        team_name = (
            value.get("name")
            or value.get("teamName")
            or value.get("team")
            or value.get("club")
        )

    else:

        team_id = fallback_id
        team_name = value

    return {
        "id": normalize_team_id(team_id),
        "name": normalize_team_name(team_name),
    }


# ---------------------------------------------------------------------------
# Match ID
# ---------------------------------------------------------------------------

def normalize_match_id(
    record: dict[str, Any],
    source_id: str,
) -> str | None:

    source_match_id = (
        record.get("id")
        or record.get("matchId")
        or record.get("match_id")
        or record.get("eventId")
        or record.get("event_id")
    )

    source_match_id = clean_string(source_match_id)

    if source_match_id:
        return f"{source_id}:{source_match_id}"

    return None


# ---------------------------------------------------------------------------
# Match extraction
# ---------------------------------------------------------------------------

def normalize_match(
    record: dict[str, Any],
    source_id: str,
) -> dict[str, Any] | None:

    match_id = normalize_match_id(
        record,
        source_id,
    )

    # ---------------------------------------------------------------
    # Home team
    # ---------------------------------------------------------------

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

    home_team = extract_team(home_value)
    away_team = extract_team(away_value)

    # ---------------------------------------------------------------
    # Date/time
    # ---------------------------------------------------------------

    scheduled_value = (
        record.get("scheduledAt")
        or record.get("scheduled_at")
        or record.get("date")
        or record.get("datetime")
        or record.get("dateTime")
        or record.get("startTime")
        or record.get("start_time")
    )

    scheduled_at = normalize_datetime(
        scheduled_value
    )

    # ---------------------------------------------------------------
    # Status
    # ---------------------------------------------------------------

    status = normalize_status(
        record.get("status")
        or record.get("state")
        or record.get("matchStatus")
    )

    # ---------------------------------------------------------------
    # Competition
    # ---------------------------------------------------------------

    competition = record.get(
        "competition"
    )

    if not isinstance(competition, dict):
        competition = {}

    competition_id = (
        competition.get("id")
        or competition.get("competitionId")
        or record.get("competitionId")
        or record.get("competition_id")
    )

    competition_name = (
        competition.get("name")
        or competition.get("competitionName")
        or record.get("competitionName")
        or record.get("league")
        or record.get("leagueName")
    )

    # ---------------------------------------------------------------
    # Reject completely unusable records.
    #
    # Detailed validation is NOT performed here.
    # We only avoid creating a canonical record from an object that
    # clearly contains no match information.
    # ---------------------------------------------------------------

    if (
        match_id is None
        and home_team["name"] is None
        and away_team["name"] is None
        and scheduled_at is None
    ):
        return None

    # ---------------------------------------------------------------
    # Canonical representation
    # ---------------------------------------------------------------

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

        "homeTeam": home_team,

        "awayTeam": away_team,

        "scheduledAt": scheduled_at,

        "status": status,
    }


# ---------------------------------------------------------------------------
# Recursive record discovery
# ---------------------------------------------------------------------------

def discover_records(
    data: Any,
) -> list[dict[str, Any]]:
    """
    Recursively search a JSON document for dictionary objects that may
    represent match records.

    This intentionally does not assume that every source uses the same
    top-level JSON structure.
    """

    records: list[dict[str, Any]] = []

    if isinstance(data, list):

        for item in data:
            records.extend(
                discover_records(item)
            )

        return records

    if not isinstance(data, dict):
        return records

    # Common collection keys used by football datasets.
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

        if isinstance(value, list):

            found_collection = True

            for item in value:

                if isinstance(item, dict):
                    records.append(item)

                else:
                    records.extend(
                        discover_records(item)
                    )

    # If no obvious match collection exists, inspect nested objects.
    if not found_collection:

        for value in data.values():

            if isinstance(value, (dict, list)):
                records.extend(
                    discover_records(value)
                )

    return records


# ---------------------------------------------------------------------------
# Raw file loading
# ---------------------------------------------------------------------------

def load_raw_file(
    path: Path,
) -> Any:

    with path.open(
        "r",
        encoding="utf-8",
        errors="replace",
    ) as file:

        return json.load(file)


# ---------------------------------------------------------------------------
# Source ID extraction
# ---------------------------------------------------------------------------

def source_id_from_filename(
    path: Path,
) -> str:

    filename = path.name

    if filename.endswith(".raw"):
        return filename[:-4]

    return path.stem


# ---------------------------------------------------------------------------
# Normalize one source
# ---------------------------------------------------------------------------

def normalize_source_file(
    raw_path: Path,
) -> tuple[str, list[dict[str, Any]]]:

    source_id = source_id_from_filename(
        raw_path
    )

    print(
        f"[NORMALIZE] Source: {source_id}"
    )

    try:
        data = load_raw_file(
            raw_path
        )

    except json.JSONDecodeError as error:

        print(
            f"[NORMALIZE][PARSE_FAILED] "
            f"{source_id}: {error}",
            file=sys.stderr,
        )

        return source_id, []

    except Exception as error:

        print(
            f"[NORMALIZE][READ_FAILED] "
            f"{source_id}: {error}",
            file=sys.stderr,
        )

        return source_id, []

    raw_records = discover_records(
        data
    )

    normalized_records: list[dict[str, Any]] = []

    for record in raw_records:

        normalized = normalize_match(
            record,
            source_id,
        )

        if normalized is not None:
            normalized_records.append(
                normalized
            )

    print(
        f"[NORMALIZE] "
        f"{source_id}: "
        f"raw={len(raw_records)} "
        f"normalized={len(normalized_records)}"
    )

    return source_id, normalized_records


# ---------------------------------------------------------------------------
# Save normalized data
# ---------------------------------------------------------------------------

def save_normalized_source(
    source_id: str,
    matches: list[dict[str, Any]],
) -> Path:

    NORMALIZED_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        NORMALIZED_DIR
        / f"{source_id}.json"
    )

    payload = {
        "schemaVersion": NORMALIZED_SCHEMA_VERSION,
        "sourceId": source_id,
        "normalizedAt": utc_now(),
        "matchCount": len(matches),
        "matches": matches,
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run() -> int:

    print("=" * 72)
    print("Football Match Data Pipeline - NORMALIZE")
    print("=" * 72)

    if not RAW_DIR.exists():

        print(
            f"[NORMALIZE][FATAL] "
            f"Raw directory does not exist: {RAW_DIR}",
            file=sys.stderr,
        )

        return 1

    raw_files = sorted(
        RAW_DIR.glob("*.raw")
    )

    if not raw_files:

        print(
            "[NORMALIZE][FATAL] "
            "No raw source files found.",
            file=sys.stderr,
        )

        return 1

    total_raw = 0
    total_normalized = 0
    failed_sources = 0

    for raw_file in raw_files:

        source_id, matches = normalize_source_file(
            raw_file
        )

        if not matches:

            print(
                f"[NORMALIZE][WARNING] "
                f"No normalized matches produced "
                f"for {source_id}"
            )

            failed_sources += 1

        output_path = save_normalized_source(
            source_id,
            matches,
        )

        total_normalized += len(matches)

        print(
            f"[NORMALIZE] "
            f"Output: "
            f"{output_path.relative_to(PROJECT_ROOT)}"
        )

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------

    print()
    print("-" * 72)
    print("NORMALIZATION SUMMARY")
    print("-" * 72)

    print(
        f"Raw source files : {len(raw_files)}"
    )

    print(
        f"Sources without normalized data : "
        f"{failed_sources}"
    )

    print(
        f"Normalized matches : "
        f"{total_normalized}"
    )

    print("-" * 72)

    # -----------------------------------------------------------------------
    # Important:
    #
    # Normalization does not determine whether the dataset is valid.
    #
    # validation.py is responsible for that decision.
    # -----------------------------------------------------------------------

    return 0


if __name__ == "__main__":
    raise SystemExit(run())