#!/usr/bin/env python3

"""
Football Match Data Pipeline
Stage: DEDUPLICATE

Responsibility:
    - Read normalized match records.
    - Detect duplicate matches across sources.
    - Keep one canonical representation of each match.
    - Preserve source provenance.
    - Produce deduplicated data for the validation stage.

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
    deduplicated/
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
        - normalize source records
        - decide final dataset validity
        - publish matches.json
        - invent match IDs
"""


from __future__ import annotations

import hashlib
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

NORMALIZED_DIR = PROJECT_ROOT / "normalized"
DEDUPLICATED_DIR = PROJECT_ROOT / "deduplicated"


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCHEMA_VERSION = "1.0"


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_text(value: Any) -> str:
    if value is None:
        return ""

    value = str(value).strip()

    value = re.sub(
        r"\s+",
        " ",
        value,
    )

    return value


def normalize_identity_text(value: Any) -> str:
    """
    Normalize text only for identity comparison.

    This does NOT modify the canonical stored team name.
    """

    value = clean_text(value).lower()

    # Remove punctuation that commonly differs between sources.
    value = re.sub(
        r"[^\w\s]",
        " ",
        value,
        flags=re.UNICODE,
    )

    value = re.sub(
        r"\s+",
        " ",
        value,
    )

    return value.strip()


def normalize_datetime_for_identity(
    value: Any,
) -> str:
    """
    Convert a datetime to a stable UTC representation when possible.

    If parsing fails, return the cleaned original value rather than
    inventing a timestamp.
    """

    value = clean_text(value)

    if not value:
        return ""

    try:
        normalized = value.replace(
            "Z",
            "+00:00",
        )

        from datetime import datetime

        parsed = datetime.fromisoformat(
            normalized
        )

        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(
                timezone.utc
            )

        return parsed.isoformat()

    except ValueError:
        return value


# ---------------------------------------------------------------------------
# Match identity
# ---------------------------------------------------------------------------

def explicit_match_id(
    match: dict[str, Any],
) -> str:
    """
    Return the canonical match ID when available.

    IMPORTANT:
        Source-prefixed IDs created during normalization are considered
        explicit identities. They are never replaced by a generated ID.
    """

    return clean_text(
        match.get("id")
    )


def team_id(
    team: Any,
) -> str:
    if not isinstance(team, dict):
        return ""

    return clean_text(
        team.get("id")
    )


def team_name(
    team: Any,
) -> str:
    if not isinstance(team, dict):
        return ""

    return clean_text(
        team.get("name")
    )


def build_team_identity(
    team: Any,
) -> str:
    """
    Prefer team ID.

    Fall back to normalized team name only when no ID exists.
    """

    identifier = team_id(team)

    if identifier:
        return f"id:{identifier.lower()}"

    name = normalize_identity_text(
        team_name(team)
    )

    if name:
        return f"name:{name}"

    return ""


def build_match_identity(
    match: dict[str, Any],
) -> tuple[str, str]:
    """
    Return:

        (identity_type, identity_key)

    Identity hierarchy:

        1. Explicit match ID
        2. Team IDs/names + scheduledAt
        3. No identity

    We deliberately do NOT use team names alone because that can
    incorrectly merge different fixtures between the same teams.
    """

    match_id = explicit_match_id(
        match
    )

    if match_id:
        return (
            "EXPLICIT_MATCH_ID",
            match_id.lower(),
        )

    home = build_team_identity(
        match.get("homeTeam")
    )

    away = build_team_identity(
        match.get("awayTeam")
    )

    scheduled_at = normalize_datetime_for_identity(
        match.get("scheduledAt")
    )

    if home and away and scheduled_at:

        key = "|".join(
            [
                home,
                away,
                scheduled_at,
            ]
        )

        return (
            "FIXTURE_COMPOSITE",
            key,
        )

    return (
        "NO_SAFE_IDENTITY",
        "",
    )


# ---------------------------------------------------------------------------
# Merge policy
# ---------------------------------------------------------------------------

def value_quality(
    value: Any,
) -> int:
    """
    Very conservative quality scoring.

    More complete values receive a higher score.
    """

    if value is None:
        return 0

    if isinstance(value, str):
        return 1 if value.strip() else 0

    if isinstance(value, dict):

        score = 0

        for item in value.values():
            score += value_quality(item)

        return score

    if isinstance(value, list):

        return sum(
            value_quality(item)
            for item in value
        )

    return 1


def choose_better_value(
    current: Any,
    candidate: Any,
) -> Any:
    """
    Keep the more complete representation.

    This function does not invent information.
    """

    current_score = value_quality(
        current
    )

    candidate_score = value_quality(
        candidate
    )

    if candidate_score > current_score:
        return candidate

    return current


def merge_team(
    current: Any,
    candidate: Any,
) -> Any:

    if not isinstance(current, dict):
        return candidate

    if not isinstance(candidate, dict):
        return current

    return {
        "id": choose_better_value(
            current.get("id"),
            candidate.get("id"),
        ),
        "name": choose_better_value(
            current.get("name"),
            candidate.get("name"),
        ),
    }


def merge_match(
    current: dict[str, Any],
    candidate: dict[str, Any],
) -> dict[str, Any]:
    """
    Merge two records judged to represent the same match.

    Existing information is preserved unless the candidate is clearly
    more complete.
    """

    merged = dict(current)

    merged["id"] = choose_better_value(
        current.get("id"),
        candidate.get("id"),
    )

    merged["competition"] = choose_better_value(
        current.get("competition"),
        candidate.get("competition"),
    )

    merged["homeTeam"] = merge_team(
        current.get("homeTeam"),
        candidate.get("homeTeam"),
    )

    merged["awayTeam"] = merge_team(
        current.get("awayTeam"),
        candidate.get("awayTeam"),
    )

    merged["scheduledAt"] = choose_better_value(
        current.get("scheduledAt"),
        candidate.get("scheduledAt"),
    )

    merged["status"] = choose_better_value(
        current.get("status"),
        candidate.get("status"),
    )

    return merged


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------

def add_provenance(
    match: dict[str, Any],
    source_id: str,
) -> dict[str, Any]:

    result = dict(match)

    provenance = result.get(
        "provenance"
    )

    if not isinstance(provenance, dict):
        provenance = {}

    source_ids = provenance.get(
        "sourceIds"
    )

    if not isinstance(source_ids, list):
        source_ids = []

    if source_id not in source_ids:
        source_ids.append(
            source_id
        )

    provenance["sourceIds"] = sorted(
        source_ids
    )

    result["provenance"] = provenance

    return result


# ---------------------------------------------------------------------------
# Normalized file loading
# ---------------------------------------------------------------------------

def load_normalized_file(
    path: Path,
) -> tuple[str, list[dict[str, Any]]]:

    with path.open(
        "r",
        encoding="utf-8",
    ) as file:

        payload = json.load(file)

    source_id = clean_text(
        payload.get(
            "sourceId"
        )
    )

    matches = payload.get(
        "matches"
    )

    if not source_id:
        source_id = path.stem

    if not isinstance(matches, list):
        matches = []

    valid_records = [
        match
        for match in matches
        if isinstance(match, dict)
    ]

    return (
        source_id,
        valid_records,
    )


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def deduplicate(
    normalized_files: list[Path],
) -> tuple[
    list[dict[str, Any]],
    dict[str, Any],
]:

    identity_map: dict[
        tuple[str, str],
        dict[str, Any],
    ] = {}

    no_identity: list[
        dict[str, Any]
    ] = []

    statistics = {
        "inputRecords": 0,
        "identifiedRecords": 0,
        "recordsWithoutSafeIdentity": 0,
        "duplicateRecords": 0,
        "uniqueRecords": 0,
        "identityCollisions": 0,
    }

    for normalized_file in normalized_files:

        source_id, matches = load_normalized_file(
            normalized_file
        )

        for match in matches:

            statistics["inputRecords"] += 1

            match = add_provenance(
                match,
                source_id,
            )

            identity_type, identity_key = build_match_identity(
                match
            )

            if identity_type == "NO_SAFE_IDENTITY":

                statistics[
                    "recordsWithoutSafeIdentity"
                ] += 1

                no_identity.append(
                    match
                )

                continue

            statistics[
                "identifiedRecords"
            ] += 1

            identity = (
                identity_type,
                identity_key,
            )

            existing = identity_map.get(
                identity
            )

            if existing is None:

                identity_map[identity] = match

            else:

                statistics[
                    "duplicateRecords"
                ] += 1

                identity_map[identity] = merge_match(
                    existing,
                    match,
                )

    unique_matches = list(
        identity_map.values()
    )

    # -----------------------------------------------------------------------
    # Records without a safe identity are NOT automatically merged.
    #
    # This is deliberate. Incorrect merging is more dangerous than keeping
    # a candidate that the validation stage can later reject or quarantine.
    # -----------------------------------------------------------------------

    unique_matches.extend(
        no_identity
    )

    statistics["uniqueRecords"] = len(
        unique_matches
    )

    return (
        unique_matches,
        statistics,
    )


# ---------------------------------------------------------------------------
# Save result
# ---------------------------------------------------------------------------

def save_deduplicated_data(
    matches: list[dict[str, Any]],
    statistics: dict[str, Any],
) -> Path:

    DEDUPLICATED_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        DEDUPLICATED_DIR
        / "matches.json"
    )

    payload = {
        "schemaVersion": SCHEMA_VERSION,
        "deduplicatedAt": utc_now(),

        "statistics": statistics,

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
    print("Football Match Data Pipeline - DEDUPLICATE")
    print("=" * 72)

    if not NORMALIZED_DIR.exists():

        print(
            "[DEDUPLICATE][FATAL] "
            "Normalized directory does not exist.",
            file=sys.stderr,
        )

        return 1

    normalized_files = sorted(
        NORMALIZED_DIR.glob("*.json")
    )

    if not normalized_files:

        print(
            "[DEDUPLICATE][FATAL] "
            "No normalized source files found.",
            file=sys.stderr,
        )

        return 1

    try:

        matches, statistics = deduplicate(
            normalized_files
        )

    except Exception as error:

        print(
            f"[DEDUPLICATE][FATAL] {error}",
            file=sys.stderr,
        )

        return 1

    output_path = save_deduplicated_data(
        matches,
        statistics,
    )

    print()
    print("-" * 72)
    print("DEDUPLICATION SUMMARY")
    print("-" * 72)

    print(
        f"Input records             : "
        f"{statistics['inputRecords']}"
    )

    print(
        f"Identified records        : "
        f"{statistics['identifiedRecords']}"
    )

    print(
        f"Duplicate records removed : "
        f"{statistics['duplicateRecords']}"
    )

    print(
        f"Records without safe ID   : "
        f"{statistics['recordsWithoutSafeIdentity']}"
    )

    print(
        f"Final unique records      : "
        f"{statistics['uniqueRecords']}"
    )

    print(
        f"Output                    : "
        f"{output_path.relative_to(PROJECT_ROOT)}"
    )

    print("-" * 72)

    return 0


if __name__ == "__main__":
    raise SystemExit(run())