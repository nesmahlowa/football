#!/usr/bin/env python3

"""
Football Match Data Pipeline
Stage: BUILD MATCHES

Responsibility:
    Build the official data/matches.json artifact from the
    deduplicated dataset only after validation has explicitly
    allowed publication.

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
    deduplicated/matches.json
        |
        v
    validate.py
        |
        v
    validation/validation-report.json
        |
        v
    build_matches.py
        |
        v
    data/matches.json

IMPORTANT:

    This module does NOT:
        - fetch remote data
        - normalize records
        - deduplicate records
        - perform validation
        - silently repair invalid records
        - publish when validation failed

Publication rule:

    validation.status == PASS
    AND
    validation.publicationAllowed == true

Only then is data/matches.json replaced.
"""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_FILE = (
    PROJECT_ROOT
    / "deduplicated"
    / "matches.json"
)

VALIDATION_REPORT = (
    PROJECT_ROOT
    / "validation"
    / "validation-report.json"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
)

OUTPUT_FILE = (
    OUTPUT_DIR
    / "matches.json"
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OUTPUT_SCHEMA_VERSION = "1.0"


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def utc_now() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def relative_path(
    path: Path,
) -> str:

    try:
        return str(
            path.relative_to(
                PROJECT_ROOT
            )
        )

    except ValueError:

        return str(path)


# ---------------------------------------------------------------------------
# JSON loading
# ---------------------------------------------------------------------------

def load_json(
    path: Path,
) -> Any:

    with path.open(
        "r",
        encoding="utf-8",
    ) as file:

        return json.load(file)


# ---------------------------------------------------------------------------
# Validation gate
# ---------------------------------------------------------------------------

def load_validation_gate() -> dict[str, Any]:
    """
    Read the validation report and determine whether publication
    is explicitly authorized.

    There is NO fallback to "probably valid".
    """

    if not VALIDATION_REPORT.exists():

        raise RuntimeError(
            "Validation report does not exist. "
            "Publication is blocked."
        )

    report = load_json(
        VALIDATION_REPORT
    )

    if not isinstance(report, dict):

        raise RuntimeError(
            "Validation report is not a JSON object. "
            "Publication is blocked."
        )

    validation = report.get(
        "validation"
    )

    if not isinstance(validation, dict):

        raise RuntimeError(
            "Validation report does not contain "
            "a valid 'validation' object. "
            "Publication is blocked."
        )

    status = validation.get(
        "status"
    )

    publication_allowed = validation.get(
        "publicationAllowed"
    )

    # -----------------------------------------------------------------------
    # Strict gate
    # -----------------------------------------------------------------------

    if status != "PASS":

        raise RuntimeError(
            "Validation status is not PASS. "
            f"Actual status: {status!r}. "
            "Publication is blocked."
        )

    if publication_allowed is not True:

        raise RuntimeError(
            "Validation did not explicitly authorize publication. "
            f"publicationAllowed={publication_allowed!r}. "
            "Publication is blocked."
        )

    return validation


# ---------------------------------------------------------------------------
# Input dataset
# ---------------------------------------------------------------------------

def load_deduplicated_dataset() -> dict[str, Any]:

    if not INPUT_FILE.exists():

        raise RuntimeError(
            "Deduplicated input dataset does not exist: "
            f"{relative_path(INPUT_FILE)}"
        )

    payload = load_json(
        INPUT_FILE
    )

    if not isinstance(payload, dict):

        raise RuntimeError(
            "Deduplicated dataset must be a JSON object."
        )

    matches = payload.get(
        "matches"
    )

    if not isinstance(matches, list):

        raise RuntimeError(
            "Deduplicated dataset does not contain "
            "a valid matches array."
        )

    for index, match in enumerate(matches):

        if not isinstance(match, dict):

            raise RuntimeError(
                "Invalid match record at index "
                f"{index}."
            )

    return payload


# ---------------------------------------------------------------------------
# Match cleanup
# ---------------------------------------------------------------------------

def clean_match(
    match: dict[str, Any],
) -> dict[str, Any]:
    """
    Prepare the canonical output representation.

    This is intentionally conservative.

    No business data is invented here.
    """

    result: dict[str, Any] = {}

    # -----------------------------------------------------------------------
    # Match identity
    # -----------------------------------------------------------------------

    result["id"] = match.get(
        "id"
    )

    # -----------------------------------------------------------------------
    # Competition
    # -----------------------------------------------------------------------

    competition = match.get(
        "competition"
    )

    if isinstance(
        competition,
        dict,
    ):

        result["competition"] = {
            "id": competition.get(
                "id"
            ),
            "name": competition.get(
                "name"
            ),
        }

    else:

        result["competition"] = {
            "id": None,
            "name": None,
        }

    # -----------------------------------------------------------------------
    # Home team
    # -----------------------------------------------------------------------

    home_team = match.get(
        "homeTeam"
    )

    if isinstance(
        home_team,
        dict,
    ):

        result["homeTeam"] = {
            "id": home_team.get(
                "id"
            ),
            "name": home_team.get(
                "name"
            ),
        }

    else:

        result["homeTeam"] = {
            "id": None,
            "name": None,
        }

    # -----------------------------------------------------------------------
    # Away team
    # -----------------------------------------------------------------------

    away_team = match.get(
        "awayTeam"
    )

    if isinstance(
        away_team,
        dict,
    ):

        result["awayTeam"] = {
            "id": away_team.get(
                "id"
            ),
            "name": away_team.get(
                "name"
            ),
        }

    else:

        result["awayTeam"] = {
            "id": None,
            "name": None,
        }

    # -----------------------------------------------------------------------
    # Date/time
    # -----------------------------------------------------------------------

    result["scheduledAt"] = match.get(
        "scheduledAt"
    )

    # -----------------------------------------------------------------------
    # Status
    # -----------------------------------------------------------------------

    result["status"] = match.get(
        "status"
    )

    # -----------------------------------------------------------------------
    # Provenance
    # -----------------------------------------------------------------------

    provenance = match.get(
        "provenance"
    )

    if isinstance(
        provenance,
        dict,
    ):

        source_ids = provenance.get(
            "sourceIds"
        )

        if isinstance(
            source_ids,
            list,
        ):

            result["provenance"] = {
                "sourceIds": sorted(
                    {
                        str(source_id)
                        for source_id in source_ids
                        if str(source_id).strip()
                    }
                )
            }

    return result


# ---------------------------------------------------------------------------
# Stable ordering
# ---------------------------------------------------------------------------

def match_sort_key(
    match: dict[str, Any],
) -> tuple[str, str, str]:

    scheduled_at = str(
        match.get(
            "scheduledAt"
        ) or ""
    )

    competition = match.get(
        "competition"
    )

    competition_name = ""

    if isinstance(
        competition,
        dict,
    ):

        competition_name = str(
            competition.get(
                "name"
            ) or ""
        ).lower()

    match_id = str(
        match.get(
            "id"
        ) or ""
    ).lower()

    return (
        scheduled_at,
        competition_name,
        match_id,
    )


# ---------------------------------------------------------------------------
# Dataset fingerprint
# ---------------------------------------------------------------------------

def calculate_dataset_hash(
    matches: list[dict[str, Any]],
) -> str:
    """
    Create a deterministic SHA-256 fingerprint of the canonical
    match collection.

    This allows future diagnostics to determine whether the published
    dataset actually changed between pipeline runs.
    """

    canonical = json.dumps(
        matches,
        ensure_ascii=False,
        sort_keys=True,
        separators=(
            ",",
            ":",
        ),
    )

    return hashlib.sha256(
        canonical.encode(
            "utf-8"
        )
    ).hexdigest()


# ---------------------------------------------------------------------------
# Build official artifact
# ---------------------------------------------------------------------------

def build_matches_payload(
    input_payload: dict[str, Any],
    matches: list[dict[str, Any]],
    validation: dict[str, Any],
) -> dict[str, Any]:

    normalized_matches = [
        clean_match(match)
        for match in matches
    ]

    normalized_matches.sort(
        key=match_sort_key
    )

    dataset_hash = calculate_dataset_hash(
        normalized_matches
    )

    # -----------------------------------------------------------------------
    # Preserve only source provenance relevant to the official artifact.
    # -----------------------------------------------------------------------

    source_ids: set[str] = set()

    for match in normalized_matches:

        provenance = match.get(
            "provenance"
        )

        if not isinstance(
            provenance,
            dict,
        ):
            continue

        ids = provenance.get(
            "sourceIds"
        )

        if not isinstance(
            ids,
            list,
        ):
            continue

        for source_id in ids:

            if str(source_id).strip():

                source_ids.add(
                    str(source_id)
                )

    # -----------------------------------------------------------------------
    # Official matches.json
    # -----------------------------------------------------------------------

    return {
        "schemaVersion": OUTPUT_SCHEMA_VERSION,

        "generatedAt": utc_now(),

        "pipeline": {
            "stage": "BUILD_MATCHES",
            "validationStatus": validation.get(
                "status"
            ),
            "publicationAllowed": validation.get(
                "publicationAllowed"
            ),
        },

        "sourceCount": len(
            source_ids
        ),

        "sourceIds": sorted(
            source_ids
        ),

        "matchCount": len(
            normalized_matches
        ),

        "datasetHash": dataset_hash,

        "matches": normalized_matches,
    }


# ---------------------------------------------------------------------------
# Atomic publication
# ---------------------------------------------------------------------------

def publish_atomically(
    payload: dict[str, Any],
) -> None:
    """
    Write to a temporary file first, then replace the official artifact.

    This prevents a partially written matches.json from becoming the
    published dataset.
    """

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_file = (
        OUTPUT_DIR
        / "matches.json.tmp"
    )

    with temporary_file.open(
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

        file.flush()

    temporary_file.replace(
        OUTPUT_FILE
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run() -> int:

    print("=" * 72)
    print("Football Match Data Pipeline - BUILD MATCHES")
    print("=" * 72)

    # -----------------------------------------------------------------------
    # 1. Validation gate
    # -----------------------------------------------------------------------

    try:

        validation = load_validation_gate()

    except Exception as error:

        print(
            f"[BUILD][BLOCKED] {error}",
            file=sys.stderr,
        )

        return 1

    print(
        "[BUILD] Validation gate: PASS"
    )

    print(
        "[BUILD] Publication authorization: TRUE"
    )

    # -----------------------------------------------------------------------
    # 2. Load deduplicated data
    # -----------------------------------------------------------------------

    try:

        input_payload = load_deduplicated_dataset()

    except Exception as error:

        print(
            f"[BUILD][FATAL] {error}",
            file=sys.stderr,
        )

        return 1

    matches = input_payload.get(
        "matches"
    )

    if not isinstance(
        matches,
        list,
    ):

        print(
            "[BUILD][FATAL] "
            "Input matches is not a list.",
            file=sys.stderr,
        )

        return 1

    # -----------------------------------------------------------------------
    # 3. Build payload
    # -----------------------------------------------------------------------

    try:

        payload = build_matches_payload(
            input_payload,
            matches,
            validation,
        )

    except Exception as error:

        print(
            f"[BUILD][FATAL] "
            f"Could not build matches.json: {error}",
            file=sys.stderr,
        )

        return 1

    # -----------------------------------------------------------------------
    # 4. Final internal safety gate
    # -----------------------------------------------------------------------

    if payload["pipeline"].get(
        "validationStatus"
    ) != "PASS":

        print(
            "[BUILD][BLOCKED] "
            "Final validation status is not PASS.",
            file=sys.stderr,
        )

        return 1

    if payload["pipeline"].get(
        "publicationAllowed"
    ) is not True:

        print(
            "[BUILD][BLOCKED] "
            "Publication was not explicitly authorized.",
            file=sys.stderr,
        )

        return 1

    # -----------------------------------------------------------------------
    # 5. Atomic publication
    # -----------------------------------------------------------------------

    try:

        publish_atomically(
            payload
        )

    except Exception as error:

        print(
            f"[BUILD][FATAL] "
            f"Publication failed: {error}",
            file=sys.stderr,
        )

        return 1

    # -----------------------------------------------------------------------
    # 6. Summary
    # -----------------------------------------------------------------------

    print()
    print("-" * 72)
    print("BUILD SUMMARY")
    print("-" * 72)

    print(
        "Validation status : PASS"
    )

    print(
        "Publication        : ALLOWED"
    )

    print(
        f"Source count       : "
        f"{payload['sourceCount']}"
    )

    print(
        f"Match count        : "
        f"{payload['matchCount']}"
    )

    print(
        f"Dataset SHA-256    : "
        f"{payload['datasetHash']}"
    )

    print(
        f"Output             : "
        f"{OUTPUT_FILE.relative_to(PROJECT_ROOT)}"
    )

    print("-" * 72)

    return 0


if __name__ == "__main__":
    raise SystemExit(run())