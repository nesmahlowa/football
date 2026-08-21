#!/usr/bin/env python3

"""
Football Match Data Pipeline
Stage: VALIDATE

Responsibility:
    Validate the deduplicated canonical match dataset according to
    config/validation.json.

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
    validation/
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
        - modify the dataset
        - publish matches.json

Validation principle:

    HTTP success != parsing success
    parsing success != data success
    data success != validation success
    validation success != publication until all required gates pass
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

VALIDATION_CONFIG = (
    PROJECT_ROOT
    / "config"
    / "validation.json"
)

INPUT_FILE = (
    PROJECT_ROOT
    / "deduplicated"
    / "matches.json"
)

SCHEMA_FILE = (
    PROJECT_ROOT
    / "schemas"
    / "matches.schema.json"
)

VALIDATION_DIR = (
    PROJECT_ROOT
    / "validation"
)

VALIDATION_REPORT = (
    VALIDATION_DIR
    / "validation-report.json"
)


# ---------------------------------------------------------------------------
# Result model
# ---------------------------------------------------------------------------

class ValidationResult:

    def __init__(self) -> None:

        self.errors: list[dict[str, Any]] = []
        self.warnings: list[dict[str, Any]] = []
        self.passed_checks: list[str] = []
        self.skipped_checks: list[str] = []

    def error(
        self,
        check: str,
        message: str,
        record_index: int | None = None,
    ) -> None:

        item = {
            "check": check,
            "message": message,
        }

        if record_index is not None:
            item["recordIndex"] = record_index

        self.errors.append(item)

    def warning(
        self,
        check: str,
        message: str,
        record_index: int | None = None,
    ) -> None:

        item = {
            "check": check,
            "message": message,
        }

        if record_index is not None:
            item["recordIndex"] = record_index

        self.warnings.append(item)

    def passed(
        self,
        check: str,
    ) -> None:

        if check not in self.passed_checks:
            self.passed_checks.append(check)

    def skipped(
        self,
        check: str,
    ) -> None:

        if check not in self.skipped_checks:
            self.skipped_checks.append(check)

    @property
    def passed_all_required(self) -> bool:

        return len(self.errors) == 0


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def load_json(
    path: Path,
) -> Any:

    with path.open(
        "r",
        encoding="utf-8",
    ) as file:

        return json.load(file)


def clean_string(
    value: Any,
) -> str:

    if value is None:
        return ""

    return str(value).strip()


def parse_datetime(
    value: Any,
) -> datetime | None:

    value = clean_string(value)

    if not value:
        return None

    try:

        return datetime.fromisoformat(
            value.replace(
                "Z",
                "+00:00",
            )
        )

    except ValueError:

        return None


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def load_validation_config() -> dict[str, Any]:

    if not VALIDATION_CONFIG.exists():

        raise FileNotFoundError(
            f"Validation configuration not found: "
            f"{VALIDATION_CONFIG}"
        )

    config = load_json(
        VALIDATION_CONFIG
    )

    if not isinstance(config, dict):

        raise ValueError(
            "validation.json must contain a JSON object."
        )

    validation = config.get(
        "validation"
    )

    if not isinstance(validation, dict):

        raise ValueError(
            "validation.json is missing the "
            "'validation' object."
        )

    return validation


# ---------------------------------------------------------------------------
# Input loading
# ---------------------------------------------------------------------------

def load_input_dataset() -> tuple[
    dict[str, Any],
    list[dict[str, Any]],
]:

    if not INPUT_FILE.exists():

        raise FileNotFoundError(
            f"Deduplicated dataset not found: "
            f"{INPUT_FILE}"
        )

    payload = load_json(
        INPUT_FILE
    )

    if not isinstance(payload, dict):

        raise ValueError(
            "Deduplicated dataset must be a JSON object."
        )

    matches = payload.get(
        "matches"
    )

    if not isinstance(matches, list):

        raise ValueError(
            "Deduplicated dataset is missing "
            "a valid 'matches' array."
        )

    records = [
        match
        for match in matches
        if isinstance(match, dict)
    ]

    return payload, records


# ---------------------------------------------------------------------------
# Required fields
# ---------------------------------------------------------------------------

def validate_required_fields(
    match: dict[str, Any],
    index: int,
    required_fields: list[str],
    result: ValidationResult,
) -> None:

    for field in required_fields:

        if field not in match:

            result.error(
                "requiredFields",
                f"Missing required field: {field}",
                index,
            )

            continue

        value = match.get(field)

        if value is None:

            result.error(
                "requiredFields",
                f"Required field is null: {field}",
                index,
            )

        elif isinstance(value, str) and not value.strip():

            result.error(
                "requiredFields",
                f"Required field is empty: {field}",
                index,
            )


# ---------------------------------------------------------------------------
# Team validation
# ---------------------------------------------------------------------------

def validate_team(
    team: Any,
    team_name_label: str,
    index: int,
    team_config: dict[str, Any],
    result: ValidationResult,
) -> None:

    if not isinstance(team, dict):

        result.error(
            "teamValidation",
            f"{team_name_label} must be an object.",
            index,
        )

        return

    team_id = clean_string(
        team.get("id")
    )

    team_name = clean_string(
        team.get("name")
    )

    if team_config.get(
        "requireHomeTeamId"
    ) and team_name_label == "homeTeam":

        if not team_id:

            result.error(
                "teamValidation",
                "Home team ID is missing.",
                index,
            )

    if team_config.get(
        "requireAwayTeamId"
    ) and team_name_label == "awayTeam":

        if not team_id:

            result.error(
                "teamValidation",
                "Away team ID is missing.",
                index,
            )

    if team_config.get(
        "requireHomeTeamName"
    ) and team_name_label == "homeTeam":

        if not team_name:

            result.error(
                "teamValidation",
                "Home team name is missing.",
                index,
            )

    if team_config.get(
        "requireAwayTeamName"
    ) and team_name_label == "awayTeam":

        if not team_name:

            result.error(
                "teamValidation",
                "Away team name is missing.",
                index,
            )


def validate_teams(
    match: dict[str, Any],
    index: int,
    validation_config: dict[str, Any],
    result: ValidationResult,
) -> None:

    team_config = validation_config.get(
        "recordValidation",
        {}
    ).get(
        "teamValidation",
        {}
    )

    home_team = match.get(
        "homeTeam"
    )

    away_team = match.get(
        "awayTeam"
    )

    if team_config.get(
        "requireHomeTeam"
    ):

        validate_team(
            home_team,
            "homeTeam",
            index,
            team_config,
            result,
        )

    if team_config.get(
        "requireAwayTeam"
    ):

        validate_team(
            away_team,
            "awayTeam",
            index,
            team_config,
            result,
        )


# ---------------------------------------------------------------------------
# Match identity
# ---------------------------------------------------------------------------

def validate_match_identity(
    match: dict[str, Any],
    index: int,
    result: ValidationResult,
) -> None:

    match_id = clean_string(
        match.get("id")
    )

    if not match_id:

        result.error(
            "matchIdentity",
            "Match ID is missing.",
            index,
        )

        return

    if len(match_id) > 512:

        result.error(
            "matchIdentity",
            "Match ID exceeds maximum length.",
            index,
        )


# ---------------------------------------------------------------------------
# Date/time
# ---------------------------------------------------------------------------

def validate_datetime(
    match: dict[str, Any],
    index: int,
    result: ValidationResult,
) -> None:

    scheduled_at = clean_string(
        match.get("scheduledAt")
    )

    if not scheduled_at:

        result.error(
            "dateTimeValidation",
            "scheduledAt is missing.",
            index,
        )

        return

    if parse_datetime(
        scheduled_at
    ) is None:

        result.error(
            "dateTimeValidation",
            "scheduledAt is not a valid ISO-8601 datetime.",
            index,
        )


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def validate_status(
    match: dict[str, Any],
    index: int,
    allowed_values: list[str],
    result: ValidationResult,
) -> None:

    status = clean_string(
        match.get("status")
    ).upper()

    if not status:

        result.error(
            "statusValidation",
            "Match status is missing.",
            index,
        )

        return

    if status not in allowed_values:

        result.error(
            "statusValidation",
            f"Invalid match status: {status}",
            index,
        )


# ---------------------------------------------------------------------------
# Duplicate validation
# ---------------------------------------------------------------------------

def build_duplicate_key(
    match: dict[str, Any],
) -> str | None:

    match_id = clean_string(
        match.get("id")
    )

    if match_id:

        return (
            "id:"
            + match_id.lower()
        )

    home = match.get(
        "homeTeam"
    )

    away = match.get(
        "awayTeam"
    )

    scheduled_at = clean_string(
        match.get("scheduledAt")
    )

    if (
        isinstance(home, dict)
        and isinstance(away, dict)
        and scheduled_at
    ):

        home_id = clean_string(
            home.get("id")
        )

        away_id = clean_string(
            away.get("id")
        )

        if home_id and away_id:

            return "|".join(
                [
                    "fixture",
                    home_id.lower(),
                    away_id.lower(),
                    scheduled_at.lower(),
                ]
            )

    return None


def validate_duplicates(
    matches: list[dict[str, Any]],
    result: ValidationResult,
) -> None:

    seen: dict[str, int] = {}

    for index, match in enumerate(matches):

        key = build_duplicate_key(
            match
        )

        if key is None:

            result.warning(
                "duplicateValidation",
                "Record has no deterministic duplicate key.",
                index,
            )

            continue

        if key in seen:

            result.error(
                "duplicateValidation",
                (
                    "Duplicate match detected. "
                    f"First occurrence: index {seen[key]}"
                ),
                index,
            )

        else:

            seen[key] = index


# ---------------------------------------------------------------------------
# Dataset validation
# ---------------------------------------------------------------------------

def validate_dataset(
    matches: list[dict[str, Any]],
    dataset_config: dict[str, Any],
    result: ValidationResult,
) -> None:

    if dataset_config.get(
        "requireMatchesArray"
    ) and not isinstance(matches, list):

        result.error(
            "datasetValidation",
            "matches must be an array.",
        )

        return

    minimum_matches = dataset_config.get(
        "minimumMatches",
        0,
    )

    maximum_matches = dataset_config.get(
        "maximumMatches",
        10000,
    )

    count = len(matches)

    if count < minimum_matches:

        result.error(
            "datasetValidation",
            (
                f"Dataset contains {count} matches; "
                f"minimum required is {minimum_matches}."
            ),
        )

    if count > maximum_matches:

        result.error(
            "datasetValidation",
            (
                f"Dataset contains {count} matches; "
                f"maximum allowed is {maximum_matches}."
            ),
        )


# ---------------------------------------------------------------------------
# Anomaly detection
# ---------------------------------------------------------------------------

def validate_match_anomalies(
    match: dict[str, Any],
    index: int,
    result: ValidationResult,
) -> None:

    home = match.get(
        "homeTeam"
    )

    away = match.get(
        "awayTeam"
    )

    if (
        isinstance(home, dict)
        and isinstance(away, dict)
    ):

        home_id = clean_string(
            home.get("id")
        )

        away_id = clean_string(
            away.get("id")
        )

        if (
            home_id
            and away_id
            and home_id == away_id
        ):

            result.error(
                "dataQuality",
                "Home and away teams have the same ID.",
                index,
            )

        home_name = clean_string(
            home.get("name")
        ).lower()

        away_name = clean_string(
            away.get("name")
        ).lower()

        if (
            home_name
            and away_name
            and home_name == away_name
        ):

            result.warning(
                "dataQuality",
                "Home and away team names are identical.",
                index,
            )


# ---------------------------------------------------------------------------
# JSON Schema validation
# ---------------------------------------------------------------------------

def validate_json_schema(
    payload: dict[str, Any],
    result: ValidationResult,
) -> None:

    if not SCHEMA_FILE.exists():

        result.skipped(
            "schemaValidation"
        )

        result.warning(
            "schemaValidation",
            (
                "matches.schema.json is not available; "
                "schema validation was skipped."
            ),
        )

        return

    try:

        import jsonschema

    except ImportError:

        result.skipped(
            "schemaValidation"
        )

        result.warning(
            "schemaValidation",
            (
                "Python package 'jsonschema' is not installed; "
                "schema validation was skipped."
            ),
        )

        return

    try:

        schema = load_json(
            SCHEMA_FILE
        )

        jsonschema.validate(
            instance=payload,
            schema=schema,
        )

        result.passed(
            "schemaValidation"
        )

    except jsonschema.ValidationError as error:

        result.error(
            "schemaValidation",
            (
                "Dataset failed JSON Schema validation: "
                f"{error.message}"
            ),
        )

    except Exception as error:

        result.error(
            "schemaValidation",
            (
                "Schema validation could not be completed: "
                f"{error}"
            ),
        )


# ---------------------------------------------------------------------------
# Main validation
# ---------------------------------------------------------------------------

def run() -> int:

    print("=" * 72)
    print("Football Match Data Pipeline - VALIDATE")
    print("=" * 72)

    # -----------------------------------------------------------------------
    # Load configuration
    # -----------------------------------------------------------------------

    try:

        validation_config = load_validation_config()

    except Exception as error:

        print(
            f"[VALIDATE][FATAL] "
            f"Cannot load validation.json: {error}",
            file=sys.stderr,
        )

        return 1

    if not validation_config.get(
        "enabled",
        True,
    ):

        print(
            "[VALIDATE] Validation is disabled."
        )

        return 0

    # -----------------------------------------------------------------------
    # Load dataset
    # -----------------------------------------------------------------------

    try:

        payload, matches = load_input_dataset()

    except Exception as error:

        print(
            f"[VALIDATE][FATAL] "
            f"Cannot load input dataset: {error}",
            file=sys.stderr,
        )

        return 1

    result = ValidationResult()

    record_validation = validation_config.get(
        "recordValidation",
        {}
    )

    required_fields = record_validation.get(
        "requiredFields",
        [],
    )

    allowed_statuses = record_validation.get(
        "statusValidation",
        {}
    ).get(
        "allowedValues",
        [],
    )

    dataset_validation = validation_config.get(
        "datasetValidation",
        {}
    )

    # -----------------------------------------------------------------------
    # Dataset checks
    # -----------------------------------------------------------------------

    validate_dataset(
        matches,
        dataset_validation,
        result,
    )

    # -----------------------------------------------------------------------
    # Record checks
    # -----------------------------------------------------------------------

    for index, match in enumerate(matches):

        validate_required_fields(
            match,
            index,
            required_fields,
            result,
        )

        validate_teams(
            match,
            index,
            validation_config,
            result,
        )

        validate_match_identity(
            match,
            index,
            result,
        )

        validate_datetime(
            match,
            index,
            result,
        )

        validate_status(
            match,
            index,
            allowed_statuses,
            result,
        )

        validate_match_anomalies(
            match,
            index,
            result,
        )

    # -----------------------------------------------------------------------
    # Duplicate checks
    # -----------------------------------------------------------------------

    duplicate_config = validation_config.get(
        "duplicateValidation",
        {}
    )

    if duplicate_config.get(
        "enabled",
        True,
    ):

        validate_duplicates(
            matches,
            result,
        )

    else:

        result.skipped(
            "duplicateValidation"
        )

    # -----------------------------------------------------------------------
    # Schema validation
    # -----------------------------------------------------------------------

    schema_config = validation_config.get(
        "schemaValidation",
        {}
    )

    if schema_config.get(
        "enabled",
        True,
    ):

        validate_json_schema(
            payload,
            result,
        )

    else:

        result.skipped(
            "schemaValidation"
        )

    # -----------------------------------------------------------------------
    # Publication gate
    # -----------------------------------------------------------------------

    publication_gate = validation_config.get(
        "publicationGate",
        {}
    )

    required_checks = publication_gate.get(
        "requiredChecks",
        [],
    )

    required_check_failures: list[str] = []

    for check in required_checks:

        if any(
            error["check"] == check
            for error in result.errors
        ):

            required_check_failures.append(
                check
            )

    validation_pass = (
        len(result.errors) == 0
        and len(required_check_failures) == 0
    )

    # -----------------------------------------------------------------------
    # Report
    # -----------------------------------------------------------------------

    VALIDATION_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    report = {
        "schemaVersion": "1.0",

        "validation": {
            "status": (
                "PASS"
                if validation_pass
                else "FAIL"
            ),

            "validatedAt": datetime.now().astimezone().isoformat(),

            "inputFile": str(
                INPUT_FILE.relative_to(
                    PROJECT_ROOT
                )
            ),

            "recordCount": len(matches),

            "errors": len(
                result.errors
            ),

            "warnings": len(
                result.warnings
            ),

            "passedChecks": result.passed_checks,

            "skippedChecks": result.skipped_checks,

            "requiredCheckFailures":
                required_check_failures,

            "publicationAllowed":
                validation_pass,
        },

        "errors": result.errors,

        "warnings": result.warnings,
    }

    with VALIDATION_REPORT.open(
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            report,
            file,
            ensure_ascii=False,
            indent=2,
        )

        file.write("\n")

    # -----------------------------------------------------------------------
    # Console summary
    # -----------------------------------------------------------------------

    print()
    print("-" * 72)
    print("VALIDATION SUMMARY")
    print("-" * 72)

    print(
        f"Input matches : {len(matches)}"
    )

    print(
        f"Errors        : {len(result.errors)}"
    )

    print(
        f"Warnings      : {len(result.warnings)}"
    )

    print(
        f"Status        : "
        f"{'PASS' if validation_pass else 'FAIL'}"
    )

    print(
        f"Publication    : "
        f"{'ALLOWED' if validation_pass else 'BLOCKED'}"
    )

    print(
        f"Report        : "
        f"{VALIDATION_REPORT.relative_to(PROJECT_ROOT)}"
    )

    print("-" * 72)

    # -----------------------------------------------------------------------
    # Exit code
    # -----------------------------------------------------------------------

    if validation_pass:

        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(run())