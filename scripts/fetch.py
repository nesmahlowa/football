#!/usr/bin/env python3

"""
Football Match Data Pipeline
Stage: FETCH

Responsibility:
    - Read source.json
    - Read the configured source definitions
    - Fetch source repository/data content
    - Store raw responses for downstream processing

This module MUST NOT:
    - Normalize match data
    - Deduplicate matches
    - Validate canonical matches
    - Build matches.json
    - Publish data

Pipeline position:

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
    deduplicate.py
        |
        v
    validate.py
        |
        v
    build_matches.py
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SOURCE_FILE = PROJECT_ROOT / "source.json"
RAW_DIR = PROJECT_ROOT / "raw"


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

USER_AGENT = "FootballMatchDataPipeline/1.0"
HTTP_TIMEOUT_SECONDS = 30


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class FetchResult:
    source_id: str
    source_url: str
    status: str
    fetched_at: str
    http_status: int | None = None
    bytes_received: int = 0
    output_file: str | None = None
    error: str | None = None


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_source_registry() -> dict[str, Any]:
    """
    Load the official source.json registry.

    The source registry is the single source of truth for configured sources.
    """

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"Source registry not found: {SOURCE_FILE}"
        )

    with SOURCE_FILE.open("r", encoding="utf-8") as file:
        data = json.load(file)

    if not isinstance(data, dict):
        raise ValueError("source.json must contain a JSON object.")

    if "sources" not in data:
        raise ValueError(
            "source.json is missing the required 'sources' array."
        )

    if not isinstance(data["sources"], list):
        raise ValueError(
            "source.json 'sources' must be an array."
        )

    return data


def validate_source_definition(source: dict[str, Any]) -> None:
    """
    Validate only the structural requirements needed by fetch.py.

    Full data validation belongs to validation.py.
    """

    required_fields = [
        "id",
        "name",
        "provider",
        "type",
        "priority",
        "url",
    ]

    for field in required_fields:
        if field not in source:
            raise ValueError(
                f"Source definition is missing required field: {field}"
            )

    if not isinstance(source["id"], str) or not source["id"].strip():
        raise ValueError("Source id must be a non-empty string.")

    if not isinstance(source["url"], str) or not source["url"].strip():
        raise ValueError(
            f"Source '{source['id']}' has an invalid URL."
        )


def build_request(url: str) -> Request:
    return Request(
        url=url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
        },
        method="GET",
    )


def fetch_url(url: str) -> tuple[int, bytes]:
    """
    Fetch a URL and return:

        HTTP status
        response body

    No parsing or normalization is performed here.
    """

    request = build_request(url)

    with urlopen(
        request,
        timeout=HTTP_TIMEOUT_SECONDS,
    ) as response:

        status = response.status
        body = response.read()

        return status, body


def safe_output_filename(source_id: str) -> str:
    """
    Convert source id into a safe raw-data filename.
    """

    safe = "".join(
        character
        if character.isalnum() or character in ("-", "_")
        else "_"
        for character in source_id
    )

    return f"{safe}.raw"


def save_raw_response(
    source_id: str,
    body: bytes,
) -> Path:

    RAW_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = RAW_DIR / safe_output_filename(source_id)

    with output_path.open("wb") as file:
        file.write(body)

    return output_path


# ---------------------------------------------------------------------------
# Source fetching
# ---------------------------------------------------------------------------

def fetch_source(
    source: dict[str, Any],
) -> FetchResult:

    source_id = source["id"]
    source_url = source["url"]
    fetched_at = utc_now()

    print(
        f"[FETCH] {source_id} -> {source_url}"
    )

    try:
        http_status, body = fetch_url(source_url)

        output_path = save_raw_response(
            source_id,
            body,
        )

        print(
            f"[FETCH] {source_id} "
            f"HTTP={http_status} "
            f"bytes={len(body)}"
        )

        return FetchResult(
            source_id=source_id,
            source_url=source_url,
            status="FETCHED",
            fetched_at=fetched_at,
            http_status=http_status,
            bytes_received=len(body),
            output_file=str(
                output_path.relative_to(PROJECT_ROOT)
            ),
        )

    except HTTPError as error:

        print(
            f"[FETCH][HTTP_ERROR] "
            f"{source_id}: "
            f"{error.code} {error.reason}",
            file=sys.stderr,
        )

        return FetchResult(
            source_id=source_id,
            source_url=source_url,
            status="FETCH_FAILED",
            fetched_at=fetched_at,
            http_status=error.code,
            error=f"HTTP {error.code}: {error.reason}",
        )

    except URLError as error:

        print(
            f"[FETCH][URL_ERROR] "
            f"{source_id}: {error.reason}",
            file=sys.stderr,
        )

        return FetchResult(
            source_id=source_id,
            source_url=source_url,
            status="FETCH_FAILED",
            fetched_at=fetched_at,
            error=f"URL error: {error.reason}",
        )

    except Exception as error:

        print(
            f"[FETCH][ERROR] "
            f"{source_id}: {error}",
            file=sys.stderr,
        )

        return FetchResult(
            source_id=source_id,
            source_url=source_url,
            status="FETCH_FAILED",
            fetched_at=fetched_at,
            error=str(error),
        )


# ---------------------------------------------------------------------------
# Main fetch operation
# ---------------------------------------------------------------------------

def run() -> int:

    print("=" * 72)
    print("Football Match Data Pipeline - FETCH")
    print("=" * 72)

    try:
        registry = load_source_registry()

    except Exception as error:

        print(
            f"[FETCH][FATAL] "
            f"Unable to load source.json: {error}",
            file=sys.stderr,
        )

        return 1

    sources = registry["sources"]

    # Respect the official priority defined in source.json.
    sources = sorted(
        sources,
        key=lambda source: source.get("priority", 999999),
    )

    results: list[FetchResult] = []

    for source in sources:

        try:
            validate_source_definition(source)

        except Exception as error:

            print(
                f"[FETCH][SOURCE_CONFIG_ERROR] "
                f"{error}",
                file=sys.stderr,
            )

            results.append(
                FetchResult(
                    source_id=source.get(
                        "id",
                        "unknown",
                    ),
                    source_url=source.get(
                        "url",
                        "",
                    ),
                    status="CONFIG_FAILED",
                    fetched_at=utc_now(),
                    error=str(error),
                )
            )

            continue

        result = fetch_source(source)
        results.append(result)

    # -----------------------------------------------------------------------
    # Fetch summary
    # -----------------------------------------------------------------------

    successful = [
        result
        for result in results
        if result.status == "FETCHED"
    ]

    failed = [
        result
        for result in results
        if result.status != "FETCHED"
    ]

    print()
    print("-" * 72)
    print("FETCH SUMMARY")
    print("-" * 72)

    print(
        f"Configured sources : {len(results)}"
    )

    print(
        f"Successful fetches : {len(successful)}"
    )

    print(
        f"Failed fetches     : {len(failed)}"
    )

    for result in results:

        print(
            f"[{result.status}] "
            f"{result.source_id}"
        )

    print("-" * 72)

    # -----------------------------------------------------------------------
    # Important:
    #
    # A source failure does NOT automatically terminate the entire pipeline.
    # This matches:
    #
    #     pipeline.json
    #     continueOnSourceFailure = true
    #
    # The validation/publication stages decide whether the resulting dataset
    # is acceptable.
    # -----------------------------------------------------------------------

    return 0


if __name__ == "__main__":
    raise SystemExit(run())
