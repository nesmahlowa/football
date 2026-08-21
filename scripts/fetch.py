#!/usr/bin/env python3

"""
Football Match Data Pipeline
Stage: FETCH

Responsibilities:
    - Read source.json
    - Resolve GitHub repository URLs
    - Discover actual data files inside repositories
    - Select current/relevant football datasets
    - Download raw JSON / Football.TXT files
    - Store them under raw/<source_id>/

This stage does NOT:
    - normalize matches
    - deduplicate matches
    - validate canonical matches
    - build matches.json
    - publish data
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


# ============================================================================
# PATHS
# ============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SOURCE_FILE = PROJECT_ROOT / "source.json"
RAW_DIR = PROJECT_ROOT / "raw"


# ============================================================================
# CONSTANTS
# ============================================================================

USER_AGENT = "FootballMatchDataPipeline/2.0"

HTTP_TIMEOUT_SECONDS = 60

GITHUB_API_BASE = "https://api.github.com"

SUPPORTED_EXTENSIONS = {
    ".json",
    ".txt",
}


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class RemoteFile:
    path: str
    download_url: str
    size: int
    extension: str


@dataclass
class FetchResult:
    source_id: str
    source_url: str
    status: str
    fetched_at: str
    files_discovered: int = 0
    files_selected: int = 0
    files_downloaded: int = 0
    bytes_received: int = 0
    error: str | None = None


# ============================================================================
# TIME
# ============================================================================

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ============================================================================
# JSON
# ============================================================================

def load_json(path: Path) -> dict[str, Any]:

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


# ============================================================================
# SOURCE REGISTRY
# ============================================================================

def load_source_registry() -> dict[str, Any]:

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"Source registry not found: {SOURCE_FILE}"
        )

    data = load_json(SOURCE_FILE)

    if not isinstance(data, dict):
        raise ValueError(
            "source.json must contain a JSON object."
        )

    sources = data.get("sources")

    if not isinstance(sources, list):
        raise ValueError(
            "source.json must contain a 'sources' array."
        )

    return data


def validate_source_definition(
    source: dict[str, Any],
) -> None:

    required = (
        "id",
        "name",
        "provider",
        "type",
        "priority",
        "url",
    )

    for field in required:
        if field not in source:
            raise ValueError(
                f"Source definition missing '{field}'."
            )

    if not isinstance(source["id"], str):
        raise ValueError("Source id must be a string.")

    if not source["id"].strip():
        raise ValueError("Source id cannot be empty.")

    if not isinstance(source["url"], str):
        raise ValueError(
            f"Source '{source['id']}' URL must be a string."
        )


# ============================================================================
# HTTP
# ============================================================================

def build_request(
    url: str,
    accept: str = "*/*",
) -> Request:

    return Request(
        url=url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": accept,
        },
        method="GET",
    )


def fetch_bytes(
    url: str,
) -> tuple[int, bytes]:

    request = build_request(url)

    with urlopen(
        request,
        timeout=HTTP_TIMEOUT_SECONDS,
    ) as response:

        return response.status, response.read()


def fetch_json_url(
    url: str,
) -> Any:

    status, body = fetch_bytes(url)

    if status != 200:
        raise RuntimeError(
            f"HTTP {status} while fetching {url}"
        )

    try:
        return json.loads(
            body.decode("utf-8")
        )

    except json.JSONDecodeError as error:
        raise RuntimeError(
            f"Invalid JSON from {url}: {error}"
        ) from error


# ============================================================================
# GITHUB REPOSITORY RESOLUTION
# ============================================================================

def parse_github_repository(
    url: str,
) -> tuple[str, str] | None:

    match = re.match(
        r"^https?://github\.com/"
        r"([^/]+)/([^/#?]+)"
        r"(?:/.*)?$",
        url.strip(),
    )

    if not match:
        return None

    owner = match.group(1)
    repo = match.group(2)

    if repo.endswith(".git"):
        repo = repo[:-4]

    return owner, repo


def github_api_url(
    owner: str,
    repo: str,
    path: str,
) -> str:

    encoded_path = quote(
        path.strip("/"),
        safe="/",
    )

    return (
        f"{GITHUB_API_BASE}/repos/"
        f"{owner}/{repo}/contents/{encoded_path}"
    )


# ============================================================================
# GITHUB TREE DISCOVERY
# ============================================================================

def discover_github_files(
    owner: str,
    repo: str,
) -> list[RemoteFile]:

    """
    Discover repository files recursively.

    We use the GitHub Git Trees API rather than downloading the repository
    HTML page.

    This is the critical fix for the original pipeline.
    """

    repo_api = (
        f"{GITHUB_API_BASE}/repos/"
        f"{owner}/{repo}"
    )

    repo_info = fetch_json_url(repo_api)

    default_branch = repo_info.get(
        "default_branch"
    )

    if not default_branch:
        raise RuntimeError(
            f"Unable to determine default branch "
            f"for {owner}/{repo}"
        )

    branch_api = (
        f"{GITHUB_API_BASE}/repos/"
        f"{owner}/{repo}/git/ref/heads/"
        f"{quote(default_branch, safe='')}"
    )

    branch_info = fetch_json_url(
        branch_api
    )

    commit_sha = (
        branch_info
        .get("object", {})
        .get("sha")
    )

    if not commit_sha:
        raise RuntimeError(
            f"Unable to resolve branch SHA "
            f"for {owner}/{repo}"
        )

    tree_url = (
        f"{GITHUB_API_BASE}/repos/"
        f"{owner}/{repo}/git/trees/"
        f"{commit_sha}?recursive=1"
    )

    tree = fetch_json_url(tree_url)

    if not isinstance(tree, dict):
        raise RuntimeError(
            "GitHub tree response is invalid."
        )

    if tree.get("truncated") is True:
        raise RuntimeError(
            f"GitHub tree for {owner}/{repo} "
            f"is truncated. Refusing incomplete "
            f"source discovery."
        )

    files: list[RemoteFile] = []

    for item in tree.get("tree", []):

        if item.get("type") != "blob":
            continue

        path = str(
            item.get("path", "")
        )

        extension = Path(path).suffix.lower()

        if extension not in SUPPORTED_EXTENSIONS:
            continue

        download_url = (
            f"https://raw.githubusercontent.com/"
            f"{owner}/{repo}/"
            f"{default_branch}/"
            f"{quote(path, safe='/')}"
        )

        files.append(
            RemoteFile(
                path=path,
                download_url=download_url,
                size=int(
                    item.get("size", 0)
                    or 0
                ),
                extension=extension,
            )
        )

    return files


# ============================================================================
# SEASON DETECTION
# ============================================================================

def season_score(path: str) -> int:

    """
    Give a score to paths containing recent/current football seasons.

    Examples:
        2026-27
        2025-26
        2026
        2025
    """

    years = [
        int(value)
        for value in re.findall(
            r"(?<!\d)(20\d{2})(?!\d)",
            path,
        )
    ]

    if not years:
        return 0

    current_year = datetime.now(
        timezone.utc
    ).year

    score = 0

    for year in years:

        distance = abs(
            year - current_year
        )

        score = max(
            score,
            1000 - distance * 10,
        )

    # Prefer current season patterns.
    if re.search(
        r"20\d{2}[-_/]20\d{2}",
        path,
    ):
        score += 100

    return score


# ============================================================================
# FILE SELECTION
# ============================================================================

def looks_like_metadata_file(
    path: str,
) -> bool:

    filename = Path(path).name.lower()

    metadata_names = {
        "readme.txt",
        "notes.txt",
        "license.txt",
        "license.md",
        "changelog.txt",
    }

    return filename in metadata_names


def looks_like_match_data(
    path: str,
) -> bool:

    lower = path.lower()

    if looks_like_metadata_file(lower):
        return False

    keywords = (
        "match",
        "league",
        "premier",
        "bundesliga",
        "serie",
        "laliga",
        "ligue",
        "cup",
        "qual",
        "championship",
        "division",
        "euro",
        "worldcup",
    )

    return any(
        keyword in lower
        for keyword in keywords
    )


def select_relevant_files(
    source_id: str,
    files: list[RemoteFile],
) -> list[RemoteFile]:

    candidates = [
        file
        for file in files
        if looks_like_match_data(
            file.path
        )
    ]

    if not candidates:
        candidates = files

    # ------------------------------------------------------------------
    # For football.json-like sources:
    #
    # Prefer current season JSON datasets.
    # ------------------------------------------------------------------

    json_files = [
        file
        for file in candidates
        if file.extension == ".json"
    ]

    txt_files = [
        file
        for file in candidates
        if file.extension == ".txt"
    ]

    selected: list[RemoteFile] = []

    if json_files:

        scored = sorted(
            json_files,
            key=lambda file: (
                season_score(file.path),
                -len(file.path),
            ),
            reverse=True,
        )

        best_score = season_score(
            scored[0].path
        )

        selected.extend(
            file
            for file in scored
            if season_score(file.path)
            >= best_score - 30
        )

    # ------------------------------------------------------------------
    # For Football.TXT sources:
    #
    # Select current/recent season files.
    # ------------------------------------------------------------------

    if txt_files:

        scored = sorted(
            txt_files,
            key=lambda file: (
                season_score(file.path),
                -len(file.path),
            ),
            reverse=True,
        )

        best_score = season_score(
            scored[0].path
        )

        selected.extend(
            file
            for file in scored
            if season_score(file.path)
            >= best_score - 30
        )

    # ------------------------------------------------------------------
    # World Cup special case.
    #
    # The 2026 source uses a tournament directory such as:
    #
    #     2026--usa/
    #
    # ------------------------------------------------------------------

    if source_id == "openfootball_worldcup":

        worldcup_2026 = [
            file
            for file in candidates
            if re.search(
                r"2026",
                file.path,
                re.IGNORECASE,
            )
        ]

        if worldcup_2026:
            selected = worldcup_2026

    # Remove duplicates while preserving order.
    unique: dict[str, RemoteFile] = {}

    for file in selected:
        unique[file.path] = file

    return list(unique.values())


# ============================================================================
# RAW OUTPUT
# ============================================================================

def safe_filename(
    source_id: str,
    index: int,
    remote_path: str,
) -> str:

    stem = Path(
        remote_path
    ).stem

    safe_source = re.sub(
        r"[^A-Za-z0-9_-]",
        "_",
        source_id,
    )

    safe_stem = re.sub(
        r"[^A-Za-z0-9_.-]",
        "_",
        stem,
    )

    extension = Path(
        remote_path
    ).suffix.lower()

    return (
        f"{safe_source}__"
        f"{index:04d}__"
        f"{safe_stem}{extension}.raw"
    )


def save_raw_file(
    source_id: str,
    index: int,
    remote_file: RemoteFile,
    body: bytes,
) -> Path:

    source_dir = (
        RAW_DIR / source_id
    )

    source_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = source_dir / safe_filename(
        source_id,
        index,
        remote_file.path,
    )

    with output_path.open(
        "wb"
    ) as file:

        file.write(body)

    return output_path


# ============================================================================
# FETCH SOURCE
# ============================================================================

def fetch_source(
    source: dict[str, Any],
) -> FetchResult:

    source_id = source["id"]
    source_url = source["url"]

    print()
    print(
        f"[FETCH] {source_id}"
    )
    print(
        f"[FETCH] Repository: {source_url}"
    )

    try:

        repository = parse_github_repository(
            source_url
        )

        if repository is None:
            raise RuntimeError(
                "Configured source URL is not a "
                "supported GitHub repository URL."
            )

        owner, repo = repository

        print(
            f"[FETCH] Resolved repository: "
            f"{owner}/{repo}"
        )

        files = discover_github_files(
            owner,
            repo,
        )

        print(
            f"[FETCH] Discovered data files: "
            f"{len(files)}"
        )

        selected = select_relevant_files(
            source_id,
            files,
        )

        print(
            f"[FETCH] Selected files: "
            f"{len(selected)}"
        )

        if not selected:
            raise RuntimeError(
                "No relevant JSON or Football.TXT "
                "files were discovered."
            )

        total_bytes = 0
        downloaded = 0

        for index, remote_file in enumerate(
            selected,
            start=1,
        ):

            print(
                f"[FETCH] Downloading "
                f"{index}/{len(selected)}: "
                f"{remote_file.path}"
            )

            status, body = fetch_bytes(
                remote_file.download_url
            )

            if status != 200:
                raise RuntimeError(
                    f"HTTP {status} while downloading "
                    f"{remote_file.path}"
                )

            output_path = save_raw_file(
                source_id,
                index,
                remote_file,
                body,
            )

            downloaded += 1
            total_bytes += len(body)

            print(
                f"[FETCH] Saved: "
                f"{output_path.relative_to(PROJECT_ROOT)} "
                f"({len(body)} bytes)"
            )

        return FetchResult(
            source_id=source_id,
            source_url=source_url,
            status="FETCHED",
            fetched_at=utc_now(),
            files_discovered=len(files),
            files_selected=len(selected),
            files_downloaded=downloaded,
            bytes_received=total_bytes,
        )

    except (
        HTTPError,
        URLError,
        RuntimeError,
        ValueError,
    ) as error:

        print(
            f"[FETCH][FAILED] "
            f"{source_id}: {error}",
            file=sys.stderr,
        )

        return FetchResult(
            source_id=source_id,
            source_url=source_url,
            status="FETCH_FAILED",
            fetched_at=utc_now(),
            error=str(error),
        )

    except Exception as error:

        print(
            f"[FETCH][UNEXPECTED_ERROR] "
            f"{source_id}: {error}",
            file=sys.stderr,
        )

        return FetchResult(
            source_id=source_id,
            source_url=source_url,
            status="FETCH_FAILED",
            fetched_at=utc_now(),
            error=str(error),
        )


# ============================================================================
# MAIN
# ============================================================================

def run() -> int:

    print("=" * 72)
    print(
        "Football Match Data Pipeline - FETCH"
    )
    print("=" * 72)

    try:

        registry = load_source_registry()

    except Exception as error:

        print(
            f"[FETCH][FATAL] {error}",
            file=sys.stderr,
        )

        return 1

    sources = sorted(
        registry["sources"],
        key=lambda source: source.get(
            "priority",
            999999,
        ),
    )

    results: list[FetchResult] = []

    for source in sources:

        try:
            validate_source_definition(
                source
            )

        except Exception as error:

            print(
                f"[FETCH][CONFIG_ERROR] "
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

        result = fetch_source(
            source
        )

        results.append(result)

    # ========================================================================
    # SUMMARY
    # ========================================================================

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
        f"Successful sources : {len(successful)}"
    )

    print(
        f"Failed sources     : {len(failed)}"
    )

    print()

    for result in results:

        print(
            f"[{result.status}] "
            f"{result.source_id} "
            f"files={result.files_downloaded}/"
            f"{result.files_selected} "
            f"bytes={result.bytes_received}"
        )

        if result.error:
            print(
                f"    error={result.error}"
            )

    print("-" * 72)

    # Source failures are handled by the validation/publication stages.
    # We do not silently create a successful empty dataset.

    return 0


if __name__ == "__main__":
    raise SystemExit(run())
