"""Resolve a trusted main push to its exact merged pull request."""

from __future__ import annotations

import json
import re
import subprocess
import sys
import time
from typing import Any


class LookupError(Exception):
    """The association lookup did not establish a safe release identity."""


def fetch(repository: str, sha: str) -> list[dict[str, Any]]:
    """Read every page of GitHub's commit-associated pull requests."""
    try:
        result = subprocess.run(
            [
                "gh", "api", "--paginate", "--slurp",
                "-H", "Accept: application/vnd.github+json",
                f"repos/{repository}/commits/{sha}/pulls?per_page=100",
            ],
            check=False,
            stdout=subprocess.PIPE,
            text=True,
        )
    except OSError as exc:
        raise LookupError(f"could not launch GitHub CLI: {exc}") from exc
    if result.returncode != 0:
        raise LookupError("GitHub API lookup failed; rerun the workflow to retry.")
    try:
        pages = json.loads(result.stdout)
    except ValueError as exc:
        raise LookupError("GitHub API returned invalid JSON.") from exc
    if not isinstance(pages, list) or not pages or any(
        not isinstance(page, list) for page in pages
    ):
        raise LookupError("GitHub API did not return pages of pull requests.")
    rows = [row for page in pages for row in page]
    if any(not isinstance(row, dict) for row in rows):
        raise LookupError("GitHub API returned an invalid pull request entry.")
    return rows


def select(rows: list[dict[str, Any]], repository: str, sha: str) -> int | None:
    matches = set()
    for row in rows:
        if row.get("merge_commit_sha") != sha or not row.get("merged_at"):
            continue
        base = row.get("base") or {}
        base_repo = (base.get("repo") or {}).get("full_name")
        if (
            base.get("ref") != "main"
            or not isinstance(base_repo, str)
            or base_repo.casefold() != repository.casefold()
        ):
            continue
        number = row.get("number")
        if type(number) is not int or number < 1:
            raise LookupError("Associated pull request has an invalid number.")
        matches.add(number)
    if len(matches) > 1:
        raise LookupError("More than one merged pull request matches the pushed commit.")
    return next(iter(matches), None)


def resolve(repository: str, sha: str) -> int | None:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise LookupError("Expected an owner/repository name.")
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise LookupError("Expected an exact GitHub commit SHA.")

    # A push can arrive before the commit-to-PR association becomes visible.
    for delay in (0, 1, 2, 4, 8):
        if delay:
            time.sleep(delay)
        number = select(fetch(repository, sha), repository, sha)
        if number is not None:
            return number
    return None


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("Usage: merged_pr.py OWNER/REPO PUSHED_SHA")
    try:
        matched = resolve(sys.argv[1], sys.argv[2])
    except LookupError as exc:
        sys.exit(str(exc))
    if matched is not None:
        print(matched)
