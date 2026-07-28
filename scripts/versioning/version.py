#!/usr/bin/env python3
"""Validate and update the wire-relay package version.

``Cargo.toml``'s ``[package].version`` is the source of truth.  The matching
``wire-relay`` package entry in ``Cargo.lock`` must contain the same stable
semantic version.

The helper intentionally uses only the Python standard library so it can run
on a stock GitHub Actions runner without a dependency-install step.
"""

from __future__ import annotations

import argparse
import os
import re
import stat
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_RELATIVE = Path("Cargo.toml")
LOCKFILE_RELATIVE = Path("Cargo.lock")
PACKAGE_NAME = "wire-relay"
TAG_PREFIX = "v"

SEMVER_RE = re.compile(
    r"(?P<major>0|[1-9][0-9]*)\."
    r"(?P<minor>0|[1-9][0-9]*)\."
    r"(?P<patch>0|[1-9][0-9]*)"
)
SECTION_RE = re.compile(r"^[ \t]*\[([^\]\r\n]+)\][ \t]*(?:#.*)?$")
ARRAY_SECTION_RE = re.compile(r"^[ \t]*\[\[([^\]\r\n]+)\]\][ \t]*(?:#.*)?$")
PACKAGE_BLOCK_RE = re.compile(r"^[ \t]*\[\[package\]\][ \t]*(?:#.*)?$")
NAME_RE = re.compile(
    r'^(?P<prefix>[ \t]*name[ \t]*=[ \t]*")'
    r'(?P<value>[^"\r\n]*)'
    r'(?P<suffix>"[ \t]*(?:#.*)?)$'
)
VERSION_RE = re.compile(
    r'^(?P<prefix>[ \t]*version[ \t]*=[ \t]*")'
    r'(?P<value>[^"\r\n]*)'
    r'(?P<suffix>"[ \t]*(?:#.*)?)$'
)
COMMIT_RE = re.compile(r"[0-9a-fA-F]{40,64}")


class VersionError(Exception):
    """A user-facing version validation or update error."""


@dataclass(frozen=True, order=True)
class SemVer:
    """A stable semantic version with numeric major, minor, and patch fields."""

    major: int
    minor: int
    patch: int

    @classmethod
    def parse(cls, value: str, *, source: str) -> "SemVer":
        match = SEMVER_RE.fullmatch(value)
        if match is None:
            raise VersionError(
                f"{source}: expected stable semantic version X.Y.Z "
                f"(no leading zeros, prerelease, or build metadata), got {value!r}"
            )
        try:
            return cls(
                int(match.group("major")),
                int(match.group("minor")),
                int(match.group("patch")),
            )
        except ValueError as exc:
            raise VersionError(f"{source}: semantic version is too large") from exc

    def bump_patch(self) -> "SemVer":
        return SemVer(self.major, self.minor, self.patch + 1)

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"


@dataclass(frozen=True)
class Assignment:
    """The location and formatting of one TOML string assignment."""

    line_index: int
    value: str
    prefix: str
    suffix: str
    ending: str

    def replace(self, lines: list[str], new_value: str) -> None:
        lines[self.line_index] = (
            f"{self.prefix}{new_value}{self.suffix}{self.ending}"
        )


@dataclass(frozen=True)
class ParsedVersion:
    version: SemVer
    assignment: Assignment


@dataclass(frozen=True)
class Comparison:
    base_version: SemVer
    head_version: SemVer

    @property
    def changed(self) -> bool:
        return self.base_version != self.head_version


def _split_line(line: str) -> tuple[str, str]:
    if line.endswith("\r\n"):
        return line[:-2], "\r\n"
    if line.endswith("\n") or line.endswith("\r"):
        return line[:-1], line[-1:]
    return line, ""


def _assignment_from_match(
    line_index: int,
    ending: str,
    match: re.Match[str],
) -> Assignment:
    return Assignment(
        line_index=line_index,
        value=match.group("value"),
        prefix=match.group("prefix"),
        suffix=match.group("suffix"),
        ending=ending,
    )


def parse_manifest(contents: str, *, source: str = "Cargo.toml") -> ParsedVersion:
    """Read the exact package name and version from ``[package]``."""

    lines = contents.splitlines(keepends=True)
    package_sections = 0
    in_package = False
    names: list[Assignment] = []
    versions: list[Assignment] = []

    for index, line in enumerate(lines):
        body, ending = _split_line(line)
        if ARRAY_SECTION_RE.fullmatch(body) is not None:
            in_package = False
            continue
        section_match = SECTION_RE.fullmatch(body)
        if section_match is not None:
            section = section_match.group(1).strip()
            in_package = section == "package"
            if in_package:
                package_sections += 1
            continue

        if not in_package:
            continue

        name_match = NAME_RE.fullmatch(body)
        if name_match is not None:
            names.append(_assignment_from_match(index, ending, name_match))
            continue

        version_match = VERSION_RE.fullmatch(body)
        if version_match is not None:
            versions.append(_assignment_from_match(index, ending, version_match))

    if package_sections != 1:
        raise VersionError(
            f"{source}: expected exactly one [package] section, "
            f"found {package_sections}"
        )
    if len(names) != 1:
        raise VersionError(
            f"{source}: expected exactly one package name assignment, "
            f"found {len(names)}"
        )
    if names[0].value != PACKAGE_NAME:
        raise VersionError(
            f"{source}: expected package name {PACKAGE_NAME!r}, "
            f"got {names[0].value!r}"
        )
    if len(versions) != 1:
        raise VersionError(
            f"{source}: expected exactly one package version assignment, "
            f"found {len(versions)}"
        )

    version = SemVer.parse(versions[0].value, source=f"{source} [package].version")
    return ParsedVersion(version=version, assignment=versions[0])


def parse_lockfile(contents: str, *, source: str = "Cargo.lock") -> ParsedVersion:
    """Read the version from the one exact ``wire-relay`` lockfile block."""

    lines = contents.splitlines(keepends=True)
    block_starts = [
        index
        for index, line in enumerate(lines)
        if PACKAGE_BLOCK_RE.fullmatch(_split_line(line)[0]) is not None
    ]
    matches: list[ParsedVersion] = []

    for position, start in enumerate(block_starts):
        end = (
            block_starts[position + 1]
            if position + 1 < len(block_starts)
            else len(lines)
        )
        names: list[Assignment] = []
        versions: list[Assignment] = []

        for index in range(start + 1, end):
            body, ending = _split_line(lines[index])
            name_match = NAME_RE.fullmatch(body)
            if name_match is not None:
                names.append(_assignment_from_match(index, ending, name_match))
                continue
            version_match = VERSION_RE.fullmatch(body)
            if version_match is not None:
                versions.append(_assignment_from_match(index, ending, version_match))

        if not any(name.value == PACKAGE_NAME for name in names):
            continue
        if len(names) != 1:
            raise VersionError(
                f"{source}: {PACKAGE_NAME!r} package block has "
                f"{len(names)} name assignments"
            )
        if len(versions) != 1:
            raise VersionError(
                f"{source}: {PACKAGE_NAME!r} package block must have exactly "
                f"one version assignment, found {len(versions)}"
            )

        version = SemVer.parse(
            versions[0].value,
            source=f"{source} {PACKAGE_NAME!r} package version",
        )
        matches.append(ParsedVersion(version=version, assignment=versions[0]))

    if len(matches) != 1:
        raise VersionError(
            f"{source}: expected exactly one {PACKAGE_NAME!r} package block, "
            f"found {len(matches)}"
        )
    return matches[0]


def replace_parsed_version(
    contents: str,
    parsed: ParsedVersion,
    new_version: SemVer,
) -> str:
    """Replace only the already-parsed version assignment."""

    lines = contents.splitlines(keepends=True)
    if parsed.assignment.line_index >= len(lines):
        raise VersionError("version assignment changed before replacement")

    body, ending = _split_line(lines[parsed.assignment.line_index])
    match = VERSION_RE.fullmatch(body)
    if (
        match is None
        or match.group("value") != str(parsed.version)
        or ending != parsed.assignment.ending
    ):
        raise VersionError("version assignment changed before replacement")

    parsed.assignment.replace(lines, str(new_version))
    return "".join(lines)


def _read_utf8(path: Path) -> str:
    try:
        return path.read_bytes().decode("utf-8")
    except FileNotFoundError as exc:
        raise VersionError(f"required file not found: {path}") from exc
    except UnicodeDecodeError as exc:
        raise VersionError(f"{path}: file is not valid UTF-8") from exc


def _atomic_write_utf8(path: Path, contents: str) -> None:
    """Atomically replace ``path`` while retaining its permission bits."""

    try:
        mode = stat.S_IMODE(path.stat().st_mode)
        file_descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
        )
    except OSError as exc:
        raise VersionError(f"failed to prepare update for {path}: {exc}") from exc

    temporary = Path(temporary_name)
    try:
        with os.fdopen(file_descriptor, "wb") as handle:
            handle.write(contents.encode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    except OSError as exc:
        raise VersionError(f"failed to update {path}: {exc}") from exc
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _paths(repo_root: Path) -> tuple[Path, Path]:
    return repo_root / MANIFEST_RELATIVE, repo_root / LOCKFILE_RELATIVE


def validate(repo_root: Path = REPO_ROOT) -> SemVer:
    manifest_path, lockfile_path = _paths(repo_root)
    manifest = parse_manifest(_read_utf8(manifest_path), source=str(manifest_path))
    lockfile = parse_lockfile(_read_utf8(lockfile_path), source=str(lockfile_path))
    if manifest.version != lockfile.version:
        raise VersionError(
            f"version mismatch: {manifest_path} has {manifest.version}, "
            f"but {lockfile_path}'s {PACKAGE_NAME!r} entry has {lockfile.version}"
        )
    return manifest.version


def bump(repo_root: Path = REPO_ROOT) -> SemVer:
    """Patch-bump both version files after fully validating their old contents."""

    manifest_path, lockfile_path = _paths(repo_root)
    old_manifest_contents = _read_utf8(manifest_path)
    old_lockfile_contents = _read_utf8(lockfile_path)
    manifest = parse_manifest(old_manifest_contents, source=str(manifest_path))
    lockfile = parse_lockfile(old_lockfile_contents, source=str(lockfile_path))

    if manifest.version != lockfile.version:
        raise VersionError(
            f"version mismatch: {manifest_path} has {manifest.version}, "
            f"but {lockfile_path}'s {PACKAGE_NAME!r} entry has {lockfile.version}"
        )

    new_version = manifest.version.bump_patch()
    new_manifest_contents = replace_parsed_version(
        old_manifest_contents, manifest, new_version
    )
    new_lockfile_contents = replace_parsed_version(
        old_lockfile_contents, lockfile, new_version
    )

    # Parse the complete results before changing either file.
    if parse_manifest(new_manifest_contents).version != new_version:
        raise VersionError("internal error: generated manifest version is invalid")
    if parse_lockfile(new_lockfile_contents).version != new_version:
        raise VersionError("internal error: generated lockfile version is invalid")

    # Write the derived lockfile first and the source-of-truth manifest last.
    _atomic_write_utf8(lockfile_path, new_lockfile_contents)
    try:
        _atomic_write_utf8(manifest_path, new_manifest_contents)
    except VersionError as update_error:
        # Best-effort rollback keeps normal write failures from leaving the two
        # files knowingly out of sync.
        try:
            _atomic_write_utf8(lockfile_path, old_lockfile_contents)
        except VersionError as rollback_error:
            raise VersionError(
                f"{update_error}; additionally failed to restore "
                f"{lockfile_path}: {rollback_error}"
            ) from rollback_error
        raise

    return new_version


def _resolve_commit(repo_root: Path, ref: str) -> str:
    if not ref:
        raise VersionError("git ref must not be empty")
    if ref.startswith("-"):
        raise VersionError(f"git ref must not start with '-': {ref!r}")

    try:
        result = subprocess.run(
            ["git", "rev-parse", "--verify", f"{ref}^{{commit}}"],
            cwd=repo_root,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except FileNotFoundError as exc:
        raise VersionError("git executable not found") from exc
    except OSError as exc:
        raise VersionError(f"failed to run git: {exc}") from exc

    commit = result.stdout.strip()
    if result.returncode != 0 or COMMIT_RE.fullmatch(commit) is None:
        detail = result.stderr.strip()
        suffix = f": {detail}" if detail else ""
        raise VersionError(f"unable to resolve git ref {ref!r}{suffix}")
    return commit.lower()


def version_at_ref(repo_root: Path, ref: str) -> SemVer:
    commit = _resolve_commit(repo_root, ref)
    object_name = f"{commit}:{MANIFEST_RELATIVE.as_posix()}"
    try:
        result = subprocess.run(
            ["git", "show", object_name],
            cwd=repo_root,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except FileNotFoundError as exc:
        raise VersionError("git executable not found") from exc
    except OSError as exc:
        raise VersionError(f"failed to run git: {exc}") from exc

    if result.returncode != 0:
        detail = result.stderr.strip()
        suffix = f": {detail}" if detail else ""
        raise VersionError(
            f"unable to read {MANIFEST_RELATIVE.as_posix()} at {ref!r}{suffix}"
        )
    return parse_manifest(
        result.stdout,
        source=f"{MANIFEST_RELATIVE.as_posix()} at {ref!r}",
    ).version


def get_version(repo_root: Path = REPO_ROOT, *, ref: str | None = None) -> SemVer:
    if ref is not None:
        return version_at_ref(repo_root, ref)
    manifest_path, _ = _paths(repo_root)
    return parse_manifest(_read_utf8(manifest_path), source=str(manifest_path)).version


def compare_refs(repo_root: Path, base_ref: str, head_ref: str) -> Comparison:
    base_version = version_at_ref(repo_root, base_ref)
    head_version = version_at_ref(repo_root, head_ref)
    comparison = Comparison(base_version=base_version, head_version=head_version)
    if comparison.changed and head_version <= base_version:
        raise VersionError(
            f"manual version change must be a strict increase: "
            f"{base_ref} has {base_version}, {head_ref} has {head_version}"
        )
    return comparison


def _github_output(values: dict[str, str]) -> None:
    output_name = os.environ.get("GITHUB_OUTPUT")
    if not output_name:
        return
    try:
        with Path(output_name).open("a", encoding="utf-8", newline="\n") as handle:
            for key, value in values.items():
                handle.write(f"{key}={value}\n")
    except OSError as exc:
        raise VersionError(f"failed to write GITHUB_OUTPUT: {exc}") from exc


def _version_values(version: SemVer) -> dict[str, str]:
    return {"version": str(version), "tag": f"{TAG_PREFIX}{version}"}


def _cmd_validate(_: argparse.Namespace) -> int:
    version = validate(REPO_ROOT)
    print(f"version files are valid: {version}")
    return 0


def _cmd_get(args: argparse.Namespace) -> int:
    version = get_version(REPO_ROOT, ref=args.ref)
    values = _version_values(version)
    print(values[args.field])
    _github_output(values)
    return 0


def _cmd_bump(_: argparse.Namespace) -> int:
    version = bump(REPO_ROOT)
    values = _version_values(version)
    print(version)
    _github_output(values)
    return 0


def _cmd_compare_refs(args: argparse.Namespace) -> int:
    comparison = compare_refs(REPO_ROOT, args.base_ref, args.head_ref)
    values = {
        "changed": str(comparison.changed).lower(),
        "base_version": str(comparison.base_version),
        "head_version": str(comparison.head_version),
    }
    for key, value in values.items():
        print(f"{key}={value}")
    _github_output(values)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser(
        "validate", help="Validate Cargo.toml and Cargo.lock versions"
    )
    validate_parser.set_defaults(func=_cmd_validate)

    get_parser = subparsers.add_parser(
        "get", help="Read a version from the worktree or a git ref"
    )
    get_parser.add_argument(
        "--field",
        choices=("version", "tag"),
        default="version",
        help="Value to print (default: version)",
    )
    get_parser.add_argument(
        "--ref",
        help="Read Cargo.toml from this git ref instead of the worktree",
    )
    get_parser.set_defaults(func=_cmd_get)

    bump_parser = subparsers.add_parser(
        "bump", help="Patch-bump Cargo.toml and Cargo.lock"
    )
    bump_parser.set_defaults(func=_cmd_bump)

    compare_parser = subparsers.add_parser(
        "compare-refs",
        help="Compare package versions and reject a manual decrease",
    )
    compare_parser.add_argument(
        "--base-ref",
        "--base",
        dest="base_ref",
        required=True,
        help="Base git commit-ish",
    )
    compare_parser.add_argument(
        "--head-ref",
        "--head",
        dest="head_ref",
        required=True,
        help="Head git commit-ish",
    )
    compare_parser.set_defaults(func=_cmd_compare_refs)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except VersionError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
