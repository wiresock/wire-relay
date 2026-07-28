#!/usr/bin/env python3
"""Tests for the dependency-free wire-relay version helper."""

from __future__ import annotations

import contextlib
import io
import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import version


def manifest_text(package_version: str, *, newline: str = "\n") -> str:
    return newline.join(
        [
            "[package]",
            'name = "wire-relay"',
            f'version = "{package_version}"',
            'edition = "2024"',
            "",
            "[dependencies]",
            'example = "9.9.9"',
            "",
        ]
    )


def lockfile_text(package_version: str, *, newline: str = "\n") -> str:
    return newline.join(
        [
            "version = 4",
            "",
            "[[package]]",
            'name = "wire-relay-helper"',
            'version = "7.7.7"',
            "",
            "[[package]]",
            'name = "wire-relay"',
            f'version = "{package_version}"',
            "dependencies = [",
            ' "example",',
            "]",
            "",
            "[[package]]",
            'name = "example"',
            'version = "9.9.9"',
            "",
        ]
    )


class TemporaryRepositoryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(
            prefix="wire-relay-version-tests-"
        )
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def write_versions(
        self,
        package_version: str,
        *,
        lock_version: str | None = None,
        newline: str = "\n",
    ) -> None:
        (self.root / "Cargo.toml").write_bytes(
            manifest_text(package_version, newline=newline).encode("utf-8")
        )
        (self.root / "Cargo.lock").write_bytes(
            lockfile_text(
                lock_version if lock_version is not None else package_version,
                newline=newline,
            ).encode("utf-8")
        )

    def git(self, *args: str) -> str:
        result = subprocess.run(
            ["git", *args],
            cwd=self.root,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        return result.stdout.strip()

    def initialize_git(self) -> None:
        self.git("init", "--quiet")
        self.git("config", "user.name", "Version Tests")
        self.git("config", "user.email", "version-tests@example.invalid")

    def commit(self, message: str) -> str:
        self.git("add", "Cargo.toml", "Cargo.lock")
        self.git("commit", "--quiet", "-m", message)
        return self.git("rev-parse", "HEAD")


class SemVerTests(unittest.TestCase):
    def test_accepts_stable_semver(self) -> None:
        for value in ("0.0.0", "0.1.23", "1.0.0", "123.456.789"):
            with self.subTest(value=value):
                self.assertEqual(str(version.SemVer.parse(value, source="test")), value)

    def test_rejects_non_stable_or_non_canonical_versions(self) -> None:
        invalid = (
            "",
            "1",
            "1.2",
            "1.2.3.4",
            "01.2.3",
            "1.02.3",
            "1.2.03",
            "1.2.3-alpha",
            "1.2.3+build",
            "v1.2.3",
            "-1.2.3",
            " 1.2.3",
            "1.2.3 ",
        )
        for value in invalid:
            with self.subTest(value=value):
                with self.assertRaises(version.VersionError):
                    version.SemVer.parse(value, source="test")

    def test_ordering_is_numeric(self) -> None:
        earlier = version.SemVer.parse("1.9.99", source="test")
        later = version.SemVer.parse("1.10.0", source="test")
        self.assertLess(earlier, later)


class ParsingTests(unittest.TestCase):
    def test_manifest_uses_only_package_version(self) -> None:
        parsed = version.parse_manifest(manifest_text("1.2.3"))
        self.assertEqual(str(parsed.version), "1.2.3")

    def test_manifest_requires_exact_package_name(self) -> None:
        contents = manifest_text("1.2.3").replace(
            'name = "wire-relay"', 'name = "other"'
        )
        with self.assertRaisesRegex(version.VersionError, "expected package name"):
            version.parse_manifest(contents)

    def test_manifest_requires_one_package_section(self) -> None:
        with self.assertRaisesRegex(version.VersionError, r"one \[package\]"):
            version.parse_manifest('[workspace]\nmembers = []\n')
        with self.assertRaisesRegex(version.VersionError, r"one \[package\]"):
            version.parse_manifest(manifest_text("1.2.3") + manifest_text("1.2.3"))

    def test_manifest_requires_one_version_assignment(self) -> None:
        missing = manifest_text("1.2.3").replace('version = "1.2.3"\n', "")
        duplicate = manifest_text("1.2.3").replace(
            'version = "1.2.3"', 'version = "1.2.3"\nversion = "1.2.4"'
        )
        with self.assertRaisesRegex(version.VersionError, "one package version"):
            version.parse_manifest(missing)
        with self.assertRaisesRegex(version.VersionError, "one package version"):
            version.parse_manifest(duplicate)

    def test_array_table_ends_the_package_section(self) -> None:
        contents = "\n".join(
            [
                "[package]",
                'name = "wire-relay"',
                'version = "1.2.3"',
                "",
                "[[bin]]",
                'name = "wire-relay"',
                'path = "src/main.rs"',
                "",
            ]
        )
        parsed = version.parse_manifest(contents)
        self.assertEqual(str(parsed.version), "1.2.3")

    def test_lockfile_matches_exact_package_name(self) -> None:
        parsed = version.parse_lockfile(lockfile_text("2.3.4"))
        self.assertEqual(str(parsed.version), "2.3.4")

    def test_lockfile_requires_exactly_one_wire_relay_block(self) -> None:
        missing = lockfile_text("1.2.3").replace(
            'name = "wire-relay"\nversion = "1.2.3"',
            'name = "not-wire-relay"\nversion = "1.2.3"',
        )
        duplicate = lockfile_text("1.2.3") + "\n".join(
            ["[[package]]", 'name = "wire-relay"', 'version = "1.2.3"', ""]
        )
        with self.assertRaisesRegex(version.VersionError, "found 0"):
            version.parse_lockfile(missing)
        with self.assertRaisesRegex(version.VersionError, "found 2"):
            version.parse_lockfile(duplicate)

    def test_lockfile_target_requires_one_version(self) -> None:
        missing = lockfile_text("1.2.3").replace(
            'name = "wire-relay"\nversion = "1.2.3"',
            'name = "wire-relay"',
        )
        with self.assertRaisesRegex(version.VersionError, "one version assignment"):
            version.parse_lockfile(missing)

    def test_parsers_reject_prereleases_and_leading_zeros(self) -> None:
        with self.assertRaises(version.VersionError):
            version.parse_manifest(manifest_text("1.2.3-rc.1"))
        with self.assertRaises(version.VersionError):
            version.parse_lockfile(lockfile_text("1.02.3"))


class WorktreeCommandTests(TemporaryRepositoryTest):
    def test_validate_accepts_synchronized_files(self) -> None:
        self.write_versions("1.2.3")
        self.assertEqual(str(version.validate(self.root)), "1.2.3")

    def test_validate_rejects_mismatched_lockfile(self) -> None:
        self.write_versions("1.2.3", lock_version="1.2.2")
        with self.assertRaisesRegex(version.VersionError, "version mismatch"):
            version.validate(self.root)

    def test_get_reads_manifest_as_source_of_truth(self) -> None:
        self.write_versions("3.4.5", lock_version="0.0.1")
        self.assertEqual(str(version.get_version(self.root)), "3.4.5")

    def test_bump_updates_only_exact_version_assignments(self) -> None:
        self.write_versions("1.2.9")
        manifest_before = (self.root / "Cargo.toml").read_text(encoding="utf-8")
        lock_before = (self.root / "Cargo.lock").read_text(encoding="utf-8")

        bumped = version.bump(self.root)

        self.assertEqual(str(bumped), "1.2.10")
        manifest_after = (self.root / "Cargo.toml").read_text(encoding="utf-8")
        lock_after = (self.root / "Cargo.lock").read_text(encoding="utf-8")
        self.assertEqual(
            manifest_after,
            manifest_before.replace('version = "1.2.9"', 'version = "1.2.10"', 1),
        )
        target_old = 'name = "wire-relay"\nversion = "1.2.9"'
        target_new = 'name = "wire-relay"\nversion = "1.2.10"'
        self.assertEqual(lock_after, lock_before.replace(target_old, target_new, 1))
        self.assertIn(
            'name = "wire-relay-helper"\nversion = "7.7.7"', lock_after
        )
        self.assertIn('name = "example"\nversion = "9.9.9"', lock_after)

    def test_bump_preserves_formatting_comments_and_crlf(self) -> None:
        manifest = manifest_text("1.2.3", newline="\r\n").replace(
            'version = "1.2.3"', '  version = "1.2.3" # source'
        )
        lockfile = lockfile_text("1.2.3", newline="\r\n").replace(
            'name = "wire-relay"\r\nversion = "1.2.3"',
            'name = "wire-relay"\r\n  version = "1.2.3" # derived',
        )
        (self.root / "Cargo.toml").write_bytes(manifest.encode("utf-8"))
        (self.root / "Cargo.lock").write_bytes(lockfile.encode("utf-8"))

        version.bump(self.root)

        manifest_after = (self.root / "Cargo.toml").read_bytes()
        lock_after = (self.root / "Cargo.lock").read_bytes()
        self.assertIn(b'  version = "1.2.4" # source\r\n', manifest_after)
        self.assertIn(b'  version = "1.2.4" # derived\r\n', lock_after)
        self.assertNotIn(b"\n", manifest_after.replace(b"\r\n", b""))
        self.assertNotIn(b"\n", lock_after.replace(b"\r\n", b""))

    @unittest.skipIf(os.name == "nt", "POSIX permission bits are not portable")
    def test_bump_preserves_file_modes(self) -> None:
        self.write_versions("1.2.3")
        manifest_path = self.root / "Cargo.toml"
        lockfile_path = self.root / "Cargo.lock"
        manifest_path.chmod(0o640)
        lockfile_path.chmod(0o644)

        version.bump(self.root)

        self.assertEqual(stat.S_IMODE(manifest_path.stat().st_mode), 0o640)
        self.assertEqual(stat.S_IMODE(lockfile_path.stat().st_mode), 0o644)

    def test_bump_does_not_modify_either_file_when_validation_fails(self) -> None:
        self.write_versions("1.2.3", lock_version="1.2.2")
        manifest_path = self.root / "Cargo.toml"
        lockfile_path = self.root / "Cargo.lock"
        before = (manifest_path.read_bytes(), lockfile_path.read_bytes())

        with self.assertRaises(version.VersionError):
            version.bump(self.root)

        self.assertEqual(
            (manifest_path.read_bytes(), lockfile_path.read_bytes()),
            before,
        )

    def test_bump_restores_lockfile_when_manifest_write_fails(self) -> None:
        self.write_versions("1.2.3")
        manifest_path = self.root / "Cargo.toml"
        lockfile_path = self.root / "Cargo.lock"
        before = (manifest_path.read_bytes(), lockfile_path.read_bytes())
        atomic_write = version._atomic_write_utf8

        def fail_manifest_write(path: Path, contents: str) -> None:
            if path == manifest_path:
                raise version.VersionError("injected manifest write failure")
            atomic_write(path, contents)

        with (
            mock.patch.object(
                version,
                "_atomic_write_utf8",
                side_effect=fail_manifest_write,
            ),
            self.assertRaisesRegex(
                version.VersionError,
                "injected manifest write failure",
            ),
        ):
            version.bump(self.root)

        self.assertEqual(
            (manifest_path.read_bytes(), lockfile_path.read_bytes()),
            before,
        )

    def test_bump_reports_update_and_rollback_failures(self) -> None:
        self.write_versions("1.2.3")
        failures = [
            None,
            version.VersionError("injected manifest write failure"),
            version.VersionError("injected rollback failure"),
        ]

        def fail_writes(_: Path, __: str) -> None:
            failure = failures.pop(0)
            if failure is not None:
                raise failure

        with (
            mock.patch.object(
                version,
                "_atomic_write_utf8",
                side_effect=fail_writes,
            ),
            self.assertRaisesRegex(
                version.VersionError,
                "manifest write failure.*rollback failure",
            ),
        ):
            version.bump(self.root)


class GitRefTests(TemporaryRepositoryTest):
    def setUp(self) -> None:
        super().setUp()
        self.initialize_git()
        self.write_versions("1.2.3")
        self.base = self.commit("base")

    def test_get_version_at_ref_ignores_worktree(self) -> None:
        self.write_versions("9.9.9")
        self.assertEqual(str(version.get_version(self.root, ref=self.base)), "1.2.3")

    def test_compare_refs_reports_unchanged_version(self) -> None:
        comparison = version.compare_refs(self.root, self.base, self.base)
        self.assertFalse(comparison.changed)
        self.assertEqual(str(comparison.base_version), "1.2.3")
        self.assertEqual(str(comparison.head_version), "1.2.3")

    def test_compare_refs_accepts_strict_increase(self) -> None:
        self.write_versions("1.3.0")
        head = self.commit("increase")
        comparison = version.compare_refs(self.root, self.base, head)
        self.assertTrue(comparison.changed)
        self.assertEqual(str(comparison.head_version), "1.3.0")

    def test_compare_refs_rejects_decrease(self) -> None:
        self.write_versions("1.2.2")
        head = self.commit("decrease")
        with self.assertRaisesRegex(version.VersionError, "strict increase"):
            version.compare_refs(self.root, self.base, head)

    def test_compare_refs_rejects_non_stable_ref_version(self) -> None:
        self.write_versions("1.2.4-alpha.1")
        head = self.commit("prerelease")
        with self.assertRaisesRegex(version.VersionError, "stable semantic version"):
            version.compare_refs(self.root, self.base, head)

    def test_ref_must_exist_and_cannot_be_an_option(self) -> None:
        with self.assertRaisesRegex(version.VersionError, "unable to resolve"):
            version.get_version(self.root, ref="does-not-exist")
        with self.assertRaisesRegex(version.VersionError, "must not start"):
            version.get_version(self.root, ref="--help")


class CliTests(TemporaryRepositoryTest):
    def setUp(self) -> None:
        super().setUp()
        self.write_versions("2.4.6")

    def run_main(
        self,
        arguments: list[str],
        *,
        github_output: Path | None = None,
    ) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        environment = (
            mock.patch.dict(
                os.environ,
                {"GITHUB_OUTPUT": str(github_output)},
                clear=False,
            )
            if github_output is not None
            else mock.patch.dict(os.environ, {}, clear=False)
        )
        with (
            mock.patch.object(version, "REPO_ROOT", self.root),
            environment,
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            if github_output is None:
                os.environ.pop("GITHUB_OUTPUT", None)
            result = version.main(arguments)
        return result, stdout.getvalue(), stderr.getvalue()

    def test_validate_cli(self) -> None:
        result, stdout, stderr = self.run_main(["validate"])
        self.assertEqual(result, 0)
        self.assertEqual(stdout, "version files are valid: 2.4.6\n")
        self.assertEqual(stderr, "")

    def test_get_tag_and_github_outputs(self) -> None:
        output = self.root / "github-output"
        result, stdout, stderr = self.run_main(
            ["get", "--field", "tag"],
            github_output=output,
        )
        self.assertEqual(result, 0)
        self.assertEqual(stdout, "v2.4.6\n")
        self.assertEqual(stderr, "")
        self.assertEqual(
            output.read_text(encoding="utf-8"),
            "version=2.4.6\ntag=v2.4.6\n",
        )

    def test_bump_cli_and_github_outputs(self) -> None:
        output = self.root / "github-output"
        result, stdout, stderr = self.run_main(["bump"], github_output=output)
        self.assertEqual(result, 0)
        self.assertEqual(stdout, "2.4.7\n")
        self.assertEqual(stderr, "")
        self.assertEqual(
            output.read_text(encoding="utf-8"),
            "version=2.4.7\ntag=v2.4.7\n",
        )
        self.assertEqual(str(version.validate(self.root)), "2.4.7")

    def test_compare_refs_cli_outputs(self) -> None:
        self.initialize_git()
        base = self.commit("base")
        self.write_versions("2.5.0")
        head = self.commit("head")
        output = self.root / "github-output"

        result, stdout, stderr = self.run_main(
            [
                "compare-refs",
                "--base-ref",
                base,
                "--head-ref",
                head,
            ],
            github_output=output,
        )

        self.assertEqual(result, 0)
        self.assertEqual(
            stdout,
            "changed=true\nbase_version=2.4.6\nhead_version=2.5.0\n",
        )
        self.assertEqual(stderr, "")
        self.assertEqual(output.read_text(encoding="utf-8"), stdout)

    def test_cli_returns_error_without_traceback(self) -> None:
        (self.root / "Cargo.lock").unlink()
        result, stdout, stderr = self.run_main(["validate"])
        self.assertEqual(result, 1)
        self.assertEqual(stdout, "")
        self.assertIn("error: required file not found:", stderr)
        self.assertNotIn("Traceback", stderr)


if __name__ == "__main__":
    unittest.main()
