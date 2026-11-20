"""Tests for resolving a pushed main tip to the exact merged pull request."""

import unittest
from unittest.mock import Mock, call, patch

import merged_pr


REPOSITORY = "wiresock/wire-relay"
SHA = "a" * 40


def pull(number=11, *, sha=SHA, merged=True, branch="main", repository=REPOSITORY):
    return {
        "number": number,
        "merge_commit_sha": sha,
        "merged_at": "2026-11-11T18:35:14Z" if merged else None,
        "base": {"ref": branch, "repo": {"full_name": repository}},
    }


class MergedPullRequestTest(unittest.TestCase):
    def test_requires_exact_commit_and_merged_state_on_repository_main(self):
        rows = [
            pull(1, sha="b" * 40),
            pull(2, merged=False),
            pull(3, branch="other"),
            pull(4, repository="fork/wire-relay"),
            pull(11, repository="WireSock/Wire-Relay"),
        ]
        self.assertEqual(merged_pr.select(rows, REPOSITORY, SHA), 11)

    def test_unassociated_push_does_not_select_a_pr(self):
        self.assertIsNone(merged_pr.select([], REPOSITORY, SHA))

    def test_ambiguous_or_malformed_exact_match_fails_closed(self):
        with self.assertRaises(merged_pr.LookupError):
            merged_pr.select([pull(11), pull(12)], REPOSITORY, SHA)
        for malformed in ("11\nGITHUB_OUTPUT=fake", 0, True):
            with self.subTest(malformed=malformed):
                with self.assertRaises(merged_pr.LookupError):
                    merged_pr.select([pull(malformed)], REPOSITORY, SHA)

    def test_retries_empty_results_to_allow_indexing(self):
        get = Mock(side_effect=[[], [pull(1, merged=False)], [pull()]])
        sleep = Mock()
        with patch.object(merged_pr, "fetch", get), patch.object(merged_pr.time, "sleep", sleep):
            self.assertEqual(merged_pr.resolve(REPOSITORY, SHA), 11)
        self.assertEqual(get.call_args_list, [call(REPOSITORY, SHA)] * 3)
        self.assertEqual(sleep.call_args_list, [call(1), call(2)])

    def test_exhausted_retries_skip_direct_push_and_api_error_is_not_a_skip(self):
        with patch.object(merged_pr, "fetch", return_value=[]) as get:
            with patch.object(merged_pr.time, "sleep") as sleep:
                self.assertIsNone(merged_pr.resolve(REPOSITORY, SHA))
        self.assertEqual(get.call_count, 5)
        self.assertEqual(sleep.call_args_list, [call(1), call(2), call(4), call(8)])
        with patch.object(merged_pr, "fetch", side_effect=merged_pr.LookupError("API failed")):
            with self.assertRaisesRegex(merged_pr.LookupError, "API failed"):
                merged_pr.resolve(REPOSITORY, SHA)

    def test_paginated_api_response_and_invalid_input(self):
        runner = Mock(return_value=Mock(returncode=0, stdout='[[{"number": 1}], [{"number": 2}]]'))
        with patch.object(merged_pr.subprocess, "run", runner):
            self.assertEqual(merged_pr.fetch(REPOSITORY, SHA), [{"number": 1}, {"number": 2}])
        self.assertIn(f"repos/{REPOSITORY}/commits/{SHA}/pulls?per_page=100", runner.call_args.args[0])
        self.assertIn("--paginate", runner.call_args.args[0])
        self.assertIn("--slurp", runner.call_args.args[0])
        with self.assertRaises(merged_pr.LookupError):
            merged_pr.resolve(REPOSITORY, "refs/heads/untrusted")
