from __future__ import annotations

from contextlib import redirect_stdout
import io
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import match_authors


ROOT = Path(__file__).resolve().parents[1]


class CandidateEligibilityTests(unittest.TestCase):
    def test_inclusive_caliper_and_idempotent_candidate_build(self) -> None:
        with sqlite3.connect(":memory:") as connection:
            connection.execute("ATTACH DATABASE ':memory:' AS rolap")
            connection.executescript(
                """
                CREATE TABLE rolap.author_profiles (
                  orcid TEXT, subject TEXT, author_tier TEXT
                );
                CREATE TABLE rolap.author_subject_h5_index (
                  orcid TEXT, subject TEXT, h5_index INTEGER
                );
                INSERT INTO rolap.author_profiles VALUES
                  ('case', 'S', 'Bottom Tier'),
                  ('control-0', 'S', 'Top Tier'),
                  ('control-1', 'S', 'Top Tier'),
                  ('control-2', 'S', 'Top Tier'),
                  ('control-3', 'S', 'Top Tier'),
                  ('control-4', 'S', 'Top Tier'),
                  ('control-5', 'S', 'Top Tier');
                INSERT INTO rolap.author_subject_h5_index VALUES
                  ('case', 'S', 10),
                  ('control-0', 'S', 10),
                  ('control-1', 'S', 11),
                  ('control-2', 'S', 12),
                  ('control-3', 'S', 13),
                  ('control-4', 'S', 14),
                  ('control-5', 'S', 15);
                """
            )
            sql = (ROOT / "author_matched_candidates.sql").read_text(encoding="utf-8")
            connection.executescript(sql)
            connection.executescript(sql)
            rows = connection.execute(
                "SELECT control_orcid, score FROM rolap.author_matched_candidates "
                "ORDER BY score"
            ).fetchall()

        self.assertEqual(
            rows,
            [("control-0", 0), ("control-1", 1), ("control-2", 2), ("control-3", 3)],
        )


class GreedyMatchingTests(unittest.TestCase):
    @staticmethod
    def _match(database: Path, candidates: list[tuple[str, str, str, int]]):
        with sqlite3.connect(database) as connection:
            connection.execute(
                "CREATE TABLE author_matched_candidates "
                "(case_orcid TEXT, control_orcid TEXT, subject TEXT, score INTEGER)"
            )
            connection.executemany(
                "INSERT INTO author_matched_candidates VALUES (?, ?, ?, ?)", candidates
            )
        match_authors.greedy_match(database, force=True)
        with sqlite3.connect(database) as connection:
            return connection.execute(
                "SELECT case_orcid, control_orcid, subject FROM author_matched_pairs "
                "ORDER BY rowid"
            ).fetchall()

    def test_total_order_is_input_invariant_and_without_replacement(self) -> None:
        candidates = [
            ("case-b", "control-a", "S", 0),
            ("case-a", "control-b", "S", 0),
            ("case-c", "control-c", "S", 1),
            ("case-a", "control-a", "S", 0),
            ("case-c", "control-d", "S", 0),
            ("case-b", "control-b", "S", 0),
            ("case-a", "control-a", "T", 0),
            ("case-d", "control-e", "S", 4),
        ]
        expected = [
            ("case-a", "control-a", "S"),
            ("case-b", "control-b", "S"),
            ("case-c", "control-d", "S"),
            ("case-a", "control-a", "T"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "first.db"
            second = Path(directory) / "second.db"
            first_pairs = self._match(first, candidates)
            second_pairs = self._match(second, list(reversed(candidates)))

            self.assertEqual(first_pairs, expected)
            self.assertEqual(second_pairs, expected)
            memberships = [
                (orcid, subject)
                for case, control, subject in first_pairs
                for orcid in (case, control)
            ]
            self.assertEqual(len(memberships), len(set(memberships)))

            with sqlite3.connect(first) as connection:
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO author_matched_pairs VALUES (?, ?, ?)",
                        ("case-a", "new-control", "S"),
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "INSERT INTO author_matched_pairs VALUES (?, ?, ?)",
                        ("new-case", "control-a", "S"),
                    )


class RetainedCohortTests(unittest.TestCase):
    @staticmethod
    def _connection(pairs, h5_rows):
        connection = sqlite3.connect(":memory:")
        connection.execute(
            "CREATE TABLE author_matched_pairs "
            "(case_orcid TEXT, control_orcid TEXT, subject TEXT)"
        )
        connection.execute(
            "CREATE TABLE author_subject_h5_index "
            "(orcid TEXT, subject TEXT, h5_index INTEGER)"
        )
        connection.executemany(
            "INSERT INTO author_matched_pairs VALUES (?, ?, ?)", pairs
        )
        connection.executemany(
            "INSERT INTO author_subject_h5_index VALUES (?, ?, ?)", h5_rows
        )
        return connection

    def test_retained_cohort_rejects_caliper_violation(self) -> None:
        connection = self._connection(
            [("case", "control", "S")],
            [("case", "S", 10), ("control", "S", 13)],
        )
        self.addCleanup(connection.close)
        with patch.object(match_authors, "EXPECTED_PRIMARY_PAIRS", 1):
            self.assertTrue(match_authors.retained_cohort_is_valid(connection))
            connection.execute(
                "UPDATE author_subject_h5_index SET h5_index = 14 "
                "WHERE orcid = 'control' AND subject = 'S'"
            )
            self.assertFalse(match_authors.retained_cohort_is_valid(connection))

    def test_retained_cohort_rejects_cross_role_reuse(self) -> None:
        connection = self._connection(
            [("author-a", "author-b", "S"), ("author-c", "author-a", "S")],
            [
                ("author-a", "S", 10),
                ("author-b", "S", 10),
                ("author-c", "S", 10),
            ],
        )
        self.addCleanup(connection.close)
        with patch.object(match_authors, "EXPECTED_PRIMARY_PAIRS", 2):
            self.assertFalse(match_authors.retained_cohort_is_valid(connection))


class MatchingAuditTests(unittest.TestCase):
    def test_audit_is_read_only_and_returns_nonzero_on_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "cohort.db"
            with sqlite3.connect(database) as connection:
                connection.executescript(
                    """
                    CREATE TABLE author_profiles (
                      orcid TEXT, subject TEXT, author_tier TEXT
                    );
                    INSERT INTO author_profiles VALUES
                      ('case-a', 'S', 'Bottom Tier'),
                      ('case-b', 'S', 'Bottom Tier'),
                      ('control-a', 'S', 'Top Tier'),
                      ('control-b', 'S', 'Top Tier');
                    CREATE TABLE author_subject_h5_index (
                      orcid TEXT, subject TEXT, h5_index INTEGER
                    );
                    INSERT INTO author_subject_h5_index VALUES
                      ('case-a', 'S', 10), ('case-b', 'S', 10),
                      ('control-a', 'S', 10), ('control-b', 'S', 10);
                    CREATE TABLE author_matched_pairs (
                      case_orcid TEXT, control_orcid TEXT, subject TEXT
                    );
                    INSERT INTO author_matched_pairs VALUES
                      ('case-a', 'control-a', 'S'),
                      ('case-b', 'control-b', 'S');
                    """
                )

            before = (database.stat().st_size, database.stat().st_mtime_ns)
            output = io.StringIO()
            report = Path(directory) / "matching_audit.txt"
            with redirect_stdout(output):
                self.assertEqual(
                    match_authors.main(
                        ["--audit", "--audit-output", str(report), str(database)]
                    ),
                    0,
                )
            self.assertIn("ordered_equal=True", output.getvalue())
            self.assertIn("source_stat_unchanged=True", output.getvalue())
            self.assertEqual(report.read_text(encoding="utf-8"), output.getvalue())
            self.assertEqual(before, (database.stat().st_size, database.stat().st_mtime_ns))

            with patch.object(match_authors, "PRESERVED_DATABASE", database.resolve()):
                self.assertEqual(match_authors.main(["--force", str(database)]), 2)

            with sqlite3.connect(database) as connection:
                connection.execute(
                    "UPDATE author_matched_pairs SET control_orcid = 'control-b' "
                    "WHERE case_orcid = 'case-a'"
                )
            with redirect_stdout(io.StringIO()):
                self.assertEqual(match_authors.main(["--audit", str(database)]), 1)


if __name__ == "__main__":
    unittest.main()
