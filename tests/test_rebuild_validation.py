import sqlite3
import unittest
from unittest.mock import patch

import rebuild_analysis_database as rebuild


class RetainedCohortValidationTests(unittest.TestCase):
    def test_h5_caliper_is_inclusive_and_enforced(self) -> None:
        for difference, accepted in ((3, True), (4, False)):
            with self.subTest(difference=difference):
                connection = sqlite3.connect(":memory:")
                self.addCleanup(connection.close)
                connection.execute("ATTACH DATABASE ':memory:' AS rolap")
                connection.executescript(
                    """
                    CREATE TABLE rolap.author_matched_pairs (
                      case_orcid TEXT, control_orcid TEXT, subject TEXT
                    );
                    INSERT INTO rolap.author_matched_pairs VALUES ('A', 'B', 'S');
                    CREATE TABLE rolap.author_subject_h5_index (
                      orcid TEXT, subject TEXT, h5_index INTEGER
                    );
                    INSERT INTO rolap.author_subject_h5_index VALUES ('A', 'S', 10);
                    """
                )
                connection.execute(
                    "INSERT INTO rolap.author_subject_h5_index VALUES ('B', 'S', ?)",
                    (10 + difference,),
                )
                with patch.object(rebuild, "EXPECTED_PAIRS", 1):
                    if accepted:
                        rebuild.validate_retained_cohort(connection)
                    else:
                        with self.assertRaisesRegex(RuntimeError, "failed validation"):
                            rebuild.validate_retained_cohort(connection)


if __name__ == "__main__":
    unittest.main()
