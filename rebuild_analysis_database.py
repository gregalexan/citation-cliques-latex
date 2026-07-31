#!/usr/bin/env python3
"""Build the corrected analysis database from a clean SQLite file.

The journal classification and 9,431-pair cohort are fixed inputs to this
revision.  This script copies only the retained pair table and its h5 covariate
from the prior derived database, then reconstructs every citation-facing table
from the raw 2020--2024 snapshot.  The output is assembled at a temporary path
and moved into place only after all schema and row-count checks pass.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import time
from pathlib import Path
from typing import Sequence


EXPECTED_PAIRS = 9_431
H5_CALIPER = 3
SQL_ORDER = (
    "works_enhanced.sql",
    "works_doi_map.sql",
    "matched_authors.sql",
    "relevant_works.sql",
    "resolved_refs.sql",
    "citing_authors_counts.sql",
    "cited_authors_counts.sql",
    "micro_edges.sql",
    "coauthor_links.sql",
    "citation_network_final.sql",
    "author_behavior_metrics.sql",
    "author_venue_metrics.sql",
    "author_subject_temporal_metrics.sql",
)


def sqlite_uri(path: Path, *, immutable: bool = False) -> str:
    suffix = "?mode=ro"
    if immutable:
        suffix += "&immutable=1"
    return f"file:{path.resolve()}{suffix}"


def validate_retained_cohort(connection: sqlite3.Connection) -> None:
    row = connection.execute(
        """
        WITH joined AS (
          SELECT p.case_orcid, p.control_orcid, p.subject,
                 case_h5.h5_index AS case_h5,
                 control_h5.h5_index AS control_h5
          FROM rolap.author_matched_pairs p
          LEFT JOIN rolap.author_subject_h5_index case_h5
            ON case_h5.orcid = p.case_orcid
           AND case_h5.subject = p.subject
          LEFT JOIN rolap.author_subject_h5_index control_h5
            ON control_h5.orcid = p.control_orcid
           AND control_h5.subject = p.subject
        )
        SELECT
          COUNT(*) AS n,
          COUNT(DISTINCT case_orcid || char(31) || CAST(subject AS TEXT)) AS cases,
          COUNT(DISTINCT control_orcid || char(31) || CAST(subject AS TEXT)) AS controls,
          COALESCE(SUM(
            CASE WHEN case_orcid IS NULL OR TRIM(case_orcid) = ''
                       OR control_orcid IS NULL OR TRIM(control_orcid) = ''
                       OR subject IS NULL
                 THEN 1 ELSE 0 END
          ), 0) AS invalid_keys,
          COALESCE(SUM(
            CASE WHEN case_h5 IS NULL OR control_h5 IS NULL
                           OR case_h5 <= 0 OR control_h5 <= 0
                 THEN 1 ELSE 0 END
          ), 0) AS invalid_h5,
          COALESCE(SUM(
            CASE WHEN case_h5 IS NOT NULL AND control_h5 IS NOT NULL
                       AND ABS(case_h5 - control_h5) > ?
                 THEN 1 ELSE 0 END
          ), 0) AS caliper_violations,
          (
            SELECT COUNT(*)
            FROM (
              SELECT case_orcid AS orcid, subject FROM joined
              INTERSECT
              SELECT control_orcid AS orcid, subject FROM joined
            )
          ) AS cross_role_reuse
        FROM joined
        """,
        (H5_CALIPER,),
    ).fetchone()
    expected = (EXPECTED_PAIRS, EXPECTED_PAIRS, EXPECTED_PAIRS, 0, 0, 0, 0)
    if row != expected:
        raise RuntimeError(
            "retained cohort failed validation: "
            f"observed {row!r}, expected {expected!r}"
        )


def validate_output(connection: sqlite3.Connection) -> None:
    required = {
        "author_matched_pairs",
        "author_subject_h5_index",
        "matched_authors",
        "citation_network_final",
        "author_behavior_metrics",
        "author_venue_metrics",
        "author_subject_temporal_metrics",
    }
    present = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM rolap.sqlite_master WHERE type='table'"
        )
    }
    missing = sorted(required - present)
    if missing:
        raise RuntimeError(f"scratch rebuild is missing tables: {', '.join(missing)}")
    validate_retained_cohort(connection)
    duplicate_membership = connection.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT orcid, subject
          FROM rolap.matched_authors
          GROUP BY orcid, subject
          HAVING COUNT(*) <> 1
        )
        """
    ).fetchone()[0]
    invalid_edges = connection.execute(
        """
        SELECT COUNT(*)
        FROM rolap.citation_network_final
        WHERE subject IS NULL OR citing_orcid IS NULL OR cited_orcid IS NULL
        """
    ).fetchone()[0]
    duplicate_edges = connection.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT subject, citing_orcid, cited_orcid, citation_year
          FROM rolap.citation_network_final
          GROUP BY subject, citing_orcid, cited_orcid, citation_year
          HAVING COUNT(*) <> 1
        )
        """
    ).fetchone()[0]
    invalid_surges = connection.execute(
        """
        SELECT COUNT(*)
        FROM rolap.author_subject_temporal_metrics
        WHERE annual_dyadic_surge_share IS NOT NULL
          AND (annual_dyadic_surge_share < 0 OR annual_dyadic_surge_share > 1)
        """
    ).fetchone()[0]
    if duplicate_membership or invalid_edges or duplicate_edges or invalid_surges:
        raise RuntimeError(
            "scratch invariants failed: "
            f"duplicate_membership={duplicate_membership}, "
            f"invalid_edges={invalid_edges}, duplicate_edges={duplicate_edges}, "
            f"invalid_surges={invalid_surges}"
        )


def rebuild(
    *,
    raw_database: Path,
    cohort_database: Path,
    output_database: Path,
    sql_directory: Path,
    force: bool,
) -> None:
    raw_database = raw_database.resolve()
    cohort_database = cohort_database.resolve()
    output_database = output_database.resolve()
    sql_directory = sql_directory.resolve()
    for source in (raw_database, cohort_database):
        if not source.is_file():
            raise FileNotFoundError(source)
    if output_database in (raw_database, cohort_database):
        raise ValueError("scratch output must differ from both input databases")
    missing_sql = [name for name in SQL_ORDER if not (sql_directory / name).is_file()]
    if missing_sql:
        raise FileNotFoundError(f"missing SQL files: {', '.join(missing_sql)}")

    output_database.parent.mkdir(parents=True, exist_ok=True)
    building = output_database.with_suffix(output_database.suffix + ".building")
    if output_database.exists() and not force:
        raise FileExistsError(
            f"{output_database} exists; pass --force to replace a generated scratch DB"
        )
    if building.exists():
        if not force:
            raise FileExistsError(
                f"incomplete build exists at {building}; pass --force to replace it"
            )
        building.unlink()

    started = time.monotonic()
    connection = sqlite3.connect(sqlite_uri(raw_database), uri=True)
    try:
        connection.execute("PRAGMA cache_size=-524288")
        connection.execute("PRAGMA mmap_size=2147483648")
        connection.execute("PRAGMA automatic_index=ON")
        connection.execute("ATTACH DATABASE ? AS rolap", (str(building),))
        connection.execute(
            "ATTACH DATABASE ? AS cohort",
            (sqlite_uri(cohort_database, immutable=True),),
        )
        connection.execute("PRAGMA rolap.journal_mode=OFF")
        connection.execute("PRAGMA rolap.synchronous=OFF")
        connection.execute("PRAGMA temp_store=FILE")
        connection.executescript(
            """
            CREATE TABLE rolap.author_matched_pairs AS
              SELECT case_orcid, control_orcid, CAST(subject AS TEXT) AS subject
              FROM cohort.author_matched_pairs;
            CREATE UNIQUE INDEX rolap.idx_amp_case_subject
              ON author_matched_pairs(case_orcid, subject);
            CREATE UNIQUE INDEX rolap.idx_amp_control_subject
              ON author_matched_pairs(control_orcid, subject);

            CREATE TABLE rolap.author_subject_h5_index AS
              SELECT orcid, CAST(subject AS TEXT) AS subject, h5_index
              FROM cohort.author_subject_h5_index;
            CREATE UNIQUE INDEX rolap.idx_ashi_key
              ON author_subject_h5_index(orcid, subject);
            """
        )
        validate_retained_cohort(connection)
        for name in SQL_ORDER:
            step_started = time.monotonic()
            print(f"[scratch rebuild] {name}", flush=True)
            connection.executescript((sql_directory / name).read_text(encoding="utf-8"))
            print(
                f"[scratch rebuild] {name} complete in "
                f"{time.monotonic() - step_started:.1f}s",
                flush=True,
            )
        validate_output(connection)
        connection.execute("DETACH DATABASE cohort")
        connection.execute("DETACH DATABASE rolap")
    finally:
        connection.close()

    if output_database.exists():
        output_database.unlink()
    os.replace(building, output_database)
    print(
        f"Scratch analysis database ready: {output_database} "
        f"({time.monotonic() - started:.1f}s)",
        flush=True,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-database", type=Path, default=Path("impact.db"))
    parser.add_argument("--cohort-database", type=Path, default=Path("rolap.db"))
    parser.add_argument(
        "--output-database",
        type=Path,
        default=Path("build/revision-v1/rolap.db"),
    )
    parser.add_argument("--sql-directory", type=Path, default=Path(__file__).parent)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    rebuild(
        raw_database=args.raw_database,
        cohort_database=args.cohort_database,
        output_database=args.output_database,
        sql_directory=args.sql_directory,
        force=args.force,
    )


if __name__ == "__main__":
    main()
