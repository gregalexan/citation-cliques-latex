#!/usr/bin/env python3
"""Build the full matched analysis cohort from the fixed raw SQLite snapshot.

The output is assembled at a temporary path and moved into place only after
all schema and row-count checks pass. Preserved source databases are read only.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import time
from pathlib import Path
from typing import Sequence

from match_authors import materialize_profile_matches

H5_CALIPER = 3
COHORT_SQL_ORDER = (
    "works_enhanced.sql",
    "work_citations.sql",
    "author_works_master.sql",
    "eigenfactor_percentiles.sql",
    "author_profiles.sql",
    "author_subject_h5_index.sql",
)
CITATION_SQL_ORDER = (
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


def validate_matched_cohort(connection: sqlite3.Connection) -> int:
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
    pair_count = row[0]
    expected = (pair_count, pair_count, pair_count, 0, 0, 0, 0)
    if pair_count <= 0 or row != expected:
        raise RuntimeError(
            "matched cohort failed validation: "
            f"observed {row!r}, expected {expected!r}"
        )
    return pair_count


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
    validate_matched_cohort(connection)
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
    output_database: Path,
    sql_directory: Path,
    force: bool,
) -> None:
    raw_database = raw_database.resolve()
    output_database = output_database.resolve()
    sql_directory = sql_directory.resolve()
    if not raw_database.is_file():
        raise FileNotFoundError(raw_database)
    if output_database == raw_database:
        raise ValueError("scratch output must differ from the input database")
    sql_order = COHORT_SQL_ORDER + CITATION_SQL_ORDER
    missing_sql = [name for name in sql_order if not (sql_directory / name).is_file()]
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
        connection.execute("PRAGMA rolap.journal_mode=OFF")
        connection.execute("PRAGMA rolap.synchronous=OFF")
        connection.execute("PRAGMA temp_store=FILE")
        for name in COHORT_SQL_ORDER:
            step_started = time.monotonic()
            print(f"[scratch rebuild] {name}", flush=True)
            connection.executescript((sql_directory / name).read_text(encoding="utf-8"))
            print(
                f"[scratch rebuild] {name} complete in "
                f"{time.monotonic() - step_started:.1f}s",
                flush=True,
            )
        candidate_count, pairs = materialize_profile_matches(
            connection, schema="rolap"
        )
        print(
            f"[scratch rebuild] matched {len(pairs):,} pairs from "
            f"{candidate_count:,} eligible candidates",
            flush=True,
        )
        validate_matched_cohort(connection)
        for name in CITATION_SQL_ORDER:
            step_started = time.monotonic()
            print(f"[scratch rebuild] {name}", flush=True)
            connection.executescript((sql_directory / name).read_text(encoding="utf-8"))
            print(
                f"[scratch rebuild] {name} complete in "
                f"{time.monotonic() - step_started:.1f}s",
                flush=True,
            )
        validate_output(connection)
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
        output_database=args.output_database,
        sql_directory=args.sql_directory,
        force=args.force,
    )


if __name__ == "__main__":
    main()
