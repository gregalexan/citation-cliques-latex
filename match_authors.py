import argparse
from collections import defaultdict
import hashlib
from pathlib import Path
import sqlite3
import sys

import pandas as pd


EXPECTED_PRIMARY_PAIRS = 9_431
H5_CALIPER = 3
PRESERVED_DATABASE = Path(__file__).with_name("rolap.db").resolve()


def retained_cohort_is_valid(con):
    """Validate cohort size, no replacement, and the inclusive h5 caliper."""
    existing_tables = con.execute(
        """
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE type = 'table'
          AND name IN ('author_matched_pairs', 'author_subject_h5_index')
        """
    ).fetchone()[0]
    if existing_tables != 2:
        return False
    row = con.execute(
        """
        SELECT
          COUNT(*) AS n,
          COUNT(DISTINCT p.case_orcid || char(31) || p.subject) AS n_cases,
          COUNT(DISTINCT p.control_orcid || char(31) || p.subject) AS n_controls,
          SUM(CASE WHEN p.case_orcid IS NULL OR TRIM(p.case_orcid) = ''
                        OR p.control_orcid IS NULL OR TRIM(p.control_orcid) = ''
                        OR p.subject IS NULL THEN 1 ELSE 0 END) AS invalid_keys,
          SUM(CASE WHEN case_h5.h5_index IS NULL OR control_h5.h5_index IS NULL
                   THEN 1 ELSE 0 END) AS missing_h5,
          SUM(CASE WHEN ABS(case_h5.h5_index - control_h5.h5_index) > ?
                   THEN 1 ELSE 0 END) AS caliper_violations,
          (
            SELECT COUNT(*)
            FROM (
              SELECT orcid, subject
              FROM (
                SELECT case_orcid AS orcid, subject FROM author_matched_pairs
                UNION ALL
                SELECT control_orcid AS orcid, subject FROM author_matched_pairs
              ) AS memberships
              WHERE orcid IS NOT NULL AND subject IS NOT NULL
              GROUP BY orcid, subject
              HAVING COUNT(*) > 1
            ) AS reused_memberships
          ) AS reused_authors
        FROM author_matched_pairs AS p
        LEFT JOIN author_subject_h5_index AS case_h5
          ON case_h5.orcid = p.case_orcid
         AND case_h5.subject = p.subject
        LEFT JOIN author_subject_h5_index AS control_h5
          ON control_h5.orcid = p.control_orcid
         AND control_h5.subject = p.subject
        """,
        (H5_CALIPER,),
    ).fetchone()
    return row == (
        EXPECTED_PRIMARY_PAIRS,
        EXPECTED_PRIMARY_PAIRS,
        EXPECTED_PRIMARY_PAIRS,
        0,
        0,
        0,
        0,
    )


def _hard_caliper_recomputation(con):
    """Recompute the exact greedy order without materializing every candidate."""
    rows = con.execute(
        """
        SELECT ap.orcid, ap.subject, ap.author_tier, h.h5_index
        FROM author_profiles AS ap
        JOIN author_subject_h5_index AS h
          ON h.orcid = ap.orcid
         AND h.subject = ap.subject
        WHERE ap.author_tier IN ('Bottom Tier', 'Top Tier')
          AND ap.orcid IS NOT NULL
          AND TRIM(ap.orcid) <> ''
          AND h.h5_index IS NOT NULL
        ORDER BY ap.subject ASC, ap.author_tier ASC, ap.orcid ASC
        """
    ).fetchall()
    subjects = {}
    for orcid, subject, tier, h5_index in rows:
        data = subjects.setdefault(
            subject, {"cases": [], "controls": defaultdict(list)}
        )
        if tier == "Bottom Tier":
            data["cases"].append((orcid, h5_index))
        else:
            data["controls"][h5_index].append(orcid)

    candidate_count = 0
    pairs = []
    for subject, data in subjects.items():
        cases = data["cases"]
        controls = data["controls"]
        for _, case_h5 in cases:
            candidate_count += sum(
                len(controls.get(control_h5, ()))
                for control_h5 in range(
                    case_h5 - H5_CALIPER, case_h5 + H5_CALIPER + 1
                )
            )

        seen = set()
        pointers = defaultdict(int)
        for distance in range(H5_CALIPER + 1):
            for case_orcid, case_h5 in cases:
                case_key = (case_orcid, subject)
                if case_key in seen:
                    continue
                eligible_h5 = (
                    (case_h5,)
                    if distance == 0
                    else (case_h5 - distance, case_h5 + distance)
                )
                available = []
                for control_h5 in eligible_h5:
                    group = controls.get(control_h5, ())
                    position = pointers[control_h5]
                    while position < len(group) and (group[position], subject) in seen:
                        position += 1
                    pointers[control_h5] = position
                    if position < len(group):
                        available.append(group[position])
                if available:
                    control_orcid = min(available)
                    pairs.append((case_orcid, control_orcid, str(subject)))
                    seen.add(case_key)
                    seen.add((control_orcid, subject))
    return candidate_count, pairs


def _pair_hash(pairs):
    ordered = sorted(pairs, key=lambda row: (row[2], row[0], row[1]))
    payload = "\n".join(
        f"{subject}\x1f{case}\x1f{control}" for case, control, subject in ordered
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def audit_matching(rolap_db, *, output_path=None):
    """Read-only equality audit of hard-caliper rematching against retention."""
    database = Path(rolap_db).resolve()
    before = (database.stat().st_size, database.stat().st_mtime_ns)
    con = sqlite3.connect(f"file:{database}?mode=ro&immutable=1", uri=True)
    try:
        con.execute("PRAGMA query_only=ON")
        candidate_count, recomputed = _hard_caliper_recomputation(con)
        retained = [
            (case, control, str(subject))
            for case, control, subject in con.execute(
                "SELECT case_orcid, control_orcid, subject "
                "FROM author_matched_pairs ORDER BY rowid"
            )
        ]
        h5 = {
            (orcid, str(subject)): h5_index
            for orcid, subject, h5_index in con.execute(
                "SELECT orcid, subject, h5_index FROM author_subject_h5_index"
            )
        }
    finally:
        con.close()

    recomputed_set = set(recomputed)
    retained_set = set(retained)
    distances = [
        abs(h5[(case, subject)] - h5[(control, subject)])
        for case, control, subject in recomputed
    ]
    memberships = [
        (orcid, subject)
        for case, control, subject in recomputed
        for orcid in (case, control)
    ]
    unchanged = before == (database.stat().st_size, database.stat().st_mtime_ns)
    ordered_equal = recomputed == retained
    membership_equal = recomputed_set == retained_set
    violations = sum(distance > H5_CALIPER for distance in distances)
    reused = len(memberships) - len(set(memberships))

    lines = [
        "connection=mode=ro,immutable=1,query_only=ON",
        f"explicit_caliper={H5_CALIPER}",
        f"derived_candidates={candidate_count}",
        f"recomputed_pairs={len(recomputed)}",
        f"retained_pairs={len(retained)}",
        f"ordered_equal={ordered_equal}",
        f"membership_equal={membership_equal}",
        f"only_recomputed={len(recomputed_set - retained_set)}",
        f"only_retained={len(retained_set - recomputed_set)}",
        f"recomputed_sha256={_pair_hash(recomputed)}",
        f"retained_sha256={_pair_hash(retained)}",
        f"max_abs_h5_distance={max(distances) if distances else None}",
        f"exact_h5_pairs={sum(distance == 0 for distance in distances)}",
        f"caliper_violations={violations}",
        f"reused_author_subjects={reused}",
        f"source_stat_unchanged={unchanged}",
    ]
    report = "\n".join(lines) + "\n"
    if output_path is not None:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix(output.suffix + ".tmp")
        temporary.write_text(report, encoding="utf-8")
        temporary.replace(output)
    print(report, end="")
    return ordered_equal and membership_equal and not violations and not reused and unchanged


def greedy_match(rolap_db, *, force=False):
    # Connect directly to the analysis database
    con = sqlite3.connect(rolap_db)

    print(f"Connected to {rolap_db}...")
    if not force and retained_cohort_is_valid(con):
        print(
            f"Retaining the prespecified {EXPECTED_PRIMARY_PAIRS:,}-pair cohort; "
            "no rematching performed."
        )
        con.close()
        return
    if Path(rolap_db).resolve() == PRESERVED_DATABASE:
        con.close()
        raise ValueError(
            "refusing to rematerialize preserved rolap.db; use --audit or a scratch copy"
        )
    print("Loading candidates...")

    # No 'rolap.' prefix needed since we are directly in rolap.db
    try:
        df = pd.read_sql_query(
            """
            SELECT case_orcid, control_orcid, subject, score
            FROM author_matched_candidates
            WHERE score <= ?
            ORDER BY subject ASC, score ASC, case_orcid ASC, control_orcid ASC
        """,
            con,
            params=(H5_CALIPER,),
        )
    except pd.errors.DatabaseError as e:
        print(f"Error reading candidates: {e}")
        print("Did you run 'make prep_candidates'?")
        con.close()
        return

    final_pairs = []
    seen = set()

    print(f"Greedy matching on {len(df)} candidates...")
    # Iterate through candidates sorted by best match score (ascending)
    for row in df.itertuples():
        # Unique constraint is (ORCID, Subject)
        case_key = (row.case_orcid, row.subject)
        ctrl_key = (row.control_orcid, row.subject)

        if case_key not in seen and ctrl_key not in seen:
            final_pairs.append((row.case_orcid, row.control_orcid, row.subject))
            seen.add(case_key)
            seen.add(ctrl_key)

    print(f"Found {len(final_pairs)} pairs. Saving...")

    with con:
        con.execute("DROP TABLE IF EXISTS author_matched_pairs")
        con.execute(
            "CREATE TABLE author_matched_pairs (case_orcid TEXT, control_orcid TEXT, subject TEXT)"
        )
        con.executemany("INSERT INTO author_matched_pairs VALUES (?,?,?)", final_pairs)

        con.execute(
            "CREATE UNIQUE INDEX idx_amp_case_subject "
            "ON author_matched_pairs(case_orcid, subject)"
        )
        con.execute(
            "CREATE UNIQUE INDEX idx_amp_control_subject "
            "ON author_matched_pairs(control_orcid, subject)"
        )

    con.close()
    print("Done.")


def main(arguments=None):
    parser = argparse.ArgumentParser(description="Materialize or audit deterministic matches.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--force", action="store_true")
    mode.add_argument("--audit", action="store_true")
    parser.add_argument("--audit-output", type=Path)
    parser.add_argument("database", nargs="?", default="rolap.db")
    args = parser.parse_args(sys.argv[1:] if arguments is None else arguments)
    if args.audit_output is not None and not args.audit:
        parser.error("--audit-output requires --audit")
    if args.audit:
        return 0 if audit_matching(args.database, output_path=args.audit_output) else 1
    try:
        greedy_match(args.database, force=args.force)
    except ValueError as error:
        print(error)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
