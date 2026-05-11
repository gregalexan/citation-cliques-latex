#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas", "numpy", "networkx", "scikit-learn", "scipy", "urllib3",
# ]
# ///
"""
investigate_authors.py — Rank outlier authors by suspiciousness, resolve
identities via the ORCID public API, audit their publications, and produce
LaTeX tables for the QSS paper.

Outputs (in analysis_results_v4/tables/):
  1. suspicious_authors_top10.tex  — LaTeX booktabs table of top-10
  2. top10_audit.tex               — per-author mini-profiles
  3. suspicious_authors_full.csv   — all outliers, ranked
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
import urllib.request
import urllib.error
import warnings
from pathlib import Path

import networkx as nx
import numpy as np
import pandas as pd
from scipy.stats import zscore as sp_zscore
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler

warnings.filterwarnings("ignore")

# ── Paths & constants ────────────────────────────────────────────────────
ROLAP_DB = Path("rolap.db")
IMPACT_DB = Path("impact.db")
OUT_DIR = Path("analysis_results_v4") / "tables"
SEED = 42
CONTAMINATION = 0.01

ML_FEATURES = [
    "coauthor_citation_rate", "self_citation_rate", "clustering",
    "triangles_norm", "citation_balance", "reciprocity_rate",
    "outgoing_hhi", "clique_strength", "eigenvector_centrality",
    "k_core_number", "citation_entropy", "citation_hhi",
    "journal_endogamy_rate",
]

# Suspiciousness weights — based on RF importance & Wilcoxon effect sizes
SUSP_WEIGHTS = {
    "coauthor_citation_rate": 4.0,
    "clique_strength":        3.5,
    "reciprocity_rate":       3.5,
    "outgoing_hhi":           3.0,
    "self_citation_rate":     2.0,
    "journal_endogamy_rate":  2.0,
}

SUBJECT_NICE = {
    "1": "Medicine", "2": "Engineering", "3": "Natural Sci.",
    "4": "Social Sci.", "5": "Arts & Hum.",
}


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  PHASE 1 — DATA ASSEMBLY                                              ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def load_data():
    """Reuse diagrams.py feature pipeline (inlined for standalone use)."""
    from scipy.stats import entropy as sp_entropy

    con = sqlite3.connect(str(ROLAP_DB))
    print("  Loading rolap.db …")

    pairs = pd.read_sql_query(
        "SELECT subject, case_orcid, control_orcid "
        "FROM author_matched_pairs", con)
    beh = pd.read_sql_query(
        "SELECT orcid, subject, self_citation_rate, coauthor_citation_rate "
        "FROM author_behavior_metrics", con)
    beh["subject"] = beh["subject"].astype(str)
    ano = pd.read_sql_query(
        "SELECT orcid, max_asymmetry, avg_velocity, max_burst "
        "FROM citation_anomalies", con)
    try:
        venue = pd.read_sql_query(
            "SELECT orcid, journal_endogamy_rate "
            "FROM author_venue_metrics", con)
    except Exception:
        venue = pd.DataFrame(columns=["orcid", "journal_endogamy_rate"])

    edges = pd.read_sql_query(
        "SELECT citing_orcid, cited_orcid, citation_year, citation_weight "
        "FROM citation_network_final "
        "WHERE is_self = 0 "
        "  AND citing_orcid IS NOT NULL "
        "  AND cited_orcid  IS NOT NULL", con)
    con.close()

    cases = (pairs[["case_orcid", "subject"]]
             .rename(columns={"case_orcid": "orcid"})
             .assign(tier_type="Case"))
    ctrls = (pairs[["control_orcid", "subject"]]
             .rename(columns={"control_orcid": "orcid"})
             .assign(tier_type="Control"))
    master = pd.concat([cases, ctrls], ignore_index=True)
    master = (master
              .merge(beh, on=["orcid", "subject"], how="left")
              .merge(ano, on="orcid", how="left")
              .merge(venue, on="orcid", how="left"))
    master["journal_endogamy_rate"] = (
        master["journal_endogamy_rate"].fillna(0))

    pop = set(master["orcid"].unique())
    feats = pd.DataFrame({"orcid": list(pop)})

    # Graph features
    print("  Computing graph features …")
    out_s = edges.groupby("citing_orcid")["citation_weight"].sum()
    in_s = edges.groupby("cited_orcid")["citation_weight"].sum()
    feats["out_strength"] = feats["orcid"].map(out_s).fillna(0)
    feats["in_strength"] = feats["orcid"].map(in_s).fillna(0)
    tot = feats["out_strength"] + feats["in_strength"] + 1e-6
    feats["citation_balance"] = (
        (feats["out_strength"] - feats["in_strength"]) / tot)

    cited_grp = (edges[edges["cited_orcid"].isin(pop)]
                 .groupby("cited_orcid")["citing_orcid"])
    ent_map, hhi_map = {}, {}
    for aid, grp in cited_grp:
        p = grp.value_counts(normalize=True)
        ent_map[aid] = float(sp_entropy(p))
        hhi_map[aid] = float((p ** 2).sum())
    feats["citation_entropy"] = feats["orcid"].map(ent_map).fillna(0)
    feats["citation_hhi"] = feats["orcid"].map(hhi_map).fillna(0)

    g_out = edges.groupby("citing_orcid")["cited_orcid"].apply(set).to_dict()
    g_in = edges.groupby("cited_orcid")["citing_orcid"].apply(set).to_dict()
    recip = {}
    for aid in pop:
        os_ = g_out.get(aid, set())
        is_ = g_in.get(aid, set())
        recip[aid] = len(os_ & is_) / max(len(os_), 1)
    feats["reciprocity_rate"] = feats["orcid"].map(recip).fillna(0)

    out_hhi = {}
    for aid, grp in edges.groupby("citing_orcid")["cited_orcid"]:
        out_hhi[aid] = float(
            (grp.value_counts(normalize=True) ** 2).sum())
    feats["outgoing_hhi"] = feats["orcid"].map(out_hhi).fillna(0)

    mask = (edges["citing_orcid"].isin(pop)
            & edges["cited_orcid"].isin(pop))
    G_dir = nx.from_pandas_edgelist(
        edges[mask], "citing_orcid", "cited_orcid",
        ["citation_weight"], create_using=nx.DiGraph())
    G_und = G_dir.to_undirected()

    feats["clustering"] = feats["orcid"].map(
        nx.clustering(G_und)).fillna(0)
    feats["triangles"] = feats["orcid"].map(
        nx.triangles(G_und)).fillna(0)
    try:
        feats["k_core_number"] = feats["orcid"].map(
            nx.core_number(G_und)).fillna(0)
    except Exception:
        feats["k_core_number"] = 0
    try:
        feats["eigenvector_centrality"] = feats["orcid"].map(
            nx.eigenvector_centrality(G_dir, max_iter=1000, weight="citation_weight")).fillna(0)
    except Exception:
        feats["eigenvector_centrality"] = 0

    master = master.merge(feats, on="orcid", how="left")
    master["coauthor_citation_rate"] = (
        master["coauthor_citation_rate"].fillna(0))
    master["clique_strength"] = (
        master["clustering"] * master["coauthor_citation_rate"])
    master["triangles_norm"] = master["triangles"] / (
        master["out_strength"] + master["in_strength"] + 1)
    master = master.fillna(0)

    # Outlier detection
    print("  Fitting Isolation Forest …")
    avail = [f for f in ML_FEATURES if f in master.columns]
    X = master[avail].fillna(0).values
    X_scaled = RobustScaler().fit_transform(X)
    clf = IsolationForest(
        n_estimators=200, contamination=CONTAMINATION, random_state=SEED)
    master["is_outlier"] = clf.fit_predict(X_scaled) == -1
    n_out = master["is_outlier"].sum()
    print(f"    → Outliers: {n_out}  ({n_out / len(master) * 100:.1f}%)")

    return pairs, master, edges, avail


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  PHASE 1B — IDENTITY RESOLUTION (ORCID public API)                    ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def _fetch_orcid_name(orcid: str) -> dict:
    """Fetch name from the ORCID public API (rate-limited, best-effort)."""
    url = f"https://pub.orcid.org/v3.0/{orcid}/person"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        name_obj = data.get("name", {}) or {}
        given = (name_obj.get("given-names", {}) or {}).get("value", "")
        family = (name_obj.get("family-name", {}) or {}).get("value", "")
        return {"given": given, "family": family,
                "full_name": f"{given} {family}".strip()}
    except (urllib.error.HTTPError, urllib.error.URLError, Exception):
        return {"given": "", "family": "", "full_name": ""}


def resolve_names(orcids: list[str], anonymise: bool = False) -> pd.DataFrame:
    """Resolve a list of ORCIDs to names via the public API."""
    if anonymise:
        rows = [{"orcid": o, "given": "", "family": "",
                 "full_name": f"Author_{i + 1:03d}"}
                for i, o in enumerate(orcids)]
        return pd.DataFrame(rows)

    print(f"  Resolving {len(orcids)} names via ORCID public API …")
    rows = []
    for i, oid in enumerate(orcids):
        info = _fetch_orcid_name(oid)
        info["orcid"] = oid
        rows.append(info)
        if (i + 1) % 10 == 0:
            print(f"    {i + 1}/{len(orcids)} resolved")
        time.sleep(0.15)          # respect rate limits
    return pd.DataFrame(rows)


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  PHASE 2 — SUSPICIOUSNESS SCORING                                     ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def compute_suspiciousness(master: pd.DataFrame) -> pd.DataFrame:
    """Z-score weighted composite suspiciousness score."""
    outliers = master[master["is_outlier"]].copy()
    avail = [c for c in SUSP_WEIGHTS if c in master.columns]

    # Z-score against full population
    for col in avail:
        mu = master[col].mean()
        sd = master[col].std()
        if sd > 0:
            outliers[f"z_{col}"] = (outliers[col] - mu) / sd
        else:
            outliers[f"z_{col}"] = 0.0

    outliers["suspiciousness_score"] = sum(
        SUSP_WEIGHTS[c] * outliers[f"z_{c}"] for c in avail)

    # Flag extreme features (>3σ)
    FLAG_NICE = {
        "coauthor_citation_rate": "co-auth. cit.",
        "clique_strength":        "clique str.",
        "reciprocity_rate":       "recip.",
        "outgoing_hhi":           "out. HHI",
        "self_citation_rate":     "self-cit.",
        "journal_endogamy_rate":  "endog.",
    }
    red_flags = []
    for _, row in outliers.iterrows():
        flags = []
        for c in avail:
            z = row[f"z_{c}"]
            nice = FLAG_NICE.get(c, c)
            if z > 5:
                flags.append(f"{nice} (> 5$\\sigma$)")
            elif z > 3:
                flags.append(f"{nice} (> 3$\\sigma$)")
        red_flags.append(", ".join(flags) if flags else "—")
    outliers["red_flags"] = red_flags

    outliers = outliers.sort_values("suspiciousness_score", ascending=False)
    outliers["rank"] = range(1, len(outliers) + 1)
    return outliers


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  PHASE 3 — PUBLICATION AUDIT                                          ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def _resolve_journal(con_impact, issn: str) -> str:
    """Look up a journal title from an ISSN."""
    if not issn:
        return ""
    row = con_impact.execute(
        "SELECT jn.title FROM journal_names jn "
        "JOIN journals_issns ji ON jn.id = ji.journal_id "
        "WHERE ji.issn = ? LIMIT 1", (issn,)).fetchone()
    return row[0].strip() if row else ""


def audit_publications(orcids: list[str],
                       edges: pd.DataFrame) -> pd.DataFrame:
    """Publication portfolio + citation flow for each ORCID."""
    con = sqlite3.connect(str(IMPACT_DB))
    con_r = sqlite3.connect(str(ROLAP_DB))

    records = []
    for oid in orcids:
        rec = {"orcid": oid}

        # Works count, year range, journal distribution
        works = pd.read_sql_query(
            "SELECT w.doi, w.published_year, "
            "       COALESCE(w.issn_print, w.issn_electronic) AS issn "
            "FROM works w JOIN work_authors wa ON w.id = wa.work_id "
            "WHERE wa.orcid = ?", con, params=(oid,))
        rec["n_works"] = len(works)
        if not works.empty:
            rec["year_min"] = int(works["published_year"].min())
            rec["year_max"] = int(works["published_year"].max())
            # Top journal
            top_issn = (works["issn"].dropna().value_counts()
                        .head(1).index.tolist())
            if top_issn:
                jname = _resolve_journal(con, top_issn[0])
                rec["top_journal"] = jname or top_issn[0]
                rec["top_journal_count"] = int(
                    works["issn"].value_counts().iloc[0])
            else:
                rec["top_journal"] = "—"
                rec["top_journal_count"] = 0
        else:
            rec["year_min"] = rec["year_max"] = 0
            rec["top_journal"] = "—"
            rec["top_journal_count"] = 0

        # Citation flow from network (distinct partners)
        out_edges = edges[edges["citing_orcid"] == oid]
        in_edges = edges[edges["cited_orcid"] == oid]
        rec["total_outgoing"] = out_edges["cited_orcid"].nunique()
        rec["total_incoming"] = in_edges["citing_orcid"].nunique()

        # Top-3 cited by this author
        if not out_edges.empty:
            top_cited = (out_edges.groupby("cited_orcid")
                         ["citation_weight"].sum()
                         .sort_values(ascending=False).head(3))
            rec["top_cited"] = "; ".join(
                f"{o} ({int(w)})" for o, w in top_cited.items())
        else:
            rec["top_cited"] = "—"

        # Reciprocal pairs
        out_set = set(out_edges["cited_orcid"])
        in_set = set(in_edges["citing_orcid"])
        rec["n_reciprocal"] = len(out_set & in_set)

        records.append(rec)

    con.close()
    con_r.close()
    return pd.DataFrame(records)


def find_syndicate_membership(
    master: pd.DataFrame, edges: pd.DataFrame
) -> dict[str, int]:
    """Map each outlier ORCID to its syndicate (connected component) ID."""
    outlier_ids = set(master[master["is_outlier"]]["orcid"])
    mask = (edges["citing_orcid"].isin(outlier_ids)
            & edges["cited_orcid"].isin(outlier_ids))
    sub = edges[mask]
    if sub.empty:
        return {}
    G = nx.from_pandas_edgelist(
        sub, "citing_orcid", "cited_orcid",
        create_using=nx.DiGraph())
    ccs = sorted(nx.connected_components(G.to_undirected()),
                 key=len, reverse=True)
    membership = {}
    for i, cc in enumerate(ccs):
        for node in cc:
            membership[node] = i + 1
    return membership


def find_coauthor_outlier_overlap(
    orcids: list[str], outlier_set: set[str]
) -> dict[str, int]:
    """Count how many co-authors of each ORCID are also outliers."""
    con = sqlite3.connect(str(ROLAP_DB))
    result = {}
    for oid in orcids:
        rows = con.execute(
            "SELECT orcid1, orcid2 FROM coauthor_links "
            "WHERE orcid1 = ? OR orcid2 = ?", (oid, oid)).fetchall()
        coauthors = set()
        for r in rows:
            other = r[1] if r[0] == oid else r[0]
            coauthors.add(other)
        result[oid] = len(coauthors & outlier_set)
    con.close()
    return result


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  PHASE 4 — LATEX OUTPUT                                               ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def _esc(s: str) -> str:
    """Escape LaTeX special characters."""
    for ch in ("&", "%", "$", "#", "_", "{", "}"):
        s = s.replace(ch, f"\\{ch}")
    return s


def write_top10_table(top10: pd.DataFrame, audit: pd.DataFrame,
                      path: Path) -> None:
    """Write a booktabs LaTeX table for the top-10 outlier authors."""
    merged = top10.merge(audit, on="orcid", how="left")
    lines = [
        r"\begin{table*}[t]",
        r"\centering",
        r"\caption{Top-10 outlier authors ranked by composite"
        r" outlier score $S$ (Eq.~1).}",
        r"\label{tab:suspicious-top10}",
        r"\small",
        r"\begin{tabular}{rl l l r r l}",
        r"\toprule",
        r"Rank & ORCID & Subject & $S$ & Works & "
        r"Primary red flags \\",
        r"\midrule",
    ]
    for _, r in merged.iterrows():
        # Truncate red flags for table width (already LaTeX-safe)
        flags = str(r.get("red_flags", "—"))
        if len(flags) > 80:
            flags = flags[:77] + "…"
        subj = SUBJECT_NICE.get(str(r.get("subject", "")),
                                str(r.get("subject", "")))
        oid = _esc(str(r["orcid"]))
        lines.append(
            f"  {int(r['rank'])} & \\texttt{{{oid}}} & "
            f"{_esc(subj)} & {r['suspiciousness_score']:.1f} & "
            f"{int(r.get('n_works', 0))} & {flags} \\\\"
        )
    lines += [
        r"\bottomrule",
        r"\end{tabular}",
        r"\begin{tablenotes}",
        r"\footnotesize",
        r"\item \textit{Note.}"
        r" Red flags indicate features exceeding 3$\sigma$ or"
        r" 5$\sigma$ of the population mean."
        r" These patterns are \emph{statistically anomalous};"
        r" they do not constitute proof of misconduct."
        r" Full author profiles are accessible via"
        r" \url{https://orcid.org/[ORCID]}.",
        r"\end{tablenotes}",
        r"\end{table*}",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  ✓  {path}")


def write_audit_profiles(top10: pd.DataFrame, audit: pd.DataFrame,
                         coauth_overlap: dict,
                         syndicates: dict, path: Path) -> None:
    """Write per-author mini-profiles as a LaTeX itemize."""
    merged = top10.merge(audit, on="orcid", how="left")
    lines = [
        r"\begin{table*}[t]",
        r"\centering",
        r"\caption{Publication audit profiles for top-10"
        r" outlier authors.}",
        r"\label{tab:author-audit}",
        r"\small",
        r"\begin{tabular}{rl rrr rrl}",
        r"\toprule",
        r"Rank & ORCID & Works & Years & Out & In & Recip. &"
        r" Primary journal \\",
        r"\midrule",
    ]
    for _, r in merged.iterrows():
        oid = _esc(str(r["orcid"]))
        yrs = f"{int(r.get('year_min', 0))}--{int(r.get('year_max', 0))}"
        tj = _esc(str(r.get("top_journal", "—")))
        tjc = int(r.get("top_journal_count", 0))
        if len(tj) > 30:
            tj = tj[:27] + "…"
        if tjc > 0:
            tj = f"{tj} ({tjc})"
        syn_id = syndicates.get(r["orcid"], "—")
        ca_ov = coauth_overlap.get(r["orcid"], 0)
        lines.append(
            f"  {int(r['rank'])} & \\texttt{{{oid}}} & "
            f"{int(r.get('n_works', 0))} & {yrs} & "
            f"{int(r.get('total_outgoing', 0))} & "
            f"{int(r.get('total_incoming', 0))} & "
            f"{int(r.get('n_reciprocal', 0))} & {tj} \\\\"
        )
    lines += [
        r"\bottomrule",
        r"\end{tabular}",
        r"\begin{tablenotes}",
        r"\footnotesize",
        r"\item Out = distinct sample authors with ORCIDs cited by this author within 2020--2024 (self-citations excluded);"
        r" In = distinct sample authors with ORCIDs who cite this author;"
        r" Recip. = reciprocal citation partners."
        r" Counts are restricted to the matched-pair sub-network and to ORCID-resolved citation links, so they substantially undercount each author's full reference list."
        r" ``Primary journal'' is the journal in which the author"
        r" published the most works within the 2020--2024 study window.",
        r"\end{tablenotes}",
        r"\end{table*}",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  ✓  {path}")


def write_full_csv(ranked: pd.DataFrame, audit: pd.DataFrame,
                   syndicates: dict, coauth_overlap: dict,
                   path: Path) -> None:
    """Write full ranked CSV for all outliers."""
    out = ranked[["rank", "orcid", "full_name", "subject", "tier_type",
                  "suspiciousness_score", "red_flags"]
                 + [c for c in ML_FEATURES if c in ranked.columns]].copy()
    out["syndicate_id"] = out["orcid"].map(syndicates).fillna(0).astype(int)
    out["coauthor_outliers"] = out["orcid"].map(coauth_overlap).fillna(0).astype(int)

    # Merge publication audit info
    if audit is not None and not audit.empty:
        out = out.merge(
            audit[["orcid", "n_works", "year_min", "year_max",
                   "top_journal", "total_outgoing", "total_incoming",
                   "n_reciprocal"]],
            on="orcid", how="left")

    out.to_csv(path, index=False)
    print(f"  ✓  {path}")


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  MAIN                                                                  ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Investigate suspicious outlier authors.")
    parser.add_argument("--anonymise", action="store_true",
                        help="Replace author names with pseudonyms.")
    parser.add_argument("--top-n", type=int, default=10,
                        help="Number of top suspicious authors (default 10).")
    parser.add_argument("--skip-api", action="store_true",
                        help="Skip ORCID API lookups (use ORCID as name).")
    args = parser.parse_args()

    bar = "=" * 62
    print(bar)
    print("  Suspicious Author Investigation")
    print(f"  Output → {OUT_DIR.resolve()}")
    print(bar)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Phase 1 — load data
    print("\n  Phase 1 — Data assembly")
    pairs, master, edges, feat_cols = load_data()
    n_outliers = master["is_outlier"].sum()
    print(f"    Total outliers: {n_outliers}")

    # Phase 2 — score & rank
    print("\n  Phase 2 — Suspiciousness scoring")
    ranked = compute_suspiciousness(master)
    top_n = ranked.head(args.top_n).copy()
    top_orcids = top_n["orcid"].tolist()
    all_orcids = ranked["orcid"].tolist()
    outlier_set = set(all_orcids)

    print(f"    Top-{args.top_n} scores: "
          f"{top_n['suspiciousness_score'].min():.1f} – "
          f"{top_n['suspiciousness_score'].max():.1f}")

    # Phase 1B — resolve names
    print("\n  Phase 1B — Identity resolution")
    if args.skip_api:
        names_top = pd.DataFrame(
            [{"orcid": o, "given": "", "family": "",
              "full_name": o} for o in top_orcids])
        names_all = pd.DataFrame(
            [{"orcid": o, "given": "", "family": "",
              "full_name": o} for o in all_orcids])
    else:
        names_top = resolve_names(top_orcids, anonymise=args.anonymise)
        names_all = resolve_names(all_orcids, anonymise=args.anonymise)

    top_n = top_n.merge(names_top[["orcid", "full_name"]],
                        on="orcid", how="left")
    ranked = ranked.merge(names_all[["orcid", "full_name"]],
                          on="orcid", how="left")

    # Fill missing names with ORCID
    top_n["full_name"] = top_n["full_name"].fillna(top_n["orcid"])
    ranked["full_name"] = ranked["full_name"].fillna(ranked["orcid"])

    # Phase 3 — publication audit
    print("\n  Phase 3 — Publication audit (top-N)")
    audit_top = audit_publications(top_orcids, edges)

    # Syndicate membership
    print("  Computing syndicate membership …")
    syndicates = find_syndicate_membership(master, edges)
    syn_count = len(set(syndicates.values()))
    largest_syn = max(
        (sum(1 for v in syndicates.values() if v == sid)
         for sid in set(syndicates.values())),
        default=0)
    print(f"    Syndicates: {syn_count}, "
          f"largest: {largest_syn} members")

    # Co-author overlap
    print("  Computing co-author outlier overlap …")
    coauth_all = find_coauthor_outlier_overlap(all_orcids, outlier_set)

    # Phase 4 — output
    print("\n  Phase 4 — Generating output files")
    write_top10_table(
        top_n, audit_top,
        OUT_DIR / "suspicious_authors_top10.tex")
    write_audit_profiles(
        top_n, audit_top, coauth_all, syndicates,
        OUT_DIR / "top10_audit.tex")
    write_full_csv(
        ranked, audit_top, syndicates, coauth_all,
        OUT_DIR / "suspicious_authors_full.csv")

    # Summary stats
    print(f"\n{bar}")
    print(f"  Summary")
    print(f"  Total outliers:     {n_outliers}")
    print(f"  Case among outliers:"
          f" {ranked[ranked['tier_type'] == 'Case'].shape[0]}"
          f" ({ranked[ranked['tier_type'] == 'Case'].shape[0] / n_outliers * 100:.0f}%)")
    print(f"  Subject distribution:")
    for subj, cnt in (ranked.groupby("subject").size()
                      .sort_values(ascending=False).items()):
        nice = SUBJECT_NICE.get(str(subj), str(subj))
        print(f"    {nice}: {cnt}")
    print(f"  In syndicates:      "
          f"{sum(1 for o in all_orcids if o in syndicates)}"
          f" / {n_outliers}")
    print(f"  Avg co-author outlier overlap: "
          f"{np.mean(list(coauth_all.values())):.1f}")
    top3 = top_n.head(3)
    print(f"\n  Top-3 most suspicious:")
    for _, r in top3.iterrows():
        print(f"    #{int(r['rank'])}  {r['full_name']}"
              f"  (score={r['suspiciousness_score']:.1f})")
    print(bar)


if __name__ == "__main__":
    main()
