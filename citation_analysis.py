#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas", "numpy", "seaborn", "matplotlib",
#     "networkx", "scikit-learn", "scipy",
# ]
# ///
"""
citation_analysis.py — Single-file reproducibility script for
"Citation Cliques in Questionable Journals".

Run:
    python citation_analysis.py                  # full pipeline
    python citation_analysis.py --skip-api       # skip ORCID API lookups
    python citation_analysis.py --anonymise      # anonymise author names
    python citation_analysis.py --top-n 20       # report top-20 suspects

Phases (executed in order)
---------------------------
  Phase 1  —  Data loading & feature engineering
  Phase 2  —  Hybrid outlier detection (IF ∩ Cohesion > 4σ)
  Phase 3  —  Statistical analysis (Wilcoxon + FDR, sensitivity)
  Phase 4  —  Publication figures (8 QSS-format figures, PNG + PDF)
  Phase 5  —  Author investigation (suspiciousness ranking + ORCID resolution)

Outputs  →  analysis_output/{figs/, tables/, reports/}
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
import urllib.error
import urllib.request
import warnings
from math import pi
from pathlib import Path

import matplotlib as mpl
import matplotlib.lines as mlines
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import networkx as nx
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.stats import entropy as sp_entropy
from scipy.stats import false_discovery_control, wilcoxon
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import StandardScaler
from sklearn.utils import resample

warnings.filterwarnings("ignore")

# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 1 — CONSTANTS & CONFIG                                        ║
# ╚══════════════════════════════════════════════════════════════════════════╝

ROLAP_DB = Path("rolap.db")
IMPACT_DB = Path("impact.db")
OUT_DIR = Path("analysis_output")
SEED = 42
CONTAMINATION = 0.01        # Isolation Forest: ~1% outliers
COHESION_THRESHOLD = 4.0    # σ above Control baseline for hybrid detection

np.random.seed(SEED)

# 13 features used for the Random Forest and Isolation Forest model.
# Normalised burst intensity is excluded here: it is available for only
# 82% of pairs, and zero-imputing the missing 18% would inject a spurious
# signal (see paper §3.3 and Table 1).
ML_FEATURES = [
    "coauthor_citation_rate", "self_citation_rate", "clustering",
    "triangles_norm", "citation_balance", "reciprocity_rate",
    "outgoing_hhi", "clique_strength", "pagerank",
    "k_core_number", "citation_entropy", "citation_hhi",
    "journal_endogamy_rate",
]

# Hand-tuned weights reflecting RF importance + Wilcoxon effect sizes.
# Authority metrics are sign-inverted before applying weights so that
# lower values indicate greater suspicion.
FEATURE_WEIGHTS = {
    "coauthor_citation_rate": 4.0,   # Top discriminator (+6.7× in Cases)
    "clique_strength":        3.5,   # Strong cohesion signal (+11×)
    "reciprocity_rate":       3.5,   # Mutual citation signal (+4.7×)
    "outgoing_hhi":           3.0,   # Tunnel vision (+3.1×)
    "self_citation_rate":     2.0,
    "journal_endogamy_rate":  1.5,
    "citation_hhi":           1.0,
    "clustering":             0.5,
    "triangles_norm":         0.5,
    "citation_entropy":       0.3,
    "citation_balance":       0.3,
    "pagerank":               0.2,
    "k_core_number":          0.2,
}

# Weights used for the composite suspiciousness score (§5.6 / Eq. 1)
SUSP_WEIGHTS = {
    "coauthor_citation_rate": 4.0,
    "clique_strength":        3.5,
    "reciprocity_rate":       3.5,
    "outgoing_hhi":           3.0,
    "self_citation_rate":     2.0,
    "journal_endogamy_rate":  2.0,
}

FEATURE_NICE = {
    "coauthor_citation_rate": "Co-author Cit. Rate",
    "self_citation_rate":     "Self-Citation Rate",
    "clustering":             "Local Clustering",
    "triangles_norm":         "Norm. Triangles",
    "citation_balance":       "Citation Balance",
    "reciprocity_rate":       "Reciprocity Rate",
    "outgoing_hhi":           "Outgoing HHI",
    "clique_strength":        "Clique Strength",
    "pagerank":               "PageRank",
    "k_core_number":          "K-Core Number",
    "citation_entropy":       "Incoming Entropy",
    "citation_hhi":           "Incoming HHI",
    "journal_endogamy_rate":  "Journal Endogamy",
}

# Semantic grouping for Figure 6 (feature importance colour coding)
FEATURE_CATEGORY = {
    "coauthor_citation_rate": "Cohesion",
    "clique_strength":        "Cohesion",
    "reciprocity_rate":       "Cohesion",
    "clustering":             "Structure",
    "triangles_norm":         "Structure",
    "k_core_number":          "Structure",
    "citation_balance":       "Flow",
    "outgoing_hhi":           "Flow",
    "citation_hhi":           "Flow",
    "pagerank":               "Authority",
    "citation_entropy":       "Diversity",
    "journal_endogamy_rate":  "Diversity",
    "self_citation_rate":     "Diversity",
}
CATEGORY_COLORS = {
    "Cohesion":  "#b2182b",
    "Structure": "#7b2d8b",
    "Flow":      "#2166ac",
    "Authority": "#878787",
    "Diversity": "#2ca02c",
}

# Colourblind-safe palette (Brewer RdBu)
C_CASE  = "#b2182b"   # deep red  — Bottom-Tier / Case / Outlier
C_CTRL  = "#2166ac"   # deep blue — Top-Tier / Control / Baseline
C_HUB   = "#f4a582"   # salmon    — hub accent
C_GIVER = "#e377c2"   # pink      — net giver
C_RECVR = "#17becf"   # cyan      — net receiver
C_GREY  = "#878787"   # neutral

SUBJECT_NICE = {
    "1": "Health Sci.",  "2": "Life Sci.",
    "3": "Multidisciplinary", "4": "Physical Sci.",
    "5": "Social Sci. & Hum.",
}

# QSS column widths (inches)
W1, W2 = 3.46, 7.09


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 2 — STYLE & HELPERS                                           ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def _apply_qss_style() -> None:
    """QSS / MIT Press rcParams — serif, 300 DPI, TrueType PDF."""
    mpl.rcParams.update({
        "font.family": "serif",
        "font.serif": ["DejaVu Serif", "Times New Roman", "Times", "serif"],
        "mathtext.fontset": "dejavuserif",
        "font.size": 8, "axes.titlesize": 9, "axes.labelsize": 7.5,
        "xtick.labelsize": 6.5, "ytick.labelsize": 6.5,
        "legend.fontsize": 6, "legend.title_fontsize": 6.5,
        "axes.spines.top": False, "axes.spines.right": False,
        "axes.linewidth": 0.5, "xtick.major.width": 0.5,
        "ytick.major.width": 0.5, "xtick.major.size": 3, "ytick.major.size": 3,
        "axes.grid": True, "grid.linewidth": 0.25,
        "grid.color": "#e8e8e8", "grid.alpha": 0.6,
        "lines.linewidth": 1.0, "lines.markersize": 3.5, "patch.linewidth": 0.4,
        "figure.dpi": 150, "savefig.dpi": 300,
        "savefig.bbox": "tight", "savefig.pad_inches": 0.03,
        "pdf.fonttype": 42, "ps.fonttype": 42,
        "figure.facecolor": "white", "axes.facecolor": "white",
    })
    sns.set_theme(style="whitegrid", rc=mpl.rcParams)


def _save(fig: plt.Figure, name: str, figs_dir: Path) -> None:
    """Save as 300 DPI PNG + PDF (vector, TrueType embedded)."""
    base = figs_dir / name
    fig.savefig(base.with_suffix(".png"), dpi=300, facecolor="white")
    fig.savefig(base.with_suffix(".pdf"), facecolor="white")
    plt.close(fig)
    print(f"    ✓  {name}")


def _ensure_dirs() -> tuple[Path, Path, Path]:
    figs_dir    = OUT_DIR / "figs"
    tables_dir  = OUT_DIR / "tables"
    reports_dir = OUT_DIR / "reports"
    for d in (figs_dir, tables_dir, reports_dir):
        d.mkdir(parents=True, exist_ok=True)
    return figs_dir, tables_dir, reports_dir


def _esc(s: str) -> str:
    """Escape LaTeX special characters."""
    for ch in ("&", "%", "$", "#", "_", "{", "}"):
        s = s.replace(ch, f"\\{ch}")
    return s


def cliff_delta(x, y):
    x, y = np.asarray(x), np.asarray(y)
    if len(x) == 0 or len(y) == 0:
        return np.nan
    mat = np.sign(np.subtract.outer(x, y))
    return np.sum(mat) / (len(x) * len(y))


def cohen_d_bootstrap(x, y, n_boot=1000):
    x, y = np.asarray(x), np.asarray(y)
    def _d(a, b):
        nx, ny = len(a), len(b)
        if nx < 2 or ny < 2: return 0.0
        pool = np.sqrt(((nx-1)*np.std(a,ddof=1)**2 + (ny-1)*np.std(b,ddof=1)**2)
                       / (nx+ny-2))
        return (np.mean(a) - np.mean(b)) / (pool + 1e-6)
    d = _d(x, y)
    boots = [_d(resample(x), resample(y)) for _ in range(n_boot)]
    return d, np.percentile(boots, 2.5), np.percentile(boots, 97.5)


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 3 — DATA LOADING & FEATURE ENGINEERING                        ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load all tables from rolap.db and compute per-author features.

    Data flow
    ---------
    rolap.db → author_matched_pairs      (9,431 pairs, 5 subjects)
             → author_behavior_metrics   (self-cit rate, coauthor-cit rate)
             → citation_anomalies        (anomaly signals including max_burst)
             → author_venue_metrics      (journal endogamy)
             → citation_network_final    (weighted citation edges)

    Graph features computed via NetworkX:
      clustering, triangles, k_core_number, pagerank,
      citation_balance, reciprocity_rate, outgoing_hhi,
      citation_entropy, citation_hhi, clique_strength,
      triangles_norm, max_burst_norm.

    Returns
    -------
    pairs    : DataFrame of matched case-control pairs
    master   : DataFrame, one row per author, all 14 features
    edges    : DataFrame of citation edges (with citation_year)
    """
    con = sqlite3.connect(str(ROLAP_DB))
    print("  Loading rolap.db …")

    pairs = pd.read_sql_query(
        "SELECT subject, case_orcid, control_orcid FROM author_matched_pairs", con)
    beh = pd.read_sql_query(
        "SELECT orcid, subject, self_citation_rate, coauthor_citation_rate "
        "FROM author_behavior_metrics", con)
    beh["subject"] = beh["subject"].astype(str)
    ano = pd.read_sql_query(
        "SELECT orcid, max_asymmetry, avg_velocity, max_burst "
        "FROM citation_anomalies", con)
    try:
        venue = pd.read_sql_query(
            "SELECT orcid, journal_endogamy_rate FROM author_venue_metrics", con)
    except Exception:
        venue = pd.DataFrame(columns=["orcid", "journal_endogamy_rate"])
    edges = pd.read_sql_query(
        "SELECT citing_orcid, cited_orcid, citation_year, citation_weight "
        "FROM citation_network_final "
        "WHERE is_self = 0 AND citing_orcid IS NOT NULL AND cited_orcid IS NOT NULL",
        con)
    con.close()

    # Master table: one row per author
    cases = (pairs[["case_orcid", "subject"]]
             .rename(columns={"case_orcid": "orcid"}).assign(tier_type="Case"))
    ctrls = (pairs[["control_orcid", "subject"]]
             .rename(columns={"control_orcid": "orcid"}).assign(tier_type="Control"))
    master = (pd.concat([cases, ctrls], ignore_index=True)
              .merge(beh,   on=["orcid", "subject"], how="left")
              .merge(ano,   on="orcid",              how="left")
              .merge(venue, on="orcid",              how="left"))
    master["journal_endogamy_rate"] = master["journal_endogamy_rate"].fillna(0)

    pop   = set(master["orcid"].unique())
    feats = pd.DataFrame({"orcid": list(pop)})

    print("  Computing graph features (this may take a few minutes) …")

    # Degree & citation balance
    out_s = edges.groupby("citing_orcid")["citation_weight"].sum()
    in_s  = edges.groupby("cited_orcid")["citation_weight"].sum()
    feats["out_strength"] = feats["orcid"].map(out_s).fillna(0)
    feats["in_strength"]  = feats["orcid"].map(in_s).fillna(0)
    tot = feats["out_strength"] + feats["in_strength"] + 1e-6
    feats["citation_balance"] = (feats["out_strength"] - feats["in_strength"]) / tot

    # Incoming entropy & HHI
    cited_grp = (edges[edges["cited_orcid"].isin(pop)]
                 .groupby("cited_orcid")["citing_orcid"])
    ent_map, hhi_map = {}, {}
    for aid, grp in cited_grp:
        p = grp.value_counts(normalize=True)
        ent_map[aid] = float(sp_entropy(p))
        hhi_map[aid] = float((p**2).sum())
    feats["citation_entropy"] = feats["orcid"].map(ent_map).fillna(0)
    feats["citation_hhi"]     = feats["orcid"].map(hhi_map).fillna(0)

    # Reciprocity
    g_out = edges.groupby("citing_orcid")["cited_orcid"].apply(set).to_dict()
    g_in  = edges.groupby("cited_orcid")["citing_orcid"].apply(set).to_dict()
    recip = {}
    for aid in pop:
        os_ = g_out.get(aid, set()); is_ = g_in.get(aid, set())
        recip[aid] = len(os_ & is_) / max(len(os_), 1)
    feats["reciprocity_rate"] = feats["orcid"].map(recip).fillna(0)

    # Outgoing concentration
    out_hhi = {}
    for aid, grp in edges.groupby("citing_orcid")["cited_orcid"]:
        out_hhi[aid] = float((grp.value_counts(normalize=True)**2).sum())
    feats["outgoing_hhi"] = feats["orcid"].map(out_hhi).fillna(0)

    # Topology: clustering, triangles, k-core, PageRank
    mask  = edges["citing_orcid"].isin(pop) & edges["cited_orcid"].isin(pop)
    G_dir = nx.from_pandas_edgelist(
        edges[mask], "citing_orcid", "cited_orcid",
        ["citation_weight"], create_using=nx.DiGraph())
    G_und = G_dir.to_undirected()
    feats["clustering"] = feats["orcid"].map(nx.clustering(G_und)).fillna(0)
    feats["triangles"]  = feats["orcid"].map(nx.triangles(G_und)).fillna(0)
    try:
        feats["k_core_number"] = feats["orcid"].map(nx.core_number(G_und)).fillna(0)
    except Exception:
        feats["k_core_number"] = 0
    try:
        feats["pagerank"] = feats["orcid"].map(
            nx.pagerank(G_dir, weight="citation_weight")).fillna(0)
    except Exception:
        feats["pagerank"] = 0

    # Merge, derive clique_strength, triangles_norm, max_burst_norm
    master = master.merge(feats, on="orcid", how="left")
    master["coauthor_citation_rate"] = master["coauthor_citation_rate"].fillna(0)
    master["clique_strength"]   = master["clustering"] * master["coauthor_citation_rate"]
    master["triangles_norm"]    = master["triangles"] / (
        master["out_strength"] + master["in_strength"] + 1)
    # max_burst_norm: 14th feature used in stat tests only (18% missing — not in RF model)
    if "max_burst" in master.columns:
        master["max_burst_norm"] = master["max_burst"] / (master["in_strength"] + 1)
    master = master.fillna(0)

    print(f"    → {len(pairs):,} pairs  ·  {len(master):,} authors  "
          f"·  {len(edges):,} citation edges")
    return pairs, master, edges


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 4 — HYBRID OUTLIER DETECTION                                  ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def detect_outliers(master: pd.DataFrame) -> tuple[pd.DataFrame, IsolationForest, np.ndarray]:
    """
    Hybrid detection: Isolation Forest ∩ Cohesion z-score > 4σ.

    Processing choices (documented in paper §3.3):
    - StandardScaler (zero mean, unit variance) for all 13 ML features
    - Hand-tuned feature weights (FEATURE_WEIGHTS) applied after scaling;
      authority metrics are sign-inverted so that lower values → higher anomaly
    - Isolation Forest: n_estimators=200, contamination=0.01, random_state=42
    - Cohesion composite = standardised sum of co-author citation rate,
      clique strength, reciprocity rate, outgoing HHI
    - An author is flagged only if BOTH the IF anomaly score exceeds the
      per-subject threshold AND the cohesion z-score > 4σ above the
      Control baseline (contamination=0.01 → ~1% of 9,431 authors)

    Returns
    -------
    master      : updated with is_outlier, cohesion_zscore columns
    clf         : fitted IsolationForest (for sensitivity analysis)
    X_weighted  : weighted+scaled feature matrix (for sensitivity analysis)
    """
    print("  Fitting hybrid outlier detector …")
    avail = [f for f in ML_FEATURES if f in master.columns]
    X = master[avail].values
    X_scaled = StandardScaler().fit_transform(X)

    # Apply feature weights and invert authority metrics
    weights = np.array([FEATURE_WEIGHTS.get(f, 1.0) for f in avail])
    X_weighted = X_scaled * weights
    invert_cols = {"pagerank", "k_core_number", "citation_entropy", "citation_balance"}
    for i, f in enumerate(avail):
        if f in invert_cols:
            X_weighted[:, i] = -X_weighted[:, i]

    # Cohesion composite z-score relative to Control baseline
    cohesion_idx = [i for i, f in enumerate(avail)
                    if f in {"coauthor_citation_rate", "clique_strength",
                              "reciprocity_rate", "outgoing_hhi"}]
    master["cohesion_score"]  = X_scaled[:, cohesion_idx].sum(axis=1)
    ctrl_coh = master.loc[master["tier_type"] == "Control", "cohesion_score"]
    master["cohesion_zscore"] = (master["cohesion_score"] - ctrl_coh.mean()) / ctrl_coh.std()

    # Isolation Forest
    clf = IsolationForest(
        n_estimators=200, contamination=CONTAMINATION, random_state=SEED)
    master["is_outlier_if"] = clf.fit_predict(X_weighted) == -1

    # Hybrid intersection
    master["is_outlier"] = (
        master["is_outlier_if"] & (master["cohesion_zscore"] > COHESION_THRESHOLD))

    n_if     = int(master["is_outlier_if"].sum())
    n_hybrid = int(master["is_outlier"].sum())
    purity   = (master[master["is_outlier"]]["tier_type"] == "Case").mean() * 100
    print(f"    IF-only: {n_if}  ·  Hybrid (IF ∩ Cohesion>{COHESION_THRESHOLD:.0f}σ): "
          f"{n_hybrid}  ·  Case purity: {purity:.1f}%")
    return master, clf, X_weighted


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 5 — STATISTICAL ANALYSIS                                       ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def report_statistical_tests(
    master: pd.DataFrame, pairs: pd.DataFrame, reports_dir: Path
) -> None:
    """
    Matched-pair Wilcoxon signed-rank tests on all 14 behavioural features
    with Benjamini-Hochberg FDR correction. 12 of 14 features are significant
    at α=0.05 after correction. Normalised burst intensity is included here
    because statistical tests drop incomplete pairs; the RF model excludes it
    due to 18% sparsity (see detect_outliers / paper §3.3).
    """
    print("\n  Statistical tests (Wilcoxon + BH-FDR) …")
    # 14 tested metrics = 13 RF features + max_burst_norm
    metrics = {
        "Rate: Co-author Citations":  "coauthor_citation_rate",
        "Rate: Self-citation":        "self_citation_rate",
        "Rate: Clustering":           "clustering",
        "Norm: Triangles":            "triangles_norm",
        "Flow: Citation Balance":     "citation_balance",
        "Interaction: Clique Strength": "clique_strength",
        "Structure: Reciprocity":     "reciprocity_rate",
        "Tunnel: Outgoing HHI":       "outgoing_hhi",
        "Auth: PageRank":             "pagerank",
        "Struct: K-Core Number":      "k_core_number",
        "Diversity: Entropy":         "citation_entropy",
        "Concentration: HHI":         "citation_hhi",
        "Silo: Journal Endogamy":     "journal_endogamy_rate",
        "Norm: Burst Intensity":      "max_burst_norm",
    }
    results, raw_pvalues = [], []
    for label, col in metrics.items():
        if col not in master.columns:
            continue
        p = (pairs
             .merge(master[master["tier_type"] == "Case"][["orcid", col]]
                    .rename(columns={"orcid": "case_orcid", col: "case_val"}),
                    on="case_orcid")
             .merge(master[master["tier_type"] == "Control"][["orcid", col]]
                    .rename(columns={"orcid": "control_orcid", col: "ctrl_val"}),
                    on="control_orcid")
             .dropna(subset=["case_val", "ctrl_val"]))
        if len(p) < 10:
            continue
        mean_c, mean_k = p["case_val"].mean(), p["ctrl_val"].mean()
        diff = mean_c - mean_k
        diffs = (p["case_val"] - p["ctrl_val"]).values
        nonzero = diffs[diffs != 0]
        try:
            pval = wilcoxon(nonzero, alternative="two-sided")[1] if len(nonzero) >= 10 else np.nan
        except Exception:
            pval = np.nan
        raw_pvalues.append(pval)
        d_val, ci_l, ci_h = cohen_d_bootstrap(p["case_val"], p["ctrl_val"])
        results.append({
            "Metric": label, "N_pairs": len(p),
            "Mean_Case": mean_c, "Mean_Ctrl": mean_k, "Diff": diff,
            "Status": "Case > Ctrl" if diff > 0 else "Ctrl > Case",
            "Wilcoxon_p": pval,
            "Cliffs_Delta": cliff_delta(p["case_val"], p["ctrl_val"]),
            "Cohens_d": d_val, "CI_Low": ci_l, "CI_High": ci_h,
        })
    df_res = pd.DataFrame(results)
    valid = np.array([v if not np.isnan(v) else 1.0 for v in raw_pvalues])
    try:
        fdr = false_discovery_control(valid, method="bh")
    except Exception:
        n = len(valid); sorted_idx = np.argsort(valid)
        ranks = np.empty(n, dtype=int); ranks[sorted_idx] = np.arange(1, n + 1)
        fdr = np.minimum(1.0, valid * n / ranks)
    df_res["FDR_adjusted_p"]   = fdr
    df_res["Significant_FDR05"] = fdr < 0.05
    n_sig = int(df_res["Significant_FDR05"].sum())
    print(f"    Significant after FDR (α=0.05): {n_sig}/{len(df_res)}")
    df_res.to_csv(reports_dir / "statistical_summary.csv", index=False)
    print(f"    → {reports_dir / 'statistical_summary.csv'}")


def report_subject_stratified_stats(
    master: pd.DataFrame, pairs: pd.DataFrame, reports_dir: Path
) -> None:
    """Cliff's δ effect sizes per subject × metric category."""
    print("  Subject-stratified statistics …")
    metric_map = {
        "Cohesion":  ["coauthor_citation_rate", "clustering"],
        "Burst":     ["max_burst_norm", "avg_velocity"],
        "Asymmetry": ["citation_balance"],
    }
    results = []
    for subj in pairs["subject"].value_counts()[
            pairs["subject"].value_counts() >= 20].index:
        sp = pairs[pairs["subject"] == subj].copy()
        cd = master[(master["tier_type"] == "Case")   & (master["subject"] == str(subj))]
        ck = master[(master["tier_type"] == "Control") & (master["subject"] == str(subj))]
        cols_needed = ["orcid"] + [m for ms in metric_map.values() for m in ms]
        sp = (sp.merge(cd[[c for c in cols_needed if c in cd.columns]]
                       .rename(columns={"orcid": "case_orcid"}), on="case_orcid", how="inner")
                .merge(ck[[c for c in cols_needed if c in ck.columns]]
                       .rename(columns={"orcid": "control_orcid"}), on="control_orcid", how="inner"))
        for cat, metrics in metric_map.items():
            for m in metrics:
                cc, ck_col = m, f"{m}_y" if f"{m}_y" in sp.columns else m
                cv_col = f"{m}_x" if f"{m}_x" in sp.columns else m
                rows = sp.dropna(subset=[cv_col, ck_col]) if cv_col in sp.columns else pd.DataFrame()
                if len(rows) < 10: continue
                results.append({
                    "Subject": subj, "N_Pairs": len(rows),
                    "Category": cat, "Metric": m,
                    "Mean_Diff": (rows[cv_col] - rows[ck_col]).mean(),
                    "Cliffs_Delta": cliff_delta(rows[cv_col], rows[ck_col]),
                })
    pd.DataFrame(results).to_csv(reports_dir / "subject_stratified_stats.csv", index=False)
    print(f"    → {reports_dir / 'subject_stratified_stats.csv'}")


def analyze_syndicate_sensitivity(
    master: pd.DataFrame, edges: pd.DataFrame,
    clf: IsolationForest, X_weighted: np.ndarray,
    reports_dir: Path,
) -> None:
    """
    Three-method sensitivity analysis at σ thresholds 1–4:
    IF-only, Cohesion-only, Hybrid (IF ∩ Cohesion).
    Produces Table 3 (contamination comparison) in the paper.
    """
    print("  Syndicate sensitivity analysis …")
    raw_if = -clf.decision_function(X_weighted)
    ctrl_if = raw_if[master["tier_type"] == "Control"]
    z_if = (raw_if - ctrl_if.mean()) / ctrl_if.std()
    z_coh = master["cohesion_zscore"].values

    def _stats(mask):
        ids = set(master[mask]["orcid"]); n = len(ids)
        if n == 0: return None
        purity = (master[mask]["tier_type"] == "Case").mean() * 100
        sub = edges[edges["citing_orcid"].isin(ids) & edges["cited_orcid"].isin(ids)]
        if not sub.empty:
            G = nx.from_pandas_edgelist(
                sub, "citing_orcid", "cited_orcid", create_using=nx.DiGraph())
            ccs = sorted(nx.connected_components(G.to_undirected()), key=len, reverse=True)
            connected = sum(len(c) for c in ccs if len(c) >= 2) / n * 100
        else:
            connected = 0.0
        return n, purity, connected

    rows = []
    for thr in [1, 2, 3, 4]:
        for method, mask in [
            ("Isolation Forest (Baseline)",  z_if > thr),
            ("Cohesion Composite Only",      z_coh > thr),
            ("Hybrid (IF ∩ Cohesion)",       (z_if > thr - 1) & (z_coh > thr)),
        ]:
            s = _stats(mask)
            if s:
                rows.append({"Method": method, "Threshold": f"{thr}σ",
                              "Outliers": s[0], "Case Purity (%)": round(s[1], 1),
                              "Connected (%)": round(s[2], 1)})
    pd.DataFrame(rows).to_csv(reports_dir / "syndicate_sensitivity.csv", index=False)
    print(f"    → {reports_dir / 'syndicate_sensitivity.csv'}")


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 6 — PUBLICATION FIGURES (QSS format)                          ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def fig1_forest_plot(
    master: pd.DataFrame, pairs: pd.DataFrame, figs_dir: Path
) -> None:
    """Figure 1 — Co-author citation gap by subject field (forest plot)."""
    m = "coauthor_citation_rate"
    merged = (pairs
              .merge(master[master["tier_type"] == "Case"][["orcid", "subject", m]]
                     .rename(columns={"orcid": "case_orcid", m: "cv"}),
                     on=["case_orcid", "subject"])
              .merge(master[master["tier_type"] == "Control"][["orcid", "subject", m]]
                     .rename(columns={"orcid": "control_orcid", m: "bv"}),
                     on=["control_orcid", "subject"]))
    merged["d"] = merged["cv"] - merged["bv"]
    rows = []
    for subj, g in merged.groupby("subject"):
        if len(g) < 10: continue
        nice = SUBJECT_NICE.get(str(subj), f"Field {subj}")
        rows.append(dict(label=f"{nice}  (n={len(g):,})",
                         mean=g["d"].mean(), ci=1.96*g["d"].sem()))
    df = pd.concat([pd.DataFrame(rows).sort_values("mean"),
                    pd.DataFrame([{"label": "Overall",
                                   "mean": merged["d"].mean(),
                                   "ci": 1.96*merged["d"].sem()}])],
                   ignore_index=True)
    fig, ax = plt.subplots(figsize=(W2, max(2.6, len(df)*0.44+0.6)))
    y = np.arange(len(df))
    for i, r in df.iterrows():
        is_ov = r["label"] == "Overall"
        c = "#222222" if is_ov else C_CASE
        ax.errorbar(r["mean"], i, xerr=r["ci"],
                    fmt="D" if is_ov else "o", color=c, ecolor=c,
                    ms=4.5 if is_ov else 3.2, mew=0.4,
                    lw=1.2 if is_ov else 0.7,
                    elinewidth=0.6, capsize=2.5, capthick=0.5, zorder=3)
    ax.axvline(0, color="#aaaaaa", lw=0.5, ls="--", zorder=1)
    x_hi = max((df["mean"]+df["ci"]).max()*1.3, 0.001)
    ax.axvspan(0, x_hi, alpha=0.04, color=C_CASE, zorder=0)
    ax.axhline(len(df)-1.5, color="#d0d0d0", lw=0.35, zorder=0)
    ax.set_yticks(y); ax.set_yticklabels(df["label"])
    for lbl in ax.get_yticklabels():
        if lbl.get_text() == "Overall": lbl.set_fontweight("bold")
    from matplotlib.transforms import blended_transform_factory as btf
    trans = btf(ax.transAxes, ax.transData)
    for i, r in df.iterrows():
        is_ov = r["label"] == "Overall"; c = "#222222" if is_ov else C_CASE
        sig = "" if is_ov else (" *" if (r["mean"]-r["ci"]>0 or r["mean"]+r["ci"]<0) else "")
        ax.text(1.01, i, f"Δ = {r['mean']:.4f}{sig}",
                transform=trans, va="center", ha="left",
                fontsize=6.5, color=c, clip_on=False)
    ax.set_xlabel("Mean Δ co-author citation rate  (Case − Control)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{x:.3f}"))
    ax.set_title("Co-author citation gap by subject field",
                 loc="left", fontweight="bold", pad=6)
    fig.tight_layout(); fig.subplots_adjust(right=0.82)
    _save(fig, "Figure1_ForestPlot", figs_dir)


def fig2a_radar_fingerprint(master: pd.DataFrame, figs_dir: Path) -> None:
    """Figure 2a — Outlier fold-change radar (polar, log-scale)."""
    METRICS = [("coauthor_citation_rate", "Co-author\ncitation rate"),
               ("clique_strength",        "Clique\nstrength"),
               ("reciprocity_rate",       "Reciprocity\nrate"),
               ("outgoing_hhi",           "Outgoing\nHHI"),
               ("self_citation_rate",     "Self-citation\nrate"),
               ("clustering",             "Local\nclustering")]
    cols = [c for c,_ in METRICS if c in master.columns]
    lbls = [l for c,l in METRICS if c in master.columns]
    out_m = master[master["is_outlier"]][cols].mean()
    nrm_m = master[~master["is_outlier"]][cols].mean()
    fc = ((out_m + 1e-7) / (nrm_m + 1e-7)).values
    log_fc = np.log10(fc)
    N = len(cols)
    angles = np.linspace(0, 2*pi, N, endpoint=False).tolist()
    log_fc_c = np.concatenate([log_fc, [log_fc[0]]])
    angles_c = angles + [angles[0]]
    fig, ax = plt.subplots(figsize=(W1+1.0, W1+1.0), subplot_kw={"projection": "polar"})
    ax.plot(angles_c, log_fc_c, color=C_CASE, lw=1.8, zorder=3)
    ax.fill(angles_c, log_fc_c, color=C_CASE, alpha=0.15, zorder=2)
    bl = np.linspace(0, 2*pi, 200)
    for val, ls in [(0, "--"), (1, ":"), (2, ":")]:
        ax.plot(bl, np.full(200, float(val)), color="#d0d0d0", lw=0.5 if val else 0.9,
                ls=ls, alpha=0.7 if not val else 0.5)
    ax.set_rlim(-0.15, max(log_fc)*1.15)
    r_ticks, r_labels = [0, 1, 2], ["1×", "10×", "100×"]
    ax.set_rticks(r_ticks); ax.set_yticklabels(r_labels, fontsize=5.5, color=C_GREY)
    ax.set_rlabel_position(60); ax.set_xticks(angles); ax.set_xticklabels(lbls, fontsize=6.5)
    ax.tick_params(axis="x", pad=12)
    for angle, lfc, val in zip(angles, log_fc, fc):
        cos_a, sin_a = np.cos(angle), np.sin(angle)
        ha = "center" if abs(cos_a) < 0.3 else ("left" if cos_a > 0 else "right")
        va = "bottom" if sin_a > -0.3 else "top"
        ax.annotate(f"{val:.1f}×", xy=(angle, lfc),
                    xytext=(6*cos_a+2, 6*sin_a+2), textcoords="offset points",
                    fontsize=6.5, fontweight="bold", color="#333", ha=ha, va=va, zorder=5)
    ax.set_title("Outlier behavioural fingerprint", fontweight="bold", pad=20, fontsize=9)
    fig.tight_layout(); _save(fig, "Figure2a_Fingerprint", figs_dir)


def fig3_permutation_test(
    master: pd.DataFrame, pairs: pd.DataFrame, figs_dir: Path, B: int = 10_000
) -> None:
    """Figure 3 — Permutation test null distribution for co-author citation rate."""
    m = "coauthor_citation_rate"
    p = (pairs
         .merge(master[master["tier_type"]=="Case"][["orcid","subject",m]]
                .rename(columns={"orcid":"case_orcid",m:"case_val"}),
                on=["case_orcid","subject"])
         .merge(master[master["tier_type"]=="Control"][["orcid","subject",m]]
                .rename(columns={"orcid":"control_orcid",m:"ctrl_val"}),
                on=["control_orcid","subject"])
         .dropna())
    obs = (p["case_val"] - p["ctrl_val"]).mean()
    vals = np.concatenate([p["case_val"].values, p["ctrl_val"].values])
    n = len(p); rng = np.random.default_rng(SEED); null = np.empty(B)
    for i in range(B):
        rng.shuffle(vals); null[i] = vals[:n].mean() - vals[n:].mean()
    pval = (np.sum(null >= obs) + 1) / (B + 1)
    p_str = "p < 0.0001" if pval < 0.0001 else f"p = {pval:.4f}"
    fig, ax = plt.subplots(figsize=(W1+0.5, 2.4))
    ax.hist(null, bins=50, color="#cccccc", edgecolor="white", lw=0.3, density=True)
    sns.kdeplot(null, color="#888888", lw=1.0, ax=ax)
    ax.axvline(obs, color=C_CASE, lw=1.2, ls="--", label=f"Observed Δ = {obs:.4f}")
    ax.text(0.97, 0.95, f"Observed Δ = {obs:.4f}\n{p_str}\nB = {B:,}",
            transform=ax.transAxes, fontsize=6.5, ha="right", va="top",
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#ccc", lw=0.4))
    ax.set_xlabel("Permuted mean Δ co-author citation rate")
    ax.set_ylabel("Density")
    ax.set_title("Permutation test — co-author citation rate",
                 loc="left", fontweight="bold", pad=6)
    ax.legend(fontsize=6, frameon=True, framealpha=0.85, edgecolor="#ddd")
    fig.tight_layout(); _save(fig, "Figure3_PermutationTest", figs_dir)


def fig4_syndicate_network(
    master: pd.DataFrame, edges: pd.DataFrame, figs_dir: Path
) -> None:
    """Figure 4 — Largest outlier citation syndicate network."""
    outlier_ids = set(master[master["is_outlier"]]["orcid"])
    mask = (edges["citing_orcid"].isin(outlier_ids) & edges["cited_orcid"].isin(outlier_ids))
    sub = edges[mask]; G = None; is_synthetic = False
    if not sub.empty:
        Gfull = nx.from_pandas_edgelist(sub, "citing_orcid", "cited_orcid",
                                        ["citation_weight"], create_using=nx.DiGraph())
        ccs = sorted(nx.connected_components(Gfull.to_undirected()), key=len, reverse=True)
        if ccs and len(ccs[0]) >= 5:
            G = Gfull.subgraph(ccs[0]).copy()
    if G is None:
        is_synthetic = True; rng = np.random.default_rng(SEED); N_s = 22
        G = nx.DiGraph(); nodes = [f"S{i:02d}" for i in range(N_s)]; G.add_nodes_from(nodes)
        for i in range(N_s):
            G.add_edge(nodes[i], nodes[(i+1)%N_s], citation_weight=int(rng.integers(3,10)))
            G.add_edge(nodes[(i+1)%N_s], nodes[i], citation_weight=int(rng.integers(1,6)))
        for _ in range(int(N_s*N_s*0.28)):
            u,v = int(rng.integers(0,N_s)), int(rng.integers(0,N_s))
            if u != v:
                w = int(rng.integers(1,7))
                if G.has_edge(nodes[u],nodes[v]): G[nodes[u]][nodes[v]]["citation_weight"] += w
                else: G.add_edge(nodes[u], nodes[v], citation_weight=w)
    nn = G.number_of_nodes(); Gu = G.to_undirected()
    pos = nx.spring_layout(Gu, seed=SEED, k=2.8/(nn**0.5), iterations=200)
    bet = nx.betweenness_centrality(Gu); hub = max(bet, key=bet.get)
    bet_vals = np.array([bet.get(n, 0) for n in Gu.nodes()])
    sizes = 80 + np.cbrt(bet_vals / (bet_vals.max() + 1e-9)) * 500
    in_deg = dict(G.in_degree()); out_deg = dict(G.out_degree())
    colors = [C_HUB if n == hub else (C_GIVER if out_deg.get(n,0)>in_deg.get(n,0) else C_RECVR)
              for n in Gu.nodes()]
    wts = [Gu[u][v].get("citation_weight",1) for u,v in Gu.edges()]; wmax = max(wts) if wts else 1
    ew = [0.3 + (w/wmax)*1.8 for w in wts]
    fig, ax = plt.subplots(figsize=(W2, W2*0.62))
    nx.draw_networkx_edges(Gu, pos, width=ew, alpha=0.35, edge_color="#999999", ax=ax)
    nx.draw_networkx_nodes(Gu, pos, node_size=sizes, node_color=colors,
                           edgecolors="#444444", linewidths=0.35, alpha=0.88, ax=ax)
    ax.annotate("Hub", xy=pos[hub], fontsize=6, fontweight="bold", ha="center", va="bottom",
                xytext=(0,7), textcoords="offset points",
                arrowprops=dict(arrowstyle="-", lw=0.25, color="#777"), color="#333")
    ax.axis("off")
    ax.text(0.02, 0.02, f"n = {nn}  ·  density = {nx.density(Gu):.2f}",
            transform=ax.transAxes, fontsize=5.5, color="#888", va="bottom")
    ax.legend(handles=[
        mpatches.Patch(fc=C_HUB, ec="#444", lw=0.3, label="Hub (max betweenness)"),
        mpatches.Patch(fc=C_GIVER, ec="#444", lw=0.3, label="Net giver"),
        mpatches.Patch(fc=C_RECVR, ec="#444", lw=0.3, label="Net receiver"),
    ], loc="upper left", frameon=True, framealpha=0.85, fontsize=5.5, edgecolor="#ddd")
    if is_synthetic:
        ax.text(0.5, 0.5, "Illustrative — synthetic data", transform=ax.transAxes,
                fontsize=8, color=C_CASE, alpha=0.35, ha="center",
                fontstyle="italic", rotation=20)
    ax.set_title(f"Largest outlier syndicate  (n = {nn})",
                 loc="left", fontweight="bold", fontsize=8)
    fig.tight_layout(); _save(fig, "Figure4_Network", figs_dir)


def fig5_temporal_evolution(
    master: pd.DataFrame, edges: pd.DataFrame, figs_dir: Path
) -> None:
    """Figure 5 — Two-panel temporal analysis of largest outlier syndicate."""
    outlier_ids = set(master[master["is_outlier"]]["orcid"])
    mask = (edges["citing_orcid"].isin(outlier_ids) & edges["cited_orcid"].isin(outlier_ids))
    syn_edges = edges[mask]
    if syn_edges.empty or "citation_year" not in syn_edges.columns:
        print("    ⚠  no temporal data — skipping Fig 5"); return
    Gfull = nx.from_pandas_edgelist(syn_edges, "citing_orcid", "cited_orcid",
                                    ["citation_weight"], create_using=nx.DiGraph())
    ccs = sorted(nx.connected_components(Gfull.to_undirected()), key=len, reverse=True)
    if not ccs or len(ccs[0]) < 5:
        print("    ⚠  syndicate too small — skipping Fig 5"); return
    cc = ccs[0]
    tl = syn_edges[syn_edges["citing_orcid"].isin(cc) & syn_edges["cited_orcid"].isin(cc)]
    yearly = (tl.groupby("citation_year")["citation_weight"].sum()
              .reset_index().sort_values("citation_year"))
    yearly["cumulative"] = yearly["citation_weight"].cumsum()
    if len(yearly) < 2: return
    fig, (ax_a, ax_b) = plt.subplots(1, 2, figsize=(W2, 2.6))
    ax_a.fill_between(yearly["citation_year"], 0, yearly["cumulative"], alpha=0.15, color=C_CASE)
    ax_a.plot(yearly["citation_year"], yearly["cumulative"],
              marker="o", color=C_CASE, lw=1.5, ms=4, zorder=3)
    for i in range(1, len(yearly)):
        prev, curr = yearly.iloc[i-1]["cumulative"], yearly.iloc[i]["cumulative"]
        gr = ((curr-prev)/prev*100) if prev > 0 else 0
        if gr > 15:
            ax_a.annotate(f"+{gr:.0f}%", xy=(yearly.iloc[i]["citation_year"], curr),
                          xytext=(0,8), textcoords="offset points",
                          ha="center", fontsize=5.5, color=C_CASE, fontweight="bold")
    ax_a.set_xlabel("Year"); ax_a.set_ylabel("Cumulative citations")
    ax_a.set_title("Cumulative growth", fontsize=7.5, fontweight="bold")
    ax_a.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    mu, std = yearly["citation_weight"].mean(), yearly["citation_weight"].std()
    burst_thr = mu + 1.5*std
    bc = [C_CASE if w <= burst_thr else "#e67e22" for w in yearly["citation_weight"]]
    ax_b.bar(yearly["citation_year"], yearly["citation_weight"],
             color=bc, alpha=0.75, edgecolor="white", lw=0.3, width=0.7)
    if len(yearly) > 2 and std > 0:
        ax_b.axhline(burst_thr, color="#e67e22", ls="--", lw=0.8, alpha=0.7,
                     label="Burst (μ+1.5σ)")
        ax_b.axhline(mu, color=C_GREY, ls=":", lw=0.6, alpha=0.6, label="Mean")
        ax_b.legend(fontsize=5.5, frameon=True, framealpha=0.85, edgecolor="#ddd")
    ax_b.set_xlabel("Year"); ax_b.set_ylabel("Internal citations")
    ax_b.set_title("Annual activity", fontsize=7.5, fontweight="bold")
    ax_b.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    fig.suptitle(f"Temporal evolution of largest syndicate  (n = {len(cc)})",
                 fontweight="bold", fontsize=8, y=1.02)
    fig.tight_layout(); _save(fig, "Figure5_TemporalEvolution", figs_dir)


def fig6_feature_importance(
    master: pd.DataFrame, figs_dir: Path
) -> None:
    """
    Figure 6 — Random Forest feature importances for all 13 ML features.
    Normalised burst intensity is not plotted because it is excluded from
    the RF model due to 18% missing data (see paper §3.3).
    """
    cols = [c for c in ML_FEATURES if c in master.columns]
    df = master.dropna(subset=cols + ["tier_type"]).copy()
    X = df[cols].values; y = (df["tier_type"] == "Case").astype(int)
    rf = RandomForestClassifier(n_estimators=300, max_depth=8, random_state=SEED)
    rf.fit(X, y)
    imp = (pd.DataFrame({"col": cols, "imp": rf.feature_importances_})
           .assign(label=lambda d: d["col"].map(FEATURE_NICE).fillna(d["col"]),
                   category=lambda d: d["col"].map(FEATURE_CATEGORY).fillna("Other"))
           .sort_values("imp"))
    tot = imp["imp"].sum(); x_max = imp["imp"].max()
    fig, ax = plt.subplots(figsize=(W2, max(2.8, len(imp)*0.28+0.6)))
    bar_c = [CATEGORY_COLORS.get(cat, C_GREY) for cat in imp["category"]]
    bars = ax.barh(imp["label"], imp["imp"], color=bar_c,
                   edgecolor="white", lw=0.25, height=0.6)
    for bar, val in zip(bars, imp["imp"]):
        pct = val/tot*100; lbl = f"{val:.3f} ({pct:.0f}%)"
        if val > x_max*0.4:
            ax.text(bar.get_width()-0.004, bar.get_y()+bar.get_height()/2,
                    lbl, va="center", ha="right", fontsize=5.5, fontweight="bold", color="white")
        else:
            ax.text(bar.get_width()+0.002, bar.get_y()+bar.get_height()/2,
                    lbl, va="center", ha="left", fontsize=5.5, color="#555", clip_on=False)
    ax.set_xlabel("Mean decrease in impurity"); ax.set_ylabel("")
    ax.set_title("Feature importances — tier classification",
                 loc="left", fontweight="bold", pad=6)
    ax.legend(handles=[mpatches.Patch(fc=CATEGORY_COLORS[c], ec="white", lw=0.3, label=c)
                       for c in ["Cohesion","Structure","Flow","Authority","Diversity"]
                       if c in set(imp["category"])],
              loc="lower right", frameon=True, framealpha=0.85, fontsize=5.5,
              edgecolor="#ddd", title="Feature category", title_fontsize=6)
    fig.tight_layout(); fig.subplots_adjust(right=0.92)
    _save(fig, "Figure6_Feature_Importance", figs_dir)


def fig7_lda_separation(master: pd.DataFrame, figs_dir: Path) -> None:
    """Figure 7 — 1-D LDA projection bimodal KDE with AUC."""
    cols = [c for c in ML_FEATURES if c in master.columns]
    df = master.dropna(subset=cols + ["tier_type"]).copy()
    X = StandardScaler().fit_transform(df[cols].values); y = df["tier_type"].values
    lda = LinearDiscriminantAnalysis(n_components=1)
    df["lda"] = lda.fit_transform(X, y).ravel()
    auc = roc_auc_score((df["tier_type"]=="Case").astype(int), df["lda"])
    q_lo, q_hi = df["lda"].quantile(0.005), df["lda"].quantile(0.995)
    dfc = df[(df["lda"]>=q_lo) & (df["lda"]<=q_hi)]
    fig, ax = plt.subplots(figsize=(W1+0.7, 2.3))
    for tier, col in [("Control", C_CTRL), ("Case", C_CASE)]:
        s = dfc[dfc["tier_type"]==tier]
        sns.kdeplot(data=s, x="lda", fill=True, color=col,
                    alpha=0.25, lw=1.0, bw_adjust=1.3,
                    label={"Control":"Top-Tier (Control)","Case":"Bottom-Tier (Case)"}[tier],
                    ax=ax)
    from matplotlib.transforms import blended_transform_factory as btf
    trans = btf(ax.transData, ax.transAxes)
    for tier, col, ha, y_off in [("Control",C_CTRL,"left",0.95), ("Case",C_CASE,"right",0.82)]:
        med = dfc[dfc["tier_type"]==tier]["lda"].median()
        ax.axvline(med, color=col, lw=0.6, ls=":", alpha=0.7)
        ax.text(med, y_off, f" {med:.2f}", fontsize=5.5, color=col, ha=ha, va="top",
                transform=trans,
                bbox=dict(boxstyle="round,pad=0.08", fc="white", ec="none", alpha=0.7))
    ax.text(0.97, 0.95, f"AUC = {auc:.3f}", transform=ax.transAxes,
            fontsize=6.5, ha="right", va="top", fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.25", fc="white", ec="#ccc", lw=0.4))
    ax.set_xlabel("Linear discriminant score"); ax.set_ylabel("Density")
    ax.set_title("Tier separability — LDA projection",
                 loc="left", fontweight="bold", pad=6)
    ax.legend(frameon=True, fontsize=5.5, framealpha=0.85, edgecolor="#ddd")
    fig.tight_layout(); _save(fig, "Figure7_LDA_Separation", figs_dir)


def fig8_mixing_matrix(
    master: pd.DataFrame, edges: pd.DataFrame, figs_dir: Path
) -> None:
    """Figure 8 — Row-normalised 2×2 citation mixing heatmap."""
    study = set(master["orcid"])
    mask = edges["citing_orcid"].isin(study) & edges["cited_orcid"].isin(study)
    G = nx.from_pandas_edgelist(
        edges[mask], "citing_orcid", "cited_orcid", create_using=nx.DiGraph())
    tier_map = {k:v for k,v in master.set_index("orcid")["tier_type"].items() if k in G}
    nx.set_node_attributes(G, tier_map, "tier")
    r_val = nx.attribute_assortativity_coefficient(G, "tier")
    mix = nx.attribute_mixing_matrix(G, "tier", mapping={"Case":0,"Control":1})
    mix_df = pd.DataFrame(mix, index=["Case","Control"], columns=["Case","Control"])
    mix_pct = mix_df.div(mix_df.sum(axis=1), axis=0)
    pretty = {"Case":"Bottom-Tier","Control":"Top-Tier"}
    mix_pct.index   = [pretty[i] for i in mix_pct.index]
    mix_pct.columns = [pretty[c] for c in mix_pct.columns]
    diag = (mix_pct.iloc[0,0] + mix_pct.iloc[1,1]) / 2
    print(f"    → Tier assortativity r = {r_val:.4f}"
          f"  |  diagonal avg = {diag:.1%}")
    fig, ax = plt.subplots(figsize=(W1+1.2, W1*0.82))
    sns.heatmap(mix_pct, annot=True, fmt=".1%", cmap="Blues", vmin=0, vmax=1,
                linewidths=0.4, linecolor="#eee",
                cbar_kws={"label":"Row-norm. probability","shrink":0.7,"pad":0.12},
                annot_kws={"size":9,"weight":"bold"}, ax=ax)
    ax.set_ylabel("Citing tier"); ax.set_xlabel("Cited tier")
    ax.set_title("Citation mixing matrix", loc="left", fontweight="bold", pad=6, fontsize=7.5)
    ax.text(1.0, 1.03, f"diagonal avg {diag:.0%}",
            transform=ax.transAxes, fontsize=5.5, color="#666", ha="right", style="italic")
    ax.tick_params(axis="x", rotation=0); ax.tick_params(axis="y", rotation=0)
    fig.tight_layout(); _save(fig, "Figure8_Mixing_Matrix", figs_dir)


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 7 — AUTHOR INVESTIGATION                                      ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def _fetch_orcid_name(orcid: str) -> dict:
    url = f"https://pub.orcid.org/v3.0/{orcid}/person"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        nm = data.get("name", {}) or {}
        given  = (nm.get("given-names",  {}) or {}).get("value", "")
        family = (nm.get("family-name",  {}) or {}).get("value", "")
        return {"given": given, "family": family,
                "full_name": f"{given} {family}".strip()}
    except Exception:
        return {"given": "", "family": "", "full_name": ""}


def resolve_names(orcids: list[str], anonymise: bool = False,
                  skip_api: bool = False) -> pd.DataFrame:
    if anonymise:
        return pd.DataFrame([{"orcid": o, "full_name": f"Author_{i+1:03d}"}
                              for i, o in enumerate(orcids)])
    if skip_api:
        return pd.DataFrame([{"orcid": o, "full_name": o} for o in orcids])
    print(f"  Resolving {len(orcids)} names via ORCID public API …")
    rows = []
    for i, oid in enumerate(orcids):
        info = _fetch_orcid_name(oid); info["orcid"] = oid; rows.append(info)
        if (i+1) % 10 == 0: print(f"    {i+1}/{len(orcids)}")
        time.sleep(0.15)
    return pd.DataFrame(rows)


def compute_suspiciousness(master: pd.DataFrame) -> pd.DataFrame:
    """
    Composite suspiciousness score S_i = Σ w_k · z_ik (Eq. 1 in paper).
    Z-scores are computed against the full population (not Control-only)
    so that Case and Control authors are directly comparable.
    StandardScaler is used for consistency with the IF feature pipeline.
    """
    outliers = master[master["is_outlier"]].copy()
    avail = [c for c in SUSP_WEIGHTS if c in master.columns]
    for col in avail:
        mu, sd = master[col].mean(), master[col].std()
        outliers[f"z_{col}"] = (outliers[col] - mu) / (sd or 1.0)
    outliers["suspiciousness_score"] = sum(
        SUSP_WEIGHTS[c] * outliers[f"z_{c}"] for c in avail)
    red_flags = []
    for _, row in outliers.iterrows():
        flags = []
        for c in avail:
            z = row[f"z_{c}"]
            if z > 5: flags.append(f"{c} (>5σ)")
            elif z > 3: flags.append(f"{c} (>3σ)")
        red_flags.append("; ".join(flags) if flags else "—")
    outliers["red_flags"] = red_flags
    outliers = outliers.sort_values("suspiciousness_score", ascending=False)
    outliers["rank"] = range(1, len(outliers)+1)
    return outliers


def audit_publications(orcids: list[str], edges: pd.DataFrame) -> pd.DataFrame:
    con = sqlite3.connect(str(IMPACT_DB))
    records = []
    for oid in orcids:
        rec = {"orcid": oid}
        works = pd.read_sql_query(
            "SELECT w.doi, w.published_year, "
            "COALESCE(w.issn_print, w.issn_electronic) AS issn "
            "FROM works w JOIN work_authors wa ON w.id = wa.work_id "
            "WHERE wa.orcid = ?", con, params=(oid,))
        rec["n_works"] = len(works)
        if not works.empty:
            rec["year_min"] = int(works["published_year"].min())
            rec["year_max"] = int(works["published_year"].max())
            top_issn = works["issn"].dropna().value_counts().head(1).index.tolist()
            if top_issn:
                row = con.execute(
                    "SELECT jn.title FROM journal_names jn "
                    "JOIN journals_issns ji ON jn.id = ji.journal_id "
                    "WHERE ji.issn = ? LIMIT 1", (top_issn[0],)).fetchone()
                rec["top_journal"] = (row[0].strip() if row else top_issn[0])
            else:
                rec["top_journal"] = "—"
        else:
            rec["year_min"] = rec["year_max"] = 0; rec["top_journal"] = "—"
        out_e = edges[edges["citing_orcid"] == oid]
        in_e  = edges[edges["cited_orcid"]  == oid]
        rec["total_outgoing"] = int(out_e["citation_weight"].sum())
        rec["total_incoming"] = int(in_e["citation_weight"].sum())
        rec["n_reciprocal"]   = len(set(out_e["cited_orcid"]) & set(in_e["citing_orcid"]))
        records.append(rec)
    con.close()
    return pd.DataFrame(records)


def find_syndicate_membership(
    master: pd.DataFrame, edges: pd.DataFrame
) -> dict[str, int]:
    ids = set(master[master["is_outlier"]]["orcid"])
    mask = edges["citing_orcid"].isin(ids) & edges["cited_orcid"].isin(ids)
    sub = edges[mask]
    if sub.empty: return {}
    G = nx.from_pandas_edgelist(sub, "citing_orcid", "cited_orcid", create_using=nx.DiGraph())
    return {node: i+1 for i, cc in enumerate(
        sorted(nx.connected_components(G.to_undirected()), key=len, reverse=True))
        for node in cc}


def write_top10_table(top10: pd.DataFrame, audit: pd.DataFrame, path: Path) -> None:
    merged = top10.merge(audit, on="orcid", how="left")
    lines = [
        r"\begin{table*}[t]", r"\centering",
        r"\caption{Top-10 most suspicious authors ranked by composite suspiciousness score.}",
        r"\label{tab:suspicious-top10}", r"\small",
        r"\begin{tabular}{rl l l r r l}", r"\toprule",
        r"Rank & ORCID & Name & Subject & Score & Works & Primary red flags \\",
        r"\midrule",
    ]
    for _, r in merged.iterrows():
        flags = str(r.get("red_flags","—"))[:52] + ("…" if len(str(r.get("red_flags","—")))>52 else "")
        subj = SUBJECT_NICE.get(str(r.get("subject","")), str(r.get("subject","")))
        lines.append(f"  {int(r['rank'])} & \\texttt{{{_esc(str(r['orcid']))}}} & "
                     f"{_esc(str(r.get('full_name','—')))} & {_esc(subj)} & "
                     f"{r['suspiciousness_score']:.1f} & {int(r.get('n_works',0))} & "
                     f"{_esc(flags)} \\\\")
    lines += [r"\bottomrule", r"\end{tabular}", r"\begin{tablenotes}", r"\footnotesize",
              r"\item \textit{Note.} Patterns are \emph{statistically anomalous};"
              r" they do not constitute proof of misconduct.",
              r"\end{tablenotes}", r"\end{table*}"]
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"    ✓  {path}")


def write_audit_profiles(top10: pd.DataFrame, audit: pd.DataFrame,
                          syndicates: dict, path: Path) -> None:
    merged = top10.merge(audit, on="orcid", how="left")
    lines = [
        r"\begin{table*}[t]", r"\centering",
        r"\caption{Publication audit profiles for top-10 suspicious authors.}",
        r"\label{tab:author-audit}", r"\small",
        r"\begin{tabular}{rl rrr rl}", r"\toprule",
        r"Rank & ORCID & Works & Years & Out & In & Recip. & Top journal \\",
        r"\midrule",
    ]
    for _, r in merged.iterrows():
        yrs = f"{int(r.get('year_min',0))}--{int(r.get('year_max',0))}"
        tj  = _esc(str(r.get("top_journal","—")))[:27] + ("…" if len(str(r.get("top_journal","—")))>27 else "")
        lines.append(f"  {int(r['rank'])} & \\texttt{{{_esc(str(r['orcid']))}}} & "
                     f"{int(r.get('n_works',0))} & {yrs} & "
                     f"{int(r.get('total_outgoing',0))} & {int(r.get('total_incoming',0))} & "
                     f"{int(r.get('n_reciprocal',0))} & {tj} \\\\")
    lines += [r"\bottomrule", r"\end{tabular}", r"\begin{tablenotes}", r"\footnotesize",
              r"\item Out = outgoing; In = incoming; Recip. = reciprocal citation pairs.",
              r"\end{tablenotes}", r"\end{table*}"]
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"    ✓  {path}")


def write_full_csv(ranked: pd.DataFrame, audit: pd.DataFrame,
                   syndicates: dict, path: Path) -> None:
    cols = (["rank","orcid","full_name","subject","tier_type",
             "suspiciousness_score","red_flags"]
            + [c for c in ML_FEATURES if c in ranked.columns])
    out = ranked[cols].copy()
    out["syndicate_id"] = out["orcid"].map(syndicates).fillna(0).astype(int)
    if audit is not None and not audit.empty:
        out = out.merge(
            audit[["orcid","n_works","year_min","year_max","top_journal",
                   "total_outgoing","total_incoming","n_reciprocal"]],
            on="orcid", how="left")
    out.to_csv(path, index=False)
    print(f"    ✓  {path}")


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 8 — MAIN ENTRY POINT                                          ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reproduce all results for 'Citation Cliques in Questionable Journals'.")
    parser.add_argument("--skip-api",   action="store_true",
                        help="Skip ORCID API lookups (use ORCID as author name).")
    parser.add_argument("--anonymise",  action="store_true",
                        help="Replace author names with pseudonyms.")
    parser.add_argument("--top-n",      type=int, default=10,
                        help="Number of top suspicious authors to report (default: 10).")
    args = parser.parse_args()

    bar = "=" * 66
    print(bar)
    print("  Citation Cliques in Questionable Journals — analysis pipeline")
    print(f"  Output → {OUT_DIR.resolve()}")
    print(bar)

    _apply_qss_style()
    figs_dir, tables_dir, reports_dir = _ensure_dirs()

    # ── Phase 1: Data ──────────────────────────────────────────────────────
    print(f"\n{'─'*66}\n  Phase 1 — Data loading & feature engineering\n{'─'*66}")
    pairs, master, edges = load_data()

    # ── Phase 2: Outlier detection ─────────────────────────────────────────
    print(f"\n{'─'*66}\n  Phase 2 — Hybrid outlier detection\n{'─'*66}")
    master, clf, X_weighted = detect_outliers(master)

    # ── Phase 3: Statistical analysis ─────────────────────────────────────
    print(f"\n{'─'*66}\n  Phase 3 — Statistical analysis\n{'─'*66}")
    report_statistical_tests(master, pairs, reports_dir)
    report_subject_stratified_stats(master, pairs, reports_dir)
    analyze_syndicate_sensitivity(master, edges, clf, X_weighted, reports_dir)
    master.to_csv(reports_dir / "author_features_final.csv", index=False)
    print(f"    → {reports_dir / 'author_features_final.csv'}")

    # ── Phase 4: Publication figures ───────────────────────────────────────
    print(f"\n{'─'*66}\n  Phase 4 — Publication figures (8 × 2 formats)\n{'─'*66}")
    fig1_forest_plot(master, pairs, figs_dir)
    fig2a_radar_fingerprint(master, figs_dir)
    fig3_permutation_test(master, pairs, figs_dir)
    fig4_syndicate_network(master, edges, figs_dir)
    fig5_temporal_evolution(master, edges, figs_dir)
    fig6_feature_importance(master, figs_dir)
    fig7_lda_separation(master, figs_dir)
    fig8_mixing_matrix(master, edges, figs_dir)

    # ── Phase 5: Author investigation ─────────────────────────────────────
    print(f"\n{'─'*66}\n  Phase 5 — Author investigation\n{'─'*66}")
    ranked    = compute_suspiciousness(master)
    top_n     = ranked.head(args.top_n).copy()
    top_orcids = top_n["orcid"].tolist()
    all_orcids = ranked["orcid"].tolist()

    names_top = resolve_names(top_orcids,  args.anonymise, args.skip_api)
    names_all = resolve_names(all_orcids,  args.anonymise, args.skip_api)
    top_n  = top_n.merge(names_top[["orcid","full_name"]], on="orcid", how="left")
    ranked = ranked.merge(names_all[["orcid","full_name"]], on="orcid", how="left")
    top_n["full_name"]  = top_n["full_name"].fillna(top_n["orcid"])
    ranked["full_name"] = ranked["full_name"].fillna(ranked["orcid"])

    print("  Publication audit …")
    audit     = audit_publications(top_orcids, edges)
    syndicates = find_syndicate_membership(master, edges)

    write_top10_table(top_n, audit, tables_dir / "suspicious_authors_top10.tex")
    write_audit_profiles(top_n, audit, syndicates, tables_dir / "top10_audit.tex")
    write_full_csv(ranked, audit, syndicates, tables_dir / "suspicious_authors_full.csv")

    # ── Summary ────────────────────────────────────────────────────────────
    n_out   = int(master["is_outlier"].sum())
    n_case  = int((master[master["is_outlier"]]["tier_type"] == "Case").sum())
    purity  = n_case / n_out * 100 if n_out else 0.0
    n_syn   = len(set(syndicates.values()))
    largest = max((sum(1 for v in syndicates.values() if v == s)
                   for s in set(syndicates.values())), default=0)
    print(f"\n{bar}")
    print(f"  Summary")
    print(f"  Matched pairs:          {len(pairs):,}")
    print(f"  Total outliers:         {n_out}  ({purity:.1f}% Case purity)")
    print(f"  Syndicates (≥2 members):{n_syn}  (largest: {largest})")
    print(f"  Output: {OUT_DIR.resolve()}")
    print(bar)


if __name__ == "__main__":
    main()
