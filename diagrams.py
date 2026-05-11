#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas", "numpy", "seaborn", "matplotlib",
#     "networkx", "scikit-learn", "scipy",
# ]
# ///
"""
diagrams.py — Six publication-ready figures for a citation-manipulation study.

┌───────────────────────────────────────────────────────────────────┐
│  Target journal : Quantitative Science Studies (QSS), MIT Press  │
│  Output         : analysis_results_v4/figs/ (PNG 300 DPI + PDF)  │
│  Data sources   : rolap.db  (derived tables)                     │
│                   impact.db (raw tables — pairs cross-check)      │
│                                                                   │
│  Phase 1 — Visual design   (see implementation_plan.md)          │
│  Phase 2 — Data extraction (load_data function below)            │
│  Phase 3 — Plotting        (fig* functions below)                │
└───────────────────────────────────────────────────────────────────┘

Figures
-------
 1   Forest Plot         — co-author citation gap by subject field
 2a  Radar Fingerprint   — outlier fold-change profile
 4   Syndicate Network   — largest outlier citation cluster
 8   Feature Importance  — Random Forest discriminant features
 9   LDA Separation      — 1-D discriminant density
 10  Mixing Matrix       — tier citation homophily heatmap
"""

from __future__ import annotations

import sqlite3
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
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import RobustScaler, StandardScaler


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  PHASE 3-A  ·  GLOBAL QSS / MIT PRESS rcParams                        ║
# ╚══════════════════════════════════════════════════════════════════════════╝

# --- Paths & constants ---------------------------------------------------
ROLAP_DB  = Path("rolap.db")
IMPACT_DB = Path("impact.db")          # raw tables (works, refs, authors)
OUT_DIR   = Path("analysis_results_v4") / "figs"
SEED      = 42
CONTAMINATION = 0.01                   # Isolation Forest: ~1 % outliers

np.random.seed(SEED)
warnings.filterwarnings("ignore")

# Colourblind-safe palette (Brewer RdBu)
C_CASE = "#b2182b"          # deep red  — Bottom-Tier / Case / Outlier
C_CTRL = "#2166ac"          # deep blue — Top-Tier / Control / Baseline
C_HUB  = "#f4a582"          # salmon    — network hub accent
C_GIVER = "#e377c2"         # pink      — net giver (sycophant)
C_RECVR = "#17becf"         # cyan      — net receiver (beneficiary)
C_GREY = "#878787"          # neutral

# Semantic category colours for Feature Importance (Fig 8)
FEATURE_CATEGORY = {
    "coauthor_citation_rate": "Cohesion",
    "clique_strength":       "Cohesion",
    "reciprocity_rate":      "Cohesion",
    "clustering":            "Structure",
    "triangles_norm":        "Structure",
    "k_core_number":         "Structure",
    "citation_balance":      "Flow",
    "outgoing_hhi":          "Flow",
    "citation_hhi":          "Flow",
    "eigenvector_centrality": "Authority",
    "citation_entropy":      "Diversity",
    "journal_endogamy_rate": "Diversity",
    "self_citation_rate":    "Diversity",
}
CATEGORY_COLORS = {
    "Cohesion":  "#b2182b",   # red
    "Structure": "#7b2d8b",   # purple
    "Flow":      "#2166ac",   # blue
    "Authority": "#878787",   # grey
    "Diversity": "#2ca02c",   # teal-green
}

# QSS column widths (inches)
W1 = 3.46                   # single column
W2 = 7.09                   # double column

# --- Features for ML figures (13 computed features) -----------------------
ML_FEATURES = [
    "coauthor_citation_rate", "self_citation_rate", "clustering",
    "triangles_norm", "citation_balance", "reciprocity_rate",
    "outgoing_hhi", "clique_strength", "eigenvector_centrality",
    "k_core_number", "citation_entropy", "citation_hhi",
    "journal_endogamy_rate",
]

FEATURE_NICE = {
    "coauthor_citation_rate": "Co-author Cit. Rate",
    "self_citation_rate":     "Self-Citation Rate",
    "clustering":             "Local Clustering",
    "triangles_norm":         "Norm. Triangles",
    "citation_balance":       "Citation Balance",
    "reciprocity_rate":       "Reciprocity Rate",
    "outgoing_hhi":           "Outgoing HHI",
    "clique_strength":        "Clique Strength",
    "eigenvector_centrality": "Eigenvector Centr.",
    "k_core_number":          "K-Core Number",
    "citation_entropy":       "Incoming Entropy",
    "citation_hhi":           "Incoming HHI",
    "journal_endogamy_rate":  "Journal Endogamy",
}

SUBJECT_NICE = {
    "1": "Medicine", "2": "Engineering", "3": "Natural Sciences",
    "4": "Social Sciences", "5": "Arts & Humanities",
}


def _apply_qss_style() -> None:
    """
    One-shot rcParams configuration — strict QSS / MIT Press compliance.

    Rules enforced:
      • Serif font (DejaVu Serif)        • Top/right spines removed
      • 300 DPI PNG + PDF TrueType (42)   • Muted grid
      • 8 pt body, descending hierarchy   • tight bbox
    """
    mpl.rcParams.update({
        # ── Typography ──────────────────────────────────────────────────
        "font.family":           "serif",
        "font.serif":            ["DejaVu Serif", "Times New Roman",
                                  "Times", "serif"],
        "mathtext.fontset":      "dejavuserif",
        "font.size":             8,
        "axes.titlesize":        9,
        "axes.labelsize":        7.5,
        "xtick.labelsize":       6.5,
        "ytick.labelsize":       6.5,
        "legend.fontsize":       6,
        "legend.title_fontsize": 6.5,
        # ── Spines & ticks ──────────────────────────────────────────────
        "axes.spines.top":       False,
        "axes.spines.right":     False,
        "axes.linewidth":        0.5,
        "xtick.major.width":     0.5,
        "ytick.major.width":     0.5,
        "xtick.major.size":      3,
        "ytick.major.size":      3,
        # ── Grid (ultra-muted) ──────────────────────────────────────────
        "axes.grid":             True,
        "grid.linewidth":        0.25,
        "grid.color":            "#e8e8e8",
        "grid.alpha":            0.6,
        # ── Lines & patches ─────────────────────────────────────────────
        "lines.linewidth":       1.0,
        "lines.markersize":      3.5,
        "patch.linewidth":       0.4,
        # ── Output ──────────────────────────────────────────────────────
        "figure.dpi":            150,
        "savefig.dpi":           300,
        "savefig.bbox":          "tight",
        "savefig.pad_inches":    0.03,
        "pdf.fonttype":          42,     # TrueType in PDF
        "ps.fonttype":           42,
        # ── Background ──────────────────────────────────────────────────
        "figure.facecolor":      "white",
        "axes.facecolor":        "white",
    })
    sns.set_theme(style="whitegrid", rc=mpl.rcParams)


_apply_qss_style()
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _save(fig: plt.Figure, name: str) -> None:
    """Save as 300 DPI PNG + PDF (vector, TrueType embedded)."""
    base = OUT_DIR / name
    fig.savefig(base.with_suffix(".png"), dpi=300, facecolor="white")
    fig.savefig(base.with_suffix(".pdf"), facecolor="white")
    plt.close(fig)
    print(f"  ✓  {name}")


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  PHASE 2  ·  DATA EXTRACTION                                          ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def load_data():
    """
    Extract and compute all features needed for the six figures.

    Data flow
    ---------
    rolap.db  →  author_matched_pairs      (9 431 pairs, 5 subjects)
              →  author_behavior_metrics   (self-cit rate, coauthor-cit rate)
              →  citation_anomalies        (asymmetry, velocity, burst)
              →  author_venue_metrics      (journal endogamy)
              →  citation_network_final    (citing→cited edges, weights)

    Graph features are computed live via NetworkX from citation_network_final:
      clustering, triangles, k_core_number, eigenvector_centrality, out/in_strength,
      citation_balance, reciprocity_rate, outgoing_hhi, citation_entropy,
      citation_hhi, clique_strength, triangles_norm.

    Outlier detection via Isolation Forest (contamination = 1 %).

    Returns
    -------
    pairs, master, edges, feat_cols, X_scaled
    """
    con = sqlite3.connect(str(ROLAP_DB))
    print("  Phase 2 — extracting from rolap.db")

    # ── 2-A. Matched pairs + behaviour metrics ─────────────────────────────
    print("    Loading tables …")
    pairs = pd.read_sql_query(
        "SELECT subject, case_orcid, control_orcid "
        "FROM author_matched_pairs", con)
    # -- author_behavior_metrics: per-(orcid, subject) rates
    beh = pd.read_sql_query(
        "SELECT orcid, subject, self_citation_rate, coauthor_citation_rate "
        "FROM author_behavior_metrics", con)
    beh["subject"] = beh["subject"].astype(str)
    # -- citation_anomalies: per-orcid malice signals
    ano = pd.read_sql_query(
        "SELECT orcid, max_asymmetry, avg_velocity, max_burst "
        "FROM citation_anomalies", con)
    # -- author_venue_metrics: journal endogamy
    try:
        venue = pd.read_sql_query(
            "SELECT orcid, journal_endogamy_rate FROM author_venue_metrics",
            con)
    except Exception:
        venue = pd.DataFrame(columns=["orcid", "journal_endogamy_rate"])

    # ── 2-B. Citation network edges (non-self only) ────────────────────────
    print("    Loading citation network …")
    edges = pd.read_sql_query(
        "SELECT citing_orcid, cited_orcid, citation_year, citation_weight "
        "FROM citation_network_final "
        "WHERE is_self = 0 "
        "  AND citing_orcid IS NOT NULL "
        "  AND cited_orcid  IS NOT NULL", con)
    con.close()

    # ── 2-C. Master table: one row per author ──────────────────────────────
    print("    Building master feature table …")
    cases = (pairs[["case_orcid", "subject"]]
             .rename(columns={"case_orcid": "orcid"})
             .assign(tier_type="Case"))
    ctrls = (pairs[["control_orcid", "subject"]]
             .rename(columns={"control_orcid": "orcid"})
             .assign(tier_type="Control"))
    master = pd.concat([cases, ctrls], ignore_index=True)
    master = (master
              .merge(beh,   on=["orcid", "subject"], how="left")
              .merge(ano,   on="orcid",              how="left")
              .merge(venue, on="orcid",              how="left"))
    master["journal_endogamy_rate"] = master["journal_endogamy_rate"].fillna(0)

    pop   = set(master["orcid"].unique())
    feats = pd.DataFrame({"orcid": list(pop)})

    # ── 2-D. Graph-derived features via NetworkX ───────────────────────────
    print("    Computing graph features …")

    # --- Degree & citation balance ---
    out_s = edges.groupby("citing_orcid")["citation_weight"].sum()
    in_s  = edges.groupby("cited_orcid")["citation_weight"].sum()
    feats["out_strength"] = feats["orcid"].map(out_s).fillna(0)
    feats["in_strength"]  = feats["orcid"].map(in_s).fillna(0)
    tot = feats["out_strength"] + feats["in_strength"] + 1e-6
    feats["citation_balance"] = (
        (feats["out_strength"] - feats["in_strength"]) / tot
    )

    # --- Incoming entropy & HHI ---
    print("      entropy & reciprocity …")
    cited_grp = (edges[edges["cited_orcid"].isin(pop)]
                 .groupby("cited_orcid")["citing_orcid"])
    ent_map, hhi_map = {}, {}
    for aid, grp in cited_grp:
        p = grp.value_counts(normalize=True)
        ent_map[aid] = float(sp_entropy(p))
        hhi_map[aid] = float((p ** 2).sum())
    feats["citation_entropy"] = feats["orcid"].map(ent_map).fillna(0)
    feats["citation_hhi"]     = feats["orcid"].map(hhi_map).fillna(0)

    # --- Reciprocity ---
    g_out = edges.groupby("citing_orcid")["cited_orcid"].apply(set).to_dict()
    g_in  = edges.groupby("cited_orcid")["citing_orcid"].apply(set).to_dict()
    recip = {}
    for aid in pop:
        os_ = g_out.get(aid, set()); is_ = g_in.get(aid, set())
        recip[aid] = len(os_ & is_) / max(len(os_), 1)
    feats["reciprocity_rate"] = feats["orcid"].map(recip).fillna(0)

    # --- Outgoing HHI ---
    print("      outgoing concentration …")
    out_hhi = {}
    for aid, grp in edges.groupby("citing_orcid")["cited_orcid"]:
        out_hhi[aid] = float((grp.value_counts(normalize=True) ** 2).sum())
    feats["outgoing_hhi"] = feats["orcid"].map(out_hhi).fillna(0)

    # --- Topology (clustering, triangles, k-core, eigenvector centrality) ---
    print("      graph topology …")
    mask  = edges["citing_orcid"].isin(pop) & edges["cited_orcid"].isin(pop)
    G_dir = nx.from_pandas_edgelist(
        edges[mask], "citing_orcid", "cited_orcid",
        ["citation_weight"], create_using=nx.DiGraph())
    G_und = G_dir.to_undirected()

    feats["clustering"] = feats["orcid"].map(nx.clustering(G_und)).fillna(0)
    feats["triangles"]  = feats["orcid"].map(nx.triangles(G_und)).fillna(0)
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

    # --- Merge & derived columns ---
    master = master.merge(feats, on="orcid", how="left")
    master["coauthor_citation_rate"] = master["coauthor_citation_rate"].fillna(0)
    master["clique_strength"]  = (
        master["clustering"] * master["coauthor_citation_rate"])
    master["triangles_norm"]   = master["triangles"] / (
        master["out_strength"] + master["in_strength"] + 1)
    master = master.fillna(0)

    # ── 2-E. Outlier detection (Isolation Forest, 1 % contamination) ──────
    print("    Fitting Isolation Forest …")
    avail = [f for f in ML_FEATURES if f in master.columns]
    X = master[avail].fillna(0).values
    X_scaled = RobustScaler().fit_transform(X)
    clf = IsolationForest(
        n_estimators=200, contamination=CONTAMINATION, random_state=SEED)
    master["is_outlier"] = clf.fit_predict(X_scaled) == -1
    n_out = master["is_outlier"].sum()
    print(f"    → Outliers detected: {n_out}  "
          f"({n_out / len(master) * 100:.1f}%)")

    return pairs, master, edges, avail, X_scaled



# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  PHASE 3-B  ·  FIGURE FUNCTIONS  (8 figures, a–h)                     ║
# ╚══════════════════════════════════════════════════════════════════════════╝

# ── FIGURE 1 (a) — Forest Plot ───────────────────────────────────────────

def fig1_forest_plot(master: pd.DataFrame, pairs: pd.DataFrame) -> None:
    """Double-column forest plot of mean paired co-author citation gap."""
    print("\n  Figure 1 — Forest Plot")
    m = "coauthor_citation_rate"

    merged = (
        pairs
        .merge(master.query("tier_type == 'Case'")
               [["orcid", "subject", m]]
               .rename(columns={"orcid": "case_orcid", m: "cv"}),
               on=["case_orcid", "subject"])
        .merge(master.query("tier_type == 'Control'")
               [["orcid", "subject", m]]
               .rename(columns={"orcid": "control_orcid", m: "bv"}),
               on=["control_orcid", "subject"]))
    merged["d"] = merged["cv"] - merged["bv"]

    rows = []
    for subj, g in merged.groupby("subject"):
        if len(g) < 10:
            continue
        nice = SUBJECT_NICE.get(str(subj), f"Field {subj}")
        rows.append(dict(label=f"{nice}  (n={len(g):,})",
                         mean=g["d"].mean(), ci=1.96 * g["d"].sem()))
    df = pd.DataFrame(rows).sort_values("mean")
    ov = dict(label="Overall", mean=merged["d"].mean(),
              ci=1.96 * merged["d"].sem())
    df = pd.concat([df, pd.DataFrame([ov])], ignore_index=True)

    fig, ax = plt.subplots(figsize=(W2, max(2.6, len(df) * 0.44 + 0.6)))
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
    x_hi = max((df["mean"] + df["ci"]).max() * 1.3, 0.001)
    ax.axvspan(0, x_hi, alpha=0.04, color=C_CASE, zorder=0)
    ax.axhline(len(df) - 1.5, color="#d0d0d0", lw=0.35, zorder=0)

    ax.set_yticks(y)
    ax.set_yticklabels(df["label"])
    for lbl in ax.get_yticklabels():
        if lbl.get_text() == "Overall":
            lbl.set_fontweight("bold")

    # Right-margin delta labels via blended transform
    from matplotlib.transforms import blended_transform_factory as btf
    trans = btf(ax.transAxes, ax.transData)
    for i, r in df.iterrows():
        is_ov = r["label"] == "Overall"
        c = "#222222" if is_ov else C_CASE
        sig = ""
        if not is_ov:
            sig = " *" if (r["mean"] - r["ci"] > 0
                           or r["mean"] + r["ci"] < 0) else ""
        ax.text(1.01, i, f"\u0394 = {r['mean']:.4f}{sig}",
                transform=trans, va="center", ha="left",
                fontsize=6.5, color=c, clip_on=False)

    ax.set_xlabel(
        "Mean \u0394 co-author citation rate  (Case \u2212 Control)")
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{x:.3f}"))

    ax.set_title("Co-author citation gap by subject field",
                 loc="left", fontweight="bold", pad=6)
    fig.tight_layout()
    fig.subplots_adjust(right=0.82)
    _save(fig, "Figure1_ForestPlot")


# ── FIGURE 2 (b) — Fingerprint Lollipop (log-scale) ─────────────────────

def fig2_fingerprint(master: pd.DataFrame) -> None:
    """Log-scale horizontal lollipop of outlier fold-change vs. normal."""
    print("  Figure 2 — Fingerprint (lollipop, log-scale)")

    METRICS = [
        ("coauthor_citation_rate", "Co-author citation rate"),
        ("clique_strength",        "Clique strength"),
        ("reciprocity_rate",       "Reciprocity rate"),
        ("outgoing_hhi",           "Outgoing HHI"),
        ("self_citation_rate",     "Self-citation rate"),
        ("clustering",             "Local clustering"),
        ("triangles_norm",         "Normalised triangles"),
    ]
    cols = [c for c, _ in METRICS if c in master.columns]
    lbls = [l for c, l in METRICS if c in master.columns]

    out_m = master[master["is_outlier"]][cols].mean()
    nrm_m = master[~master["is_outlier"]][cols].mean()
    fc = ((out_m + 1e-7) / (nrm_m + 1e-7)).values

    order = np.argsort(fc)
    fc = fc[order]
    lbls = [lbls[i] for i in order]

    fig, ax = plt.subplots(figsize=(W1 + 0.5, max(2.4, len(cols) * 0.38)))
    y_pos = np.arange(len(cols))

    # Colour gradient by fold-change magnitude
    norm = mpl.colors.LogNorm(vmin=max(fc.min(), 1.01), vmax=fc.max())
    cmap = mpl.colormaps["Reds"]
    stem_colors = [cmap(norm(v)) for v in fc]

    for i, (v, sc) in enumerate(zip(fc, stem_colors)):
        ax.hlines(i, 1.0, v, color=sc, alpha=0.7, linewidth=2.0)
    ax.scatter(fc, y_pos, color=[cmap(norm(v)) for v in fc],
               s=50, zorder=3, edgecolors="white", linewidths=0.4)

    ax.axvline(1.0, color=C_GREY, lw=0.7, ls="--", alpha=0.6)
    ax.text(1.0, len(cols) - 0.3, "1\u00d7", fontsize=5.5,
            color=C_GREY, ha="center", va="bottom")

    # Labels via annotate with pixel offset
    for i, v in enumerate(fc):
        ax.annotate(f"{v:.1f}\u00d7", xy=(v, i),
                    xytext=(8, 0), textcoords="offset points",
                    va="center", ha="left",
                    fontsize=6, fontweight="bold", color="#333")

    ax.set_xscale("log")
    ax.xaxis.set_major_formatter(mticker.ScalarFormatter())
    ax.xaxis.get_major_formatter().set_scientific(False)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(lbls)
    ax.set_xlabel("Fold-change vs. normal baseline (log scale)")

    fig.tight_layout()
    _save(fig, "Figure2_Fingerprint")


# ── FIGURE 2a — Radar Fingerprint (polar chart) ─────────────────────────

def fig2a_radar_fingerprint(master: pd.DataFrame) -> None:
    """Polar radar chart of outlier fold-change vs. normal baseline (log-scale)."""
    print("  Figure 2a — Radar Fingerprint (polar, log-scale)")

    METRICS = [
        ("coauthor_citation_rate", "Co-author\ncitation rate"),
        ("clique_strength",        "Clique\nstrength"),
        ("reciprocity_rate",       "Reciprocity\nrate"),
        ("outgoing_hhi",           "Outgoing\nHHI"),
        ("self_citation_rate",     "Self-citation\nrate"),
        ("clustering",             "Local\nclustering"),
    ]
    cols = [c for c, _ in METRICS if c in master.columns]
    lbls = [l for c, l in METRICS if c in master.columns]
    N = len(cols)
    if N == 0:
        print("    \u26a0  no metrics available \u2014 skipping")
        return

    # Fold-change: outlier mean / normal mean
    out_m = master[master["is_outlier"]][cols].mean()
    nrm_m = master[~master["is_outlier"]][cols].mean()
    fc = ((out_m + 1e-7) / (nrm_m + 1e-7)).values

    # Log-transform for visual balance (plot in log-space, annotate linear)
    log_fc = np.log10(fc)

    # Compute angles (evenly spaced) and close the polygon
    angles = np.linspace(0, 2 * pi, N, endpoint=False).tolist()
    log_fc_closed = np.concatenate([log_fc, [log_fc[0]]])
    angles_closed = angles + [angles[0]]

    fig, ax = plt.subplots(
        figsize=(W1 + 1.0, W1 + 1.0),
        subplot_kw={"projection": "polar"},
    )

    # Plot filled polygon (log-space)
    ax.plot(angles_closed, log_fc_closed,
            color=C_CASE, linewidth=1.8, linestyle="-", zorder=3)
    ax.fill(angles_closed, log_fc_closed,
            color=C_CASE, alpha=0.15, zorder=2)

    # Dashed baseline ring at 1x (log10(1) = 0)
    baseline_angles = np.linspace(0, 2 * pi, 200)
    ax.plot(baseline_angles, np.zeros(200),
            color=C_GREY, linewidth=0.9, linestyle="--", alpha=0.7,
            zorder=1)
    # 10x reference ring
    ax.plot(baseline_angles, np.ones(200),
            color="#d0d0d0", linewidth=0.5, linestyle=":", alpha=0.5,
            zorder=1)
    # 100x reference ring
    ax.plot(baseline_angles, np.full(200, 2.0),
            color="#d0d0d0", linewidth=0.5, linestyle=":", alpha=0.5,
            zorder=1)

    # Radial limits: from slightly below 0 to above max
    ax.set_rlim(-0.15, max(log_fc) * 1.15)

    # Radial tick labels in linear-scale notation
    r_ticks = [0, 1, 2]
    r_labels = ["1\u00d7", "10\u00d7", "100\u00d7"]
    if max(log_fc) > 2.0:
        r_ticks.append(np.ceil(max(log_fc)))
        r_labels.append(f"{10**np.ceil(max(log_fc)):.0f}\u00d7")
    ax.set_rticks(r_ticks)
    ax.set_yticklabels(r_labels, fontsize=5.5, color=C_GREY)
    ax.set_rlabel_position(60)

    # Spoke labels with padding
    ax.set_xticks(angles)
    ax.set_xticklabels(lbls, fontsize=6.5)
    ax.tick_params(axis="x", pad=12)

    # Grid styling
    ax.yaxis.grid(True, linewidth=0.3, color="#d8d8d8")
    ax.xaxis.grid(True, linewidth=0.3, color="#d8d8d8")
    ax.spines["polar"].set_visible(False)

    # Annotate each spoke with the actual fold-change value
    for i, (angle, lfc, value) in enumerate(zip(angles, log_fc, fc)):
        # Place annotation radially outward from data point
        # Adaptive alignment based on angle quadrant
        cos_a, sin_a = np.cos(angle), np.sin(angle)
        if abs(cos_a) < 0.3:
            ha = "center"
        elif cos_a > 0:
            ha = "left"
        else:
            ha = "right"
        va = "bottom" if sin_a > -0.3 else "top"

        ax.annotate(
            f"{value:.1f}\u00d7",
            xy=(angle, lfc),
            xytext=(6 * cos_a + 2, 6 * sin_a + 2),
            textcoords="offset points",
            fontsize=6.5, fontweight="bold", color="#333",
            ha=ha, va=va, zorder=5,
        )

    fig.tight_layout()
    _save(fig, "Figure2a_Fingerprint")


# ── FIGURE 3 (c) — Permutation Test ─────────────────────────────────────

def fig3_permutation_test(master: pd.DataFrame,
                          pairs: pd.DataFrame) -> None:
    """Null-distribution histogram for co-author citation rate gap."""
    print("  Figure 3 — Permutation Test")
    B = 10_000
    m = "coauthor_citation_rate"

    p = (pairs
         .merge(master[["orcid", "subject", m]]
                .rename(columns={"orcid": "case_orcid", m: "case_val"}),
                on=["case_orcid", "subject"])
         .merge(master[["orcid", "subject", m]]
                .rename(columns={"orcid": "control_orcid", m: "ctrl_val"}),
                on=["control_orcid", "subject"])
         .dropna())

    obs_diff = (p["case_val"] - p["ctrl_val"]).mean()
    values = np.concatenate([p["case_val"].values, p["ctrl_val"].values])
    n = len(p)

    rng = np.random.default_rng(SEED)
    null_diffs = np.empty(B)
    for i in range(B):
        rng.shuffle(values)
        null_diffs[i] = values[:n].mean() - values[n:].mean()

    p_val = (np.sum(null_diffs >= obs_diff) + 1) / (B + 1)

    fig, ax = plt.subplots(figsize=(W1 + 0.5, 2.4))
    ax.hist(null_diffs, bins=50, color="#cccccc", edgecolor="white",
            linewidth=0.3, density=True, zorder=1, label="Null distribution")
    sns.kdeplot(null_diffs, color="#888888", lw=1.0, ax=ax, zorder=2)

    ax.axvline(obs_diff, color=C_CASE, lw=1.2, ls="--", zorder=3,
               label=f"Observed \u0394 = {obs_diff:.4f}")

    p_str = "p < 0.0001" if p_val < 0.0001 else f"p = {p_val:.4f}"
    ax.text(0.97, 0.95,
            f"Observed \u0394 = {obs_diff:.4f}\n{p_str}\nB = {B:,}",
            transform=ax.transAxes, fontsize=6.5,
            ha="right", va="top",
            bbox=dict(boxstyle="round,pad=0.3", fc="white",
                      ec="#ccc", lw=0.4, alpha=0.9))

    ax.set_xlabel("Permuted mean \u0394 co-author citation rate")
    ax.set_ylabel("Density")
    ax.set_title("Permutation test \u2014 co-author citation rate",
                 loc="left", fontweight="bold", pad=6)
    ax.legend(fontsize=6, frameon=True, framealpha=0.85,
              edgecolor="#ddd", loc="upper left")

    fig.tight_layout()
    _save(fig, "Figure3_PermutationTest")


# ── FIGURE 4 (d) — Syndicate Network ────────────────────────────────────

def fig4_syndicate_network(
    master: pd.DataFrame, edges: pd.DataFrame
) -> None:
    """Largest connected component among outliers."""
    print("  Figure 4 — Syndicate Network")

    outlier_ids = set(master[master["is_outlier"]]["orcid"])
    G = None
    is_synthetic = False

    mask = (edges["citing_orcid"].isin(outlier_ids)
            & edges["cited_orcid"].isin(outlier_ids))
    sub = edges[mask]
    if not sub.empty:
        Gfull = nx.from_pandas_edgelist(
            sub, "citing_orcid", "cited_orcid", ["citation_weight"],
            create_using=nx.DiGraph())
        ccs = sorted(nx.connected_components(Gfull.to_undirected()),
                     key=len, reverse=True)
        if ccs and len(ccs[0]) >= 5:
            G = Gfull.subgraph(ccs[0]).copy()
            print(f"    \u2192 real syndicate: {G.number_of_nodes()} members")

    if G is None:
        is_synthetic = True
        print("    \u2192 synthetic 22-node syndicate (illustrative)")
        rng = np.random.default_rng(SEED)
        N_s = 22
        G = nx.DiGraph()
        nodes = [f"S{i:02d}" for i in range(N_s)]
        G.add_nodes_from(nodes)
        for i in range(N_s):
            G.add_edge(nodes[i], nodes[(i + 1) % N_s],
                       citation_weight=int(rng.integers(3, 10)))
            G.add_edge(nodes[(i + 1) % N_s], nodes[i],
                       citation_weight=int(rng.integers(1, 6)))
        for _ in range(int(N_s * N_s * 0.28)):
            u, v = int(rng.integers(0, N_s)), int(rng.integers(0, N_s))
            if u != v:
                w = int(rng.integers(1, 7))
                if G.has_edge(nodes[u], nodes[v]):
                    G[nodes[u]][nodes[v]]["citation_weight"] += w
                else:
                    G.add_edge(nodes[u], nodes[v], citation_weight=w)

    nn = G.number_of_nodes()
    Gu = G.to_undirected() if G.is_directed() else G

    pos = nx.spring_layout(Gu, seed=SEED,
                           k=2.8 / (nn ** 0.5), iterations=200)

    bet = nx.betweenness_centrality(Gu)
    hub = max(bet, key=bet.get)
    bet_vals = np.array([bet.get(n, 0) for n in Gu.nodes()])
    sizes = 80 + np.cbrt(bet_vals / (bet_vals.max() + 1e-9)) * 500

    in_deg = dict(G.in_degree()) if G.is_directed() else {}
    out_deg = dict(G.out_degree()) if G.is_directed() else {}
    colors = []
    for n in Gu.nodes():
        if n == hub:
            colors.append(C_HUB)
        elif out_deg.get(n, 0) > in_deg.get(n, 0):
            colors.append(C_GIVER)
        else:
            colors.append(C_RECVR)

    wts = [Gu[u][v].get("citation_weight", 1) for u, v in Gu.edges()]
    wmax = max(wts) if wts else 1
    ew = [0.3 + (w / wmax) * 1.8 for w in wts]

    fig, ax = plt.subplots(figsize=(W2, W2 * 0.62))
    nx.draw_networkx_edges(Gu, pos, width=ew, alpha=0.35,
                           edge_color="#999999", ax=ax)
    nx.draw_networkx_nodes(Gu, pos, node_size=sizes, node_color=colors,
                           edgecolors="#444444", linewidths=0.35,
                           alpha=0.88, ax=ax)

    ax.annotate("Hub", xy=pos[hub], fontsize=6, fontweight="bold",
                ha="center", va="bottom",
                xytext=(0, 7), textcoords="offset points",
                arrowprops=dict(arrowstyle="-", lw=0.25, color="#777"),
                color="#333")
    ax.axis("off")

    ne = Gu.number_of_edges()
    dens = nx.density(Gu)
    ax.text(0.02, 0.02,
            f"n = {nn}  \u00b7  edges = {ne}  \u00b7  density = {dens:.2f}",
            transform=ax.transAxes, fontsize=5.5, color="#888",
            va="bottom", ha="left")

    lg = [
        mpatches.Patch(fc=C_HUB, ec="#444", lw=0.3,
                       label="Hub (max betweenness)"),
        mpatches.Patch(fc=C_GIVER, ec="#444", lw=0.3,
                       label="Net giver"),
        mpatches.Patch(fc=C_RECVR, ec="#444", lw=0.3,
                       label="Net receiver"),
    ]
    ax.legend(handles=lg, loc="upper left", frameon=True,
              framealpha=0.85, fontsize=5.5, edgecolor="#ddd")

    if is_synthetic:
        ax.text(0.5, 0.5, "Illustrative example \u2014 synthetic data",
                transform=ax.transAxes, fontsize=8, color=C_CASE,
                alpha=0.35, ha="center", va="center",
                fontstyle="italic", rotation=20)

    ax.set_title(
        f"Largest outlier syndicate  (n = {nn})",
        loc="left", fontweight="bold", fontsize=8)
    fig.tight_layout()
    _save(fig, "Figure4_Network")


# ── FIGURE 5 (e) — Temporal Syndicate Evolution ─────────────────────────

def fig5_temporal_evolution(
    master: pd.DataFrame, edges: pd.DataFrame
) -> None:
    """Two-panel temporal analysis of the largest outlier syndicate."""
    print("  Figure 5 — Temporal Syndicate Evolution")

    outlier_ids = set(master[master["is_outlier"]]["orcid"])
    mask = (edges["citing_orcid"].isin(outlier_ids)
            & edges["cited_orcid"].isin(outlier_ids))
    syn_edges = edges[mask]
    if syn_edges.empty or "citation_year" not in syn_edges.columns:
        print("    \u2192 skipped (no temporal data)")
        return

    Gfull = nx.from_pandas_edgelist(
        syn_edges, "citing_orcid", "cited_orcid", ["citation_weight"],
        create_using=nx.DiGraph())
    ccs = sorted(nx.connected_components(Gfull.to_undirected()),
                 key=len, reverse=True)
    if not ccs or len(ccs[0]) < 5:
        print("    \u2192 skipped (syndicate too small)")
        return

    largest_cc = ccs[0]
    syn_timeline = syn_edges[
        syn_edges["citing_orcid"].isin(largest_cc)
        & syn_edges["cited_orcid"].isin(largest_cc)]

    yearly = (syn_timeline.groupby("citation_year")["citation_weight"]
              .sum().reset_index().sort_values("citation_year"))
    yearly["cumulative"] = yearly["citation_weight"].cumsum()

    if len(yearly) < 2:
        print("    \u2192 skipped (only 1 year of data)")
        return

    print(f"    \u2192 {len(largest_cc)} members, "
          f"{len(yearly)} years of data")

    fig, (ax_a, ax_b) = plt.subplots(1, 2, figsize=(W2, 2.6))

    # Panel A: Cumulative internal citations
    ax_a.fill_between(yearly["citation_year"], 0, yearly["cumulative"],
                      alpha=0.15, color=C_CASE)
    ax_a.plot(yearly["citation_year"], yearly["cumulative"],
              marker="o", color=C_CASE, lw=1.5, ms=4, zorder=3)

    for i in range(1, len(yearly)):
        prev = yearly.iloc[i - 1]["cumulative"]
        curr = yearly.iloc[i]["cumulative"]
        growth = ((curr - prev) / prev * 100) if prev > 0 else 0
        if growth > 15:
            ax_a.annotate(f"+{growth:.0f}%",
                          xy=(yearly.iloc[i]["citation_year"], curr),
                          xytext=(0, 8), textcoords="offset points",
                          ha="center", fontsize=5.5, color=C_CASE,
                          fontweight="bold")

    ax_a.set_xlabel("Year")
    ax_a.set_ylabel("Cumulative citations")
    ax_a.set_title("Cumulative growth", fontsize=7.5, fontweight="bold")
    ax_a.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))

    # Panel B: Annual activity with burst threshold
    mean_act = yearly["citation_weight"].mean()
    std_act = yearly["citation_weight"].std()
    burst_thr = mean_act + 1.5 * std_act

    bar_colors = [C_CASE if w <= burst_thr else "#e67e22"
                  for w in yearly["citation_weight"]]
    ax_b.bar(yearly["citation_year"], yearly["citation_weight"],
             color=bar_colors, alpha=0.75, edgecolor="white",
             linewidth=0.3, width=0.7)

    if len(yearly) > 2 and std_act > 0:
        ax_b.axhline(burst_thr, color="#e67e22", ls="--", lw=0.8,
                     alpha=0.7, label=f"Burst (\u03bc+1.5\u03c3)")
        ax_b.axhline(mean_act, color=C_GREY, ls=":", lw=0.6,
                     alpha=0.6, label="Mean")
        ax_b.legend(fontsize=5.5, frameon=True, framealpha=0.85,
                    edgecolor="#ddd", loc="upper left")

    ax_b.set_xlabel("Year")
    ax_b.set_ylabel("Internal citations")
    ax_b.set_title("Annual activity", fontsize=7.5, fontweight="bold")
    ax_b.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))

    fig.suptitle(
        f"Temporal evolution of largest syndicate  "
        f"(n = {len(largest_cc)})",
        fontweight="bold", fontsize=8, y=1.02)
    fig.tight_layout()
    _save(fig, "Figure5_TemporalEvolution")


# ── FIGURE 6 (f) — Feature Importance ───────────────────────────────────

def fig6_feature_importance(
    master: pd.DataFrame, feat_cols: list[str]
) -> None:
    """Horizontal bar chart with semantic category colouring."""
    print("  Figure 6 — Feature Importance")

    cols = [c for c in feat_cols if c in master.columns]
    df = master.dropna(subset=cols + ["tier_type"]).copy()
    X = df[cols].values
    y = (df["tier_type"] == "Case").astype(int)

    rf = RandomForestClassifier(
        n_estimators=300, max_depth=8, random_state=SEED)
    rf.fit(X, y)

    imp = (pd.DataFrame({"col": cols, "imp": rf.feature_importances_})
           .assign(label=lambda d: d["col"].map(FEATURE_NICE)
                                             .fillna(d["col"]),
                   category=lambda d: d["col"].map(FEATURE_CATEGORY)
                                               .fillna("Other"))
           .sort_values("imp"))

    tot = imp["imp"].sum()
    x_max = imp["imp"].max()

    fig, ax = plt.subplots(
        figsize=(W2, max(2.8, len(imp) * 0.28 + 0.6)))
    bar_c = [CATEGORY_COLORS.get(cat, C_GREY) for cat in imp["category"]]
    bars = ax.barh(imp["label"], imp["imp"], color=bar_c,
                   edgecolor="white", linewidth=0.25, height=0.6)

    for bar, val in zip(bars, imp["imp"]):
        pct = val / tot * 100
        lbl = f"{val:.3f} ({pct:.0f}%)"
        if val > x_max * 0.4:
            ax.text(bar.get_width() - 0.004,
                    bar.get_y() + bar.get_height() / 2,
                    lbl, va="center", ha="right",
                    fontsize=5.5, fontweight="bold", color="white")
        else:
            ax.text(bar.get_width() + 0.002,
                    bar.get_y() + bar.get_height() / 2,
                    lbl, va="center", ha="left",
                    fontsize=5.5, color="#555", clip_on=False)

    ax.set_xlabel("Mean decrease in impurity", fontsize=6)
    ax.set_ylabel("")
    ax.set_title("Feature importances \u2014 tier classification",
                 loc="left", fontweight="bold", pad=6)

    cat_handles = [
        mpatches.Patch(fc=CATEGORY_COLORS[c], ec="white", lw=0.3,
                       label=c)
        for c in ["Cohesion", "Structure", "Flow", "Authority", "Diversity"]
        if c in set(imp["category"])
    ]
    ax.legend(handles=cat_handles, loc="lower right", frameon=True,
              framealpha=0.85, fontsize=5.5, edgecolor="#ddd",
              title="Feature category", title_fontsize=6)

    fig.tight_layout()
    fig.subplots_adjust(right=0.92)
    _save(fig, "Figure6_Feature_Importance")


# ── FIGURE 7 (g) — LDA Separation ───────────────────────────────────────

def fig7_lda_separation(
    master: pd.DataFrame, feat_cols: list[str]
) -> None:
    """Bimodal KDE of 1-D LDA projection with AUC."""
    print("  Figure 7 — LDA Separation")

    cols = [c for c in feat_cols if c in master.columns]
    df = master.dropna(subset=cols + ["tier_type"]).copy()

    X = StandardScaler().fit_transform(df[cols].values)
    y = df["tier_type"].values
    lda = LinearDiscriminantAnalysis(n_components=1)
    df["lda"] = lda.fit_transform(X, y).ravel()

    y_bin = (df["tier_type"] == "Case").astype(int)
    auc = roc_auc_score(y_bin, df["lda"])

    q_lo, q_hi = df["lda"].quantile(0.005), df["lda"].quantile(0.995)
    dfc = df[(df["lda"] >= q_lo) & (df["lda"] <= q_hi)]

    fig, ax = plt.subplots(figsize=(W1 + 0.7, 2.3))

    tier_lbl = {"Control": "Top-Tier (Control)",
                "Case": "Bottom-Tier (Case)"}
    for tier, col in [("Control", C_CTRL), ("Case", C_CASE)]:
        s = dfc[dfc["tier_type"] == tier]
        sns.kdeplot(data=s, x="lda", fill=True, color=col,
                    alpha=0.25, lw=1.0, bw_adjust=1.3,
                    label=tier_lbl[tier], ax=ax)

    # Staggered median labels
    from matplotlib.transforms import blended_transform_factory as btf
    trans_med = btf(ax.transData, ax.transAxes)
    med_y = {"Control": 0.95, "Case": 0.82}
    for tier, col, ha in [("Control", C_CTRL, "left"),
                          ("Case", C_CASE, "right")]:
        med = dfc[dfc["tier_type"] == tier]["lda"].median()
        ax.axvline(med, color=col, lw=0.6, ls=":", alpha=0.7)
        ax.text(med, med_y[tier], f" {med:.2f}",
                fontsize=5.5, color=col, ha=ha, va="top",
                transform=trans_med,
                bbox=dict(boxstyle="round,pad=0.08", fc="white",
                          ec="none", alpha=0.7))

    ax.text(0.97, 0.95, f"AUC = {auc:.3f}",
            transform=ax.transAxes, fontsize=6.5,
            ha="right", va="top", fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.25", fc="white",
                      ec="#ccc", lw=0.4, alpha=0.9))

    ax.set_xlabel("Linear discriminant score", fontsize=6)
    ax.set_ylabel("Density", fontsize=6)
    ax.set_title("Tier separability \u2014 LDA projection",
                 loc="left", fontweight="bold", pad=6)

    ax.legend(frameon=True, fontsize=5.5, framealpha=0.85,
              edgecolor="#ddd")
    fig.tight_layout()
    _save(fig, "Figure7_LDA_Separation")


# ── FIGURE 8 (h) — Mixing Matrix ────────────────────────────────────────

def fig8_mixing_matrix(
    master: pd.DataFrame, edges: pd.DataFrame
) -> None:
    """Row-normalised 2x2 citation mixing heatmap."""
    print("  Figure 8 — Mixing Matrix")

    study = set(master["orcid"])
    mask = (edges["citing_orcid"].isin(study)
            & edges["cited_orcid"].isin(study))
    G = nx.from_pandas_edgelist(
        edges[mask], "citing_orcid", "cited_orcid",
        create_using=nx.DiGraph())
    tier_map = {k: v for k, v in
                master.set_index("orcid")["tier_type"].items()
                if k in G}
    nx.set_node_attributes(G, tier_map, "tier")

    r_val = nx.attribute_assortativity_coefficient(G, "tier")
    mix = nx.attribute_mixing_matrix(
        G, "tier", mapping={"Case": 0, "Control": 1})
    mix_df = pd.DataFrame(mix, index=["Case", "Control"],
                          columns=["Case", "Control"])
    mix_pct = mix_df.div(mix_df.sum(axis=1), axis=0)

    pretty = {"Case": "Bottom-Tier", "Control": "Top-Tier"}
    mix_pct.index = [pretty[i] for i in mix_pct.index]
    mix_pct.columns = [pretty[c] for c in mix_pct.columns]

    interp = ("strong" if r_val > 0.5 else
              "moderate" if r_val > 0.2 else "weak") + " homophily"
    diag = (mix_pct.iloc[0, 0] + mix_pct.iloc[1, 1]) / 2

    print(f"    \u2192 Tier assortativity r = {r_val:.4f}")

    fig, ax = plt.subplots(figsize=(W1 + 1.2, W1 * 0.82))
    sns.heatmap(
        mix_pct, annot=True, fmt=".1%",
        cmap="Blues", vmin=0, vmax=1,
        linewidths=0.4, linecolor="#eee",
        cbar_kws={"label": "Row-norm. probability", "shrink": 0.7,
                  "pad": 0.12},
        annot_kws={"size": 9, "weight": "bold"},
        ax=ax)

    ax.set_ylabel("Citing tier")
    ax.set_xlabel("Cited tier")
    ax.set_title(
        f"Citation mixing matrix  ($r = {r_val:.2f}$, {interp})",
        loc="center", fontweight="bold", pad=8, fontsize=7.5,
        wrap=True)
    ax.tick_params(axis="x", rotation=0)
    ax.tick_params(axis="y", rotation=0)

    fig.tight_layout()
    _save(fig, "Figure8_Mixing_Matrix")
    _save(fig, "Figure10_Mixing_Matrix")


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  MAIN                                                                  ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def main() -> None:
    bar = "=" * 62
    print(bar)
    print("  Citation Manipulation \u2014 QSS Figure Pipeline  (8 figures)")
    print(f"  Output \u2192 {OUT_DIR.resolve()}")
    print(bar)

    pairs, master, edges, feat_cols, X_scaled = load_data()

    print("\n  Phase 3 \u2014 generating figures")
    fig1_forest_plot(master, pairs)
    fig2_fingerprint(master)
    fig2a_radar_fingerprint(master)
    fig3_permutation_test(master, pairs)
    fig4_syndicate_network(master, edges)
    fig5_temporal_evolution(master, edges)
    fig6_feature_importance(master, feat_cols)
    fig7_lda_separation(master, feat_cols)
    fig8_mixing_matrix(master, edges)

    print(f"\n{bar}")
    print(f"  Done \u2014 8 figures \u00d7 2 formats = 16 files")
    print(f"  {OUT_DIR.resolve()}")
    print(bar)


if __name__ == "__main__":
    main()
