#!/usr/bin/env python3
"""Offline, subject-keyed analysis for the matched citation-network study.

The script consumes only tables in a local SQLite database and writes every
reported number, table, and figure to one versioned output directory.  It is
also importable: the numerical routines are deliberately small so that the
definitions used by the manuscript can be tested directly.
"""

from __future__ import annotations

import argparse
import hashlib
from itertools import combinations
import json
import math
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
from scipy import sparse
from scipy.stats import binomtest, fisher_exact, rankdata, spearmanr, wilcoxon
from sklearn.ensemble import IsolationForest


ANALYSIS_VERSION = "revision-v1"
DEFAULT_DATABASE = Path("rolap.db")
DEFAULT_OUTPUT_DIRECTORY = Path("results") / ANALYSIS_VERSION
DEFAULT_SEED = 42
MATCHING_H5_CALIPER = 3
YEARS = tuple(range(2020, 2025))
CLIQUE_MIN_SIZES = (3, 4, 5)
CLIQUE_RECIPROCITY_THRESHOLDS = (0.25, 0.50, 0.75)
CLIQUE_DENSITY_THRESHOLD = 0.75

PRIMARY_METRICS = (
    "coauthor_citation_rate",
    "reciprocity",
    "local_clustering",
    "outgoing_hhi",
)
SECONDARY_METRICS = (
    "self_citation_rate",
    "journal_endogamy",
    "within_sample_citation_balance",
    "annual_dyadic_surge_share",
)
EIGHT_METRICS = PRIMARY_METRICS + SECONDARY_METRICS
DETECTOR_FEATURES = (
    "self_citation_rate",
    "coauthor_citation_rate",
    "reciprocity",
    "local_clustering",
    "outgoing_hhi",
)
COHESION_FEATURES = (
    "coauthor_citation_rate",
    "reciprocity",
    "local_clustering",
    "outgoing_hhi",
)
METRIC_LABELS = {
    "coauthor_citation_rate": "Coauthor-citation rate",
    "self_citation_rate": "Self-citation rate",
    "reciprocity": "Reciprocity",
    "local_clustering": "Local clustering",
    "outgoing_hhi": "Outgoing HHI",
    "journal_endogamy": "Journal endogamy",
    "within_sample_citation_balance": "Within-sample citation balance",
    "annual_dyadic_surge_share": "Maximum annual dyadic surge share",
    "coauthor_citation_rate_same_year": "Coauthor-citation rate (same-year included)",
}


def _progress(message: str, *, started_at: float) -> None:
    elapsed_minutes = max(0.0, time.monotonic() - started_at) / 60
    print(f"[{elapsed_minutes:7.1f} min] {message}", flush=True)
PAIRED_EFFECT_XLABEL = "Median paired difference (Case minus Control)"
TIER_ORDER = ("Case", "Control")


class SchemaError(RuntimeError):
    """Raised when a stale database cannot support the revised analysis."""


@dataclass(frozen=True)
class AnalysisData:
    pairs: pd.DataFrame
    membership: pd.DataFrame
    features: pd.DataFrame
    edges: pd.DataFrame


@dataclass(frozen=True)
class ComponentResults:
    summary: pd.DataFrame
    nodes: pd.DataFrame
    dyads: pd.DataFrame


@dataclass(frozen=True)
class MixingResults:
    matrix: pd.DataFrame
    same_tier_share: float
    assortativity: float
    permutation_p: float
    null_same_tier_share: np.ndarray


@dataclass(frozen=True)
class CliqueResults:
    summary: pd.DataFrame
    sensitivity: pd.DataFrame
    membership: pd.DataFrame
    subject_consistency: pd.DataFrame


def _require_columns(frame: pd.DataFrame, columns: Iterable[str], table: str) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise SchemaError(
            f"{table} is stale: missing required column(s) {', '.join(missing)}"
        )


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def _read_table(connection: sqlite3.Connection, table: str) -> pd.DataFrame:
    if not _table_exists(connection, table):
        raise SchemaError(f"required table {table!r} is missing")
    return pd.read_sql_query(f'SELECT * FROM "{table}"', connection)


def _normalise_keys(frame: pd.DataFrame, *, orcid_columns: Sequence[str]) -> pd.DataFrame:
    result = frame.copy()
    if "subject" in result:
        result["subject"] = (
            result["subject"]
            .astype("string")
            .str.strip()
            .str.replace(r"^([+-]?\d+)\.0+$", r"\1", regex=True)
        )
    for column in orcid_columns:
        if column in result:
            result[column] = result[column].astype("string").str.strip()
    return result


def _assert_unique(frame: pd.DataFrame, keys: Sequence[str], name: str) -> None:
    duplicates = frame.duplicated(list(keys), keep=False)
    if duplicates.any():
        example = frame.loc[duplicates, list(keys)].head(3).to_dict("records")
        raise SchemaError(f"{name} is not unique on {tuple(keys)}; examples: {example}")


def weighted_hhi(weights: Iterable[float]) -> float:
    """Herfindahl concentration for nonnegative fractional weights.

    A zero denominator is unavailable rather than a structural zero.
    """

    values = np.asarray(list(weights), dtype=float)
    values = values[np.isfinite(values)]
    if values.size == 0 or np.any(values < 0):
        return math.nan
    total = float(values.sum())
    if total <= 0:
        return math.nan
    shares = values / total
    return float(np.square(shares).sum())


def aggregate_cumulative_dyads(edges: pd.DataFrame) -> pd.DataFrame:
    """Sum annual fractional weights to one row per subject-directed dyad."""

    _require_columns(
        edges,
        ("subject", "citing_orcid", "cited_orcid", "citation_weight"),
        "edges",
    )
    if edges.empty:
        return pd.DataFrame(
            columns=["subject", "citing_orcid", "cited_orcid", "citation_weight"]
        )
    clean = edges.dropna(subset=["subject", "citing_orcid", "cited_orcid"]).copy()
    clean["citation_weight"] = pd.to_numeric(clean["citation_weight"], errors="coerce")
    clean = clean[np.isfinite(clean["citation_weight"]) & (clean["citation_weight"] > 0)]
    return (
        clean.groupby(
            ["subject", "citing_orcid", "cited_orcid"], as_index=False, sort=False
        )["citation_weight"]
        .sum()
        .reset_index(drop=True)
    )


def enumerate_subject_cliques(
    dyads: pd.DataFrame,
    nodes: Iterable[str],
    *,
    min_size: int = 4,
) -> list[tuple[str, ...]]:
    """Return maximal cliques in a subject's positive undirected projection."""

    if min_size < 3:
        raise ValueError("min_size must be at least 3")
    cumulative = aggregate_cumulative_dyads(dyads)
    node_set = {str(node) for node in nodes}
    graph = nx.Graph()
    graph.add_nodes_from(node_set)
    peer = cumulative[cumulative["citing_orcid"] != cumulative["cited_orcid"]]
    peer = peer[
        peer["citing_orcid"].isin(node_set)
        & peer["cited_orcid"].isin(node_set)
    ]
    graph.add_edges_from(
        peer[["citing_orcid", "cited_orcid"]].itertuples(index=False, name=None)
    )
    groups = {
        tuple(sorted(str(node) for node in clique))
        for clique in nx.find_cliques(graph)
        if len(clique) >= min_size
    }
    return sorted(groups, key=lambda group: (len(group), group))


def clique_group_metrics(
    clique: Sequence[str],
    dyads: pd.DataFrame,
    tier_by_orcid: Mapping[str, str],
) -> dict[str, object]:
    """Measure directed density and weighted reciprocity for one clique."""

    members = tuple(sorted(str(node) for node in clique))
    if len(members) < 2:
        raise ValueError("a clique must contain at least two nodes")
    member_set = set(members)
    cumulative = aggregate_cumulative_dyads(dyads)
    peer = cumulative[
        cumulative["citing_orcid"].isin(member_set)
        & cumulative["cited_orcid"].isin(member_set)
        & (cumulative["citing_orcid"] != cumulative["cited_orcid"])
    ]
    weights = {
        (str(row.citing_orcid), str(row.cited_orcid)): float(row.citation_weight)
        for row in peer.itertuples(index=False)
    }
    return _clique_group_metrics_from_weights(members, weights, tier_by_orcid)


def _clique_group_metrics_from_weights(
    members: Sequence[str],
    weights: Mapping[tuple[str, str], float],
    tier_by_orcid: Mapping[str, str],
) -> dict[str, object]:
    members = tuple(sorted(str(node) for node in members))
    possible_arcs = len(members) * (len(members) - 1)
    directed_dyads = sum(
        (source, target) in weights
        for source in members
        for target in members
        if source != target
    )
    pair_maximum = 0.0
    pair_minimum = 0.0
    for left, right in combinations(members, 2):
        forward = weights.get((left, right), 0.0)
        reverse = weights.get((right, left), 0.0)
        pair_maximum += max(forward, reverse)
        pair_minimum += min(forward, reverse)
    return {
        "clique_size": len(members),
        "directed_dyads": directed_dyads,
        "directed_density": (
            directed_dyads / possible_arcs if possible_arcs else math.nan
        ),
        "weighted_reciprocity": (
            pair_minimum / pair_maximum if pair_maximum > 0 else math.nan
        ),
        "case_memberships": sum(
            tier_by_orcid.get(node) == "Case" for node in members
        ),
        "control_memberships": sum(
            tier_by_orcid.get(node) == "Control" for node in members
        ),
    }


def _clique_rows(
    edges: pd.DataFrame,
    membership: pd.DataFrame,
    *,
    progress: bool = False,
) -> list[dict[str, object]]:
    started_at = time.monotonic()
    cumulative = aggregate_cumulative_dyads(edges)
    rows: list[dict[str, object]] = []
    for subject, members in membership.groupby("subject", sort=True):
        subject = str(subject)
        nodes = [str(node) for node in members["orcid"].drop_duplicates()]
        subject_dyads = cumulative[cumulative["subject"].astype(str) == subject]
        groups = enumerate_subject_cliques(subject_dyads, nodes, min_size=3)
        if progress:
            _progress(
                f"Clique subject {subject}: enumerated {len(groups):,} maximal groups",
                started_at=started_at,
            )
        if not groups:
            continue
        tier_by_orcid = {
            str(row.orcid): str(row.tier_type)
            for row in members.itertuples(index=False)
        }
        weights = {
            (str(row.citing_orcid), str(row.cited_orcid)): float(row.citation_weight)
            for row in subject_dyads.itertuples(index=False)
            if row.citing_orcid != row.cited_orcid
        }
        scoring_started = time.monotonic()
        for index, clique in enumerate(groups, start=1):
            metrics = _clique_group_metrics_from_weights(
                clique, weights, tier_by_orcid
            )
            metrics.update({"subject": subject, "members": clique})
            rows.append(metrics)
            if progress and (index % 1_000 == 0 or index == len(groups)):
                elapsed = max(time.monotonic() - scoring_started, 1e-9)
                eta_minutes = (len(groups) - index) / (index / elapsed) / 60
                _progress(
                    f"Clique subject {subject}: scored {index:,}/{len(groups):,} "
                    f"({100 * index / len(groups):.1f}%); ETA {eta_minutes:.1f} min",
                    started_at=started_at,
                )
    return rows


def _matched_binary_summary(pairs: pd.DataFrame) -> dict[str, object]:
    case = pairs["Case"].astype(bool)
    control = pairs["Control"].astype(bool)
    pair_count = len(pairs)
    case_only = int((case & ~control).sum())
    control_only = int((~case & control).sum())
    discordant = case_only + control_only
    case_rate = float(case.mean()) if pair_count else math.nan
    control_rate = float(control.mean()) if pair_count else math.nan
    return {
        "matched_pair_count": pair_count,
        "unique_member_count": int(case.sum() + control.sum()),
        "case_member_count": int(case.sum()),
        "control_member_count": int(control.sum()),
        "case_membership_rate": case_rate,
        "control_membership_rate": control_rate,
        "paired_difference": case_rate - control_rate if pair_count else math.nan,
        "case_only_pairs": case_only,
        "control_only_pairs": control_only,
        "discordant_pairs": discordant,
        "both_member_pairs": int((case & control).sum()),
        "neither_member_pairs": int((~case & ~control).sum()),
        "exact_p": (
            float(binomtest(case_only, discordant, 0.5).pvalue)
            if discordant
            else 1.0
        ),
    }


def matched_clique_inference(
    rows: Sequence[Mapping[str, object]],
    membership: pd.DataFrame,
    *,
    min_size: int,
    reciprocity_threshold: float,
) -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    """Compare unique reciprocal-clique membership within matched pairs."""

    _require_columns(
        membership, ("pair_id", "subject", "orcid", "tier_type"), "membership"
    )
    qualifying = [
        row
        for row in rows
        if int(row["clique_size"]) >= min_size
        and float(row["directed_density"]) >= CLIQUE_DENSITY_THRESHOLD
        and np.isfinite(float(row["weighted_reciprocity"]))
        and float(row["weighted_reciprocity"]) >= reciprocity_threshold
    ]
    keys = {
        (str(row["subject"]), str(orcid))
        for row in qualifying
        for orcid in row["members"]
    }
    assignments = membership.copy()
    assignments["subject"] = assignments["subject"].astype(str)
    assignments["orcid"] = assignments["orcid"].astype(str)
    assignments["clique_member"] = [
        (row.subject, row.orcid) in keys
        for row in assignments.itertuples(index=False)
    ]
    pair_flags = assignments.pivot(
        index=["pair_id", "subject"],
        columns="tier_type",
        values="clique_member",
    )
    if set(pair_flags.columns) != set(TIER_ORDER):
        raise ValueError("each matched pair must contain one Case and one Control")
    pair_flags = pair_flags[list(TIER_ORDER)].astype(bool)
    summary = _matched_binary_summary(pair_flags)
    summary["subjects_case_higher"] = 0
    summary["subjects_control_higher"] = 0
    summary["subjects_tied"] = 0
    subject_rows = []
    for subject, subject_pairs in pair_flags.groupby(level="subject", sort=True):
        subject_summary = _matched_binary_summary(subject_pairs)
        subject_summary["subject"] = str(subject)
        subject_rows.append(subject_summary)
        difference = float(subject_summary["paired_difference"])
        if difference > 0:
            summary["subjects_case_higher"] += 1
        elif difference < 0:
            summary["subjects_control_higher"] += 1
        else:
            summary["subjects_tied"] += 1
    selected = assignments[assignments["clique_member"]].copy()
    return summary, pd.DataFrame(subject_rows), selected


def _clique_threshold_summary(
    rows: Sequence[Mapping[str, object]],
    *,
    min_size: int,
    reciprocity_threshold: float,
) -> dict[str, object]:
    return _clique_threshold_summaries(
        rows, [(min_size, reciprocity_threshold)]
    )[(min_size, reciprocity_threshold)]


def _clique_threshold_summaries(
    rows: Sequence[Mapping[str, object]],
    configurations: Sequence[tuple[int, float]],
) -> dict[tuple[int, float], dict[str, object]]:
    """Summarize all clique thresholds with one vectorized pass."""

    sizes = np.fromiter(
        (int(row["clique_size"]) for row in rows), dtype=np.int64
    )
    densities = np.fromiter(
        (float(row["directed_density"]) for row in rows), dtype=float
    )
    reciprocities = np.fromiter(
        (float(row["weighted_reciprocity"]) for row in rows), dtype=float
    )
    output: dict[tuple[int, float], dict[str, object]] = {}
    for min_size, reciprocity_threshold in configurations:
        structural = (sizes >= min_size) & (densities >= CLIQUE_DENSITY_THRESHOLD)
        qualifying = (
            structural
            & np.isfinite(reciprocities)
            & (reciprocities >= reciprocity_threshold)
        )
        selected_density = densities[qualifying]
        selected_reciprocity = reciprocities[qualifying]
        output[(min_size, reciprocity_threshold)] = {
            "minimum_clique_size": min_size,
            "reciprocity_threshold": reciprocity_threshold,
            "density_threshold": CLIQUE_DENSITY_THRESHOLD,
            "observed_clique_count": int(structural.sum()),
            "observed_reciprocal_clique_count": int(qualifying.sum()),
            "observed_mean_density": (
                float(np.mean(selected_density))
                if selected_density.size
                else math.nan
            ),
            "observed_mean_reciprocity": (
                float(np.mean(selected_reciprocity))
                if selected_reciprocity.size
                else math.nan
            ),
        }
    return output


def run_clique_analysis(
    edges: pd.DataFrame,
    membership: pd.DataFrame,
    *,
    progress: bool = False,
) -> CliqueResults:
    """Summarize unique reciprocal-clique membership within matched pairs."""

    _require_columns(
        membership, ("subject", "orcid", "pair_id", "tier_type"), "membership"
    )
    observed_rows = _clique_rows(edges, membership, progress=progress)
    configurations = [
        (minimum, threshold)
        for minimum in CLIQUE_MIN_SIZES
        for threshold in CLIQUE_RECIPROCITY_THRESHOLDS
    ]
    observed = _clique_threshold_summaries(observed_rows, configurations)
    sensitivity_rows: list[dict[str, object]] = []
    membership_frames = []
    subject_frames = []
    primary_config = (4, 0.50)
    for config in configurations:
        row = dict(observed[config])
        inference, subjects, selected = matched_clique_inference(
            observed_rows,
            membership,
            min_size=config[0],
            reciprocity_threshold=config[1],
        )
        row.update(inference)
        sensitivity_rows.append(row)
        for frame in (subjects, selected):
            frame.insert(0, "reciprocity_threshold", config[1])
            frame.insert(0, "minimum_clique_size", config[0])
        subject_frames.append(subjects)
        membership_frames.append(selected)
    sensitivity = pd.DataFrame(sensitivity_rows)
    primary_mask = (sensitivity["minimum_clique_size"] == 4) & np.isclose(
        sensitivity["reciprocity_threshold"], 0.50
    )
    summary = sensitivity.loc[primary_mask].reset_index(drop=True)
    return CliqueResults(
        summary=summary,
        sensitivity=sensitivity,
        membership=pd.concat(membership_frames, ignore_index=True),
        subject_consistency=pd.concat(subject_frames, ignore_index=True),
    )


def reciprocity_from_dyads(
    dyads: pd.DataFrame, nodes: Iterable[str]
) -> dict[str, float]:
    """Outgoing-weight share reciprocated by reverse cumulative dyad weight.

    The numerator is ``sum_j min(w_ij, w_ji)`` and the denominator is
    ``sum_j w_ij``. Self-loops are excluded. No outgoing peer makes the measure
    unavailable; outgoing weight without a reverse link contributes zero.
    """

    peer = dyads[dyads["citing_orcid"] != dyads["cited_orcid"]]
    weight = {
        (str(row.citing_orcid), str(row.cited_orcid)): float(row.citation_weight)
        for row in peer.itertuples(index=False)
    }
    outgoing: dict[str, list[tuple[str, float]]] = {}
    for (source, target), value in weight.items():
        outgoing.setdefault(source, []).append((target, value))
    result: dict[str, float] = {}
    for node in nodes:
        out = outgoing.get(node, [])
        denominator = sum(value for _, value in out)
        result[node] = (
            float(
                sum(min(value, weight.get((target, node), 0.0)) for target, value in out)
                / denominator
            )
            if denominator > 0
            else math.nan
        )
    return result


def local_clustering_from_dyads(
    dyads: pd.DataFrame, nodes: Iterable[str]
) -> dict[str, float]:
    """Unweighted undirected local closure in the matched-author induced graph.

    Degree zero or one has the explicitly defined structural value zero.
    Fractional weights are not used for topology because they reflect team-size
    denominators, not tie strength on a common scale.
    """

    node_list = list(nodes)
    graph = nx.Graph()
    graph.add_nodes_from(node_list)
    peer = dyads[dyads["citing_orcid"] != dyads["cited_orcid"]]
    graph.add_edges_from(peer[["citing_orcid", "cited_orcid"]].itertuples(index=False, name=None))
    return {key: float(value) for key, value in nx.clustering(graph).items()}


def compute_annual_dyadic_surge(
    edges: pd.DataFrame, author_keys: pd.DataFrame | None = None
) -> pd.DataFrame:
    """Compute the study-defined maximum annual dyadic surge share.

    For author ``i``, this is
    ``max_[j,t=2021..2024] (x_ijt-x_ij,t-1)_+ / sum_[j,t=2020..2024] x_ijt``.
    Missing dyad-years are zero.  Authors with no identified outgoing weight
    remain missing when ``author_keys`` is supplied.
    """

    _require_columns(
        edges,
        ("subject", "citing_orcid", "cited_orcid", "citation_year", "citation_weight"),
        "edges",
    )
    annual = edges.copy()
    annual["citation_year"] = pd.to_numeric(annual["citation_year"], errors="coerce")
    annual["citation_weight"] = pd.to_numeric(annual["citation_weight"], errors="coerce")
    annual = annual[
        annual["citation_year"].isin(YEARS)
        & np.isfinite(annual["citation_weight"])
        & (annual["citation_weight"] > 0)
    ]
    keys = ["subject", "citing_orcid"]
    output_columns = keys + ["identified_outgoing_weight", "annual_dyadic_surge_share"]
    if annual.empty:
        result = pd.DataFrame(columns=output_columns)
    else:
        annual = (
            annual.groupby(
                ["subject", "citing_orcid", "cited_orcid", "citation_year"],
                as_index=False,
            )["citation_weight"]
            .sum()
        )
        wide = annual.pivot_table(
            index=["subject", "citing_orcid", "cited_orcid"],
            columns="citation_year",
            values="citation_weight",
            aggfunc="sum",
            fill_value=0.0,
        )
        for year in YEARS:
            if year not in wide:
                wide[year] = 0.0
        wide = wide[list(YEARS)]
        increases = np.maximum(wide.iloc[:, 1:].to_numpy() - wide.iloc[:, :-1].to_numpy(), 0.0)
        dyad_surge = pd.Series(
            increases.max(axis=1), index=wide.index, name="dyad_maximum_increase"
        )
        numerator = dyad_surge.groupby(level=[0, 1]).max()
        denominator = wide.sum(axis=1).groupby(level=[0, 1]).sum()
        result = pd.concat(
            [
                denominator.rename("identified_outgoing_weight"),
                (numerator / denominator).rename("annual_dyadic_surge_share"),
            ],
            axis=1,
        ).reset_index()
    if author_keys is not None:
        _require_columns(author_keys, ("subject", "orcid"), "author_keys")
        base = author_keys[["subject", "orcid"]].drop_duplicates().rename(
            columns={"orcid": "citing_orcid"}
        )
        result = base.merge(result, on=keys, how="left", validate="one_to_one")
    return result


def compute_graph_features(membership: pd.DataFrame, edges: pd.DataFrame) -> pd.DataFrame:
    """Compute the five graph-derived measures on their declared populations."""

    rows: list[dict[str, object]] = []
    for subject, subject_members in membership.groupby("subject", sort=True):
        nodes = subject_members["orcid"].drop_duplicates().tolist()
        node_set = set(nodes)
        subject_edges = edges[
            (edges["subject"] == subject) & edges["citing_orcid"].isin(node_set)
        ]

        # Outgoing ego layer: a matched author may cite any identified endpoint.
        ego_dyads = aggregate_cumulative_dyads(subject_edges)
        ego_peer_dyads = ego_dyads[
            ego_dyads["citing_orcid"] != ego_dyads["cited_orcid"]
        ]
        hhi: dict[str, float] = {}
        for author, group in ego_peer_dyads.groupby("citing_orcid"):
            hhi[str(author)] = weighted_hhi(group["citation_weight"])

        # Subject-specific matched-author induced graph; self-loops do not
        # describe peer closure or internal net flow.
        induced = ego_dyads[ego_dyads["cited_orcid"].isin(node_set)]
        peer = induced[induced["citing_orcid"] != induced["cited_orcid"]]
        reciprocity = reciprocity_from_dyads(peer, nodes)
        clustering = local_clustering_from_dyads(peer, nodes)
        out_weight = peer.groupby("citing_orcid")["citation_weight"].sum().to_dict()
        in_weight = peer.groupby("cited_orcid")["citation_weight"].sum().to_dict()

        for author in nodes:
            outgoing = float(out_weight.get(author, 0.0))
            incoming = float(in_weight.get(author, 0.0))
            flow_total = outgoing + incoming
            rows.append(
                {
                    "subject": subject,
                    "orcid": author,
                    "reciprocity": reciprocity[author],
                    "local_clustering": clustering[author],
                    "outgoing_hhi": hhi.get(author, math.nan),
                    "within_sample_citation_balance": (
                        (outgoing - incoming) / flow_total if flow_total > 0 else math.nan
                    ),
                }
            )
    return pd.DataFrame(rows)


def _load_pairs(connection: sqlite3.Connection) -> tuple[pd.DataFrame, pd.DataFrame]:
    pairs = _normalise_keys(
        _read_table(connection, "author_matched_pairs"),
        orcid_columns=("case_orcid", "control_orcid"),
    )
    _require_columns(pairs, ("subject", "case_orcid", "control_orcid"), "author_matched_pairs")
    if pairs[["subject", "case_orcid", "control_orcid"]].isna().any().any():
        raise SchemaError("author_matched_pairs contains NULL analysis keys")
    _assert_unique(pairs, ("subject", "case_orcid", "control_orcid"), "author_matched_pairs")
    pairs = pairs.reset_index(drop=True)
    pairs.insert(0, "pair_id", np.arange(len(pairs), dtype=int))

    h5 = _normalise_keys(
        _read_table(connection, "author_subject_h5_index"), orcid_columns=("orcid",)
    )
    _require_columns(h5, ("subject", "orcid", "h5_index"), "author_subject_h5_index")
    _assert_unique(h5, ("subject", "orcid"), "author_subject_h5_index")
    case_h5 = h5.rename(columns={"orcid": "case_orcid", "h5_index": "case_h5"})[
        ["subject", "case_orcid", "case_h5"]
    ]
    control_h5 = h5.rename(
        columns={"orcid": "control_orcid", "h5_index": "control_h5"}
    )[["subject", "control_orcid", "control_h5"]]
    pairs = pairs.merge(
        case_h5, on=["subject", "case_orcid"], how="left", validate="many_to_one"
    ).merge(
        control_h5,
        on=["subject", "control_orcid"],
        how="left",
        validate="many_to_one",
    )

    cases = pairs[["pair_id", "subject", "case_orcid"]].rename(
        columns={"case_orcid": "orcid"}
    )
    cases["tier_type"] = "Case"
    controls = pairs[["pair_id", "subject", "control_orcid"]].rename(
        columns={"control_orcid": "orcid"}
    )
    controls["tier_type"] = "Control"
    membership = pd.concat([cases, controls], ignore_index=True)
    _assert_unique(membership, ("subject", "orcid", "tier_type"), "author membership")
    _assert_unique(membership, ("subject", "orcid"), "author-tier mapping")
    return pairs, membership


def load_analysis_data(database: Path | str) -> AnalysisData:
    """Load and validate the corrected SQL interfaces, then assemble features."""

    database = Path(database)
    if not database.is_file():
        raise FileNotFoundError(database)
    uri = f"file:{database.resolve()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        pairs, membership = _load_pairs(connection)

        behavior = _normalise_keys(
            _read_table(connection, "author_behavior_metrics"), orcid_columns=("orcid",)
        )
        _require_columns(
            behavior,
            (
                "subject",
                "orcid",
                "self_citation_rate",
                "coauthor_citation_rate",
                "coauthor_citation_rate_same_year",
            ),
            "author_behavior_metrics",
        )
        _assert_unique(behavior, ("subject", "orcid"), "author_behavior_metrics")

        venue = _normalise_keys(
            _read_table(connection, "author_venue_metrics"), orcid_columns=("orcid",)
        )
        _require_columns(
            venue, ("subject", "orcid", "journal_endogamy_rate"), "author_venue_metrics"
        )
        _assert_unique(venue, ("subject", "orcid"), "author_venue_metrics")

        edges = _normalise_keys(
            _read_table(connection, "citation_network_final"),
            orcid_columns=("citing_orcid", "cited_orcid"),
        )
        _require_columns(
            edges,
            (
                "subject",
                "citing_orcid",
                "cited_orcid",
                "citation_year",
                "citation_weight",
                "is_self_citation",
                "is_coauthor_citation",
            ),
            "citation_network_final",
        )
        if edges[["subject", "citing_orcid", "cited_orcid"]].isna().any().any():
            raise SchemaError("citation_network_final contains NULL analytic endpoints")
        _assert_unique(
            edges,
            ("subject", "citing_orcid", "cited_orcid", "citation_year"),
            "citation_network_final",
        )
        edges["citation_weight"] = pd.to_numeric(edges["citation_weight"], errors="coerce")
        if (~np.isfinite(edges["citation_weight"]) | (edges["citation_weight"] < 0)).any():
            raise SchemaError("citation_network_final has invalid fractional weights")

        temporal = _normalise_keys(
            _read_table(connection, "author_subject_temporal_metrics"),
            orcid_columns=("orcid",),
        )
        _require_columns(
            temporal,
            (
                "subject",
                "orcid",
                "identified_outgoing_weight",
                "nonself_identified_outgoing_weight",
                "annual_dyadic_surge_share",
            ),
            "author_subject_temporal_metrics",
        )
        _assert_unique(temporal, ("subject", "orcid"), "author_subject_temporal_metrics")

    graph_features = compute_graph_features(membership, edges)
    calculated_temporal = compute_annual_dyadic_surge(edges, membership)
    calculated_temporal = calculated_temporal.rename(columns={"citing_orcid": "orcid"})

    # The SQL temporal table is the public interface, while this independent
    # calculation makes its operational definition executable and auditable.
    comparison = temporal.merge(
        calculated_temporal,
        on=["subject", "orcid"],
        how="inner",
        suffixes=("_sql", "_python"),
    )
    both = comparison.dropna(
        subset=["annual_dyadic_surge_share_sql", "annual_dyadic_surge_share_python"]
    )
    if not both.empty and not np.allclose(
        both["annual_dyadic_surge_share_sql"],
        both["annual_dyadic_surge_share_python"],
        atol=1e-10,
        rtol=1e-8,
    ):
        raise SchemaError("SQL and Python annual dyadic surge definitions disagree")

    features = membership[["pair_id", "subject", "orcid", "tier_type"]].copy()
    features = features.merge(
        behavior[
            [
                "subject",
                "orcid",
                "self_citation_rate",
                "coauthor_citation_rate",
                "coauthor_citation_rate_same_year",
            ]
        ],
        on=["subject", "orcid"],
        how="left",
        validate="one_to_one",
    )
    features = features.merge(
        venue[["subject", "orcid", "journal_endogamy_rate"]].rename(
            columns={"journal_endogamy_rate": "journal_endogamy"}
        ),
        on=["subject", "orcid"],
        how="left",
        validate="one_to_one",
    )
    features = features.merge(
        graph_features,
        on=["subject", "orcid"],
        how="left",
        validate="one_to_one",
    )
    features = features.merge(
        temporal[
            [
                "subject",
                "orcid",
                "identified_outgoing_weight",
                "nonself_identified_outgoing_weight",
                "annual_dyadic_surge_share",
            ]
        ],
        on=["subject", "orcid"],
        how="left",
        validate="one_to_one",
    )
    numeric = list(EIGHT_METRICS) + [
        "coauthor_citation_rate_same_year",
        "identified_outgoing_weight",
        "nonself_identified_outgoing_weight",
    ]
    for column in numeric:
        features[column] = pd.to_numeric(features[column], errors="coerce")
    features["eligible"] = features["nonself_identified_outgoing_weight"].fillna(0).gt(0)

    validate_analysis_inputs(pairs, membership, features, edges)
    return AnalysisData(pairs=pairs, membership=membership, features=features, edges=edges)


def validate_analysis_inputs(
    pairs: pd.DataFrame,
    membership: pd.DataFrame,
    features: pd.DataFrame,
    edges: pd.DataFrame,
) -> None:
    """Fail fast on the full-data invariants relevant to Python outputs."""

    _assert_unique(membership, ("subject", "orcid", "tier_type"), "membership")
    _assert_unique(features, ("subject", "orcid", "tier_type"), "features")
    if edges[["subject", "citing_orcid", "cited_orcid"]].isna().any().any():
        raise ValueError("NULL endpoints entered the analytic graph")
    surge = features["annual_dyadic_surge_share"].dropna()
    if ((surge < -1e-12) | (surge > 1 + 1e-12)).any():
        raise ValueError("annual dyadic surge share lies outside [0, 1]")


def assemble_paired_metric(
    pairs: pd.DataFrame, features: pd.DataFrame, metric: str
) -> pd.DataFrame:
    """Join a metric on ORCID *and subject*, deleting only incomplete pairs."""

    _require_columns(pairs, ("pair_id", "subject", "case_orcid", "control_orcid"), "pairs")
    _require_columns(features, ("subject", "orcid", "tier_type", metric), "features")
    case = features[features["tier_type"] == "Case"][["subject", "orcid", metric]].rename(
        columns={"orcid": "case_orcid", metric: "case_value"}
    )
    control = features[features["tier_type"] == "Control"][
        ["subject", "orcid", metric]
    ].rename(columns={"orcid": "control_orcid", metric: "control_value"})
    _assert_unique(case, ("subject", "case_orcid"), f"Case {metric}")
    _assert_unique(control, ("subject", "control_orcid"), f"Control {metric}")
    joined = pairs.merge(
        case,
        on=["subject", "case_orcid"],
        how="left",
        validate="one_to_one",
    ).merge(
        control,
        on=["subject", "control_orcid"],
        how="left",
        validate="one_to_one",
    )
    joined["case_value"] = pd.to_numeric(joined["case_value"], errors="coerce")
    joined["control_value"] = pd.to_numeric(joined["control_value"], errors="coerce")
    complete = joined.dropna(subset=["case_value", "control_value"]).copy()
    complete["difference"] = complete["case_value"] - complete["control_value"]
    if len(complete) > len(pairs):
        raise AssertionError("per-metric pair count exceeds matched cohort")
    return complete


def matched_rank_biserial(differences: Iterable[float]) -> float:
    """Matched-pairs rank-biserial effect, positive for Case > Control."""

    values = np.asarray(list(differences), dtype=float)
    values = values[np.isfinite(values) & (values != 0)]
    if values.size == 0:
        return 0.0
    ranks = rankdata(np.abs(values), method="average")
    denominator = float(ranks.sum())
    return float((ranks[values > 0].sum() - ranks[values < 0].sum()) / denominator)


def paired_bootstrap_ci(
    differences: Iterable[float],
    *,
    n_resamples: int = 2_000,
    seed: int = DEFAULT_SEED,
    confidence: float = 0.95,
    statistic: str = "median",
) -> tuple[float, float]:
    """Percentile paired-bootstrap CI for a within-pair difference summary."""

    values = np.asarray(list(differences), dtype=float)
    values = values[np.isfinite(values)]
    if values.size == 0:
        return math.nan, math.nan
    if n_resamples <= 0:
        raise ValueError("n_resamples must be positive")
    if statistic not in {"mean", "median"}:
        raise ValueError("statistic must be 'mean' or 'median'")
    rng = np.random.default_rng(seed)
    statistics = np.empty(n_resamples, dtype=float)
    chunk = 256
    for start in range(0, n_resamples, chunk):
        stop = min(start + chunk, n_resamples)
        sample_index = rng.integers(0, len(values), size=(stop - start, len(values)))
        sample = values[sample_index]
        statistics[start:stop] = (
            np.mean(sample, axis=1)
            if statistic == "mean"
            else np.median(sample, axis=1)
        )
    alpha = (1.0 - confidence) / 2.0
    low, high = np.quantile(statistics, [alpha, 1.0 - alpha])
    return float(low), float(high)


def sign_flip_pvalue(
    differences: Iterable[float],
    *,
    n_resamples: int = 10_000,
    seed: int = DEFAULT_SEED,
) -> float:
    """Two-sided randomisation p-value from within-pair sign flips.

    The test statistic is the mean paired difference.  Chunking avoids a
    one allocation spanning every randomization and matched pair.
    """

    values = np.asarray(list(differences), dtype=float)
    values = values[np.isfinite(values)]
    if values.size == 0:
        return math.nan
    if np.all(values == 0):
        return 1.0
    if n_resamples <= 0:
        raise ValueError("n_resamples must be positive")
    observed = abs(float(values.mean()))
    rng = np.random.default_rng(seed)
    exceedances = 0
    chunk = 256
    for start in range(0, n_resamples, chunk):
        count = min(chunk, n_resamples - start)
        signs = rng.integers(0, 2, size=(count, len(values)), dtype=np.int8)
        signs = signs * 2 - 1
        permuted = np.abs((signs * values).mean(axis=1))
        exceedances += int(np.count_nonzero(permuted >= observed - 1e-15))
    return float((exceedances + 1) / (n_resamples + 1))


def benjamini_hochberg(pvalues: Iterable[float]) -> np.ndarray:
    """Benjamini-Hochberg adjusted p-values, preserving missing values."""

    values = np.asarray(list(pvalues), dtype=float)
    adjusted = np.full(values.shape, np.nan, dtype=float)
    valid_index = np.flatnonzero(np.isfinite(values))
    if len(valid_index) == 0:
        return adjusted
    ordered_local = np.argsort(values[valid_index])
    ordered_index = valid_index[ordered_local]
    ordered = values[ordered_index]
    m = len(ordered)
    raw = ordered * m / np.arange(1, m + 1)
    monotone = np.minimum.accumulate(raw[::-1])[::-1]
    adjusted[ordered_index] = np.minimum(monotone, 1.0)
    return adjusted


def _wilcoxon_pvalue(differences: np.ndarray) -> float:
    finite = differences[np.isfinite(differences)]
    if finite.size == 0 or np.all(finite == 0):
        return 1.0 if finite.size else math.nan
    try:
        return float(
            wilcoxon(
                finite,
                zero_method="wilcox",
                alternative="two-sided",
                method="auto",
            ).pvalue
        )
    except ValueError:
        return 1.0


def run_paired_inference(
    pairs: pd.DataFrame,
    features: pd.DataFrame,
    metrics: Sequence[str],
    *,
    family: str,
    seed: int = DEFAULT_SEED,
    bootstrap_resamples: int = 2_000,
    sign_flips: int = 10_000,
) -> pd.DataFrame:
    """Run the declared paired tests and BH correction for one family."""

    rows: list[dict[str, object]] = []
    for index, metric in enumerate(metrics):
        paired = assemble_paired_metric(pairs, features, metric)
        differences = paired["difference"].to_numpy(dtype=float)
        ci_low, ci_high = paired_bootstrap_ci(
            differences,
            n_resamples=bootstrap_resamples,
            seed=seed + 10_007 * index,
        )
        mean_ci_low, mean_ci_high = paired_bootstrap_ci(
            differences,
            n_resamples=bootstrap_resamples,
            seed=seed + 30_013 * index,
            statistic="mean",
        )
        rows.append(
            {
                "family": family,
                "metric": metric,
                "metric_label": METRIC_LABELS[metric],
                "n_pairs": len(paired),
                "case_median": float(paired["case_value"].median()) if len(paired) else math.nan,
                "control_median": (
                    float(paired["control_value"].median()) if len(paired) else math.nan
                ),
                "median_difference": float(np.median(differences)) if len(paired) else math.nan,
                "mean_difference": float(np.mean(differences)) if len(paired) else math.nan,
                "bootstrap_ci_low": ci_low,
                "bootstrap_ci_high": ci_high,
                "mean_bootstrap_ci_low": mean_ci_low,
                "mean_bootstrap_ci_high": mean_ci_high,
                "rank_biserial": matched_rank_biserial(differences),
                "wilcoxon_p": _wilcoxon_pvalue(differences),
                "sign_flip_p": sign_flip_pvalue(
                    differences, n_resamples=sign_flips, seed=seed + 20_011 * index
                ),
            }
        )
    result = pd.DataFrame(rows)
    result["bh_adjusted_p"] = benjamini_hochberg(result["wilcoxon_p"])
    result["bh_significant_05"] = result["bh_adjusted_p"] < 0.05
    if (result["n_pairs"] > len(pairs)).any():
        raise AssertionError("an inference pair count exceeds the cohort")
    return result


def exact_h5_pairs(pairs: pd.DataFrame) -> pd.DataFrame:
    """Select pairs with positive, exactly equal h5 indices."""

    _require_columns(pairs, ("case_h5", "control_h5"), "pairs")
    case_h5 = pd.to_numeric(pairs["case_h5"], errors="coerce")
    control_h5 = pd.to_numeric(pairs["control_h5"], errors="coerce")
    return pairs[case_h5.gt(0) & control_h5.gt(0) & case_h5.eq(control_h5)].copy()


def matching_balance(pairs: pd.DataFrame) -> pd.DataFrame:
    """Summarise cohort size and h5 balance overall and by subject."""

    work = pairs.copy()
    work["case_h5"] = pd.to_numeric(work["case_h5"], errors="coerce")
    work["control_h5"] = pd.to_numeric(work["control_h5"], errors="coerce")
    if work[["case_h5", "control_h5"]].isna().any().any():
        raise AssertionError("a matched pair is missing a subject-keyed h5 value")
    if work[["case_h5", "control_h5"]].le(0).any().any():
        raise AssertionError("a matched pair has a nonpositive h5 value")
    work["h5_difference"] = work["case_h5"] - work["control_h5"]
    work["exact_h5"] = work["h5_difference"].eq(0) & work["h5_difference"].notna()
    if work["h5_difference"].abs().gt(MATCHING_H5_CALIPER).any():
        raise AssertionError("a matched pair exceeds the inclusive h5 caliper")
    rows: list[dict[str, object]] = []
    groups: list[tuple[str, pd.DataFrame]] = [("Overall", work)]
    groups.extend((str(subject), group) for subject, group in work.groupby("subject", sort=True))
    for label, group in groups:
        observed = group["h5_difference"].dropna()
        rows.append(
            {
                "subject": label,
                "n_pairs": len(group),
                "n_h5_observed": len(observed),
                "n_exact_h5": int(group["exact_h5"].sum()),
                "exact_h5_share": float(group["exact_h5"].mean()) if len(group) else math.nan,
                "median_h5_difference": float(observed.median()) if len(observed) else math.nan,
                "median_absolute_h5_difference": (
                    float(observed.abs().median()) if len(observed) else math.nan
                ),
                "max_absolute_h5_difference": (
                    float(observed.abs().max()) if len(observed) else math.nan
                ),
            }
        )
    return pd.DataFrame(rows)


def robust_scale_against_controls(
    frame: pd.DataFrame,
    features: Sequence[str] = DETECTOR_FEATURES,
    *,
    control_mask: pd.Series | np.ndarray | None = None,
) -> tuple[np.ndarray, dict[str, dict[str, float]]]:
    """Median/IQR-scale rows against a designated Control reference set."""

    _require_columns(frame, features, "detector frame")
    if control_mask is None:
        _require_columns(frame, ("tier_type",), "detector frame")
        control_mask = frame["tier_type"].eq("Control")
    control_mask = np.asarray(control_mask, dtype=bool)
    values = frame[list(features)].to_numpy(dtype=float)
    controls = values[control_mask]
    if controls.shape[0] == 0:
        raise ValueError("no Controls are available for robust scaling")
    median = np.nanmedian(controls, axis=0)
    q25 = np.nanquantile(controls, 0.25, axis=0)
    q75 = np.nanquantile(controls, 0.75, axis=0)
    scale = q75 - q25
    scale[~np.isfinite(scale) | (scale == 0)] = 1.0
    transformed = (values - median) / scale
    parameters = {
        feature: {"median": float(median[i]), "iqr": float(scale[i])}
        for i, feature in enumerate(features)
    }
    return transformed, parameters


def screen_anomalies(
    features: pd.DataFrame,
    *,
    quantile: float = 0.99,
    seed: int = DEFAULT_SEED,
    detector_features: Sequence[str] = DETECTOR_FEATURES,
) -> pd.DataFrame:
    """Subject-Control Isolation Forest with a four-measure confirmation.

    Eligibility means at least one identified non-self outgoing citation.
    Scoring additionally requires all five declared detector features so that
    feature-ablation fits use the same rows as the canonical model.  A final
    flag requires a score above the subject's Control quantile and at least two
    of four raw cohesion measures above their corresponding Control quantiles.
    """

    if not 0 < quantile < 1:
        raise ValueError("quantile must lie in (0, 1)")
    detector_features = tuple(detector_features)
    if not detector_features:
        raise ValueError("at least one detector feature is required")
    if len(set(detector_features)) != len(detector_features):
        raise ValueError("detector features must be unique")
    unknown_features = sorted(set(detector_features) - set(DETECTOR_FEATURES))
    if unknown_features:
        raise ValueError(
            f"unknown detector feature(s): {', '.join(unknown_features)}"
        )
    required = ("subject", "orcid", "tier_type", "eligible") + DETECTOR_FEATURES
    _require_columns(features, required, "features")
    result = features.copy()
    result["detector_complete"] = np.isfinite(
        result[list(DETECTOR_FEATURES)].to_numpy(dtype=float)
    ).all(axis=1)
    result["detector_score"] = np.nan
    result["detector_threshold"] = np.nan
    result["detector_exceeds_threshold"] = pd.Series(
        pd.NA, index=result.index, dtype="boolean"
    )
    result["cohesion_exceedance_count"] = pd.Series(
        pd.NA, index=result.index, dtype="Int64"
    )
    result["cohesion_confirmation"] = pd.Series(
        pd.NA, index=result.index, dtype="boolean"
    )
    result["final_flag"] = pd.Series(False, index=result.index, dtype="boolean")
    result.loc[result["eligible"] & ~result["detector_complete"], "final_flag"] = pd.NA
    result["screen_quantile"] = float(quantile)
    result["screen_seed"] = int(seed)

    for _subject_index, (subject, subject_rows) in enumerate(
        result.groupby("subject", sort=True)
    ):
        scored_index = subject_rows.index[
            subject_rows["eligible"] & subject_rows["detector_complete"]
        ]
        control_index = subject_rows.index[
            subject_rows["eligible"]
            & subject_rows["detector_complete"]
            & subject_rows["tier_type"].eq("Control")
        ]
        if len(control_index) < 2:
            raise ValueError(
                f"subject {subject!r} has fewer than two eligible complete Controls"
            )
        if len(scored_index) == 0:
            continue
        subject_scored = result.loc[scored_index]
        transformed, _ = robust_scale_against_controls(
            subject_scored,
            detector_features,
            control_mask=subject_scored.index.isin(control_index),
        )
        model = IsolationForest(
            n_estimators=200,
            max_samples="auto",
            contamination="auto",
            random_state=seed,
            n_jobs=-1,
        )
        local_control = subject_scored.index.isin(control_index)
        model.fit(transformed[local_control])
        scores = -model.score_samples(transformed)
        threshold = float(np.quantile(scores[local_control], quantile))
        score_exceeds = scores > threshold

        control_values = result.loc[control_index, list(COHESION_FEATURES)]
        cohesion_thresholds = control_values.quantile(quantile)
        exceedance_count = (
            result.loc[scored_index, list(COHESION_FEATURES)]
            .gt(cohesion_thresholds, axis="columns")
            .sum(axis=1)
            .astype(int)
        )
        confirmation = exceedance_count.ge(2)

        result.loc[scored_index, "detector_score"] = scores
        result.loc[scored_index, "detector_threshold"] = threshold
        result.loc[scored_index, "detector_exceeds_threshold"] = score_exceeds
        result.loc[scored_index, "cohesion_exceedance_count"] = exceedance_count
        result.loc[scored_index, "cohesion_confirmation"] = confirmation
        result.loc[scored_index, "final_flag"] = score_exceeds & confirmation.to_numpy()

    flag_mask = result["final_flag"].fillna(False).astype(bool)
    confirmation_mask = result["cohesion_confirmation"].fillna(False).astype(bool)
    if (flag_mask & ~result["eligible"]).any():
        raise AssertionError("an ineligible row was flagged")
    if (flag_mask & ~confirmation_mask).any():
        raise AssertionError("a flag bypassed cohesion confirmation")
    return result


def anomaly_enrichment(screened: pd.DataFrame) -> pd.DataFrame:
    """Report tier-specific eligible-row shares and Case/Control enrichment."""

    _require_columns(screened, ("tier_type", "eligible", "final_flag"), "screened")
    rows: list[dict[str, object]] = []
    counts: dict[str, tuple[int, int]] = {}
    for tier in TIER_ORDER:
        subset = screened[(screened["tier_type"] == tier) & screened["eligible"]]
        flagged = int(subset["final_flag"].fillna(False).sum())
        eligible = len(subset)
        counts[tier] = (flagged, eligible)
        rows.append(
            {
                "tier_type": tier,
                "eligible_rows": eligible,
                "screenable_rows": int(subset["final_flag"].notna().sum()),
                "unavailable_flag_rows": int(subset["final_flag"].isna().sum()),
                "flagged_rows": flagged,
                "flagged_share": flagged / eligible if eligible else math.nan,
            }
        )
    case_flagged, case_total = counts["Case"]
    control_flagged, control_total = counts["Control"]
    case_share = case_flagged / case_total if case_total else math.nan
    control_share = control_flagged / control_total if control_total else math.nan
    total_flagged = case_flagged + control_flagged
    case_share_among_flagged = case_flagged / total_flagged if total_flagged else math.nan
    enrichment = (
        case_share / control_share
        if np.isfinite(case_share) and np.isfinite(control_share) and control_share > 0
        else math.inf if np.isfinite(case_share) and case_share > 0 and control_share == 0
        else math.nan
    )
    table = np.array(
        [
            [case_flagged, max(case_total - case_flagged, 0)],
            [control_flagged, max(control_total - control_flagged, 0)],
        ]
    )
    fisher_p = float(fisher_exact(table, alternative="two-sided").pvalue) if table.sum() else math.nan
    for row in rows:
        row["case_to_control_enrichment"] = enrichment
        row["total_flagged_rows"] = total_flagged
        row["case_share_among_flagged"] = case_share_among_flagged
        row["fisher_exact_p"] = fisher_p
    return pd.DataFrame(rows)


def _flag_key_set(frame: pd.DataFrame) -> set[tuple[str, str]]:
    _require_columns(frame, ("subject", "orcid", "final_flag"), "screened features")
    return set(
        frame.loc[frame["final_flag"].fillna(False), ["subject", "orcid"]]
        .astype(str)
        .itertuples(index=False, name=None)
    )


def flag_set_hash(flagged_keys: set[tuple[str, str]]) -> str:
    payload = "\n".join(f"{subject}\t{orcid}" for subject, orcid in sorted(flagged_keys))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def run_anomaly_sensitivity(
    features: pd.DataFrame,
    *,
    quantiles: Sequence[float] = (0.975, 0.99, 0.995),
    seeds: Sequence[int] = tuple(range(DEFAULT_SEED, DEFAULT_SEED + 10)),
    reference_keys: set[tuple[str, str]] | None = None,
    detector_features: Sequence[str] = DETECTOR_FEATURES,
) -> pd.DataFrame:
    """Repeat the single screen across three thresholds and ten fixed seeds."""

    rows: list[dict[str, object]] = []
    for quantile in quantiles:
        for seed in seeds:
            screened = screen_anomalies(
                features,
                quantile=quantile,
                seed=int(seed),
                detector_features=detector_features,
            )
            keys = _flag_key_set(screened)
            summary = anomaly_enrichment(screened).set_index("tier_type")
            union = keys | (reference_keys or set())
            intersection = keys & (reference_keys or set())
            rows.append(
                {
                    "quantile": quantile,
                    "seed": int(seed),
                    "flagged_rows": len(keys),
                    "case_flagged_rows": int(summary.loc["Case", "flagged_rows"]),
                    "control_flagged_rows": int(summary.loc["Control", "flagged_rows"]),
                    "case_flagged_share": float(summary.loc["Case", "flagged_share"]),
                    "control_flagged_share": float(summary.loc["Control", "flagged_share"]),
                    "case_to_control_enrichment": float(
                        summary.loc["Case", "case_to_control_enrichment"]
                    ),
                    "reference_jaccard": (
                        len(intersection) / len(union) if union else 1.0
                    ),
                    "flag_set_hash": flag_set_hash(keys),
                }
            )
    return pd.DataFrame(rows)


def detector_confirmation_overlap(screened: pd.DataFrame) -> pd.DataFrame:
    """Return the detector/confirmation 2-by-2 table and rank association."""

    required = (
        "eligible",
        "detector_complete",
        "detector_score",
        "detector_exceeds_threshold",
        "cohesion_exceedance_count",
        "cohesion_confirmation",
        "final_flag",
    )
    _require_columns(screened, required, "screened features")
    screenable = screened[screened["eligible"] & screened["detector_complete"]].copy()
    diagnostics = (
        "detector_score",
        "detector_exceeds_threshold",
        "cohesion_exceedance_count",
        "cohesion_confirmation",
        "final_flag",
    )
    if screenable[list(diagnostics)].isna().any().any():
        raise AssertionError("a screenable row has an undefined screen diagnostic")
    expected_flags = (
        screenable["detector_exceeds_threshold"].astype(bool)
        & screenable["cohesion_confirmation"].astype(bool)
    )
    observed_flags = (
        screenable["final_flag"].astype("boolean").fillna(False).astype(bool)
    )
    if not expected_flags.equals(observed_flags):
        raise AssertionError("final_flag does not equal detector/confirmation overlap")

    association_rows = (
        screenable[["detector_score", "cohesion_exceedance_count"]]
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
    )
    if (
        len(association_rows) < 2
        or association_rows["detector_score"].nunique() < 2
        or association_rows["cohesion_exceedance_count"].nunique() < 2
    ):
        rho = p_value = math.nan
    else:
        rho, p_value = spearmanr(
            association_rows["detector_score"],
            association_rows["cohesion_exceedance_count"],
        )
    denominator = len(screenable)
    rows: list[dict[str, object]] = []
    for detector_exceeds in (False, True):
        for cohesion_confirms in (False, True):
            count = int(
                (
                    screenable["detector_exceeds_threshold"].eq(detector_exceeds)
                    & screenable["cohesion_confirmation"].eq(cohesion_confirms)
                ).sum()
            )
            rows.append(
                {
                    "detector_exceeds_threshold": detector_exceeds,
                    "cohesion_confirmation": cohesion_confirms,
                    "row_count": count,
                    "share_of_screenable": count / denominator if denominator else math.nan,
                    "screenable_rows": denominator,
                    "spearman_n": len(association_rows),
                    "spearman_rho": float(rho),
                    "spearman_p": float(p_value),
                }
            )
    return pd.DataFrame(rows)


def run_anomaly_feature_ablation(
    features: pd.DataFrame,
    *,
    quantile: float = 0.99,
    seed: int = DEFAULT_SEED,
    reference_keys: set[tuple[str, str]] | None = None,
) -> pd.DataFrame:
    """Leave out each detector feature at the canonical threshold and seed."""

    if reference_keys is None:
        reference_keys = _flag_key_set(
            screen_anomalies(features, quantile=quantile, seed=seed)
        )
    rows: list[dict[str, object]] = []
    for omitted_feature in DETECTOR_FEATURES:
        retained_features = tuple(
            feature for feature in DETECTOR_FEATURES if feature != omitted_feature
        )
        row = run_anomaly_sensitivity(
            features,
            quantiles=(quantile,),
            seeds=(seed,),
            reference_keys=reference_keys,
            detector_features=retained_features,
        ).iloc[0].to_dict()
        rows.append(
            {
                "omitted_feature": omitted_feature,
                "omitted_feature_label": METRIC_LABELS[omitted_feature],
                "retained_features": "|".join(retained_features),
                **row,
            }
        )
    return pd.DataFrame(rows)


def build_outlier_components(
    edges: pd.DataFrame,
    flagged_keys: set[tuple[str, str]],
    *,
    min_nodes: int = 5,
) -> ComponentResults:
    """Build neutral components from exactly the canonical author-subject flags."""

    if min_nodes < 2:
        raise ValueError("min_nodes must be at least two")
    empty_summary = pd.DataFrame(
        columns=[
            "subject",
            "component_id",
            "n_nodes",
            "n_directed_dyads",
            "total_fractional_weight",
            "n_net_givers",
            "n_net_receivers",
            "highest_betweenness_node",
            "highest_betweenness_label",
        ]
    )
    empty_nodes = pd.DataFrame(
        columns=[
            "subject",
            "component_id",
            "orcid",
            "outgoing_weight",
            "incoming_weight",
            "net_flow",
            "flow_role",
            "betweenness",
            "structural_role",
        ]
    )
    empty_dyads = pd.DataFrame(
        columns=["subject", "component_id", "citing_orcid", "cited_orcid", "citation_weight"]
    )
    if not flagged_keys:
        return ComponentResults(empty_summary, empty_nodes, empty_dyads)

    flagged = pd.DataFrame(sorted(flagged_keys), columns=["subject", "orcid"])
    cumulative = aggregate_cumulative_dyads(edges)
    source = flagged.rename(columns={"orcid": "citing_orcid"})
    target = flagged.rename(columns={"orcid": "cited_orcid"})
    selected = cumulative.merge(
        source.assign(_source_flag=True),
        on=["subject", "citing_orcid"],
        how="inner",
        validate="many_to_one",
    ).merge(
        target.assign(_target_flag=True),
        on=["subject", "cited_orcid"],
        how="inner",
        validate="many_to_one",
    )
    selected = selected[selected["citing_orcid"] != selected["cited_orcid"]].copy()

    summary_rows: list[dict[str, object]] = []
    node_rows: list[dict[str, object]] = []
    dyad_frames: list[pd.DataFrame] = []
    component_number = 0
    for subject, subject_flags in flagged.groupby("subject", sort=True):
        subject_nodes = set(subject_flags["orcid"])
        subject_edges = selected[selected["subject"] == subject]
        graph = nx.DiGraph()
        graph.add_nodes_from(subject_nodes)
        graph.add_weighted_edges_from(
            subject_edges[["citing_orcid", "cited_orcid", "citation_weight"]].itertuples(
                index=False, name=None
            )
        )
        components = sorted(
            nx.connected_components(graph.to_undirected()),
            key=lambda values: (-len(values), sorted(values)),
        )
        for component_nodes in components:
            if len(component_nodes) < min_nodes:
                continue
            component_number += 1
            component_id = f"C{component_number}"
            subgraph = graph.subgraph(component_nodes).copy()
            for source_node, target_node, attributes in subgraph.edges(data=True):
                weight = float(attributes.get("weight", 0.0))
                attributes["distance"] = 1.0 / weight if weight > 0 else math.inf
            betweenness = nx.betweenness_centrality(
                subgraph, weight="distance", normalized=True
            )
            highest = sorted(
                component_nodes, key=lambda node: (-betweenness.get(node, 0.0), node)
            )[0]
            anonymous_labels = {
                node: f"N{index + 1}" for index, node in enumerate(sorted(component_nodes))
            }
            component_edges = subject_edges[
                subject_edges["citing_orcid"].isin(component_nodes)
                & subject_edges["cited_orcid"].isin(component_nodes)
            ].copy()
            component_edges.insert(1, "component_id", component_id)
            dyad_frames.append(component_edges[
                ["subject", "component_id", "citing_orcid", "cited_orcid", "citation_weight"]
            ])
            outgoing = component_edges.groupby("citing_orcid")["citation_weight"].sum()
            incoming = component_edges.groupby("cited_orcid")["citation_weight"].sum()
            roles: dict[str, str] = {}
            for node in sorted(component_nodes):
                out_weight = float(outgoing.get(node, 0.0))
                in_weight = float(incoming.get(node, 0.0))
                net_flow = out_weight - in_weight
                tolerance = 1e-12 * max(out_weight + in_weight, 1.0)
                flow_role = (
                    "net giver"
                    if net_flow > tolerance
                    else "net receiver"
                    if net_flow < -tolerance
                    else "balanced flow"
                )
                roles[node] = flow_role
                node_rows.append(
                    {
                        "subject": subject,
                        "component_id": component_id,
                        "orcid": node,
                        "outgoing_weight": out_weight,
                        "incoming_weight": in_weight,
                        "net_flow": net_flow,
                        "flow_role": flow_role,
                        "betweenness": float(betweenness.get(node, 0.0)),
                        "structural_role": (
                            "highest-betweenness node" if node == highest else "component member"
                        ),
                    }
                )
            summary_rows.append(
                {
                    "subject": subject,
                    "component_id": component_id,
                    "n_nodes": len(component_nodes),
                    "n_directed_dyads": len(component_edges),
                    "total_fractional_weight": float(component_edges["citation_weight"].sum()),
                    "n_net_givers": sum(role == "net giver" for role in roles.values()),
                    "n_net_receivers": sum(role == "net receiver" for role in roles.values()),
                    "highest_betweenness_node": highest,
                    "highest_betweenness_label": anonymous_labels[highest],
                }
            )
    return ComponentResults(
        pd.DataFrame(summary_rows, columns=empty_summary.columns),
        pd.DataFrame(node_rows, columns=empty_nodes.columns),
        pd.concat(dyad_frames, ignore_index=True) if dyad_frames else empty_dyads,
    )


def weighted_assortativity(matrix: pd.DataFrame | np.ndarray) -> float:
    """Directed weighted nominal assortativity from a 2-by-2 mixing matrix."""

    values = np.asarray(matrix, dtype=float)
    total = float(values.sum())
    if total <= 0:
        return math.nan
    probabilities = values / total
    observed = float(np.trace(probabilities))
    expected = float(np.dot(probabilities.sum(axis=1), probabilities.sum(axis=0)))
    return (observed - expected) / (1.0 - expected) if expected < 1.0 else math.nan


def weighted_tier_mixing(
    edges: pd.DataFrame,
    membership: pd.DataFrame,
    pairs: pd.DataFrame,
    *,
    n_swaps: int = 10_000,
    seed: int = DEFAULT_SEED,
) -> MixingResults:
    """Fractional tier mixing and within-pair tier-label-swap comparison.

    The permutation statistic is the within-tier fractional-weight share.  Its
    sparse quadratic form permits 10,000 independent pair swaps without
    materialising an edge-by-permutation array.
    """

    if n_swaps <= 0:
        raise ValueError("n_swaps must be positive")
    mapping = membership[["pair_id", "subject", "orcid", "tier_type"]].copy()
    _assert_unique(mapping, ("subject", "orcid"), "tier mapping")
    pair_ids = sorted(mapping["pair_id"].unique())
    pair_position = {pair_id: index for index, pair_id in enumerate(pair_ids)}
    mapping["pair_position"] = mapping["pair_id"].map(pair_position).astype(int)
    mapping["base_spin"] = mapping["tier_type"].map({"Case": 1.0, "Control": -1.0})
    if mapping["base_spin"].isna().any():
        raise ValueError("unknown tier label")

    cumulative = aggregate_cumulative_dyads(edges)
    cumulative = cumulative[cumulative["citing_orcid"] != cumulative["cited_orcid"]]
    source = mapping.rename(
        columns={
            "orcid": "citing_orcid",
            "tier_type": "citing_tier",
            "pair_position": "source_pair",
            "base_spin": "source_spin",
        }
    )[
        ["subject", "citing_orcid", "citing_tier", "source_pair", "source_spin"]
    ]
    target = mapping.rename(
        columns={
            "orcid": "cited_orcid",
            "tier_type": "cited_tier",
            "pair_position": "target_pair",
            "base_spin": "target_spin",
        }
    )[["subject", "cited_orcid", "cited_tier", "target_pair", "target_spin"]]
    induced = cumulative.merge(
        source,
        on=["subject", "citing_orcid"],
        how="inner",
        validate="many_to_one",
    ).merge(
        target,
        on=["subject", "cited_orcid"],
        how="inner",
        validate="many_to_one",
    )
    matrix = (
        induced.groupby(["citing_tier", "cited_tier"])["citation_weight"]
        .sum()
        .unstack(fill_value=0.0)
        .reindex(index=TIER_ORDER, columns=TIER_ORDER, fill_value=0.0)
    )
    total = float(matrix.to_numpy().sum())
    if total <= 0:
        return MixingResults(matrix, math.nan, math.nan, math.nan, np.array([]))
    same_tier_share = float(np.trace(matrix.to_numpy()) / total)
    assortativity = weighted_assortativity(matrix)

    coefficient = (
        induced["citation_weight"].to_numpy(dtype=float)
        * induced["source_spin"].to_numpy(dtype=float)
        * induced["target_spin"].to_numpy(dtype=float)
    )
    quadratic = sparse.coo_matrix(
        (
            coefficient,
            (
                induced["source_pair"].to_numpy(dtype=int),
                induced["target_pair"].to_numpy(dtype=int),
            ),
        ),
        shape=(len(pair_ids), len(pair_ids)),
    ).tocsr()
    rng = np.random.default_rng(seed)
    null = np.empty(n_swaps, dtype=float)
    chunk_size = 128
    for start in range(0, n_swaps, chunk_size):
        stop = min(start + chunk_size, n_swaps)
        pair_spins = rng.integers(
            0, 2, size=(stop - start, len(pair_ids)), dtype=np.int8
        ).astype(float)
        pair_spins = pair_spins * 2.0 - 1.0
        # (B^T Z^T)^T = ZB; sum_p (ZB)_p Z_p is z^T B z.
        product = quadratic.T.dot(pair_spins.T).T
        qform = np.sum(product * pair_spins, axis=1)
        null[start:stop] = 0.5 * (total + qform) / total
    null_centre = float(null.mean())
    deviation = abs(same_tier_share - null_centre)
    permutation_p = float(
        (np.count_nonzero(np.abs(null - null_centre) >= deviation - 1e-15) + 1)
        / (n_swaps + 1)
    )
    return MixingResults(matrix, same_tier_share, assortativity, permutation_p, null)


def _latex_escape(value: object) -> str:
    text = str(value)
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
    }
    return "".join(replacements.get(character, character) for character in text)


def _format_number(value: object, digits: int = 3) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "--"
    if math.isnan(numeric):
        return "--"
    if math.isinf(numeric):
        return r"$\infty$"
    return f"{numeric:.{digits}f}"


def _format_p(value: object) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "--"
    if not np.isfinite(numeric):
        return "--"
    return r"$<0.001$" if numeric < 0.001 else f"{numeric:.3f}"


def _format_p_relation(value: object, digits: int = 3) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "=NA"
    if not np.isfinite(numeric):
        return "=NA"
    return "<0.001" if numeric < 0.001 else f"={numeric:.{digits}f}"


def _tabularx_spec(alignment: str) -> str:
    columns = []
    for index, column in enumerate(alignment):
        if index == 0 and column == "l":
            columns.append("l")
        elif column == "l":
            columns.append(r">{\raggedright\arraybackslash}X")
        elif column == "r":
            columns.append(r">{\raggedleft\arraybackslash}X")
        else:
            raise ValueError(f"Unsupported table alignment: {alignment}")
    return "@{}" + "".join(columns) + "@{}"


def _write_complete_table(
    path: Path,
    *,
    caption: str,
    label: str,
    alignment: str,
    headers: Sequence[str],
    rows: Sequence[Sequence[str]],
    note: str | None = None,
) -> None:
    lines = [
        r"\begin{table*}[t]",
        r"\centering",
        f"\\caption{{{caption}}}",
        f"\\label{{{label}}}",
        r"\scriptsize",
        r"\setlength{\tabcolsep}{1pt}",
        f"\\begin{{tabularx}}{{\\textwidth}}{{{_tabularx_spec(alignment)}}}",
        r"\toprule",
        " & ".join(headers) + r" \\",
        r"\midrule",
    ]
    lines.extend(" & ".join(row) + r" \\" for row in rows)
    lines.extend([r"\bottomrule", r"\end{tabularx}"])
    if note:
        lines.append(r"\par\vspace{2pt}\begin{minipage}{0.96\linewidth}\scriptsize " + note)
        lines.append(r"\end{minipage}")
    lines.append(r"\end{table*}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_matching_table(balance: pd.DataFrame, path: Path) -> None:
    rows = []
    for row in balance.itertuples(index=False):
        rows.append(
            [
                _latex_escape(row.subject),
                f"{row.n_pairs:,}",
                f"{row.n_h5_observed:,}",
                f"{row.n_exact_h5:,}",
                _format_number(row.median_h5_difference, 2),
                _format_number(row.median_absolute_h5_difference, 2),
                _format_number(row.max_absolute_h5_difference, 0),
            ]
        )
    _write_complete_table(
        path,
        caption="Matched authors by subject.",
        label="tab:matching-balance",
        alignment="lrrrrrr",
        headers=("Subject", "Pairs", "$h_5$ observed", "Exact $h_5$", r"Median $\Delta h_5$", r"Median $|\Delta h_5|$", r"Maximum $|\Delta h_5|$"),
        rows=rows,
        note=(
            r"$\Delta h_5$ is Case minus Control. Every pair must satisfy "
            r"$|\Delta h_5|\leq3$. Exact-$h_5$ pairs form a sensitivity cohort; "
            r"they do not replace the primary matched cohort."
        ),
    )


def write_paired_table(
    result: pd.DataFrame,
    path: Path,
    *,
    label: str,
    caption: str,
    note: str | None = None,
) -> None:
    rows = []
    for row in result.itertuples(index=False):
        rows.append(
            [
                _latex_escape(row.metric_label),
                f"{row.n_pairs:,}",
                _format_number(row.case_median),
                _format_number(row.control_median),
                _format_number(row.median_difference),
                f"[{_format_number(row.bootstrap_ci_low)}, {_format_number(row.bootstrap_ci_high)}]",
                _format_number(row.mean_difference),
                f"[{_format_number(row.mean_bootstrap_ci_low)}, {_format_number(row.mean_bootstrap_ci_high)}]",
                _format_number(row.rank_biserial),
                _format_p(row.bh_adjusted_p),
                _format_p(row.sign_flip_p),
            ]
        )
    _write_complete_table(
        path,
        caption=caption,
        label=label,
        alignment="lrrrrrrrrrr",
        headers=(
            "Metric",
            "Pairs",
            "Case median",
            "Control median",
            "Median diff.",
            r"Boot. CI",
            "Mean diff.",
            r"Mean boot. CI",
            "Rank effect",
            "Adj. $p$",
            "Sign-flip $p$",
        ),
        rows=rows,
        note=note
        or (
            "The Pairs column gives the number of matched pairs with both values present; "
            "this count can differ by metric. Each confidence interval (CI) is a 95\\% range "
            "from the paired bootstrap. The rank effect is the rank-biserial effect size. Median differences can be zero because "
            "many values are tied at zero, while mean differences show the average shift. "
            "Positive differences and positive rank-biserial effects indicate Case $>$ Control. "
            "BH adjusts the four displayed $p$-values for multiple testing; the sign-flip "
            "$p$-value comes from randomly reversing pair directions."
        ),
    )


def write_anomaly_enrichment_table(enrichment: pd.DataFrame, path: Path) -> None:
    rows = []
    for row in enrichment.itertuples(index=False):
        rows.append(
            [
                row.tier_type,
                f"{row.eligible_rows:,}",
                f"{row.screenable_rows:,}",
                f"{row.flagged_rows:,}",
                f"{100 * row.flagged_share:.2f}\\%",
                _format_number(row.case_to_control_enrichment, 2),
                (
                    f"{100 * row.case_share_among_flagged:.2f}\\%"
                    if row.tier_type == "Case" and np.isfinite(row.case_share_among_flagged)
                    else "--"
                ),
                _format_p(row.fisher_exact_p),
            ]
        )
    _write_complete_table(
        path,
        caption="Screen results by journal-impact tier.",
        label="tab:anomaly-enrichment",
        alignment="lrrrrrrr",
        headers=("Tier", "Eligible", "Screenable", "Flagged", "Flagged share", "Case/Control enrichment", "Case share of flags", "Fisher exact $p$"),
        rows=rows,
        note=(
            "Eligibility requires identified non-self outgoing citation weight. "
            "Enrichment is the Case flagged share divided by the Control flagged share. "
            "The Fisher exact $p$-value tests whether the two flagged shares differ. "
            "Flags are screening results and do not establish intent or misconduct."
        ),
    )


def summarise_anomaly_sensitivity(sensitivity: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for quantile, group in sensitivity.groupby("quantile", sort=True):
        rows.append(
            {
                "quantile": quantile,
                "median_flagged": float(group["flagged_rows"].median()),
                "min_flagged": int(group["flagged_rows"].min()),
                "max_flagged": int(group["flagged_rows"].max()),
                "median_case_share": float(group["case_flagged_share"].median()),
                "median_control_share": float(group["control_flagged_share"].median()),
                "median_enrichment": float(group["case_to_control_enrichment"].median()),
                "median_reference_jaccard": float(group["reference_jaccard"].median()),
            }
        )
    return pd.DataFrame(rows)


def write_anomaly_sensitivity_table(summary: pd.DataFrame, path: Path) -> None:
    rows = []
    for row in summary.itertuples(index=False):
        rows.append(
            [
                f"{100 * row.quantile:.1f}\\%",
                f"{row.median_flagged:.0f} [{row.min_flagged}--{row.max_flagged}]",
                f"{100 * row.median_case_share:.2f}\\%",
                f"{100 * row.median_control_share:.2f}\\%",
                _format_number(row.median_enrichment, 2),
                _format_number(row.median_reference_jaccard, 2),
            ]
        )
    _write_complete_table(
        path,
        caption="Sensitivity of screen results.",
        label="tab:anomaly-sensitivity",
        alignment="lrrrrr",
        headers=("Control percentile", "Flagged count: median [range]", "Case share", "Control share", "Enrichment", "Overlap with main screen"),
        rows=rows,
        note=(
            "Each row summarises ten fixed seeds; the main screen uses the 99th percentile "
            "and seed 42. Enrichment is the Case flagged share divided by the Control flagged "
            "share. Overlap is the Jaccard share of flagged rows retained by both screens."
        ),
    )


def write_anomaly_overlap_table(overlap: pd.DataFrame, path: Path) -> None:
    counts = overlap.set_index(
        ["detector_exceeds_threshold", "cohesion_confirmation"]
    )["row_count"]
    screenable_rows = int(overlap["screenable_rows"].iloc[0])

    def cell(count: int) -> str:
        rate = 100 * count / screenable_rows if screenable_rows else math.nan
        return f"{count:,} ({_format_number(rate, 2)}\\%)"

    neither = int(counts.loc[(False, False)])
    confirmation_only = int(counts.loc[(False, True)])
    detector_only = int(counts.loc[(True, False)])
    both = int(counts.loc[(True, True)])
    association = overlap.iloc[0]
    association_p = (
        "$p<0.001$"
        if np.isfinite(association.spearman_p) and association.spearman_p < 0.001
        else f"$p={_format_number(association.spearman_p, 3)}$"
    )
    rows = [
        [
            "No",
            cell(neither),
            cell(confirmation_only),
            cell(neither + confirmation_only),
        ],
        ["Yes", cell(detector_only), cell(both), cell(detector_only + both)],
        [
            "Total",
            cell(neither + detector_only),
            cell(confirmation_only + both),
            cell(screenable_rows),
        ],
    ]
    _write_complete_table(
        path,
        caption="The two screen rules and their overlap.",
        label="tab:anomaly-overlap",
        alignment="lrrr",
        headers=(
            "Detector exceeds threshold",
            "Cohesion rule: no",
            "Cohesion rule: yes",
            "Total",
        ),
        rows=rows,
        note=(
            f"Cells are counts (percent of {screenable_rows:,} screenable author--subject rows). "
            f"Spearman's $\\rho$ between detector score and cohesion exceedance count was "
            f"{_format_number(association.spearman_rho, 3)} "
            f"($n={int(association.spearman_n):,}$; {association_p}). "
            "The four cohesion measures are detector inputs, so this is an "
            "overlapping interpretable restriction rather than independent corroboration."
        ),
    )


def write_anomaly_feature_ablation_table(ablation: pd.DataFrame, path: Path) -> None:
    rows = []
    for row in ablation.itertuples(index=False):
        rows.append(
            [
                _latex_escape(row.omitted_feature_label),
                f"{row.flagged_rows:,}",
                f"{row.case_flagged_rows:,} ({100 * row.case_flagged_share:.2f}\\%)",
                (
                    f"{row.control_flagged_rows:,} "
                    f"({100 * row.control_flagged_share:.2f}\\%)"
                ),
                _format_number(row.case_to_control_enrichment, 2),
                _format_number(row.reference_jaccard, 3),
            ]
        )
    quantile = float(ablation["quantile"].iloc[0])
    seed = int(ablation["seed"].iloc[0])
    _write_complete_table(
        path,
        caption="Screen stability when one input is removed.",
        label="tab:anomaly-feature-ablation",
        alignment="lrrrrr",
        headers=(
            "Detector feature omitted",
            "Flags",
            "Case flags",
            "Control flags",
            "Enrichment",
            "Overlap with main screen",
        ),
        rows=rows,
        note=(
            f"Each fit uses the {100 * quantile:.1f}\\% Control percentile and seed {seed}. "
            "The main detector-complete population and cohesion restriction are held fixed; "
            "Case and Control percentages retain eligible-row denominators. Overlap is the "
            "Jaccard share of flagged rows retained by both screens."
        ),
    )


def write_tier_mixing_table(mixing: MixingResults, path: Path) -> None:
    matrix = mixing.matrix
    rows = [
        [
            citing,
            _format_number(matrix.loc[citing, "Case"], 2),
            _format_number(matrix.loc[citing, "Control"], 2),
        ]
        for citing in TIER_ORDER
    ]
    note = (
        f"Cells are cumulative fractional citation weights. The within-tier share is "
        f"{100 * mixing.same_tier_share:.2f}\\%, and weighted assortativity is "
        f"{_format_number(mixing.assortativity, 3)}; this compares same-tier weight "
        f"with the amount expected from the row and column totals. The two-sided 10,000 "
        f"within-pair tier-label-swap p-value is {_format_p(mixing.permutation_p)}."
    )
    _write_complete_table(
        path,
        caption="Where matched authors cite.",
        label="tab:tier-mixing",
        alignment="lrr",
        headers=("Citing group", "Cited Case", "Cited Control"),
        rows=rows,
        note=note,
    )


def write_component_table(components: ComponentResults, path: Path) -> None:
    if components.summary.empty:
        rows = [[r"\multicolumn{8}{c}{No component contained at least five flagged nodes.}"]]
        alignment = "lrrrrrrr"
    else:
        rows = []
        for row in components.summary.itertuples(index=False):
            rows.append(
                [
                    _latex_escape(row.subject),
                    row.component_id,
                    f"{row.n_nodes:,}",
                    f"{row.n_directed_dyads:,}",
                    _format_number(row.total_fractional_weight, 2),
                    f"{row.n_net_givers:,}",
                    f"{row.n_net_receivers:,}",
                    row.highest_betweenness_label,
                ]
            )
        alignment = "lrrrrrrr"
    _write_complete_table(
        path,
        caption="Flagged components with at least five nodes.",
        label="tab:outlier-components",
        alignment=alignment,
        headers=("Subject", "Component", "Nodes", "Dyads", "Weight", "Net givers", "Net receivers", "Highest betweenness"),
        rows=rows,
        note=(
            "Components use only the canonical final flag and cumulative weighted dyads. "
            "Flow labels are descriptive and do not imply coordination or intent."
        ),
    )


def write_clique_summary_table(cliques: CliqueResults, path: Path) -> None:
    row = cliques.summary.iloc[0]
    rows = [
        [
            f"$k\\geq{int(row.minimum_clique_size)}, r\\geq{row.reciprocity_threshold:.2f}$",
            f"{int(row.observed_clique_count):,}",
            f"{int(row.observed_reciprocal_clique_count):,}",
            f"{int(row.case_member_count):,} ({100 * row.case_membership_rate:.2f}\\%)",
            f"{int(row.control_member_count):,} ({100 * row.control_membership_rate:.2f}\\%)",
            _format_number(100 * row.paired_difference, 2) + r" pp",
            f"{int(row.case_only_pairs):,}",
            f"{int(row.control_only_pairs):,}",
            _format_p(row.exact_p),
        ]
    ]
    _write_complete_table(
        path,
        caption="Matched reciprocal-clique membership comparison.",
        label="tab:clique-summary",
        alignment="lrrrrrrrr",
        headers=(
            "Primary rule",
            "All groups",
            "Recip. groups",
            "Case members",
            "Control members",
            "Paired diff.",
            "Case only",
            "Control only",
            "Exact $p$",
        ),
        rows=rows,
        note=(
            "A candidate group is a maximal group in which every pair has a citation in at least "
            "one direction. The primary rule requires at least four members, density at least 0.75, "
            "and weighted reciprocity at least 0.50. Authors in overlapping qualifying groups are "
            "counted once per subject. The exact two-sided matched test uses only discordant pairs."
        ),
    )


def write_clique_sensitivity_table(cliques: CliqueResults, path: Path) -> None:
    rows = []
    for row in cliques.sensitivity.itertuples(index=False):
        rows.append(
            [
                f"{int(row.minimum_clique_size)}",
                f"{row.reciprocity_threshold:.2f}",
                f"{int(row.observed_reciprocal_clique_count):,}",
                f"{100 * row.case_membership_rate:.2f}\\%",
                f"{100 * row.control_membership_rate:.2f}\\%",
                _format_number(100 * row.paired_difference, 2) + r" pp",
                f"{int(row.case_only_pairs):,}",
                f"{int(row.control_only_pairs):,}",
                _format_p(row.exact_p),
                f"{int(row.subjects_case_higher):,}/{int(row.subjects_control_higher):,}/{int(row.subjects_tied):,}",
            ]
        )
    _write_complete_table(
        path,
        caption="Sensitivity of the clique rule.",
        label="tab:clique-sensitivity",
        alignment="lrrrrrrrrr",
        headers=(
            "Minimum group size",
            "Recip. threshold",
            "Recip. groups",
            "Case rate",
            "Control rate",
            "Paired diff.",
            "Case only",
            "Control only",
            "Exact $p$",
            "Subjects $+/-/=$",
        ),
        rows=rows,
        note=(
            "The directed density threshold is fixed at 0.75. The reciprocity threshold is "
            "the minimum share of two-way citation weight. Membership is unique by author--subject, "
            "and the last column reports subjects with higher Case rates, higher Control rates, or ties."
        ),
    )


def _save_figure(fig: plt.Figure, directory: Path, stem: str) -> None:
    fig.tight_layout()
    fig.savefig(directory / f"{stem}.pdf", bbox_inches="tight")
    fig.savefig(directory / f"{stem}.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def plot_paired_effects(primary: pd.DataFrame, secondary: pd.DataFrame, directory: Path) -> None:
    data = pd.concat([primary, secondary], ignore_index=True)
    data = data.iloc[::-1].reset_index(drop=True)
    fig, ax = plt.subplots(figsize=(7.0, 4.2))
    y = np.arange(len(data))
    colors = ["#b2182b" if family == "primary" else "#2166ac" for family in data["family"]]
    for index, row in data.iterrows():
        ax.plot(
            [row.bootstrap_ci_low, row.bootstrap_ci_high],
            [index, index],
            color=colors[index],
            linewidth=1.4,
        )
        ax.scatter(row.median_difference, index, color=colors[index], s=24, zorder=3)
    ax.axvline(0, color="black", linestyle="--", linewidth=0.7)
    ax.set_yticks(y, data["metric_label"])
    ax.set_xlabel(PAIRED_EFFECT_XLABEL)
    ax.grid(axis="x", alpha=0.25)
    ax.spines[["top", "right"]].set_visible(False)
    _save_figure(fig, directory, "paired_effects")


def plot_anomaly_enrichment(enrichment: pd.DataFrame, directory: Path) -> None:
    fig, ax = plt.subplots(figsize=(4.5, 3.2))
    shares = enrichment.set_index("tier_type").loc[list(TIER_ORDER), "flagged_share"] * 100
    bars = ax.bar(TIER_ORDER, shares, color=["#b2182b", "#2166ac"], width=0.62)
    for bar, value in zip(bars, shares):
        ax.text(bar.get_x() + bar.get_width() / 2, value, f"{value:.2f}%", ha="center", va="bottom")
    ax.set_ylabel("Flagged eligible rows (%)")
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", alpha=0.25)
    _save_figure(fig, directory, "anomaly_enrichment")


def plot_tier_mixing(mixing: MixingResults, directory: Path) -> None:
    values = mixing.matrix.to_numpy(dtype=float)
    row_totals = values.sum(axis=1, keepdims=True)
    normalised = np.divide(values, row_totals, out=np.zeros_like(values), where=row_totals > 0)
    fig, ax = plt.subplots(figsize=(4.2, 3.5))
    image = ax.imshow(normalised, cmap="Blues", vmin=0, vmax=1)
    for row in range(2):
        for column in range(2):
            ax.text(
                column,
                row,
                f"{normalised[row, column]:.1%}",
                ha="center",
                va="center",
                color="white" if normalised[row, column] >= 0.5 else "black",
            )
    ax.set_xticks([0, 1], TIER_ORDER)
    ax.set_yticks([0, 1], TIER_ORDER)
    ax.set_xlabel("Cited tier")
    ax.set_ylabel("Citing tier")
    fig.colorbar(image, ax=ax, label="Row-normalised fractional weight")
    _save_figure(fig, directory, "tier_mixing")


def plot_clique_membership(cliques: CliqueResults, directory: Path) -> None:
    primary = cliques.summary.iloc[0]
    rates = 100 * np.array(
        [primary["case_membership_rate"], primary["control_membership_rate"]],
        dtype=float,
    )
    fig, ax = plt.subplots(figsize=(4.5, 3.2))
    bars = ax.bar(TIER_ORDER, rates, color=["#b2182b", "#2166ac"], width=0.62)
    for bar, value in zip(bars, rates):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            value,
            f"{value:.2f}%",
            ha="center",
            va="bottom",
        )
    ax.set_ylabel("Authors in reciprocal cliques (%)")
    ax.set_title(f"Exact matched p {_format_p_relation(primary['exact_p'])}")
    ax.set_ylim(0, max(1.0, float(rates.max()) * 1.2))
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", alpha=0.25)
    _save_figure(fig, directory, "clique_membership")


def plot_largest_component(components: ComponentResults, directory: Path, *, seed: int) -> bool:
    if components.summary.empty:
        return False
    largest = components.summary.sort_values(
        ["n_nodes", "total_fractional_weight"], ascending=False
    ).iloc[0]
    component_id = largest["component_id"]
    dyads = components.dyads[components.dyads["component_id"] == component_id]
    nodes = components.nodes[components.nodes["component_id"] == component_id]
    graph = nx.DiGraph()
    graph.add_weighted_edges_from(
        dyads[["citing_orcid", "cited_orcid", "citation_weight"]].itertuples(index=False, name=None)
    )
    position = nx.spring_layout(graph, seed=seed, weight="weight")
    role = nodes.set_index("orcid")["flow_role"].to_dict()
    betweenness = nodes.set_index("orcid")["betweenness"].to_dict()
    colors = {
        "net giver": "#b2182b",
        "net receiver": "#2166ac",
        "balanced flow": "#878787",
    }
    node_order = sorted(graph.nodes())
    labels = {node: f"N{index + 1}" for index, node in enumerate(node_order)}
    weights = np.asarray([graph[u][v]["weight"] for u, v in graph.edges()], dtype=float)
    widths = 0.4 + 2.2 * weights / weights.max() if len(weights) else []
    fig, ax = plt.subplots(figsize=(6.2, 5.2))
    nx.draw_networkx_edges(
        graph, position, width=widths, alpha=0.45, arrows=True, arrowsize=8, ax=ax
    )
    nx.draw_networkx_nodes(
        graph,
        position,
        nodelist=node_order,
        node_color=[colors[role.get(node, "balanced flow")] for node in node_order],
        node_size=[220 + 650 * betweenness.get(node, 0.0) for node in node_order],
        edgecolors="white",
        linewidths=0.7,
        ax=ax,
    )
    nx.draw_networkx_labels(graph, position, labels=labels, font_size=7, ax=ax)
    ax.set_title(
        f"Largest outlier component ({largest['subject']}; {int(largest['n_nodes'])} nodes)"
    )
    ax.axis("off")
    _save_figure(fig, directory, "outlier_components")
    return True


def _macro_stem(metric: str) -> str:
    return "".join(part.capitalize() for part in metric.split("_"))


def _macro_number(value: object, digits: int = 3) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "NA"
    if math.isnan(number):
        return "NA"
    if math.isinf(number):
        return "Infinity"
    return f"{number:.{digits}f}"


def write_result_macros(
    path: Path,
    *,
    pairs: pd.DataFrame,
    primary: pd.DataFrame,
    secondary: pd.DataFrame,
    sensitivity_inference: pd.DataFrame,
    enrichment: pd.DataFrame,
    overlap: pd.DataFrame,
    feature_ablation: pd.DataFrame,
    mixing: MixingResults,
    components: ComponentResults,
    cliques: CliqueResults,
) -> None:
    """Write all prose-facing quantitative values as generated LaTeX macros."""

    commands: dict[str, str] = {
        "AnalysisVersion": ANALYSIS_VERSION,
        "MatchedPairCount": f"{len(pairs):,}",
        "ExactHFivePairCount": f"{len(exact_h5_pairs(pairs)):,}",
        "MaximumAbsoluteHFiveDifference": _macro_number(
            (pd.to_numeric(pairs["case_h5"], errors="coerce")
             - pd.to_numeric(pairs["control_h5"], errors="coerce"))
            .abs()
            .max(),
            0,
        ),
        "HFiveCaliperViolationCount": str(
            int(
                (
                    pd.to_numeric(pairs["case_h5"], errors="coerce")
                    - pd.to_numeric(pairs["control_h5"], errors="coerce")
                )
                .abs()
                .gt(MATCHING_H5_CALIPER)
                .sum()
            )
        ),
    }
    for family_name, result in (("Primary", primary), ("Secondary", secondary)):
        for row in result.itertuples(index=False):
            prefix = family_name + _macro_stem(row.metric)
            commands.update(
                {
                    prefix + "PairCount": f"{row.n_pairs:,}",
                    prefix + "CaseMedian": _macro_number(row.case_median),
                    prefix + "ControlMedian": _macro_number(row.control_median),
                    prefix + "MedianDifference": _macro_number(row.median_difference),
                    prefix + "MeanDifference": _macro_number(row.mean_difference),
                    prefix + "BootstrapCILow": _macro_number(row.bootstrap_ci_low),
                    prefix + "BootstrapCIHigh": _macro_number(row.bootstrap_ci_high),
                    prefix + "MeanBootstrapCILow": _macro_number(row.mean_bootstrap_ci_low),
                    prefix + "MeanBootstrapCIHigh": _macro_number(row.mean_bootstrap_ci_high),
                    prefix + "RankBiserial": _macro_number(row.rank_biserial),
                    prefix + "BHAdjustedP": _macro_number(row.bh_adjusted_p, 4),
                    prefix + "SignFlipP": _macro_number(row.sign_flip_p, 4),
                }
            )
    significant_primary = int(primary["bh_significant_05"].sum())
    min_pairs = int(primary["n_pairs"].min()) if len(primary) else 0
    max_pairs = int(primary["n_pairs"].max()) if len(primary) else 0
    commands["PrimarySignificantCount"] = str(significant_primary)
    commands["PrimaryMinimumPairCount"] = f"{min_pairs:,}"
    commands["PrimaryMaximumPairCount"] = f"{max_pairs:,}"
    zero_median_count = int(np.isclose(primary["median_difference"], 0.0).sum())
    case_favored = int(primary["mean_difference"].gt(0).sum())
    control_favored = int(primary["mean_difference"].lt(0).sum())
    tied_means = len(primary) - case_favored - control_favored
    if case_favored == len(primary):
        primary_direction = "All four primary mean differences favored Cases"
    elif control_favored == len(primary):
        primary_direction = "All four primary mean differences favored Controls"
    else:
        primary_direction = (
            f"{case_favored} of four primary mean differences favored Cases, "
            f"{control_favored} favored Controls, and {tied_means} were zero"
        )
    if significant_primary == len(primary):
        significance_summary = "all four comparisons were significant after adjustment"
    else:
        significance_summary = (
            f"{significant_primary} of four comparisons were significant after adjustment"
        )
    # Kept deliberately concise so the assembled abstract remains below 200 words.
    commands["PrimaryFindingText"] = (
        f"{primary_direction}; {significance_summary}, and "
        f"{zero_median_count} median differences were zero."
    )

    primary_by_metric = primary.set_index("metric")
    hhi = primary_by_metric.loc["outgoing_hhi"]
    mean_summary = [
        primary_by_metric.loc[metric]
        for metric in (
            "coauthor_citation_rate",
            "reciprocity",
            "local_clustering",
            "outgoing_hhi",
        )
    ]
    commands["PrimaryDetailedFindingText"] = (
        "The four primary mean paired differences were "
        + "; ".join(
            f"{row.metric_label} {_macro_number(row.mean_difference)}, 95\\% bootstrap CI "
            f"[{_macro_number(row.mean_bootstrap_ci_low)}, {_macro_number(row.mean_bootstrap_ci_high)}]"
            for row in mean_summary
        )
        + ". "
        f"{zero_median_count} median differences were zero. Outgoing HHI had a median paired difference of "
        f"{_macro_number(hhi.median_difference)}, 95\\% bootstrap CI "
        f"[{_macro_number(hhi.bootstrap_ci_low)}, "
        f"{_macro_number(hhi.bootstrap_ci_high)}]."
    )

    secondary_by_metric = secondary.set_index("metric")
    endogamy = secondary_by_metric.loc["journal_endogamy"]
    surge = secondary_by_metric.loc["annual_dyadic_surge_share"]
    commands["SecondaryDetailedFindingText"] = (
        f"The median difference in same-journal referencing was "
        f"{_macro_number(endogamy.median_difference)}, 95\\% CI "
        f"[{_macro_number(endogamy.bootstrap_ci_low)}, {_macro_number(endogamy.bootstrap_ci_high)}]; "
        f"the median difference in the maximum year-to-year increase to one recipient was "
        f"{_macro_number(surge.median_difference)}, 95\\% CI "
        f"[{_macro_number(surge.bootstrap_ci_low)}, {_macro_number(surge.bootstrap_ci_high)}]."
    )

    sensitivity_by_metric = sensitivity_inference.set_index("metric")
    exact_rows = sensitivity_inference[
        sensitivity_inference["family"].eq("exact_h5_primary")
    ]
    sensitivity_sentences: list[str] = []
    if "outgoing_hhi" in sensitivity_by_metric.index:
        exact_hhi = sensitivity_by_metric.loc["outgoing_hhi"]
        exact_case_favored = int(exact_rows["mean_difference"].gt(0).sum())
        exact_direction = (
            "all four"
            if exact_case_favored == len(exact_rows)
            else f"{exact_case_favored} of four"
        )
        sensitivity_sentences.append(
            f"The exact-$h_5$ check had positive mean differences for "
            f"{exact_direction} primary comparisons; "
            f"outgoing HHI had a median difference of "
            f"{_macro_number(exact_hhi.median_difference)} (95\\% CI "
            f"[{_macro_number(exact_hhi.bootstrap_ci_low)}, "
            f"{_macro_number(exact_hhi.bootstrap_ci_high)}])."
        )
    if "coauthor_citation_rate_same_year" in sensitivity_by_metric.index:
        same_year = sensitivity_by_metric.loc["coauthor_citation_rate_same_year"]
        sensitivity_sentences.append(
            f"Using the same-year coauthor definition, the median difference was "
            f"{_macro_number(same_year.median_difference)} (95\\% CI "
            f"[{_macro_number(same_year.bootstrap_ci_low)}, "
            f"{_macro_number(same_year.bootstrap_ci_high)}])."
        )
    commands["SensitivityFindingText"] = " ".join(sensitivity_sentences)

    tier_summary = enrichment.set_index("tier_type")
    case = tier_summary.loc["Case"]
    control = tier_summary.loc["Control"]
    overlap_counts = overlap.set_index(
        ["detector_exceeds_threshold", "cohesion_confirmation"]
    )["row_count"]
    neither = int(overlap_counts.loc[(False, False)])
    cohesion_only = int(overlap_counts.loc[(False, True)])
    detector_only = int(overlap_counts.loc[(True, False)])
    detector_and_cohesion = int(overlap_counts.loc[(True, True)])
    screenable_rows = int(overlap["screenable_rows"].iloc[0])
    association = overlap.iloc[0]
    if detector_and_cohesion != int(case.total_flagged_rows):
        raise AssertionError("overlap and enrichment flag counts disagree")
    detector_count = detector_only + detector_and_cohesion
    cohesion_count = cohesion_only + detector_and_cohesion
    ablation_jaccard_min = float(feature_ablation["reference_jaccard"].min())
    ablation_jaccard_max = float(feature_ablation["reference_jaccard"].max())
    ablation_enrichment_min = float(
        feature_ablation["case_to_control_enrichment"].min()
    )
    ablation_enrichment_max = float(
        feature_ablation["case_to_control_enrichment"].max()
    )
    association_p_text = (
        "$p<0.001$"
        if np.isfinite(association.spearman_p) and association.spearman_p < 0.001
        else f"$p={_macro_number(association.spearman_p, 3)}$"
    )
    anomaly_p_text = (
        "$p<0.001$"
        if np.isfinite(case.fisher_exact_p) and case.fisher_exact_p < 0.001
        else f"$p={_macro_number(case.fisher_exact_p, 3)}$"
    )
    mixing_p_text = (
        "$p<0.001$"
        if np.isfinite(mixing.permutation_p) and mixing.permutation_p < 0.001
        else f"$p={_macro_number(mixing.permutation_p, 3)}$"
    )
    commands.update(
        {
            "EligibleCaseCount": f"{int(case.eligible_rows):,}",
            "EligibleControlCount": f"{int(control.eligible_rows):,}",
            "ScreenableCaseCount": f"{int(case.screenable_rows):,}",
            "ScreenableControlCount": f"{int(control.screenable_rows):,}",
            "FlaggedCaseCount": f"{int(case.flagged_rows):,}",
            "FlaggedControlCount": f"{int(control.flagged_rows):,}",
            "TotalFlaggedCount": f"{int(case.total_flagged_rows):,}",
            "CaseShareAmongFlagsPercent": _macro_number(
                100 * case.case_share_among_flagged, 2
            ),
            "FlaggedCasePercent": _macro_number(100 * case.flagged_share, 2),
            "FlaggedControlPercent": _macro_number(100 * control.flagged_share, 2),
            "CaseControlEnrichment": _macro_number(case.case_to_control_enrichment, 2),
            "AnomalyFisherP": _macro_number(case.fisher_exact_p, 4),
            "AnomalyScreenableCount": f"{screenable_rows:,}",
            "DetectorThresholdCount": f"{detector_count:,}",
            "DetectorThresholdPercent": _macro_number(
                100 * detector_count / screenable_rows, 2
            ),
            "CohesionConfirmationCount": f"{cohesion_count:,}",
            "CohesionConfirmationPercent": _macro_number(
                100 * cohesion_count / screenable_rows, 2
            ),
            "DetectorConfirmationOverlapCount": f"{detector_and_cohesion:,}",
            "DetectorConfirmationOverlapPercent": _macro_number(
                100 * detector_and_cohesion / screenable_rows, 2
            ),
            "DetectorOnlyCount": f"{detector_only:,}",
            "CohesionOnlyCount": f"{cohesion_only:,}",
            "DetectorCohesionSpearmanN": f"{int(association.spearman_n):,}",
            "DetectorCohesionSpearmanRho": _macro_number(
                association.spearman_rho, 3
            ),
            "DetectorCohesionSpearmanP": _macro_number(
                association.spearman_p, 4
            ),
            "AnomalyFeatureAblationFlaggedMinimum": str(
                int(feature_ablation["flagged_rows"].min())
            ),
            "AnomalyFeatureAblationFlaggedMaximum": str(
                int(feature_ablation["flagged_rows"].max())
            ),
            "AnomalyFeatureAblationJaccardMinimum": _macro_number(
                ablation_jaccard_min, 3
            ),
            "AnomalyFeatureAblationJaccardMaximum": _macro_number(
                ablation_jaccard_max, 3
            ),
            "AnomalyFeatureAblationEnrichmentMinimum": _macro_number(
                ablation_enrichment_min, 2
            ),
            "AnomalyFeatureAblationEnrichmentMaximum": _macro_number(
                ablation_enrichment_max, 2
            ),
            "AnomalyOverlapFindingText": (
                f"The two screen rules agreed for {detector_and_cohesion:,} rows; "
                f"{detector_only:,} passed only the first rule and {cohesion_only:,} only the second. "
                f"Their scores were positively related ($\\rho={_macro_number(association.spearman_rho, 3)}$, "
                f"{association_p_text})."
            ),
            "AnomalyFeatureAblationFindingText": (
                f"Removing one input at a time changed little: flag overlap ranged from "
                f"{_macro_number(ablation_jaccard_min, 3)} to {_macro_number(ablation_jaccard_max, 3)}, "
                f"and Case/Control enrichment ranged from {_macro_number(ablation_enrichment_min, 2)} "
                f"to {_macro_number(ablation_enrichment_max, 2)}."
            ),
            "AnomalyFindingText": (
                f"The screen flagged {100 * case.flagged_share:.2f}\\% of Case rows and "
                f"{100 * control.flagged_share:.2f}\\% of Control rows, a "
                f"{_macro_number(case.case_to_control_enrichment, 2)}-fold difference."
            ),
            "AnomalyDetailedFindingText": (
                f"The screen flagged {int(case.flagged_rows)} Cases and {int(control.flagged_rows)} Controls "
                f"({100 * case.flagged_share:.2f}\\% versus {100 * control.flagged_share:.2f}\\%; "
                f"{_macro_number(case.case_to_control_enrichment, 2)}-fold difference; "
                f"Fisher exact {anomaly_p_text})."
            ),
            "WithinTierMixingPercent": _macro_number(100 * mixing.same_tier_share, 2),
            "TierMixingAssortativity": _macro_number(mixing.assortativity, 3),
            "TierMixingPermutationP": _macro_number(mixing.permutation_p, 4),
            "MixingFindingText": (
                f"{100 * mixing.same_tier_share:.2f}\\% of matched citation weight stayed "
                f"within the same group (label-swap {mixing_p_text})."
            ),
            "OutlierComponentCount": str(len(components.summary)),
            "LargestOutlierComponentSize": (
                str(int(components.summary["n_nodes"].max()))
                if not components.summary.empty
                else "0"
            ),
            "ComponentFindingText": (
                f"{len(components.summary)} outlier components contained at least five nodes; "
                f"the largest contained {int(components.summary['n_nodes'].max())} nodes."
                if not components.summary.empty
                else "No outlier component contained at least five flagged nodes."
            ),
        }
    )
    clique = cliques.summary.iloc[0]
    clique_p = _format_p_relation(clique.exact_p, 4)
    commands.update(
        {
            "CliqueStructuralCount": str(int(clique.observed_clique_count)),
            "CliqueReciprocalCount": str(
                int(clique.observed_reciprocal_clique_count)
            ),
            "CliqueUniqueMemberCount": str(int(clique.unique_member_count)),
            "CliqueCaseMemberCount": str(int(clique.case_member_count)),
            "CliqueControlMemberCount": str(int(clique.control_member_count)),
            "CliqueCaseMembershipPercent": _macro_number(
                100 * clique.case_membership_rate, 2
            ),
            "CliqueControlMembershipPercent": _macro_number(
                100 * clique.control_membership_rate, 2
            ),
            "CliquePairedDifferencePoints": _macro_number(
                100 * clique.paired_difference, 2
            ),
            "CliqueCaseOnlyPairs": str(int(clique.case_only_pairs)),
            "CliqueControlOnlyPairs": str(int(clique.control_only_pairs)),
            "CliqueDiscordantPairs": str(int(clique.discordant_pairs)),
            "CliqueExactP": clique_p,
            "CliqueSubjectsCaseHigher": str(int(clique.subjects_case_higher)),
            "CliqueSubjectsControlHigher": str(
                int(clique.subjects_control_higher)
            ),
            "CliqueSubjectsTied": str(int(clique.subjects_tied)),
            "CliqueFindingText": (
                f"Primary-rule reciprocal-clique membership was "
                f"{100 * clique.case_membership_rate:.2f}\\% for Cases and "
                f"{100 * clique.control_membership_rate:.2f}\\% for Controls "
                f"(paired difference {_macro_number(100 * clique.paired_difference, 2)} "
                f"percentage points; exact $p{clique_p}$)."
            ),
        }
    )
    lines = ["% Generated by citation_analysis.py; do not edit manually."]
    lines.extend(f"\\newcommand{{\\{name}}}{{{value}}}" for name, value in commands.items())
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_artifacts(
    *,
    output_directory: Path,
    database: Path,
    seed: int,
    data: AnalysisData,
    balance: pd.DataFrame,
    primary: pd.DataFrame,
    secondary: pd.DataFrame,
    exact: pd.DataFrame,
    screened: pd.DataFrame,
    enrichment: pd.DataFrame,
    sensitivity: pd.DataFrame,
    overlap: pd.DataFrame,
    feature_ablation: pd.DataFrame,
    components: ComponentResults,
    mixing: MixingResults,
    cliques: CliqueResults,
) -> None:
    tables_directory = output_directory / "tables"
    figures_directory = output_directory / "figures"
    tables_directory.mkdir(parents=True, exist_ok=True)
    figures_directory.mkdir(parents=True, exist_ok=True)
    for stale in (
        tables_directory / "clique_null_samples.csv",
        figures_directory / "clique_nulls.pdf",
        figures_directory / "clique_nulls.png",
    ):
        stale.unlink(missing_ok=True)

    feature_columns = [
        "pair_id",
        "subject",
        "orcid",
        "tier_type",
        *EIGHT_METRICS,
        "detector_score",
        "detector_exceeds_threshold",
        "cohesion_exceedance_count",
        "cohesion_confirmation",
        "eligible",
        "detector_complete",
        "final_flag",
    ]
    screened[feature_columns].to_csv(output_directory / "author_features_final.csv", index=False)
    screened.loc[screened["final_flag"].fillna(False), feature_columns].to_csv(
        output_directory / "flagged_author_subjects.csv", index=False
    )
    balance.to_csv(tables_directory / "matching_balance.csv", index=False)
    primary.to_csv(tables_directory / "paired_primary.csv", index=False)
    secondary.to_csv(tables_directory / "paired_secondary.csv", index=False)
    exact.to_csv(tables_directory / "exact_h5_sensitivity.csv", index=False)
    enrichment.to_csv(tables_directory / "anomaly_enrichment.csv", index=False)
    sensitivity.to_csv(tables_directory / "anomaly_sensitivity_full.csv", index=False)
    sensitivity_summary = summarise_anomaly_sensitivity(sensitivity)
    sensitivity_summary.to_csv(tables_directory / "anomaly_sensitivity.csv", index=False)
    overlap.to_csv(tables_directory / "anomaly_overlap.csv", index=False)
    feature_ablation.to_csv(
        tables_directory / "anomaly_feature_ablation.csv", index=False
    )
    cliques.summary.to_csv(tables_directory / "clique_summary.csv", index=False)
    cliques.sensitivity.to_csv(tables_directory / "clique_sensitivity.csv", index=False)
    cliques.membership.to_csv(
        tables_directory / "clique_membership.csv", index=False
    )
    cliques.subject_consistency.to_csv(
        tables_directory / "clique_subject_consistency.csv", index=False
    )
    components.summary.to_csv(tables_directory / "outlier_components.csv", index=False)
    components.nodes.to_csv(tables_directory / "outlier_component_nodes.csv", index=False)
    components.dyads.to_csv(tables_directory / "outlier_component_dyads.csv", index=False)
    mixing.matrix.to_csv(tables_directory / "tier_mixing_matrix.csv")
    pd.DataFrame(
        {"permutation": np.arange(len(mixing.null_same_tier_share)), "same_tier_share": mixing.null_same_tier_share}
    ).to_csv(tables_directory / "tier_mixing_permutations.csv", index=False)

    write_matching_table(balance, tables_directory / "matching_balance.tex")
    write_paired_table(
        primary,
        tables_directory / "paired_primary.tex",
        label="tab:paired-primary",
        caption="Matched comparisons of the four primary measures.",
    )
    write_paired_table(
        secondary,
        tables_directory / "paired_secondary.tex",
        label="tab:paired-secondary",
        caption="Matched comparisons of secondary measures.",
    )
    write_paired_table(
        exact,
        tables_directory / "exact_h5_sensitivity.tex",
        label="tab:exact-h5",
        caption="Checks using exact $h_5$ matches and same-year collaborations.",
        note=(
            "The Pairs column gives the number of matched pairs with both values present; this count "
            "can differ by metric. Each confidence interval (CI) is a 95\\% range from the paired "
            "bootstrap. The rank effect is the rank-biserial effect size. The first four rows use exact-$h_5$ matches and BH correction "
            "across the primary outcomes. The same-year coauthor row uses the full cohort as a "
            "separate one-outcome check. A confidence interval (CI) gives the range supported by "
            "the paired bootstrap. Median differences can be zero because many values are tied at "
            "zero, while mean differences show the average shift."
        ),
    )
    write_anomaly_enrichment_table(enrichment, tables_directory / "anomaly_enrichment.tex")
    write_anomaly_sensitivity_table(
        sensitivity_summary, tables_directory / "anomaly_sensitivity.tex"
    )
    write_anomaly_overlap_table(overlap, tables_directory / "anomaly_overlap.tex")
    write_anomaly_feature_ablation_table(
        feature_ablation, tables_directory / "anomaly_feature_ablation.tex"
    )
    write_tier_mixing_table(mixing, tables_directory / "tier_mixing.tex")
    write_clique_summary_table(cliques, tables_directory / "clique_summary.tex")
    write_clique_sensitivity_table(
        cliques, tables_directory / "clique_sensitivity.tex"
    )
    write_component_table(components, tables_directory / "outlier_components.tex")

    plot_paired_effects(primary, secondary, figures_directory)
    plot_anomaly_enrichment(enrichment, figures_directory)
    plot_tier_mixing(mixing, figures_directory)
    plot_clique_membership(cliques, figures_directory)
    component_figure = plot_largest_component(components, figures_directory, seed=seed)

    write_result_macros(
        output_directory / "results_macros.tex",
        pairs=data.pairs,
        primary=primary,
        secondary=secondary,
        sensitivity_inference=exact,
        enrichment=enrichment,
        overlap=overlap,
        feature_ablation=feature_ablation,
        mixing=mixing,
        components=components,
        cliques=cliques,
    )
    canonical_keys = _flag_key_set(screened)
    metadata = {
        "analysis_version": ANALYSIS_VERSION,
        "database": str(database.resolve()),
        "seed": seed,
        "matched_pairs": len(data.pairs),
        "matching_h5_caliper": MATCHING_H5_CALIPER,
        "maximum_absolute_h5_difference": float(
            balance.loc[balance["subject"].eq("Overall"), "max_absolute_h5_difference"].iloc[0]
        ),
        "h5_caliper_violations": int(
            (
                pd.to_numeric(data.pairs["case_h5"], errors="coerce")
                - pd.to_numeric(data.pairs["control_h5"], errors="coerce")
            )
            .abs()
            .gt(MATCHING_H5_CALIPER)
            .sum()
        ),
        "author_subject_tier_rows": len(screened),
        "canonical_flag_count": len(canonical_keys),
        "canonical_flag_set_sha256": flag_set_hash(canonical_keys),
        "detector_cohesion_spearman_n": int(overlap["spearman_n"].iloc[0]),
        "detector_cohesion_spearman_rho": float(overlap["spearman_rho"].iloc[0]),
        "feature_ablation_runs": len(feature_ablation),
        "feature_ablation_reference": (
            "canonical final_flag at requested seed and 0.99 quantile"
        ),
        "component_minimum_nodes": 5,
        "component_figure_generated": component_figure,
        "component_and_figure_flag_source": "author_features_final.csv:final_flag",
        "synthetic_or_alternate_flags_used": False,
        "tier_label_swaps": len(mixing.null_same_tier_share),
        "clique_inference": "exact matched binary membership",
        "clique_membership_key": "subject,orcid",
    }
    (output_directory / "run_metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def run_analysis(
    *,
    database: Path,
    output_directory: Path,
    seed: int = DEFAULT_SEED,
    bootstrap_resamples: int = 2_000,
    sign_flips: int = 10_000,
    tier_swaps: int = 10_000,
    validate_only: bool = False,
) -> None:
    """Execute the canonical offline workflow."""

    started_at = time.monotonic()
    _progress("Loading and validating analysis data", started_at=started_at)
    data = load_analysis_data(database)
    _progress(
        f"Loaded {len(data.pairs):,} pairs, {len(data.features):,} author rows, "
        f"and {len(data.edges):,} annual dyads",
        started_at=started_at,
    )
    if validate_only:
        return
    output_directory.mkdir(parents=True, exist_ok=True)
    _progress("Computing matching balance and paired inference", started_at=started_at)
    balance = matching_balance(data.pairs)
    primary = run_paired_inference(
        data.pairs,
        data.features,
        PRIMARY_METRICS,
        family="primary",
        seed=seed,
        bootstrap_resamples=bootstrap_resamples,
        sign_flips=sign_flips,
    )
    secondary = run_paired_inference(
        data.pairs,
        data.features,
        SECONDARY_METRICS,
        family="secondary",
        seed=seed + 1_000,
        bootstrap_resamples=bootstrap_resamples,
        sign_flips=sign_flips,
    )
    exact_pairs = exact_h5_pairs(data.pairs)
    exact = run_paired_inference(
        exact_pairs,
        data.features,
        PRIMARY_METRICS,
        family="exact_h5_primary",
        seed=seed + 2_000,
        bootstrap_resamples=bootstrap_resamples,
        sign_flips=sign_flips,
    )
    same_year = run_paired_inference(
        data.pairs,
        data.features,
        ("coauthor_citation_rate_same_year",),
        family="same_year_coauthor_sensitivity",
        seed=seed + 3_000,
        bootstrap_resamples=bootstrap_resamples,
        sign_flips=sign_flips,
    )
    sensitivity_inference = pd.concat([exact, same_year], ignore_index=True)
    _progress("Running anomaly screens and sensitivities", started_at=started_at)
    screened = screen_anomalies(data.features, quantile=0.99, seed=seed)
    canonical_keys = _flag_key_set(screened)
    enrichment = anomaly_enrichment(screened)
    sensitivity = run_anomaly_sensitivity(
        data.features,
        seeds=tuple(range(seed, seed + 10)),
        reference_keys=canonical_keys,
    )
    overlap = detector_confirmation_overlap(screened)
    feature_ablation = run_anomaly_feature_ablation(
        data.features,
        seed=seed,
        reference_keys=canonical_keys,
    )
    _progress("Building flagged-author components", started_at=started_at)
    components = build_outlier_components(data.edges, canonical_keys, min_nodes=5)
    _progress("Computing matched tier-mixing null", started_at=started_at)
    mixing = weighted_tier_mixing(
        data.edges, data.membership, data.pairs, n_swaps=tier_swaps, seed=seed
    )
    _progress("Enumerating and scoring citation cliques", started_at=started_at)
    cliques = run_clique_analysis(
        data.edges,
        data.membership,
        progress=True,
    )
    _progress("Writing tables, figures, and macros", started_at=started_at)
    write_artifacts(
        output_directory=output_directory,
        database=database,
        seed=seed,
        data=data,
        balance=balance,
        primary=primary,
        secondary=secondary,
        exact=sensitivity_inference,
        screened=screened,
        enrichment=enrichment,
        sensitivity=sensitivity,
        overlap=overlap,
        feature_ablation=feature_ablation,
        components=components,
        mixing=mixing,
        cliques=cliques,
    )
    _progress(f"Analysis complete: {output_directory.resolve()}", started_at=started_at)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reproduce the revised matched citation-network analysis offline."
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=DEFAULT_DATABASE,
        help="Corrected SQLite analysis database (default: rolap.db).",
    )
    parser.add_argument(
        "--output-directory",
        "--output-dir",
        dest="output_directory",
        type=Path,
        default=DEFAULT_OUTPUT_DIRECTORY,
        help=f"Versioned artifact directory (default: {DEFAULT_OUTPUT_DIRECTORY}).",
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--bootstrap-resamples", type=int, default=2_000)
    parser.add_argument("--sign-flips", type=int, default=10_000)
    parser.add_argument("--tier-swaps", type=int, default=10_000)
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate schemas and invariants without writing artifacts.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    run_analysis(
        database=args.database,
        output_directory=args.output_directory,
        seed=args.seed,
        bootstrap_resamples=args.bootstrap_resamples,
        sign_flips=args.sign_flips,
        tier_swaps=args.tier_swaps,
        validate_only=args.validate_only,
    )


if __name__ == "__main__":
    main()
