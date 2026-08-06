from __future__ import annotations

import math
import sqlite3
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

import citation_analysis as analysis
import rebuild_analysis_database as database_rebuild


class MetricDefinitionTests(unittest.TestCase):
    def test_paired_effect_axis_label_is_compact(self) -> None:
        self.assertLessEqual(len(analysis.PAIRED_EFFECT_XLABEL), 50)
        self.assertNotIn("paired-bootstrap", analysis.PAIRED_EFFECT_XLABEL)

    def test_weighted_hhi(self) -> None:
        self.assertAlmostEqual(analysis.weighted_hhi([1, 1]), 0.5)
        self.assertAlmostEqual(analysis.weighted_hhi([1, 3]), 0.625)
        self.assertTrue(math.isnan(analysis.weighted_hhi([])))
        self.assertTrue(math.isnan(analysis.weighted_hhi([0, 0])))

    def test_cumulative_dyads_are_subject_keyed_and_weighted(self) -> None:
        edges = pd.DataFrame(
            {
                "subject": ["s1", "s1", "s1", "s2"],
                "citing_orcid": ["A", "A", "A", "A"],
                "cited_orcid": ["B", "B", "C", "B"],
                "citation_weight": [0.25, 0.75, 2.0, 4.0],
            }
        )
        result = analysis.aggregate_cumulative_dyads(edges).set_index(
            ["subject", "citing_orcid", "cited_orcid"]
        )["citation_weight"]
        self.assertEqual(len(result), 3)
        self.assertAlmostEqual(result.loc[("s1", "A", "B")], 1.0)
        self.assertAlmostEqual(result.loc[("s2", "A", "B")], 4.0)

    def test_weighted_reciprocity(self) -> None:
        dyads = pd.DataFrame(
            {
                "subject": ["s"] * 4,
                "citing_orcid": ["A", "B", "A", "C"],
                "cited_orcid": ["B", "A", "C", "A"],
                "citation_weight": [3.0, 1.0, 1.0, 2.0],
            }
        )
        result = analysis.reciprocity_from_dyads(dyads, ["A", "B", "C", "D"])
        self.assertAlmostEqual(result["A"], 0.5)
        self.assertAlmostEqual(result["B"], 1.0)
        self.assertAlmostEqual(result["C"], 0.5)
        self.assertTrue(math.isnan(result["D"]))

    def test_local_clustering_structural_zeros(self) -> None:
        triangle = pd.DataFrame(
            {
                "subject": ["s", "s", "s"],
                "citing_orcid": ["A", "B", "C"],
                "cited_orcid": ["B", "C", "A"],
                "citation_weight": [1.0, 1.0, 1.0],
            }
        )
        result = analysis.local_clustering_from_dyads(triangle, ["A", "B", "C", "D"])
        self.assertEqual(result["A"], 1.0)
        self.assertEqual(result["B"], 1.0)
        self.assertEqual(result["C"], 1.0)
        self.assertEqual(result["D"], 0.0)

    def test_clique_metrics_use_directed_density_and_weighted_reciprocity(self) -> None:
        dyads = pd.DataFrame(
            {
                "subject": ["s"] * 9,
                "citing_orcid": ["A", "B", "A", "C", "A", "D", "B", "B", "C"],
                "cited_orcid": ["B", "A", "C", "A", "D", "A", "C", "D", "D"],
                "citation_weight": [2, 2, 3, 3, 2, 2, 1, 1, 1],
            }
        )
        result = analysis.clique_group_metrics(
            ("A", "B", "C", "D"),
            dyads,
            {"A": "Case", "B": "Control", "C": "Case", "D": "Control"},
        )
        self.assertAlmostEqual(result["directed_density"], 9 / 12)
        self.assertAlmostEqual(result["weighted_reciprocity"], 7 / 10)
        self.assertEqual(result["case_memberships"], 2)

    def test_clique_enumeration_returns_maximal_groups_at_threshold(self) -> None:
        dyads = pd.DataFrame(
            {
                "subject": ["s"] * 7,
                "citing_orcid": ["A", "A", "A", "B", "B", "C", "D"],
                "cited_orcid": ["B", "C", "D", "C", "D", "D", "A"],
                "citation_weight": [1.0] * 7,
            }
        )
        groups = analysis.enumerate_subject_cliques(
            dyads, ["A", "B", "C", "D"], min_size=3
        )
        self.assertEqual(groups, [("A", "B", "C", "D")])

    def test_rewire_preserves_directed_degrees_and_weights(self) -> None:
        dyads = pd.DataFrame(
            {
                "subject": ["s"] * 6,
                "citing_orcid": ["A", "A", "B", "B", "C", "D"],
                "cited_orcid": ["B", "C", "C", "D", "D", "A"],
                "citation_weight": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            }
        )
        rewired = analysis.rewire_subject_dyads(dyads, seed=7, swaps=1)
        before = dyads.groupby("citing_orcid").size().sort_index()
        after = rewired.groupby("citing_orcid").size().sort_index()
        self.assertEqual(before.to_dict(), after.to_dict())
        self.assertEqual(
            dyads.groupby("cited_orcid").size().sort_index().to_dict(),
            rewired.groupby("cited_orcid").size().sort_index().to_dict(),
        )
        self.assertEqual(
            sorted(dyads["citation_weight"]), sorted(rewired["citation_weight"])
        )

    def test_annual_dyadic_surge_with_zero_filled_years(self) -> None:
        edges = pd.DataFrame(
            {
                "subject": ["s"] * 6,
                "citing_orcid": ["A", "A", "A", "A", "B", "B"],
                "cited_orcid": ["B", "B", "B", "C", "A", "A"],
                "citation_year": [2020, 2021, 2023, 2024, 2020, 2021],
                "citation_weight": [1.0, 3.0, 2.0, 4.0, 1.0, 1.0],
            }
        )
        authors = pd.DataFrame({"subject": ["s", "s", "s"], "orcid": ["A", "B", "D"]})
        result = analysis.compute_annual_dyadic_surge(edges, authors).set_index(
            "citing_orcid"
        )
        # A's largest increase is four (C in 2024), over total weight ten.
        self.assertAlmostEqual(result.loc["A", "annual_dyadic_surge_share"], 0.4)
        self.assertEqual(result.loc["B", "annual_dyadic_surge_share"], 0.0)
        self.assertTrue(math.isnan(result.loc["D", "annual_dyadic_surge_share"]))
        observed = result["annual_dyadic_surge_share"].dropna()
        self.assertTrue(observed.between(0, 1).all())


class CliqueAnalysisTests(unittest.TestCase):
    @staticmethod
    def clique_fixture() -> tuple[pd.DataFrame, pd.DataFrame]:
        edges = pd.DataFrame(
            {
                "subject": ["s"] * 9,
                "citing_orcid": ["A", "B", "A", "C", "A", "D", "B", "B", "C"],
                "cited_orcid": ["B", "A", "C", "A", "D", "A", "C", "D", "D"],
                "citation_weight": [2, 2, 3, 3, 2, 2, 1, 1, 1],
            }
        )
        membership = pd.DataFrame(
            {
                "pair_id": [0, 0, 1, 1],
                "subject": ["s"] * 4,
                "orcid": ["A", "B", "C", "D"],
                "tier_type": ["Case", "Control", "Case", "Control"],
            }
        )
        return edges, membership

    def test_clique_analysis_reports_primary_and_sensitivity_nulls(self) -> None:
        edges, membership = self.clique_fixture()
        result = analysis.run_clique_analysis(
            edges,
            membership,
            flagged_keys=set(),
            seed=7,
            null_replicates=3,
            label_swaps=3,
        )
        primary = result.summary.iloc[0]
        self.assertEqual(primary["minimum_clique_size"], 4)
        self.assertAlmostEqual(primary["reciprocity_threshold"], 0.50)
        self.assertIn("observed_reciprocal_clique_count", result.summary.columns)
        self.assertIn("null_mean_reciprocal_clique_count", result.summary.columns)
        self.assertEqual(
            len(result.sensitivity[result.sensitivity["minimum_clique_size"] == 4]),
            3,
        )
        self.assertGreater(int(primary["valid_null_replicates"]), 0)
        self.assertTrue(
            result.sensitivity["null_p_reciprocal_clique_count"].between(0, 1).all()
        )


class PairedInferenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.pairs = pd.DataFrame(
            {
                "pair_id": [0, 1],
                "subject": ["s1", "s2"],
                "case_orcid": ["A", "A"],
                "control_orcid": ["C1", "C2"],
            }
        )
        self.features = pd.DataFrame(
            {
                "subject": ["s1", "s1", "s2", "s2"],
                "orcid": ["A", "C1", "A", "C2"],
                "tier_type": ["Case", "Control", "Case", "Control"],
                "outgoing_hhi": [0.2, 0.1, 0.9, np.nan],
            }
        )

    def test_subject_keyed_join_and_per_metric_deletion(self) -> None:
        paired = analysis.assemble_paired_metric(self.pairs, self.features, "outgoing_hhi")
        self.assertEqual(len(paired), 1)
        self.assertEqual(paired.iloc[0]["subject"], "s1")
        self.assertAlmostEqual(paired.iloc[0]["difference"], 0.1)
        # Input missingness is not overwritten during assembly.
        self.assertTrue(math.isnan(self.features.iloc[-1]["outgoing_hhi"]))

    def test_rank_biserial(self) -> None:
        self.assertAlmostEqual(analysis.matched_rank_biserial([1, -2, -3]), -2 / 3)
        self.assertEqual(analysis.matched_rank_biserial([0, 0]), 0.0)

    def test_sign_flip_is_deterministic_and_pairwise(self) -> None:
        first = analysis.sign_flip_pvalue([1, 2, 3, 4], n_resamples=5_000, seed=7)
        second = analysis.sign_flip_pvalue([1, 2, 3, 4], n_resamples=5_000, seed=7)
        self.assertEqual(first, second)
        self.assertLess(first, 0.2)
        self.assertEqual(
            analysis.sign_flip_pvalue([0, 0, 0], n_resamples=100, seed=1), 1.0
        )

    def test_bh_correction_is_monotone_in_rank(self) -> None:
        adjusted = analysis.benjamini_hochberg([0.01, 0.04, 0.03, np.nan])
        self.assertAlmostEqual(adjusted[0], 0.03)
        self.assertAlmostEqual(adjusted[1], 0.04)
        self.assertAlmostEqual(adjusted[2], 0.04)
        self.assertTrue(math.isnan(adjusted[3]))

    def test_matching_balance_reports_and_enforces_h5_caliper(self) -> None:
        pairs = self.pairs.assign(case_h5=[1, 5], control_h5=[4, 5])
        overall = analysis.matching_balance(pairs).iloc[0]
        self.assertEqual(overall["max_absolute_h5_difference"], 3)
        pairs.loc[0, "control_h5"] = 5
        with self.assertRaisesRegex(AssertionError, "h5 caliper"):
            analysis.matching_balance(pairs)
        pairs.loc[0, "control_h5"] = np.nan
        with self.assertRaisesRegex(AssertionError, "missing.*h5"):
            analysis.matching_balance(pairs)
        pairs.loc[0, ["case_h5", "control_h5"]] = 0
        with self.assertRaisesRegex(AssertionError, "nonpositive h5"):
            analysis.matching_balance(pairs)


class ScreenAndReuseTests(unittest.TestCase):
    @staticmethod
    def detector_fixture() -> pd.DataFrame:
        rng = np.random.default_rng(8)
        controls = pd.DataFrame(
            rng.normal(0.1, 0.01, size=(120, len(analysis.DETECTOR_FEATURES))),
            columns=analysis.DETECTOR_FEATURES,
        )
        controls["subject"] = "s"
        controls["orcid"] = [f"C{i:03d}" for i in range(len(controls))]
        controls["tier_type"] = "Control"
        controls["eligible"] = True
        cases = pd.DataFrame(
            [
                {**{feature: 0.9 for feature in analysis.DETECTOR_FEATURES}, "subject": "s", "orcid": "X", "tier_type": "Case", "eligible": True},
                {**{feature: 0.2 for feature in analysis.DETECTOR_FEATURES}, "subject": "s", "orcid": "M", "tier_type": "Case", "eligible": True},
            ]
        )
        cases.loc[cases["orcid"] == "M", "reciprocity"] = np.nan
        return pd.concat([controls, cases], ignore_index=True)

    def test_control_referenced_screen_and_unavailable_flag(self) -> None:
        screened = analysis.screen_anomalies(self.detector_fixture(), seed=42)
        extreme = screened.set_index("orcid").loc["X"]
        incomplete = screened.set_index("orcid").loc["M"]
        self.assertTrue(bool(extreme["cohesion_confirmation"]))
        self.assertTrue(bool(extreme["final_flag"]))
        self.assertTrue(pd.isna(incomplete["final_flag"]))
        self.assertTrue(pd.isna(incomplete["detector_exceeds_threshold"]))
        self.assertTrue(pd.isna(incomplete["cohesion_exceedance_count"]))
        self.assertTrue(pd.isna(incomplete["cohesion_confirmation"]))

    def test_detector_confirmation_overlap_and_rank_association(self) -> None:
        screened = pd.DataFrame(
            {
                "eligible": [True, True, True, True, False, True],
                "detector_complete": [True, True, True, True, True, False],
                "detector_score": [1.0, 2.0, 3.0, 4.0, 99.0, np.nan],
                "detector_exceeds_threshold": [
                    False,
                    False,
                    True,
                    True,
                    False,
                    False,
                ],
                "cohesion_exceedance_count": [0, 2, 1, 3, 4, 0],
                "cohesion_confirmation": [False, True, False, True, False, False],
                "final_flag": pd.Series(
                    [False, False, False, True, False, pd.NA], dtype="boolean"
                ),
            }
        )
        overlap = analysis.detector_confirmation_overlap(screened)
        counts = overlap.set_index(
            ["detector_exceeds_threshold", "cohesion_confirmation"]
        )["row_count"]
        self.assertEqual(set(counts), {1})
        self.assertEqual(set(overlap["screenable_rows"]), {4})
        self.assertTrue(np.allclose(overlap["share_of_screenable"], 0.25))
        self.assertEqual(set(overlap["spearman_n"]), {4})
        self.assertTrue(np.allclose(overlap["spearman_rho"], 0.8))

    def test_feature_ablation_keeps_canonical_complete_rows(self) -> None:
        features = self.detector_fixture()
        reduced = analysis.screen_anomalies(
            features,
            seed=42,
            detector_features=tuple(
                feature
                for feature in analysis.DETECTOR_FEATURES
                if feature != "reciprocity"
            ),
        )
        self.assertTrue(pd.isna(reduced.set_index("orcid").loc["M", "final_flag"]))

        canonical = analysis.screen_anomalies(features, seed=42)
        ablation = analysis.run_anomaly_feature_ablation(
            features,
            seed=42,
            reference_keys=analysis._flag_key_set(canonical),
        )
        self.assertEqual(
            list(ablation["omitted_feature"]), list(analysis.DETECTOR_FEATURES)
        )
        self.assertEqual(set(ablation["quantile"]), {0.99})
        self.assertEqual(set(ablation["seed"]), {42})
        self.assertTrue(ablation["reference_jaccard"].between(0, 1).all())
        for row in ablation.itertuples(index=False):
            self.assertNotIn(row.omitted_feature, row.retained_features.split("|"))

    def test_components_reuse_only_canonical_keys(self) -> None:
        flagged = {("s", node) for node in ["A", "B", "C", "D", "E"]}
        edges = pd.DataFrame(
            {
                "subject": ["s"] * 6,
                "citing_orcid": ["A", "B", "C", "D", "E", "A"],
                "cited_orcid": ["B", "C", "D", "E", "A", "X"],
                "citation_weight": [1.0] * 6,
            }
        )
        components = analysis.build_outlier_components(edges, flagged, min_nodes=5)
        self.assertEqual(len(components.summary), 1)
        self.assertEqual(set(components.nodes["orcid"]), {"A", "B", "C", "D", "E"})
        self.assertNotIn("X", set(components.dyads["cited_orcid"]))
        self.assertAlmostEqual(components.nodes["net_flow"].sum(), 0.0)
        self.assertRegex(components.summary.iloc[0]["highest_betweenness_label"], r"^N\d+$")


class MixingTests(unittest.TestCase):
    def test_fractional_mixing_and_pair_label_swaps(self) -> None:
        pairs = pd.DataFrame(
            {
                "pair_id": [0, 1],
                "subject": ["s", "s"],
                "case_orcid": ["A", "C"],
                "control_orcid": ["B", "D"],
            }
        )
        membership = pd.DataFrame(
            {
                "pair_id": [0, 0, 1, 1],
                "subject": ["s"] * 4,
                "orcid": ["A", "B", "C", "D"],
                "tier_type": ["Case", "Control", "Case", "Control"],
            }
        )
        edges = pd.DataFrame(
            {
                "subject": ["s"] * 4,
                "citing_orcid": ["A", "B", "A", "B"],
                "cited_orcid": ["C", "D", "D", "C"],
                "citation_weight": [3.0, 1.0, 2.0, 2.0],
            }
        )
        result = analysis.weighted_tier_mixing(
            edges, membership, pairs, n_swaps=1_000, seed=12
        )
        self.assertAlmostEqual(result.matrix.loc["Case", "Case"], 3.0)
        self.assertAlmostEqual(result.matrix.loc["Case", "Control"], 2.0)
        self.assertAlmostEqual(result.matrix.loc["Control", "Case"], 2.0)
        self.assertAlmostEqual(result.matrix.loc["Control", "Control"], 1.0)
        self.assertAlmostEqual(result.same_tier_share, 0.5)
        self.assertEqual(len(result.null_same_tier_share), 1_000)
        self.assertTrue(np.all((result.null_same_tier_share >= 0) & (result.null_same_tier_share <= 1)))


class ArtifactWriterTests(unittest.TestCase):
    def test_corrected_sqlite_interface_loads_subject_keyed_features(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "fixture.db"
            with sqlite3.connect(database) as connection:
                pd.DataFrame(
                    {"case_orcid": ["A"], "control_orcid": ["B"], "subject": ["s"]}
                ).to_sql("author_matched_pairs", connection, index=False)
                pd.DataFrame(
                    {
                        "orcid": ["A", "B"],
                        "subject": ["s", "s"],
                        "h5_index": [3, 3],
                    }
                ).to_sql("author_subject_h5_index", connection, index=False)
                pd.DataFrame(
                    {
                        "orcid": ["A", "B"],
                        "subject": ["s", "s"],
                        "self_citation_rate": [0.0, 0.0],
                        "coauthor_citation_rate": [0.2, 0.1],
                        "coauthor_citation_rate_same_year": [0.3, 0.1],
                    }
                ).to_sql("author_behavior_metrics", connection, index=False)
                pd.DataFrame(
                    {
                        "orcid": ["A", "B"],
                        "subject": ["s", "s"],
                        "journal_endogamy_rate": [0.4, np.nan],
                    }
                ).to_sql("author_venue_metrics", connection, index=False)
                pd.DataFrame(
                    {
                        "subject": ["s", "s", "s"],
                        "citing_orcid": ["A", "A", "B"],
                        "cited_orcid": ["B", "B", "A"],
                        "citation_year": [2020, 2021, 2020],
                        "raw_count": [1, 1, 1],
                        "citation_weight": [0.5, 1.0, 0.25],
                        "is_self_citation": [0, 0, 0],
                        "is_coauthor_citation": [0, 1, 0],
                        "is_coauthor_citation_same_year": [1, 1, 0],
                    }
                ).to_sql("citation_network_final", connection, index=False)
                pd.DataFrame(
                    {
                        "orcid": ["A", "B"],
                        "subject": ["s", "s"],
                        "identified_outgoing_weight": [1.5, 0.25],
                        "nonself_identified_outgoing_weight": [1.5, 0.25],
                        "annual_dyadic_surge_share": [1 / 3, 0.0],
                    }
                ).to_sql("author_subject_temporal_metrics", connection, index=False)
            loaded = analysis.load_analysis_data(database)
            self.assertEqual(len(loaded.pairs), 1)
            self.assertEqual(len(loaded.features), 2)
            by_author = loaded.features.set_index("orcid")
            self.assertAlmostEqual(by_author.loc["A", "reciprocity"], 1 / 6)
            self.assertAlmostEqual(by_author.loc["A", "outgoing_hhi"], 1.0)
            self.assertTrue(math.isnan(by_author.loc["B", "journal_endogamy"]))
            self.assertAlmostEqual(
                by_author.loc["A", "coauthor_citation_rate_same_year"], 0.3
            )

    def test_writer_emits_stable_offline_interface(self) -> None:
        pair_count = 6
        pairs = pd.DataFrame(
            {
                "pair_id": np.arange(pair_count),
                "subject": ["s"] * pair_count,
                "case_orcid": [f"A{i}" for i in range(pair_count)],
                "control_orcid": [f"B{i}" for i in range(pair_count)],
                "case_h5": np.arange(1, pair_count + 1),
                "control_h5": np.arange(1, pair_count + 1),
            }
        )
        membership = pd.concat(
            [
                pairs[["pair_id", "subject", "case_orcid"]]
                .rename(columns={"case_orcid": "orcid"})
                .assign(tier_type="Case"),
                pairs[["pair_id", "subject", "control_orcid"]]
                .rename(columns={"control_orcid": "orcid"})
                .assign(tier_type="Control"),
            ],
            ignore_index=True,
        )
        features = membership.copy()
        for offset, metric in enumerate(analysis.EIGHT_METRICS):
            features[metric] = (
                features["tier_type"].map({"Case": 0.20, "Control": 0.10}).astype(float)
                + offset * 0.01
                + features["pair_id"] * 0.001
            )
        features["coauthor_citation_rate_same_year"] = features["coauthor_citation_rate"] + 0.01
        features["identified_outgoing_weight"] = 1.0
        features["nonself_identified_outgoing_weight"] = 1.0
        features["eligible"] = True
        primary = analysis.run_paired_inference(
            pairs,
            features,
            analysis.PRIMARY_METRICS,
            family="primary",
            bootstrap_resamples=50,
            sign_flips=100,
        )
        secondary = analysis.run_paired_inference(
            pairs,
            features,
            analysis.SECONDARY_METRICS,
            family="secondary",
            bootstrap_resamples=50,
            sign_flips=100,
        )
        self.assertTrue(
            {"mean_difference", "mean_bootstrap_ci_low", "mean_bootstrap_ci_high"}
            .issubset(primary.columns)
        )
        self.assertTrue((primary["mean_difference"] > 0).all())
        exact = primary.assign(family="exact_h5_primary")
        screened = features.copy()
        screened["detector_score"] = 0.0
        screened["detector_exceeds_threshold"] = False
        screened["cohesion_exceedance_count"] = 0
        screened["cohesion_confirmation"] = False
        screened["detector_complete"] = True
        screened["final_flag"] = pd.Series(False, index=screened.index, dtype="boolean")
        fixture_flag = screened["orcid"].eq("A0")
        screened.loc[fixture_flag, "detector_score"] = 1.0
        screened.loc[fixture_flag, "detector_exceeds_threshold"] = True
        screened.loc[fixture_flag, "cohesion_exceedance_count"] = 2
        screened.loc[fixture_flag, "cohesion_confirmation"] = True
        screened.loc[fixture_flag, "final_flag"] = True
        enrichment = analysis.anomaly_enrichment(screened)
        overlap = analysis.detector_confirmation_overlap(screened)
        sensitivity = pd.DataFrame(
            {
                "quantile": [0.975, 0.99, 0.995],
                "seed": [42, 42, 42],
                "flagged_rows": [1, 1, 1],
                "case_flagged_rows": [1, 1, 1],
                "control_flagged_rows": [0, 0, 0],
                "case_flagged_share": [1 / pair_count] * 3,
                "control_flagged_share": [0.0] * 3,
                "case_to_control_enrichment": [math.inf] * 3,
                "reference_jaccard": [1.0] * 3,
                "flag_set_hash": ["fixture"] * 3,
            }
        )
        feature_ablation = pd.DataFrame(
            {
                "omitted_feature": analysis.DETECTOR_FEATURES,
                "omitted_feature_label": [
                    analysis.METRIC_LABELS[feature]
                    for feature in analysis.DETECTOR_FEATURES
                ],
                "retained_features": [
                    "|".join(
                        retained
                        for retained in analysis.DETECTOR_FEATURES
                        if retained != omitted
                    )
                    for omitted in analysis.DETECTOR_FEATURES
                ],
                "quantile": [0.99] * len(analysis.DETECTOR_FEATURES),
                "seed": [42] * len(analysis.DETECTOR_FEATURES),
                "flagged_rows": [1] * len(analysis.DETECTOR_FEATURES),
                "case_flagged_rows": [1] * len(analysis.DETECTOR_FEATURES),
                "control_flagged_rows": [0] * len(analysis.DETECTOR_FEATURES),
                "case_flagged_share": [1 / pair_count]
                * len(analysis.DETECTOR_FEATURES),
                "control_flagged_share": [0.0] * len(analysis.DETECTOR_FEATURES),
                "case_to_control_enrichment": [math.inf]
                * len(analysis.DETECTOR_FEATURES),
                "reference_jaccard": [1.0] * len(analysis.DETECTOR_FEATURES),
                "flag_set_hash": ["fixture"] * len(analysis.DETECTOR_FEATURES),
            }
        )
        empty_edges = pd.DataFrame(
            columns=["subject", "citing_orcid", "cited_orcid", "citation_weight"]
        )
        components = analysis.build_outlier_components(empty_edges, {("s", "A0")})
        matrix = pd.DataFrame([[2.0, 1.0], [1.0, 2.0]], index=analysis.TIER_ORDER, columns=analysis.TIER_ORDER)
        mixing = analysis.MixingResults(
            matrix=matrix,
            same_tier_share=2 / 3,
            assortativity=1 / 3,
            permutation_p=0.5,
            null_same_tier_share=np.array([0.5, 2 / 3]),
        )
        data = analysis.AnalysisData(
            pairs=pairs,
            membership=membership,
            features=features,
            edges=empty_edges,
        )
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "revision-v1"
            analysis.write_artifacts(
                output_directory=output,
                database=Path(temporary) / "fixture.db",
                seed=42,
                data=data,
                balance=analysis.matching_balance(pairs),
                primary=primary,
                secondary=secondary,
                exact=exact,
                screened=screened,
                enrichment=enrichment,
                sensitivity=sensitivity,
                overlap=overlap,
                feature_ablation=feature_ablation,
                components=components,
                mixing=mixing,
            )
            expected = [
                "results_macros.tex",
                "author_features_final.csv",
                "tables/paired_primary.tex",
                "tables/anomaly_enrichment.tex",
                "tables/anomaly_overlap.csv",
                "tables/anomaly_overlap.tex",
                "tables/anomaly_feature_ablation.csv",
                "tables/anomaly_feature_ablation.tex",
                "tables/tier_mixing.tex",
                "figures/paired_effects.pdf",
                "figures/anomaly_enrichment.pdf",
                "figures/tier_mixing.pdf",
                "run_metadata.json",
            ]
            for relative in expected:
                self.assertTrue((output / relative).is_file(), relative)
            self.assertFalse((output / "figures/outlier_components.pdf").exists())
            macros = (output / "results_macros.tex").read_text(encoding="utf-8")
            self.assertIn(r"\newcommand{\PrimaryFindingText}", macros)
            self.assertIn(r"\newcommand{\CaseShareAmongFlagsPercent}", macros)
            self.assertIn(r"\newcommand{\AnomalyOverlapFindingText}", macros)
            self.assertIn(
                r"\newcommand{\AnomalyFeatureAblationFindingText}", macros
            )
            paired_primary = (output / "tables/paired_primary.tex").read_text(
                encoding="utf-8"
            )
            self.assertIn(r"Mean $\Delta$", paired_primary)
            self.assertIn(r"Mean boot. 95\% CI", paired_primary)
            self.assertIn("zero-inflated", paired_primary)


class ScratchDatabaseTests(unittest.TestCase):
    def test_rebuild_uses_retained_cohort_and_fresh_output(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / "raw.db"
            cohort = root / "cohort.db"
            output = root / "scratch.db"
            with sqlite3.connect(raw) as connection:
                connection.executescript(
                    """
                    CREATE TABLE works (
                      id INTEGER, doi TEXT, published_year INTEGER,
                      issn_print TEXT, issn_electronic TEXT
                    );
                    INSERT INTO works VALUES
                      (1, 'd1', 2020, 'i1', NULL),
                      (2, 'd2', 2021, 'i2', NULL);
                    CREATE TABLE issn_subjects (issn TEXT, subject TEXT);
                    INSERT INTO issn_subjects VALUES ('i1', 'S'), ('i2', 'S');
                    CREATE TABLE eigenfactor_scores (
                      issn TEXT, subject TEXT, eigenfactor_score REAL
                    );
                    INSERT INTO eigenfactor_scores VALUES
                      ('i1', 'S', 0.1), ('i2', 'S', 0.9);
                    CREATE TABLE work_authors (work_id INTEGER, orcid TEXT);
                    INSERT INTO work_authors VALUES (1, 'A'), (2, 'B');
                    CREATE TABLE work_references (work_id INTEGER, doi TEXT, year INTEGER);
                    INSERT INTO work_references VALUES (1, 'd2', 2020);
                    """
                )
            # The production cohort size is an invariant; repeat a valid tiny
            # pair to exercise the scratch builder without weakening that check.
            with sqlite3.connect(cohort) as connection:
                connection.execute(
                    "CREATE TABLE author_matched_pairs "
                    "(case_orcid TEXT, control_orcid TEXT, subject TEXT)"
                )
                rows = [
                    (f"A{index}", f"B{index}", "S")
                    for index in range(database_rebuild.EXPECTED_PAIRS)
                ]
                # Ensure one retained pair has works in the raw fixture.
                rows[0] = ("A", "B", "S")
                connection.executemany(
                    "INSERT INTO author_matched_pairs VALUES (?, ?, ?)", rows
                )
                connection.execute(
                    "CREATE TABLE author_subject_h5_index "
                    "(orcid TEXT, subject TEXT, h5_index INTEGER)"
                )
                h5_rows = [(case, subject, 1) for case, _, subject in rows]
                h5_rows += [(control, subject, 1) for _, control, subject in rows]
                connection.executemany(
                    "INSERT INTO author_subject_h5_index VALUES (?, ?, ?)", h5_rows
                )
            database_rebuild.rebuild(
                raw_database=raw,
                cohort_database=cohort,
                output_database=output,
                sql_directory=Path(analysis.__file__).parent,
                force=False,
            )
            self.assertTrue(output.is_file())
            self.assertFalse(output.with_suffix(".db.building").exists())
            with sqlite3.connect(output) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM author_matched_pairs"
                    ).fetchone()[0],
                    database_rebuild.EXPECTED_PAIRS,
                )
                edge = connection.execute(
                    "SELECT subject, citing_orcid, cited_orcid "
                    "FROM citation_network_final"
                ).fetchone()
                self.assertEqual(edge, ("S", "A", "B"))


if __name__ == "__main__":
    unittest.main()
