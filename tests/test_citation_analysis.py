from __future__ import annotations

import io
import math
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

import numpy as np
import pandas as pd

import citation_analysis as analysis
import rebuild_analysis_database as database_rebuild


class MetricDefinitionTests(unittest.TestCase):
    def test_normalise_keys_canonicalises_nullable_integer_subjects(self) -> None:
        frame = pd.DataFrame({"subject": pd.Series([1, None], dtype="float64")})

        result = analysis._normalise_keys(frame, orcid_columns=())

        self.assertEqual(result.loc[0, "subject"], "1")

    def test_paired_effect_axis_label_is_compact(self) -> None:
        self.assertLessEqual(len(analysis.PAIRED_EFFECT_XLABEL), 50)
        self.assertNotIn("paired-bootstrap", analysis.PAIRED_EFFECT_XLABEL)

    def test_p_value_relation_does_not_render_underflow_as_zero(self) -> None:
        self.assertEqual(analysis._format_p_relation(0.0), "<0.001")
        self.assertEqual(analysis._format_p_relation(0.1234), "=0.123")

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

    def test_clique_scoring_does_not_scan_subject_edges(self) -> None:
        class NonIterableWeights(dict[tuple[str, str], float]):
            def __iter__(self):
                raise RuntimeError("subject-wide edge scan")

        weights = NonIterableWeights(
            {
                ("A", "B"): 2.0,
                ("B", "A"): 2.0,
                ("A", "C"): 3.0,
                ("B", "C"): 1.0,
                ("C", "B"): 1.0,
                ("X", "Y"): 999.0,
            }
        )

        result = analysis._clique_group_metrics_from_weights(
            ("A", "B", "C"),
            weights,
            {"A": "Case", "B": "Control", "C": "Case"},
        )

        self.assertEqual(result["directed_dyads"], 5)
        self.assertAlmostEqual(result["directed_density"], 5 / 6)
        self.assertAlmostEqual(result["weighted_reciprocity"], 0.5)

    def test_progress_flushes(self) -> None:
        class FlushTrackingStream(io.StringIO):
            flushed = False

            def flush(self) -> None:
                self.flushed = True
                super().flush()

        stream = FlushTrackingStream()
        with redirect_stdout(stream):
            analysis._progress("loading", started_at=0.0)

        self.assertTrue(stream.flushed)
        self.assertIn("loading", stream.getvalue())

    def test_clique_progress_reports_completion(self) -> None:
        dyads = pd.DataFrame(
            {
                "subject": ["s"] * 6,
                "citing_orcid": ["A", "B", "A", "C", "B", "C"],
                "cited_orcid": ["B", "A", "C", "A", "C", "B"],
                "citation_weight": [1.0] * 6,
            }
        )
        membership = pd.DataFrame(
            {
                "subject": ["s", "s", "s"],
                "orcid": ["A", "B", "C"],
                "pair_id": [0, 1, 2],
                "tier_type": ["Case", "Control", "Case"],
            }
        )
        stream = io.StringIO()

        with redirect_stdout(stream):
            rows = analysis._clique_rows(dyads, membership, progress=True)

        self.assertEqual(len(rows), 1)
        self.assertIn("Clique subject s: scored 1/1 (100.0%)", stream.getvalue())
        self.assertIn("ETA", stream.getvalue())

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

    def test_clique_analysis_reports_primary_and_all_sensitivities(self) -> None:
        edges, membership = self.clique_fixture()
        result = analysis.run_clique_analysis(
            edges,
            membership,
        )
        primary = result.summary.iloc[0]
        self.assertEqual(primary["minimum_clique_size"], 4)
        self.assertAlmostEqual(primary["reciprocity_threshold"], 0.50)
        self.assertIn("observed_reciprocal_clique_count", result.summary.columns)
        self.assertIn("case_membership_rate", result.summary.columns)
        self.assertIn("control_membership_rate", result.summary.columns)
        self.assertIn("exact_p", result.summary.columns)
        self.assertEqual(
            len(result.sensitivity[result.sensitivity["minimum_clique_size"] == 4]),
            3,
        )
        self.assertEqual(len(result.sensitivity), 9)
        self.assertTrue(result.sensitivity["exact_p"].between(0, 1).all())

    def test_clique_membership_plot_writes_vector_figure(self) -> None:
        edges, membership = self.clique_fixture()
        result = analysis.run_clique_analysis(
            edges,
            membership,
        )
        with tempfile.TemporaryDirectory() as temporary:
            analysis.plot_clique_membership(result, Path(temporary))
            figure = Path(temporary) / "clique_membership.pdf"
            self.assertTrue(figure.is_file())
            self.assertGreater(figure.stat().st_size, 0)

    def test_clique_threshold_vectorisation_matches_scalar_summary(self) -> None:
        edges, membership = self.clique_fixture()
        rows = analysis._clique_rows(edges, membership)
        configurations = [(3, 0.25), (4, 0.50)]
        vectorised = analysis._clique_threshold_summaries(rows, configurations)
        for config in configurations:
            scalar = analysis._clique_threshold_summary(
                rows, min_size=config[0], reciprocity_threshold=config[1]
            )
            for key, value in scalar.items():
                if isinstance(value, float) and math.isnan(value):
                    self.assertTrue(math.isnan(vectorised[config][key]))
                else:
                    self.assertEqual(vectorised[config][key], value)

    def test_overlapping_cliques_count_each_author_subject_once(self) -> None:
        membership = pd.DataFrame(
            {
                "pair_id": [0, 0, 1, 1, 2, 2],
                "subject": ["s"] * 6,
                "orcid": ["A", "B", "C", "D", "E", "F"],
                "tier_type": ["Case", "Control"] * 3,
            }
        )
        rows = [
            {"subject": "s", "members": ("A", "B", "C", "D"), "clique_size": 4,
             "directed_density": 1.0, "weighted_reciprocity": 1.0},
            {"subject": "s", "members": ("A", "C", "E", "F"), "clique_size": 4,
             "directed_density": 1.0, "weighted_reciprocity": 1.0},
        ]
        summary, subjects, selected = analysis.matched_clique_inference(
            rows, membership, min_size=4, reciprocity_threshold=0.5
        )
        self.assertEqual(len(selected), 6)
        self.assertEqual(selected["orcid"].nunique(), 6)
        self.assertEqual(int(summary["unique_member_count"]), 6)
        self.assertEqual(len(subjects), 1)

    def test_exact_matched_membership_uses_discordant_pairs(self) -> None:
        membership = pd.DataFrame(
            {
                "pair_id": np.repeat(np.arange(5), 2),
                "subject": ["s"] * 10,
                "orcid": [
                    value
                    for index in range(5)
                    for value in (f"A{index}", f"B{index}")
                ],
                "tier_type": ["Case", "Control"] * 5,
            }
        )
        rows = [
            {
                "subject": "s",
                "members": tuple(f"A{index}" for index in range(5)),
                "clique_size": 5,
                "directed_density": 1.0,
                "weighted_reciprocity": 1.0,
            }
        ]
        summary, _subjects, _selected = analysis.matched_clique_inference(
            rows, membership, min_size=4, reciprocity_threshold=0.5
        )
        self.assertEqual(int(summary["case_only_pairs"]), 5)
        self.assertEqual(int(summary["control_only_pairs"]), 0)
        self.assertEqual(float(summary["case_membership_rate"]), 1.0)
        self.assertEqual(float(summary["control_membership_rate"]), 0.0)
        self.assertEqual(float(summary["paired_difference"]), 1.0)
        self.assertAlmostEqual(float(summary["exact_p"]), 0.0625)

    def test_cliques_are_restricted_to_matched_subject_nodes(self) -> None:
        edges, _ = self.clique_fixture()
        edges = pd.concat(
            [
                edges,
                pd.DataFrame(
                    {
                        "subject": ["s"] * 6,
                        "citing_orcid": ["A", "X", "B", "X", "C", "X"],
                        "cited_orcid": ["X", "A", "X", "B", "X", "C"],
                        "citation_weight": [1.0] * 6,
                    }
                ),
            ],
            ignore_index=True,
        )
        groups = analysis.enumerate_subject_cliques(
            edges, ["A", "B", "C", "D"], min_size=3
        )
        self.assertTrue(all("X" not in group for group in groups))

    def test_clique_rows_report_bounded_internal_density(self) -> None:
        edges, membership = self.clique_fixture()
        extra = pd.DataFrame(
            {
                "subject": ["s"] * 20,
                "citing_orcid": ["A"] * 20,
                "cited_orcid": [f"X{i}" for i in range(20)],
                "citation_weight": [1.0] * 20,
            }
        )
        edges = pd.concat([edges, extra], ignore_index=True)
        rows = analysis._clique_rows(edges, membership)
        self.assertTrue(
            all(0.0 <= row["directed_density"] <= 1.0 for row in rows)
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

    def test_validation_accepts_observed_cohort_larger_than_old_sample(self) -> None:
        pair_count = 9_432
        pairs = pd.DataFrame({"pair_id": np.arange(pair_count)})
        membership = pd.DataFrame(
            {
                "subject": ["s"] * pair_count,
                "orcid": [f"A{index}" for index in range(pair_count)],
                "tier_type": ["Case"] * pair_count,
            }
        )
        features = membership.assign(annual_dyadic_surge_share=0.0)
        edges = pd.DataFrame(
            columns=["subject", "citing_orcid", "cited_orcid"]
        )
        analysis.validate_analysis_inputs(pairs, membership, features, edges)


class FullCohortSQLTests(unittest.TestCase):
    def test_author_works_include_orcids_regardless_of_final_character(self) -> None:
        with sqlite3.connect(":memory:") as connection:
            connection.execute("ATTACH DATABASE ':memory:' AS rolap")
            connection.executescript(
                """
                CREATE TABLE work_authors (work_id INTEGER, orcid TEXT);
                INSERT INTO work_authors VALUES (1, 'A0'), (2, 'BX'), (3, '');
                CREATE TABLE rolap.works_enhanced (
                  work_id INTEGER, subject TEXT, eigenfactor_score REAL, doi TEXT
                );
                INSERT INTO rolap.works_enhanced VALUES
                  (1, 'S', 0.1, 'd1'), (2, 'S', 0.9, 'd2'), (3, 'S', 0.5, 'd3');
                CREATE TABLE rolap.filtered_authors (orcid TEXT);
                INSERT INTO rolap.filtered_authors VALUES ('A0');
                CREATE TABLE rolap.work_citations (doi TEXT, citations_number INTEGER);
                INSERT INTO rolap.work_citations VALUES ('d1', 1), ('d2', 1), ('d3', 1);
                """
            )
            connection.executescript(
                (Path(analysis.__file__).parent / "author_works_master.sql").read_text(
                    encoding="utf-8"
                )
            )
            authors = {
                row[0]
                for row in connection.execute(
                    "SELECT DISTINCT orcid FROM rolap.author_works_master"
                )
            }
        self.assertEqual(authors, {"A0", "BX"})


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
    def test_generated_tables_use_one_fixed_readable_format(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "table.tex"
            analysis._write_complete_table(
                path,
                caption="Example table.",
                label="tab:example",
                alignment="lrr",
                headers=("Measure", "Case", "Control"),
                rows=(("Example", "1", "2"),),
            )
            content = path.read_text(encoding="utf-8")
            self.assertIn(r"\scriptsize", content)
            self.assertIn(r"\setlength{\tabcolsep}{1pt}", content)
            self.assertIn(r"\begin{tabularx}{\textwidth}", content)
            self.assertNotIn(r"\resizebox{\textwidth}{!}", content)

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
        cliques = analysis.run_clique_analysis(
            empty_edges,
            membership,
        )
        cliques.summary.loc[:, "exact_p"] = 0.0
        data = analysis.AnalysisData(
            pairs=pairs,
            membership=membership,
            features=features,
            edges=empty_edges,
        )
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "revision-v1"
            (output / "tables").mkdir(parents=True)
            (output / "figures").mkdir()
            (output / "tables/clique_null_samples.csv").write_text("stale\n")
            (output / "figures/clique_nulls.pdf").write_text("stale\n")
            (output / "figures/clique_nulls.png").write_text("stale\n")
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
                cliques=cliques,
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
                "tables/clique_summary.tex",
                "tables/clique_sensitivity.tex",
                "tables/clique_summary.csv",
                "tables/clique_sensitivity.csv",
                "tables/clique_membership.csv",
                "tables/clique_subject_consistency.csv",
                "figures/paired_effects.pdf",
                "figures/anomaly_enrichment.pdf",
                "figures/tier_mixing.pdf",
                "figures/clique_membership.pdf",
                "run_metadata.json",
            ]
            for relative in expected:
                self.assertTrue((output / relative).is_file(), relative)
            for stale in (
                "tables/clique_null_samples.csv",
                "figures/clique_nulls.pdf",
                "figures/clique_nulls.png",
            ):
                self.assertFalse((output / stale).exists(), stale)
            self.assertFalse((output / "figures/outlier_components.pdf").exists())
            macros = (output / "results_macros.tex").read_text(encoding="utf-8")
            self.assertIn(r"\newcommand{\PrimaryFindingText}", macros)
            self.assertIn(r"\newcommand{\CaseShareAmongFlagsPercent}", macros)
            self.assertIn(r"\newcommand{\AnomalyOverlapFindingText}", macros)
            self.assertIn(r"\newcommand{\CliqueFindingText}", macros)
            self.assertIn(r"\newcommand{\CliqueExactP}{<0.001}", macros)
            self.assertIn(r"exact $p<0.001$", macros)
            self.assertIn(
                r"\newcommand{\AnomalyFeatureAblationFindingText}", macros
            )
            self.assertIn("All four primary mean differences favored Cases", macros)
            self.assertIn(
                "all four comparisons were significant after adjustment", macros
            )
            self.assertNotIn("4 of four comparisons", macros)
            self.assertIn("all four primary comparisons", macros)
            self.assertNotIn("4 of four primary comparisons", macros)
            self.assertIn(r"label-swap $p=0.500$", macros)
            paired_primary = (output / "tables/paired_primary.tex").read_text(
                encoding="utf-8"
            )
            self.assertIn("Mean diff.", paired_primary)
            self.assertIn("Mean boot. CI", paired_primary)
            self.assertIn("rank-biserial", paired_primary)


class ScratchDatabaseTests(unittest.TestCase):
    def test_rebuild_constructs_full_cohort_from_raw_and_fresh_output(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / "raw.db"
            output = root / "scratch.db"
            with sqlite3.connect(raw) as connection:
                connection.executescript(
                    """
                    CREATE TABLE works (
                      id INTEGER, doi TEXT, published_year INTEGER,
                      issn_print TEXT, issn_electronic TEXT
                    );
                    CREATE TABLE issn_subjects (issn TEXT, subject TEXT);
                    INSERT INTO issn_subjects VALUES ('low', 'S'), ('high', 'S');
                    CREATE TABLE eigenfactor_scores (
                      issn TEXT, subject TEXT, eigenfactor_score REAL
                    );
                    INSERT INTO eigenfactor_scores VALUES
                      ('low', 'S', 0.1), ('high', 'S', 0.9);
                    CREATE TABLE work_authors (work_id INTEGER, orcid TEXT);
                    CREATE TABLE work_references (work_id INTEGER, doi TEXT, year INTEGER);
                    """
                )
                works = [
                    (
                        index,
                        f"d{index}",
                        2020 + index % 5,
                        "low" if index <= 25 else "high",
                        None,
                    )
                    for index in range(1, 51)
                ]
                connection.executemany(
                    "INSERT INTO works VALUES (?, ?, ?, ?, ?)", works
                )
                connection.executemany(
                    "INSERT INTO work_authors VALUES (?, ?)",
                    [(index, "case-X") for index in (1, 2, 3)]
                    + [(index, "control-Y") for index in (26, 27, 28)],
                )
                connection.executemany(
                    "INSERT INTO work_references VALUES (?, ?, ?)",
                    [(50, f"d{index}", 2024) for index in (1, 2, 3, 26, 27, 28)]
                    + [(1, "d26", 2020)],
                )
            before = (raw.stat().st_size, raw.stat().st_mtime_ns)
            database_rebuild.rebuild(
                raw_database=raw,
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
                    1,
                )
                pair = connection.execute(
                    "SELECT case_orcid, control_orcid, subject "
                    "FROM author_matched_pairs"
                ).fetchone()
                self.assertEqual(pair, ("case-X", "control-Y", "S"))
                edge = connection.execute(
                    "SELECT subject, citing_orcid, cited_orcid "
                    "FROM citation_network_final"
                ).fetchone()
                self.assertEqual(edge, ("S", "case-X", "control-Y"))
            self.assertEqual(before, (raw.stat().st_size, raw.stat().st_mtime_ns))


if __name__ == "__main__":
    unittest.main()
