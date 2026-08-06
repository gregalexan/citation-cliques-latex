# Formal Clique Analysis Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a full matched-graph clique analysis with explicit group criteria, directed density, weighted reciprocity, degree-preserving null comparisons, threshold sensitivity, and manuscript reporting.

**Architecture:** Reuse the existing cumulative subject-keyed dyads and matched membership. Add small pure helpers for clique enumeration, group metrics, directed rewiring, and threshold summaries in `citation_analysis.py`, then call one orchestration function from the existing analysis run. Generated CSV, LaTeX, and macro artifacts remain under the current revision output and are consumed by new Methods and Results text.

**Tech Stack:** Python 3, pandas, NetworkX, NumPy, existing unittest suite, SQLite-backed offline pipeline, LaTeX.

## Global Constraints

- Never use the em-dash character in code, comments, documentation, or manuscript text.
- Keep the primary graph subject-specific and keyed by `(subject, ORCID)`.
- Define a clique as a maximal clique in the positive undirected projection with minimum size 4.
- Use directed density threshold 0.75 and weighted reciprocity thresholds 0.25, 0.50, and 0.75; minimum-size sensitivity is 3, 4, and 5.
- Preserve directed in-degree and out-degree in the null rewiring and preserve the subject-level cumulative-weight multiset.
- Use seeded deterministic null draws, no new dependencies, no historical artifacts, and no database edits.
- Keep the final flag as a descriptive overlay only; do not use it to define the primary clique population.
- Source every manuscript number from generated macros or generated tables.

---

### Task 1: Add failing tests for clique definitions and null invariants

**Files:**
- Modify: `tests/test_citation_analysis.py`

**Interfaces:**
- Tests will target `clique_group_metrics`, `enumerate_subject_cliques`, and `rewire_subject_dyads` in `citation_analysis.py`.

- [ ] **Step 1: Write the failing tests**

Add these focused tests after the existing graph metric tests:

```python
    def test_clique_metrics_use_directed_density_and_weighted_reciprocity(self) -> None:
        dyads = pd.DataFrame(
            {
                "subject": ["s"] * 9,
                "citing_orcid": ["A", "B", "A", "C", "A", "D", "B", "B", "C"],
                "cited_orcid":  ["B", "A", "C", "A", "D", "A", "C", "D", "D"],
                "citation_weight": [2, 2, 3, 3, 2, 2, 1, 1, 1],
            }
        )
        result = analysis.clique_group_metrics(
            ("A", "B", "C", "D"), dyads, {"A": "Case", "B": "Control", "C": "Case", "D": "Control"}
        )
        self.assertAlmostEqual(result["directed_density"], 9 / 12)
        self.assertAlmostEqual(result["weighted_reciprocity"], 7 / 10)
        self.assertEqual(result["case_memberships"], 2)

    def test_clique_enumeration_returns_maximal_groups_at_threshold(self) -> None:
        dyads = pd.DataFrame(
            {
                "subject": ["s"] * 7,
                "citing_orcid": ["A", "A", "A", "B", "B", "C", "D"],
                "cited_orcid":  ["B", "C", "D", "C", "D", "D", "A"],
                "citation_weight": [1.0] * 7,
            }
        )
        groups = analysis.enumerate_subject_cliques(dyads, ["A", "B", "C", "D"], min_size=3)
        self.assertEqual(groups, [("A", "B", "C", "D")])

    def test_rewire_preserves_directed_degrees_and_weights(self) -> None:
        dyads = pd.DataFrame(
            {
                "subject": ["s"] * 6,
                "citing_orcid": ["A", "A", "B", "B", "C", "D"],
                "cited_orcid":  ["B", "C", "C", "D", "D", "A"],
                "citation_weight": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            }
        )
        rewired = analysis.rewire_subject_dyads(dyads, seed=7, swaps=20)
        before = dyads.groupby("citing_orcid").size().sort_index()
        after = rewired.groupby("citing_orcid").size().sort_index()
        self.assertEqual(before.to_dict(), after.to_dict())
        self.assertEqual(
            dyads.groupby("cited_orcid").size().sort_index().to_dict(),
            rewired.groupby("cited_orcid").size().sort_index().to_dict(),
        )
        self.assertEqual(sorted(dyads["citation_weight"]), sorted(rewired["citation_weight"]))
```

- [ ] **Step 2: Run the focused tests and verify the expected failure**

Run: `rtk .venv/bin/python -m unittest tests.test_citation_analysis.MetricDefinitionTests -v`

Expected: FAIL because the three clique helpers do not yet exist.

- [ ] **Step 3: Commit the tests**

```sh
rtk git add tests/test_citation_analysis.py
rtk git commit -m "test: specify formal clique metrics and null invariants"
```

### Task 2: Implement pure clique and rewiring helpers

**Files:**
- Modify: `citation_analysis.py` near the existing graph helpers

**Interfaces:**
- Produce `enumerate_subject_cliques(dyads, nodes, min_size=4) -> list[tuple[str, ...]]`.
- Produce `clique_group_metrics(clique, dyads, tier_by_orcid) -> dict[str, object]`.
- Produce `rewire_subject_dyads(dyads, seed, swaps) -> pd.DataFrame`.

- [ ] **Step 1: Implement the minimal helpers**

Use `aggregate_cumulative_dyads` and NetworkX. Build the positive undirected projection for clique enumeration. Use `nx.find_cliques`, retain maximal cliques with `len(clique) >= min_size`, and return sorted tuples. For group metrics, count positive directed arcs, compute `m / (k * (k - 1))`, and compute weighted reciprocity as the ratio of sums of pairwise minima and maxima across unordered member pairs. Keep an unavailable reciprocal denominator as `math.nan`.

For rewiring, use seeded degree-preserving directed edge swaps on a simple edge list and assign a seeded permutation of the original weights to the new directed edges. Return the same four columns as cumulative dyads. Raise `ValueError` for fewer than four nodes, fewer than three edges, or nonpositive swaps; retain a valid partial-swap state when a rigid degree sequence cannot accept every requested swap.

- [ ] **Step 2: Run the focused tests and verify they pass**

Run: `rtk .venv/bin/python -m unittest tests.test_citation_analysis.MetricDefinitionTests -v`

Expected: PASS for the three new tests and all existing metric tests.

- [ ] **Step 3: Commit the implementation**

```sh
rtk git add citation_analysis.py tests/test_citation_analysis.py
rtk git commit -m "feat: add formal clique graph helpers"
```

### Task 3: Add threshold summaries and seeded null comparisons

**Files:**
- Modify: `citation_analysis.py` near the existing analysis orchestration and artifact writers
- Modify: `tests/test_citation_analysis.py`

**Interfaces:**
- Produce `CliqueResults(summary, sensitivity)` as a frozen dataclass.
- Produce `run_clique_analysis(edges, membership, flagged_keys, seed=42, null_replicates=499, label_swaps=499) -> CliqueResults`.

- [ ] **Step 1: Write a failing orchestration test**

Add a four-node complete-projection fixture with two Case and two Control members and call `run_clique_analysis` with `null_replicates=3` and `label_swaps=3`. Assert that the primary row has `minimum_clique_size == 4`, `reciprocity_threshold == 0.50`, includes observed and null columns, and reports three threshold rows for minimum size 4. Assert that `valid_null_replicates` is positive and every empirical p-value lies in `[0, 1]`.

- [ ] **Step 2: Run the new test and verify it fails**

Run: `rtk .venv/bin/python -m unittest tests.test_citation_analysis.CliqueAnalysisTests -v`

Expected: FAIL because `CliqueResults` and `run_clique_analysis` do not yet exist.

- [ ] **Step 3: Implement the smallest orchestration**

Aggregate cumulative dyads once. For each subject, enumerate maximal cliques at minimum size 3 and cache their group metrics. Aggregate observed rows for each size and reciprocity threshold, retaining density threshold 0.75. For each null replicate, rewire eligible subject graphs with deterministic per-subject seeds and rescore the observed maximal-clique candidate memberships instead of re-enumerating a new null clique population. Calculate conditional-null means and empirical count p-values. Apply independent within-pair tier-label swaps to the observed group memberships for the Case-share p-value. Count canonical flagged members only as a descriptive share. If no qualifying group exists, retain zero counts and missing means rather than inventing a result.

- [ ] **Step 4: Run the new test and all Python tests**

Run: `rtk .venv/bin/python -m unittest tests.test_citation_analysis.CliqueAnalysisTests -v` and then `rtk make verify`.

Expected: PASS with no schema fixture regressions.

- [ ] **Step 5: Commit the orchestration**

```sh
rtk git add citation_analysis.py tests/test_citation_analysis.py
rtk git commit -m "feat: compare clique structure with seeded nulls"
```

### Task 4: Write generated clique artifacts and macros

**Files:**
- Modify: `citation_analysis.py`
- Modify: `tests/test_citation_analysis.py` only if an artifact contract test is needed

**Interfaces:**
- Extend `write_artifacts` with a `cliques: CliqueResults` argument.
- Add `write_clique_summary_table` and `write_clique_sensitivity_table` using the existing complete-table writer.
- Extend `write_result_macros` with primary clique count, null mean, empirical p, density, reciprocity, Case share, flagged share, and label-swap p macros.

- [ ] **Step 1: Add CSV, LaTeX, and macro writers**

Write `clique_summary.csv`, `clique_summary.tex`, `clique_sensitivity.csv`, and `clique_sensitivity.tex`. Keep table notes short and define the density threshold, reciprocity thresholds, maximal-clique rule, and null p-value in the generated notes. Ensure empty observed groups display numeric zero counts and `--` for unavailable means.

- [ ] **Step 2: Thread clique results through `run_analysis`**

Call `run_clique_analysis` after the canonical flags and mixing result are available. Add CLI options `--clique-null-replicates` and `--clique-label-swaps` with defaults 499, pass them through `run_analysis`, and include their values in `run_metadata.json`.

- [ ] **Step 3: Run unit tests and a writer smoke test**

Run: `rtk make verify` and a temporary-directory call to `write_artifacts` using the existing fixture data. Confirm both LaTeX tables and all clique macros are created without absolute data paths.

- [ ] **Step 4: Commit generated-writer changes**

```sh
rtk git add citation_analysis.py tests/test_citation_analysis.py
rtk git commit -m "feat: export clique summaries and manuscript macros"
```

### Task 5: Integrate Methods, Results, response, and title

**Files:**
- Modify: `main.tex`
- Modify: `response_to_reviewer.tex`

**Interfaces:**
- Consume `clique_summary` and `clique_sensitivity` generated tables and clique macros.
- Keep all quantitative values generated, with no hand-entered estimates.

- [ ] **Step 1: Add the formal Methods subsection**

Define `G_s`, `U_s`, maximal clique minimum size, directed density, weighted reciprocity, the 0.75 density threshold, the 0.50 primary reciprocity threshold, the 0.25/0.50/0.75 sensitivity grid, directed degree-preserving swaps, weight permutation, and empirical p-value. State that the canonical flag is an overlay and that a clique is a structural pattern, not evidence of intent.

- [ ] **Step 2: Add the Results tables and interpretation**

Add a formal clique subsection after tier mixing. Report the generated primary row and sensitivity table. Use the primary macros to state whether qualifying groups exceed their rewired null and whether Case membership exceeds pair-label null. Do not call the result coordination, manipulation, or misconduct.

- [ ] **Step 3: Update the reviewer response**

Replace the prior statement that all topology analysis was removed. Explain that the revision now supplies a defined maximal-clique population, directed density, weighted reciprocity, explicit thresholds, subject-stratified degree-preserving null rewiring, and label-swap enrichment. Point to the new Methods and Results tables.

- [ ] **Step 4: Update the title last**

After the analysis outputs and manuscript compile successfully, change the title to `Citation-Network Cohesion and Reciprocal Citation Cliques Across Journal-Impact Strata: A Matched Author Analysis` in the manuscript and response cover text. Keep the title structural and associational, with no allegation of misconduct.

- [ ] **Step 5: Compile and inspect**

Run the canonical analysis against the available rebuilt database when present, then compile the manuscript and response. Run `rtk qpdf --check` on all submission PDFs and search logs for undefined references, overfull boxes, stale title text, and em dashes.

- [ ] **Step 6: Commit the manuscript integration**

```sh
rtk git add main.tex response_to_reviewer.tex
rtk git commit -m "docs: report formal reciprocal clique analysis"
```

### Task 6: Final verification and handoff

**Files:**
- No new source files

- [ ] **Step 1: Run `rtk make verify`**

Expected: all SQL fixtures and Python tests pass.

- [ ] **Step 2: Verify artifact contracts**

Confirm clique sensitivity rows cover all nine size and reciprocity combinations, null replicate counts are positive where graphs are eligible, p-values are in `[0, 1]`, and no database or historical artifact is staged.

- [ ] **Step 3: Review the final diff**

Run `rtk git diff --check`, inspect `rtk git status --short`, and confirm only intended tracked files are committed.

- [ ] **Step 4: Report the title and integration state**

Tell the user the exact final title, the verification results, and that the branch is ready for their chosen push or merge command.
