# Null-Model Runtime Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce formal clique null-analysis runtime while preserving the observed groups, seeded null draws, and reported statistics.

**Architecture:** Prepare subject edge arrays and fixed candidate-clique pair codes once. Score each independent graph-null replicate from those arrays in a deterministic worker process, then combine results in replicate order. Keep the existing Pandas and NetworkX pipeline outside the hot loop.

**Tech Stack:** Python standard library `ProcessPoolExecutor`, NumPy, existing Pandas, NetworkX, SciPy, and scikit-learn.

## Global Constraints

- Keep 500 graph-null replicates and 500 within-pair label swaps.
- Preserve the conditional null on observed maximal-clique candidate memberships.
- Preserve deterministic seed derivation and add-one empirical p-values.
- Do not add Polars or another dependency unless a benchmark proves table aggregation is the bottleneck.
- Do not stage generated PDFs, databases, historical directories, or unrelated files.
- Do not use em dashes in code, documentation, or manuscript prose.

---

### Task 1: Add a failing serial-versus-parallel regression test

**Files:**
- Modify: `tests/test_citation_analysis.py`

**Interfaces:**
- Consumes: `CliqueAnalysisTests.clique_fixture()` and `analysis.run_clique_analysis`.
- Produces: A regression test requiring a `workers` argument and exact serial/parallel summary equality.

- [ ] **Step 1: Write the failing test**

```python
    def test_clique_null_serial_and_parallel_results_match(self) -> None:
        edges, membership = self.clique_fixture()
        serial = analysis.run_clique_analysis(
            edges,
            membership,
            flagged_keys=set(),
            seed=17,
            null_replicates=5,
            label_swaps=5,
            workers=1,
        )
        parallel = analysis.run_clique_analysis(
            edges,
            membership,
            flagged_keys=set(),
            seed=17,
            null_replicates=5,
            label_swaps=5,
            workers=2,
        )
        pd.testing.assert_frame_equal(serial.summary, parallel.summary)
        pd.testing.assert_frame_equal(serial.sensitivity, parallel.sensitivity)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `rtk .venv/bin/python -m unittest tests.test_citation_analysis.CliqueAnalysisTests.test_clique_null_serial_and_parallel_results_match`

Expected: FAIL because `run_clique_analysis` does not yet accept `workers`.

### Task 2: Implement compact deterministic clique-null scoring

**Files:**
- Modify: `citation_analysis.py` near the existing clique helpers at lines 290-760.
- Test: `tests/test_citation_analysis.py` from Task 1.

**Interfaces:**
- Consumes: cumulative subject dyads, observed clique rows, and the existing seed formula.
- Produces: `_CliqueNullSubject`, `_CliqueNullContext`, `_prepare_clique_null_context`, `_run_clique_null_replicate`, and worker initializer helpers.

- [ ] **Step 1: Add compact context dataclasses and array preparation**

Store integer endpoint arrays, the subject weight array, flattened forward and reverse candidate pair codes, candidate offsets, clique sizes, fixed Case counts, and fixed flagged counts. Build the context once from `aggregate_cumulative_dyads` and `observed_rows`.

- [ ] **Step 2: Add the failing compact-scoring invariant test**

```python
    def test_compact_clique_null_scoring_preserves_subject_weights(self) -> None:
        edges, membership = self.clique_fixture()
        observed = analysis._clique_rows(edges, membership, set())
        context = analysis._prepare_clique_null_context(edges, membership, observed)
        subject = context.subjects[0]
        original_weights = sorted(subject.weights.tolist())
        result = analysis._run_clique_null_replicate(context, 0, 17)
        self.assertTrue(result.valid)
        self.assertEqual(original_weights, sorted(result.subject_weights[0].tolist()))
```

- [ ] **Step 3: Run the focused test to verify the expected missing interface**

Run: `rtk .venv/bin/python -m unittest tests.test_citation_analysis.CliqueAnalysisTests.test_compact_clique_null_scoring_preserves_subject_weights`

Expected: FAIL because the compact context and replicate result do not yet exist.

- [ ] **Step 4: Implement array rewiring and lookup scoring**

Use the existing `random.Random` seed derivation and swap rules on integer endpoint pairs. Shuffle only the copied weight array. Sort encoded directed pairs once per subject draw, use `numpy.searchsorted` for flattened candidate pairs, and aggregate candidate segments with `numpy.add.reduceat`. Return fixed configuration-order summary arrays and the rewired weight arrays needed by the invariant test.

- [ ] **Step 5: Run focused tests**

Run: `rtk .venv/bin/python -m unittest tests.test_citation_analysis.CliqueAnalysisTests.test_compact_clique_null_scoring_preserves_subject_weights tests.test_citation_analysis.CliqueAnalysisTests.test_clique_null_serial_and_parallel_results_match`

Expected: PASS.

### Task 3: Parallelize independent graph-null replicates

**Files:**
- Modify: `citation_analysis.py` in `run_clique_analysis` and module imports.
- Test: `tests/test_citation_analysis.py` from Task 1.

**Interfaces:**
- Consumes: compact context and deterministic replicate worker from Task 2.
- Produces: `run_clique_analysis(..., workers=1)` serial mode and process-parallel mode for `workers > 1`.

- [ ] **Step 1: Add process-pool initializer and ordered collection**

Use `ProcessPoolExecutor(max_workers=workers, initializer=..., initargs=(context, configurations, seed))`. Map replicate indices in ascending order so null-value accumulation and empirical p-values remain deterministic. Return invalid replicates exactly as the existing path does.

- [ ] **Step 2: Replace the per-replicate DataFrame and dictionary path**

Keep observed rows and label swaps unchanged. Replace only the graph-null loop with compact worker summaries. Preserve existing sensitivity columns, valid-replicate counts, and empirical upper-tail calculations.

- [ ] **Step 3: Run the full Python test suite**

Run: `rtk make verify`

Expected: 36 or more Python tests pass and all SQL fixtures pass.

### Task 4: Apply 500-replicate configuration and manuscript explanation

**Files:**
- Modify: `citation_analysis.py` constants, `run_analysis`, parser, and metadata.
- Modify: `main.tex` clique-null methods paragraph.
- Modify: `docs/superpowers/specs/2026-08-06-formal-clique-analysis-design.md` and `docs/superpowers/plans/2026-08-06-formal-clique-analysis.md` references from 499 to 500.
- Test: `tests/test_citation_analysis.py` constant and metadata checks if needed.

**Interfaces:**
- Consumes: Task 3 worker configuration.
- Produces: 500 graph-null and 500 label-swap defaults, plus manuscript text explaining that 500 is a precision/runtime compromise under the add-one rule.

- [ ] **Step 1: Change defaults and pass worker count through the CLI**

Set `CLIQUE_NULL_REPLICATES = 500` and `CLIQUE_LABEL_SWAPS = 500`. Add `DEFAULT_CLIQUE_WORKERS = min(4, os.cpu_count() or 1)`, pass `clique_workers` through `run_analysis`, and expose `--clique-workers` with that default. Set `IsolationForest(n_jobs=-1)`.

- [ ] **Step 2: Update manuscript and design records**

Replace 499 with 500 and state that the add-one rule gives a minimum attainable empirical p-value of `1/501`, while 500 draws balance resolution and runtime. Do not imply that 500 is a theoretical requirement.

- [ ] **Step 3: Run the full test suite and manuscript checks**

Run: `rtk make verify`

Expected: all tests and SQL fixtures pass; no em dash appears in changed prose.

### Task 5: Benchmark and retain only a material speedup

**Files:**
- Modify: `citation_analysis.py` only if benchmark exposes a correctness or performance issue.

- [ ] **Step 1: Benchmark current and optimized paths on the fixture**

Run a 49-replicate analysis with `workers=1` and `workers=2` using the existing fixture, recording wall-clock time and exact output equality.

- [ ] **Step 2: Retain the optimization if it materially reduces wall-clock time**

Use the optimized path for the next canonical 500-replicate run only if output equality holds and runtime improves without an unacceptable memory increase.

- [ ] **Step 3: Verify repository scope**

Run: `rtk git diff --stat` and `rtk git status --short`.

Expected: only the intended source, test, documentation, and manuscript files are staged or committed; generated run outputs remain unstaged.
