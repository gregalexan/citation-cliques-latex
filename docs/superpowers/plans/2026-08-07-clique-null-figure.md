# Formal Clique Null-Model Figure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Generate and include a two-panel vector figure that visualizes the formal reciprocal-clique graph-null and label-swap comparisons.

**Architecture:** Extend the existing `CliqueResults` value object with the primary null replicate arrays already produced in memory by `run_clique_analysis`. Add one plotting function to the existing analysis artifact writer, then include its generated PDF immediately after the primary clique table in `main.tex`. No new dependency or alternate analysis path is introduced.

**Tech Stack:** Python, NumPy, pandas, Matplotlib, LaTeX, existing versioned results directory.

## Global Constraints

- Use the canonical 500 graph-null replicates and 500 label swaps.
- Preserve deterministic seeds and serial/parallel equality.
- Use aggregate values only; never display author identifiers.
- Do not add a node-level network hairball or imply intent.
- Keep all tables and figures generated from the canonical analysis.
- Do not use em dashes.

---

### Task 1: Preserve primary null samples

**Files:**
- Modify: `tests/test_citation_analysis.py:169-215`
- Modify: `citation_analysis.py:122-126, 1010-1038`

**Interfaces:**
- `CliqueResults.null_reciprocal_clique_counts` is a one-dimensional NumPy array of valid primary graph-null counts.
- `CliqueResults.label_case_membership_shares` is a one-dimensional NumPy array of primary within-pair label-swap shares.

- [ ] **Step 1: Extend the existing clique test with sample-length assertions**

```python
self.assertEqual(len(result.null_reciprocal_clique_counts), 3)
self.assertEqual(len(result.label_case_membership_shares), 3)
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `.venv/bin/python -m unittest tests.test_citation_analysis.CliqueAnalysisTests.test_clique_analysis_reports_primary_and_sensitivity_nulls`

Expected: FAIL because `CliqueResults` does not yet expose the two arrays.

- [ ] **Step 3: Add the two arrays to `CliqueResults` and return the primary samples**

After collecting `null_values`, select the primary configuration `(4, 0.50)`. Return its first tuple element for graph-null counts and compute the primary label-share array once, reusing it for the summary row and the returned result.

- [ ] **Step 4: Run the focused test and serial/parallel tests**

Run: `.venv/bin/python -m unittest tests.test_citation_analysis.CliqueAnalysisTests`

Expected: all clique analysis tests pass, including deterministic serial and parallel equality.

### Task 2: Generate the aggregate figure

**Files:**
- Modify: `citation_analysis.py:2785-2830, 3290-3395`
- Modify: `tests/test_citation_analysis.py:169-195`

**Interfaces:**
- `plot_clique_nulls(cliques, directory)` writes `clique_nulls.pdf` and `clique_nulls.png` through `_save_figure`.
- The plot consumes only `CliqueResults` arrays and the primary summary row.

- [ ] **Step 1: Add an artifact assertion to the focused test only if the test fixture invokes the writer**

The existing unit fixture does not run the full writer, so do not add filesystem-heavy plotting tests. The array assertions from Task 1 provide the numerical guardrail.

- [ ] **Step 2: Implement the smallest two-panel plot**

Use `plt.subplots(1, 2, figsize=(7.0, 3.1), constrained_layout=True)`. Plot integer count samples in panel A and percentage-point samples in panel B. Add observed vertical lines, axis labels, short panel titles, light y-grid lines, and a shared legend entry for “Observed”. Keep the palette consistent with existing figures.

- [ ] **Step 3: Call the plot from `write_artifacts`**

Call `plot_clique_nulls(cliques, figures_directory)` beside the existing paired, anomaly, and mixing plots. Do not change any existing output names.

- [ ] **Step 4: Run the focused tests**

Run: `.venv/bin/python -m unittest tests.test_citation_analysis.CliqueAnalysisTests`

Expected: PASS.

### Task 3: Include and caption the figure

**Files:**
- Modify: `main.tex:453-462`

- [ ] **Step 1: Add a conditional figure after `clique_summary` and before `clique_sensitivity`**

Use the existing `\IfFileExists` pattern and include `\GeneratedRoot/figures/clique_nulls.pdf` at `0.9\linewidth`.

- [ ] **Step 2: Add a caption that states the two nulls and observed values**

The caption must say that panel A preserves subject-specific directed degree sequences through rewiring and panel B swaps Case and Control labels within pairs. It must state 17 reciprocal cliques, 91.6% Case membership, and that the lines are observed values.

- [ ] **Step 3: Compile after the canonical artifact rebuild**

The figure is conditional so an intermediate source build remains valid, while the canonical build must include the generated PDF.

### Task 4: Rebuild and verify

**Files:**
- Generated: `results/revision-v1/figures/clique_nulls.pdf`
- Generated: `results/revision-v1/figures/clique_nulls.png`
- Generated: `main.pdf`
- Generated: `response_to_reviewer.pdf`

- [ ] **Step 1: Run the canonical analysis with 500 replicates and four workers**

Run:

```bash
.venv/bin/python citation_analysis.py \
  --database build/revision-v1/rolap.db \
  --output-dir results/revision-v1 \
  --seed 42 \
  --clique-null-replicates 500 \
  --clique-label-swaps 500 \
  --clique-workers 4
```

- [ ] **Step 2: Build the manuscript and response**

Run `pdflatex`, `biber`, and the required final `pdflatex` passes for `main.tex`, followed by two `pdflatex` passes for `response_to_reviewer.tex`.

- [ ] **Step 3: Run verification**

Run `make verify` and scan both LaTeX logs for errors, undefined references, and overfull boxes. Confirm the figure exists and the PDF text contains the formal clique section and caption.
