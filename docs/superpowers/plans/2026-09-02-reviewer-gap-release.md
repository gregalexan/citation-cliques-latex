# Reviewer-Gap Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align the full-cohort manuscript with every claim in the reviewer response, verify release v4.0.0, push it, and publish its archive on Zenodo.

**Architecture:** Keep the completed analysis and its estimates unchanged. Make focused LaTeX and generated-prose corrections, regenerate the release documents through the existing workflow, and publish the already established archive layout.

**Tech Stack:** Python 3, unittest, LaTeX, biber, Make, Git, Zip, Zenodo REST API.

## Global Constraints

- Do not modify `impact.db` or `rolap.db`; use only the fresh scratch database under `build/revision-v1/`.
- Keep all claims associational and do not infer coordination, intent, manipulation, or misconduct.
- Do not change matching, metrics, clique definitions, thresholds, or numerical estimates.
- Do not add dependencies or remote calls to the canonical offline workflow.
- Do not use Unicode em dashes.

---

### Task 1: Clarify generated primary-result prose

**Files:**
- Modify: `tests/test_citation_analysis.py:900`
- Modify: `citation_analysis.py:2558`

**Interfaces:**
- Consumes: `write_artifacts(...)` and its primary-result DataFrame.
- Produces: `\PrimaryFindingText` with the phrase "all four comparisons were significant after adjustment" when all four adjusted comparisons are significant.

- [ ] **Step 1: Tighten the artifact-contract assertion**

```python
self.assertIn("all four comparisons were significant after adjustment", macros)
self.assertNotIn("4 of four comparisons", macros)
```

- [ ] **Step 2: Run the focused test and confirm the old prose fails**

Run: `rtk .venv/bin/python -m unittest tests.test_citation_analysis.ArtifactTests -v`

Expected: FAIL because the generated macro still contains `4 of four comparisons`.

- [ ] **Step 3: Generate natural count-aware prose**

When `significant_primary == len(primary)`, emit `all four comparisons were significant after adjustment`; otherwise retain the numeric count form.

- [ ] **Step 4: Run the focused test again**

Run: `rtk .venv/bin/python -m unittest tests.test_citation_analysis.ArtifactTests -v`

Expected: PASS.

---

### Task 2: Close reviewer-facing manuscript gaps

**Files:**
- Modify: `main.tex:138-142,340-370,430-490`
- Modify: `response_to_reviewer.tex:50-145` only if wording must be aligned with the manuscript.

**Interfaces:**
- Consumes: existing full-cohort result macros and formal clique definitions.
- Produces: reviewer-facing prose and a study-design figure that include the clique branch, Control-based screen preprocessing, feature rationale, and the Kojaku comparison.

- [ ] **Step 1: Align the research question and pipeline figure**

Add reciprocal-clique membership to the first research question. Add a third downstream node labeled `Reciprocal-clique analysis` to Figure 1, connected from the subject-keyed citation construction node.

- [ ] **Step 2: State anomaly preprocessing and feature rationale**

State that each screen input is centered by the subject-specific Control median and divided by the Control IQR, with a nonfinite or zero IQR replaced by 1 only to avoid division by zero. Explain that the five inputs describe author-edge cohesion or recipient concentration, while journal endogamy uses work references, balance requires internal incident weight, and surge share is temporal.

- [ ] **Step 3: Add the promised literature comparison**

Add one Discussion paragraph explaining that Kojaku et al. detect anomalous journal groups against network expectations, whereas this study compares author-level matched membership under a declared structural rule. State that the estimates are conceptually related but not numerically interchangeable and do not establish misconduct.

- [ ] **Step 4: Check prohibited and stale claims**

Run: `rtk grep -nE "hub-and-spoke|Louvain|widening gap|4 of four" main.tex`

Expected: no matches.

---

### Task 3: Update release metadata

**Files:**
- Modify: `references.bib:231-239`
- Modify: `main.tex:511-515`
- Modify: `README.md:382-385`

**Interfaces:**
- Consumes: stable Zenodo concept DOI `10.5281/zenodo.19786936` and archive name `citation-cliques-v4.0.0.zip`.
- Produces: v4.0.0 citation metadata and an availability statement that remains accurate after deposition.

- [ ] **Step 1: Update the dataset citation**

Change the dataset title to the current manuscript title and the version to `v4.0.0`; cite the stable all-versions DOI `10.5281/zenodo.19786936`.

- [ ] **Step 2: Update availability wording**

State that the v4.0.0 software and aggregate replication materials are available on Zenodo, while the fixed raw snapshot is not redistributed.

- [ ] **Step 3: Check release identifiers**

Run: `rtk grep -nE "v3\.0\.0|will be deposited" main.tex references.bib README.md scholarone_response.txt`

Expected: no stale v3 or prospective-deposition wording in current release materials.

---

### Task 4: Regenerate and verify the release documents

**Files:**
- Regenerate: `results/revision-v1/results_macros.tex`
- Regenerate: `main.pdf`
- Regenerate: `response_to_reviewer.pdf`
- Regenerate: `QSS-2026-0077-R1-marked.pdf`

**Interfaces:**
- Consumes: completed full-cohort scratch database and corrected source files.
- Produces: three warning-free PDFs whose values come from generated artifacts.

- [ ] **Step 1: Run the canonical verification and reproduction**

Run: `rtk make reproduce`

Expected: all SQL fixtures and 46 Python tests pass, the full-cohort analysis completes and reports 102,626 pairs, and the clean manuscript and response letter compile.

- [ ] **Step 2: Regenerate the marked manuscript**

Use the existing additions-only marked-source workflow in `/tmp/QSS-2026-0077.rftvfb/`, refreshing its `new/main.tex`, generated tables, figures, and bibliography from the verified workspace before compiling `QSS-2026-0077-R1-marked.pdf`.

- [ ] **Step 3: Scan compilation logs and PDF text**

Run: `rtk grep -nE "LaTeX Warning|undefined references|Citation .* undefined|Reference .* undefined" main.log response_to_reviewer.log`

Expected: no matches.

Run: `rtk pdftotext main.pdf -`

Expected: the text contains the full-cohort clique result and none of the stale phrases checked above.

---

### Task 5: Rebuild, verify, and publish the archive

**Files:**
- Regenerate: `citation-cliques-v4.0.0.zip`

**Interfaces:**
- Consumes: the existing v4 archive layout and all verified workspace artifacts.
- Produces: a self-contained archive, a pushed Git branch, and a published Zenodo v4 record.

- [ ] **Step 1: Rebuild the archive without changing its layout**

Extract the current archive to a fresh temporary directory, overlay every included source, result, and PDF from the verified workspace, then create a replacement archive at `/tmp/citation-cliques-v4.0.0.zip`.

- [ ] **Step 2: Verify from an extracted copy**

Run `make verify` inside a fresh extraction with `PYTHON` set to the repository virtual environment. Confirm 46 Python tests and every SQL fixture pass. Compare the archive's key PDFs and result tables byte-for-byte with the workspace versions.

- [ ] **Step 3: Review and commit only intended release files**

Run: `rtk git diff --check`

Run: `rtk git status --short`

Stage the canonical source, tests, generated tables and figures, three PDFs, archive, and release documentation. Exclude preserved databases, caches, scratch output, historical directories, and unrelated untracked files.

- [ ] **Step 4: Push without force**

Run: `rtk git push origin agent/qss-response-package`

Expected: the remote branch advances to the verified release commit.

- [ ] **Step 5: Publish Zenodo v4.0.0**

Use the authenticated Zenodo new-version API for record `21720914`, upload `citation-cliques-v4.0.0.zip`, set the version to `v4.0.0`, publish the draft, and record the returned version DOI and URL. Do not alter the canonical offline workflow or store the access token in the repository.
