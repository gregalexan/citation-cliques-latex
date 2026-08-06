# Results-First Conclusion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the current defensive conclusion with a direct, evidence-led statement of the primary matched result and its contribution.

**Architecture:** Keep the canonical analysis and generated tables unchanged. Revise one manuscript section, regenerate the clean and marked PDF deliverables, then verify the rendered conclusion and package integrity.

**Tech Stack:** LaTeX, Biber, pdfTeX, Python unittest and SQL fixture verification, qpdf, pdftotext, and pdftoppm.

## Global Constraints

- Keep all numerical claims sourced from the existing generated tables and macros.
- Modify conclusion prose only; do not change the database, cohort, metrics, tests, or inference code.
- State the four primary mean paired differences and their Benjamini-Hochberg result.
- Interpret the primary pattern as greater reciprocity, local clustering, and outgoing citation concentration.
- Include the 93.91% within-tier citation-weight result and its label-swap result below 0.001.
- Do not add a separate caveat paragraph or repeat screening limitations in the conclusion.
- Do not introduce em dashes.

---

### Task 1: Replace the conclusion with the results-first argument

**Files:**
- Modify: `main.tex:462-465`, the `Conclusion` section only.
- Read: `results/revision-v1/results_macros.tex` and the generated primary and tier-mixing tables to confirm values.

**Interfaces:**
- Consumes: Existing `Case`, `Control`, primary paired-comparison, and tier-mixing results.
- Produces: A two-paragraph conclusion that answers the research question and states the contribution without changing analysis outputs.

- [ ] **Step 1: Confirm the source values**

Run:

```bash
rtk sed -n '100,155p' results/revision-v1/results_macros.tex
rtk sed -n '1,45p' results/revision-v1/tables/paired_primary.tex
rtk sed -n '1,30p' results/revision-v1/tables/tier_mixing.tex
```

Expected: primary mean differences are 0.045, 0.129, 0.026, and 0.097; all four primary adjusted values are below 0.001; within-tier share is 93.91% with a label-swap value below 0.001.

- [ ] **Step 2: Apply the minimal prose replacement**

Replace the current conclusion paragraph with:

```tex
Within the matched author and subject cohort, Cases exhibit a coherent pattern of greater citation-network cohesion and concentration.  Their mean values exceed Controls on all four primary outcomes: coauthor-citation rate (paired mean difference 0.045), reciprocity (0.129), local clustering (0.026), and outgoing HHI (0.097); all four comparisons pass the 5\% Benjamini--Hochberg threshold.  The largest paired-ordering effects are outgoing HHI ($r_{rb}=0.580$) and reciprocity ($r_{rb}=0.468$), showing that the directional pattern is concentrated in how citations are reciprocated and distributed across recipients.

This is a selective cohesion pattern rather than a generic difference in citation volume.  Cases combine more reciprocal and locally clustered citation neighborhoods with greater concentration of outgoing citation weight.  At the sample level, 93.91\% of matched citation weight remains within tier (within-pair label-swap $p<0.001$), placing the author-level pattern in a strongly stratified citation environment.  Together, these findings support the initial directional hypothesis and identify citation-network organization as a distinguishing feature of the two venue strata.
```

- [ ] **Step 3: Check wording constraints**

Run:

```bash
rtk sed -n '/\\section{Conclusion}/,/\\section{Author Contributions}/p' main.tex | rtk grep -n -E 'coordination|intent|misconduct|incomplete|caveat'
rtk git grep -n $'\\u2014' -- main.tex
```

Expected: no output.

- [ ] **Step 4: Commit the prose change**

```bash
rtk git add main.tex
rtk git commit -m "Strengthen results-first manuscript conclusion"
```

### Task 2: Rebuild the manuscript deliverables

**Files:**
- Modify: `main.pdf` and `response_to_reviewer.pdf` through compilation.
- Modify: `QSS-2026-0077-R1-marked.pdf` through the existing marked-source build.

**Interfaces:**
- Consumes: The revised `main.tex` and existing generated tables, figures, and bibliography.
- Produces: Clean and tracked-change PDFs with the new conclusion and valid cross-references.

- [ ] **Step 1: Rebuild the clean manuscript**

Run from the manuscript directory:

```bash
rtk biber main
rtk pdflatex -interaction=nonstopmode -halt-on-error main.tex
rtk pdflatex -interaction=nonstopmode -halt-on-error main.tex
rtk pdflatex -interaction=nonstopmode -halt-on-error main.tex
```

Expected: pdfTeX completes successfully and writes `main.pdf`.

- [ ] **Step 2: Rebuild the reviewer response**

```bash
rtk biber response_to_reviewer
rtk pdflatex -interaction=nonstopmode -halt-on-error response_to_reviewer.tex
rtk pdflatex -interaction=nonstopmode -halt-on-error response_to_reviewer.tex
rtk pdflatex -interaction=nonstopmode -halt-on-error response_to_reviewer.tex
```

Expected: pdfTeX completes successfully and writes `response_to_reviewer.pdf`.

- [ ] **Step 3: Rebuild the tracked-change manuscript**

From `/tmp/QSS-2026-0077.rftvfb/new`, compile the maintained marked source:

```bash
rtk biber QSS-2026-0077-R1-marked2
rtk pdflatex -interaction=nonstopmode -halt-on-error QSS-2026-0077-R1-marked2.tex
rtk pdflatex -interaction=nonstopmode -halt-on-error QSS-2026-0077-R1-marked2.tex
rtk pdflatex -interaction=nonstopmode -halt-on-error QSS-2026-0077-R1-marked2.tex
rtk cp QSS-2026-0077-R1-marked2.pdf /home/panspan/final_indexes/citation_manipulation/QSS-2026-0077-R1-marked.pdf
```

Expected: the marked PDF contains the same results-first conclusion and no stale conclusion text.

- [ ] **Step 4: Check pagination references**

Run:

```bash
rtk pdfinfo main.pdf
rtk pdftotext -layout main.pdf /tmp/qss-main-conclusion.txt
rtk grep -n -A8 -B2 'Conclusion' /tmp/qss-main-conclusion.txt
```

Expected: the conclusion appears as two paragraphs, and any page change is reflected in the reviewer response location references before packaging.

### Task 3: Verify the result and package the change

**Files:**
- Test: repository Python tests and SQL fixtures.
- Inspect: `main.log`, `response_to_reviewer.log`, and the marked-source log.
- Stage: only the changed manuscript source and generated PDF deliverables.

**Interfaces:**
- Consumes: PDFs produced by Task 2 and the unchanged analysis verification suite.
- Produces: Verified deliverables and a clean commit containing only this revision.

- [ ] **Step 1: Run analysis and fixture verification**

```bash
rtk make verify
```

Expected: all Python tests and SQL fixtures pass.

- [ ] **Step 2: Validate PDF structure and logs**

```bash
rtk qpdf --check main.pdf
rtk qpdf --check response_to_reviewer.pdf
rtk qpdf --check QSS-2026-0077-R1-marked.pdf
rtk grep -n -E 'Warning:|undefined|Overfull|Underfull' main.log response_to_reviewer.log
```

Expected: qpdf reports no syntax or stream errors, and the log search returns no unresolved-reference or box warnings.

- [ ] **Step 3: Inspect the rendered conclusion**

Render the conclusion page with `pdftoppm` and inspect it with the image viewer. Confirm that the four estimates, the within-tier result, and the final contribution statement are legible and not clipped.

- [ ] **Step 4: Check the staged scope**

```bash
rtk git diff --check
rtk git status --short
```

Expected: only the intended manuscript source and PDF artifacts are modified; unrelated untracked analysis files remain unstaged.

- [ ] **Step 5: Commit the deliverables**

```bash
rtk git add main.tex main.pdf response_to_reviewer.pdf QSS-2026-0077-R1-marked.pdf
rtk git commit -m "Publish results-first QSS conclusion"
```

Expected: one commit contains the revised conclusion and rebuilt submission PDFs.
