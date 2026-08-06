# QSS Reader-Facing Revision Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the QSS manuscript readable for a broad audience while preserving the generated analysis, qualified directional findings, and exact reproducibility definitions.

**Architecture:** Keep `main.tex` as the only manuscript prose source and retain generated macros and tables as the numerical source of truth. Use plain-language framing in the main text, with equations and implementation parameters retained in the appendix where they are needed for auditability. Regenerate every tracked PDF from the revised sources and verify both clean and marked artifacts.

**Tech Stack:** LaTeX, BibLaTeX/Biber, `make verify`, `pdflatex`, `qpdf`, and the existing generated results under `results/revision-v1/`.

## Global Constraints

- Do not change analysis code, generated numerical values, cohort membership, or statistical conclusions.
- Do not use em dashes in prose or source text.
- Replace repeated “incomplete coverage” wording with one neutral scope statement tied to the fixed snapshot and stated inclusion criteria.
- Do not erase factual scope boundaries or imply causal, integrity, or universal author-level claims.
- Leave the external Zenodo record unchanged until a new version is actually created.
- Stage only intended tracked manuscript, response, generated PDF, and plan/spec files; leave existing untracked artifacts untouched.

---

### Task 1: Rewrite the abstract and introduction signposts

**Files:**
- Modify: `main.tex:113-137`

**Interfaces:**
- Consumes: generated macros such as `\MatchedPairCount`, `\PrimaryFindingText`, and `\AnomalyFindingText`.
- Produces: an abstract that states the research question, matched design, primary directional result, zero-inflation qualification, and non-causal interpretation before method detail.

- [ ] **Step 1: Replace the abstract with a reader-first version.**

Use this structure, keeping the existing generated macros for values:

```tex
We ask whether authors concentrated in lower-impact journal strata show different citation-network patterns from matched authors concentrated in higher-impact strata. Using a fixed 2020--2024 Crossref snapshot, we compare \MatchedPairCount\ author--subject pairs matched within subject and on recent citation productivity. Cases had higher average values on all four primary measures, although three median paired differences were zero because the outcomes were highly tied. We therefore interpret the result as a directional distributional association, not a universal increase for every author. A separate descriptive screen identifies unusual profiles for follow-up; it does not establish coordination, manipulation, or misconduct.
```

Remove the abstract's inventory of fractional endpoints, sign flips, graph populations, and library implementation details.

- [ ] **Step 2: Keep the hypothesis and labels, but explain them in ordinary language.**

Retain the explicit four-measure directional hypothesis. Add one sentence that Case and Control are labels for the lower and higher venue strata, not judgments about author quality.

- [ ] **Step 3: Compile only the clean manuscript once to catch prose or macro errors.**

Run:

```bash
rtk pdflatex -interaction=nonstopmode -halt-on-error main.tex
```

Expected: exit code 0; warnings may remain until the full regeneration task.

---

### Task 2: Add plain-language method signposts and move mechanics to the appendix

**Files:**
- Modify: `main.tex:151-240`
- Modify: `main.tex:513-549`

**Interfaces:**
- Consumes: current operational definitions and generated audit outputs.
- Produces: a main Methods narrative that explains purpose before notation while preserving exact formulas in the appendix.

- [ ] **Step 1: Simplify the venue-score explanation in the main text.**

Replace the two displayed upstream Eigenfactor equations with a short explanation: the fixed journal scores pass influence from citing journals to cited journals, exclude journal self-citations, and use article counts for redistribution and restart. State that the scores are an upstream exposure and are not recomputed in this study. Move the exact equations and symbols to the appendix.

- [ ] **Step 2: Make matching reader-facing before retaining audit detail.**

Lead with: “We pair lower-stratum and higher-stratum author--subjects within the same subject when their recent citation-productivity scores differ by at most three points.” Keep the deterministic order and no-replacement rule in one sentence. Move pair hashes, materialized-table language, and audit-file mechanics to the matching appendix paragraph.

- [ ] **Step 3: Add a plain-language graph paragraph before the equations.**

Use: “For the network analysis, authors are nodes and citations are directed weighted edges. Some measures follow each author’s outgoing citations to any identified recipient; others use only edges between matched authors. Direction is ignored only for the binary neighbor graph used for local clustering and weak components.” Keep the formal definitions immediately afterward.

- [ ] **Step 4: Shorten the metric notes without losing operational detail.**

Keep each formula, denominator, analytic population, and zero or missing rule. Replace phrases such as “capped dyad by dyad” and “nonblank normalized ISSN” with plain descriptions in the interpretation column. Define HHI as a 0-to-1 concentration measure in the table note.

- [ ] **Step 5: Compress the anomaly-screen narrative.**

Keep the rationale for the five features, the Control-referenced 99th-percentile rule, the overlapping cohesion restriction, and the sensitivity analyses. Move tree counts, library score details, IQR fallback mechanics, and seed implementation details to the appendix. Describe Isolation Forest once as repeated random partitioning in which quickly isolated profiles receive higher departure scores.

- [ ] **Step 6: Preserve the exact venue-score specification in the appendix.**

Add an appendix subsection immediately before `Matching and Sensitivity Specifications` that retains the current definitions of `Z`, `H`, `a`, `d`, `\alpha`, and `\pi`, followed by the sentence that received influence is normalized within subject. This keeps the reviewer-requested directed-network definition available without making the main narrative carry the derivation.

---

### Task 3: Reframe results, discussion, conclusion, and scope wording

**Files:**
- Modify: `main.tex:361-490`

**Interfaces:**
- Consumes: unchanged generated result macros and tables.
- Produces: a substantive results narrative that readers can understand before reading the tables.

- [ ] **Step 1: Insert a plain-language results lead before the generated tables.**

Add:

```tex
Across the matched pairs, Cases had higher average values on all four primary measures. Three median differences were zero because many pairs tied at zero, so the primary evidence describes a distributional shift rather than a uniform increase for every author.
```

- [ ] **Step 2: Translate statistical labels when first used.**

Retain exact terms such as Wilcoxon, rank-biserial effect, assortativity, and label-swap test in tables or parenthetical explanations, but give the reader the plain meaning first: paired ordering, within-tier citation share, and a comparison created by swapping tier labels within each pair.

- [ ] **Step 3: Rewrite the Discussion opening and conclusion.**

Lead with the observed directional association and its zero-inflated qualification. Remove the pipeline inventory from the first and last paragraphs. Keep the distinction between association, screening, and evidence of intent.

- [ ] **Step 4: Replace repeated “incomplete coverage” wording.**

Use one neutral sentence in the limitations or data-availability discussion: “The estimates apply to the fixed 2020--2024 snapshot and the author--subject observations that meet the stated inclusion criteria.” Remove the same phrase from the conclusion and repeated discussion caveats. Retain non-coverage limitations that concern subject granularity, matching scope, and causal interpretation.

- [ ] **Step 5: Align the response letter with the actual revision.**

Update `response_to_reviewer.tex` only if its statement that every method has a verbal interpretation is no longer accurate after the edits. Do not add new claims about validation or coverage.

---

### Task 4: Regenerate and verify manuscript artifacts

**Files:**
- Regenerate: `main.pdf`
- Regenerate: `response_to_reviewer.pdf`
- Regenerate: `QSS-2026-0077-R1-marked.pdf`

**Interfaces:**
- Consumes: revised `main.tex`, `response_to_reviewer.tex`, current generated tables, and the existing marked source workflow.
- Produces: clean and marked PDFs with matching reader-facing prose and current tables.

- [ ] **Step 1: Run the canonical verification suite.**

Run:

```bash
rtk make verify
```

Expected: all existing SQL and Python tests pass.

- [ ] **Step 2: Compile the clean manuscript and response.**

Run the repository's existing LaTeX targets, then check:

```bash
rtk qpdf --check main.pdf
rtk qpdf --check response_to_reviewer.pdf
```

Expected: both checks report no syntax or stream encoding errors.

- [ ] **Step 3: Regenerate the marked manuscript from the current marked source.**

Use `/tmp/QSS-2026-0077.rftvfb/new/QSS-2026-0077-R1-marked2.tex` when it is present. Apply the same prose changes and current generated tables, run `rtk biber QSS-2026-0077-R1-marked2` followed by three `rtk pdflatex -interaction=nonstopmode -halt-on-error QSS-2026-0077-R1-marked2.tex` passes in that directory, run `rtk qpdf --check QSS-2026-0077-R1-marked2.pdf`, and copy that verified PDF to `QSS-2026-0077-R1-marked.pdf`. If the temporary source is absent, stop before overwriting the marked PDF and report that the marked source must be recreated from the prior revision source.

- [ ] **Step 4: Verify reader-facing text and stale wording.**

Run:

```bash
rtk pdftotext -layout main.pdf main-review.txt
rtk grep -n -E 'incomplete coverage|proof of technical capability|normalised burst intensity|hub-and-spoke|Louvain|k-means' main-review.txt
```

Expected: no stale claims; the abstract and results lead contain the new plain-language wording.

- [ ] **Step 5: Inspect the staged file list and commit only intended changes.**

Run:

```bash
rtk git diff --check
rtk git status -sb
```

Stage only the revised manuscript sources, response source, regenerated PDFs, and this plan if it is not already committed. Do not stage the pre-existing untracked data and scripts.
