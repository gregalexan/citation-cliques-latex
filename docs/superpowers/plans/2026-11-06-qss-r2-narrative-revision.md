# QSS R2 Narrative Revision Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Answer Reviewer 1 of QSS-2026-0077.R1 by turning the manuscript from a documented pipeline into a scientometric argument: concepts, reasoning, interpretation, and literature in the main text; implementation detail in a supplement.

**Design:** `docs/superpowers/specs/2026-11-06-qss-r2-narrative-revision-design.md`

**Architecture:** One analysis regeneration (Phase A) batches every table and macro change, so the build server runs `make reproduce` once. The manuscript restructure (Phase B) can start in parallel on text that does not depend on new numbers. `main.tex` remains the only prose source; a new `supplement.tex` receives moved material and shares the generated macros.

**Tech stack:** LaTeX, BibLaTeX/Biber, `citation_analysis.py`, `make verify`, `make reproduce` (build server), `latexdiff` for the marked copy, `paper-search` skill for literature verification.

## Global constraints

- Association-first. Add interpretation through mechanisms and alternative explanations, never through stronger claims about intent, misconduct, or causation.
- Every number comes from `results/revision-v1/results_macros.tex` or a generated table. No hand-copied estimates.
- No em-dash characters in any file.
- Do not edit `impact.db` or `rolap.db`. Full rebuilds run on the build server; this machine has no `build/revision-v1/rolap.db`.
- New analysis code needs pytest or RDBUnit coverage, subject-keyed joins, and only `final_flag` for flag-dependent outputs.

---

## Phase 0: Decisions before work starts

- [ ] **Decision 1, scope of new analysis.** Recommended: Tier 1 plus A5 only. Field differences use the per-subject clique results that already exist (`clique_subject_consistency.csv`); A4 and A6 are deferred unless the editor asks. Reason: the reviewer asks for less technical content, so new analysis is limited to the one check that turns a Discussion caveat into evidence.
  - Tier 1 (no new estimates): table formatting, subject names, denominators, mixing shares, prose macros. Needs a regeneration but no new statistics.
  - Tier 2 (recommended, small): per-subject primary paired outcomes (A4); clique membership with prior-coauthor dyads removed (A5). These answer "field differences", "collaboration", and "meaning of cliques" with evidence rather than prose.
  - Tier 3 (optional): team-size context (A6), descriptive only.
- [x] **Decision 2, supplement versus appendix.** Decided (2026-11-06): appendices A to H after the references in `main.tex`, one PDF. The QSS supplementary-file policy could not be verified.
- [ ] **Decision 3, page target.** Recommended: main text (Introduction to Conclusion) at most about 25 pages in the current layout, down from 51 pages in the marked R1 PDF.

## Phase A: Analysis and generated artifacts (one regeneration)

### Task A1: Name subjects in every table and figure

**Files:** `citation_analysis.py` (`write_matching_table`, clique subject outputs, any subject-indexed figure), `tests/test_citation_analysis.py`

- [ ] Find the authoritative subject code to name mapping in the snapshot or classification cache (do not infer it from order). Codes 1 to 5 currently appear as bare numbers in Table 1.
- [ ] Add one `SUBJECT_LABELS` mapping and use it in every table writer that prints a subject.
- [ ] Test: every subject-indexed table contains names and no bare codes.

### Task A2: State denominators in every generated table

**Files:** `citation_analysis.py` table writers (all route through `_write_complete_table`, line ~1885), tests

- [ ] `write_anomaly_feature_ablation_table` (R1 Table 12): replace "Enrichment" with "Case/Control flag-rate ratio"; header "Case flags (% of N screenable Cases)"; say in the note which population is the denominator (eligible versus screenable rows) and give N; define overlap in words ("share of flags common to both screens, Jaccard").
- [ ] `write_anomaly_enrichment_table`: show eligible, screenable, and flagged counts as a clear funnel with the denominator of each percentage.
- [ ] `write_paired_table`: add Case and Control medians, the number of complete pairs, and a note explaining "median difference 0 with positive rank effect" once.
- [ ] `write_clique_summary_table` and `write_clique_sensitivity_table`: spell out "Case only" and "Control only" as discordant pairs; give the pair denominator.
- [ ] Rename every "tier" label in reader-facing output to "group".
- [ ] Test: each table note contains an explicit "N =" or "of N" string.

### Task A3: Report group mixing as shares

**Files:** `write_tier_mixing_table`, `plot_tier_mixing`, tests

- [ ] Main table: row shares (percentage of each citing group's weight going to each cited group) plus row totals. Keep raw fractional weights in the CSV and supplement.
- [ ] Add a macro for the share expected from row and column totals, so the text can contrast observed with expected.

### Task A4 (Tier 2): Per-subject primary paired outcomes

**Files:** `citation_analysis.py` (`run_paired_inference` callers, new writer and figure), tests

- [ ] Run the existing paired inference within each subject for the four primary measures (same complete-pair rule, same bootstrap and seed).
- [ ] Output: `paired_primary_by_subject.csv/.tex` and a small-multiples figure (subject by measure, mean difference with CI).
- [ ] Macros: count of subjects with positive mean difference for each measure.
- [ ] Test: per-subject pair counts sum to the overall pair count for each measure; no subject exceeds its matched-pair count.

### Task A5 (Tier 2): Collaboration-aware clique check

**Files:** `citation_analysis.py` (`run_clique_analysis`, `_clique_threshold_summaries`), coauthor timing input already loaded for the coauthor rate, tests

- [ ] Repeat the primary clique rule after removing dyads whose members had collaborated before the citation (same `c_ij < t` rule used elsewhere). Implementation: edges already carry `is_coauthor_citation` per subject, dyad and year, so call `run_clique_analysis` on `edges[~edges["is_coauthor_citation"]]`; no SQL change is needed.
- [ ] Report Case and Control membership, paired difference, and exact p in the clique summary as a named sensitivity row.
- [ ] Purpose for the text: if the gap persists without collaborator links, cliques are not only research teams; if it shrinks, collaboration explains much of it. Either result is reported.
- [ ] Test fixture: a clique formed only by coauthor links disappears under the check.

### Task A6 (Tier 3, optional): Team-size context

- [ ] Descriptive median authors per eligible work for Cases and Controls, by subject, added to the matching table. Not a matching variable and not an outcome.

### Task A7: Prose macros become number macros

**Files:** `write_result_macros` (line ~2481), `main.tex`

- [ ] Stop generating long finding sentences (`\PrimaryDetailedFindingText` and similar list every estimate and CI inline, which is the "software output" voice). Generate number macros only; write the sentences in `main.tex`.
- [ ] Keep at most a short `\PrimaryFindingText` for the abstract.
- [ ] Add macros for the facts the narrative needs, for example: Case and Control medians of the coauthor-citation rate (Case median is lower although the mean difference is positive, which the text must explain), the direction of same-journal referencing (Cases lower), the share of exact-$h_5$ pairs, and the maximum observed $|\Delta h_5|$.

### Task A8: Regenerate and verify

- [ ] `make verify` locally.
- [ ] `make reproduce` on the build server; copy back `results/revision-v1/`.
- [ ] Confirm AGENTS.md invariants (unique keys, pair counts, surge share in [0, 1], `final_flag` only, no synthetic fallback) and that unchanged estimates are byte-identical to the R1 values where definitions did not change.

## Phase B: Manuscript restructure

### Task B0: Remove reviewer-reply residue from the main text

**Files:** `main.tex`

The R1 text answers round-1 comments inline. Those sentences are the clearest "software documentation" signal and belong in the response letter or supplement.

- [ ] Remove or relocate at least: `main.tex:162` ("No new venue classification was undertaken for this revision", "consumes the fixed database scores as an upstream exposure"); `main.tex:168` ("no final-character sampling rule is applied"); `main.tex:177-179` (independent audit; double join on `case_orcid`/`control_orcid`; "ORCID-only joins are not used"); `main.tex:254` ("not a state-transition burst-detection model"); `main.tex:347` (former burst-intensity feature; "revised analysis therefore reports eight distinct outcomes"); `main.tex:364-366` (nonfinite IQR replacement; "No additional feature weights ... no Gaussian assumption"); `main.tex:406` ("checked not to exceed"); `main.tex:496` (closing "we therefore make no claim that a Case--Control gap widens"); `main.tex:522` ("for this revision").
- [ ] For each removed sentence, confirm the round-1 point is still covered by one plain sentence in the main text or by the supplement, using the round-1 crosswalk table in the design note. Do not lose any round-1 fix.
- [ ] Search for similar residue: "revision", "former", "previous version", "no longer", "is not used", "rather than".

### Task B1: Create the supplement and move technical material

**Files:** new `supplement.tex`, `main.tex`, `Makefile` (add a target)

- [ ] Move to the supplement: venue-score equations (App. A); construction rules and the fractional-weight equation; the three-view population taxonomy; notation and graph definitions ($G_s$, $U_s$); the full measure formulas (keep a plain-language measure table in the main text); the zero/missing-value paragraph; the matching audit and caliper-violation sentences; the per-metric pair-count check sentence; the screen specification (App. C) and ablation table; anomaly overlap and sensitivity tables; the exact-$h_5$ table; full clique sensitivity grid and subject table; reproducibility checks (App. D); classification details (App. E).
- [ ] Main text keeps one sentence pointing to each moved item ("Supplement S3 gives the full matching audit.").
- [ ] Cross-reference with `xr-hyper`, as `response_to_reviewer.tex` already does.

### Task B2: Rewrite the Introduction around a conceptual question

**Files:** `main.tex` Introduction

- [ ] Open with the problem: author-level citation cohesion has several possible meanings (specialization, collaboration, local communities, strategic exchange), and indicators cannot separate them by structure alone.
- [ ] Explain why journal-impact groups are an informative comparison (stratification of venues, different audiences and communities), with literature.
- [ ] Keep three research questions; state expectations as mechanism-linked, association-only hypotheses.
- [ ] End with a one-paragraph preview of the findings and of what they do and do not show.

### Task B3: Rebuild Background and extend the literature

**Files:** `main.tex` Related Work (rename Background), `references.bib`

- [ ] Organize into four short themes: theories of citing (normative versus social-constructivist); collaboration, self-citation, and coauthor citation; reciprocity, citation rings, and cartels at journal and author level; specialization, invisible colleges, and journal stratification including field normalization and team size.
- [ ] Use the `paper-search` skill to find and verify each candidate (DOI, year, venue) before adding it. Do not add a reference from memory. Current bibliography has 28 entries; expect roughly 15 to 25 additions.
- [ ] Each theme ends with the gap this study fills, so the section argues rather than lists.

### Task B4: Compact Data and Methods with explicit rationale

**Files:** `main.tex` sections 3 to 7 become one section

- [ ] One subsection each: Data and groups; Matching; Measures; Reciprocal cliques; Analysis. No equations except, if useful, the clique density and reciprocity rule.
- [ ] Rationale sentences (one or two each), each pointing to evidence:
  - Eigenfactor and 25th/75th percentile cutoffs: a citation-flow venue score that excludes journal self-citation; the cutoffs leave a middle buffer so groups are clearly separated; cutoffs are subject-specific because citation levels differ by field.
  - 70% portfolio rule: an author is assigned to a group only when most of their output is in that group, while allowing some publication outside it.
  - $h_5$ matching: a subject-specific, recent productivity measure that matches authors on citation visibility in the same window as the outcomes, so differences are not just size effects.
  - Caliper of 3: an upper bound; achieved balance was much tighter (macros for exact-match share and maximum observed difference of 2), and the exact-$h_5$ subset reproduces the direction of all four primary results.
  - Clique size 4, density 0.75, reciprocity 0.50: three authors can be connected by chance or one team; four is the smallest group that is not just a triangle; density 0.75 allows a few missing links; reciprocity 0.50 means at least half the exchanged weight goes both ways. All nine size-reciprocity combinations give the same direction (supplement).
  - Screen features: the five author-link measures of cohesion and concentration; the other three measures use different populations or time and are reported separately; leaving any one out changes little (macro).
  - Control-referenced percentile: thresholds come only from Controls, so the screen asks how often Cases exceed a Control norm.
- [ ] Explain once, in plain words, why medians can be zero while effects are positive (many authors have no matched-peer links).

### Task B5: Results organized by question

**Files:** `main.tex` Results

- [ ] Four subsections: matched differences; differences across fields (existing per-subject clique results; A4 only if Decision 1 includes it); reciprocal cliques including the collaboration-aware check (A5); screen and group mixing.
- [ ] Each opens with a plain takeaway sentence, then the figure, then at most one compact table. Sensitivity summarized in one sentence with a supplement pointer.
- [ ] Report mixed signs honestly: same-journal referencing is lower for Cases; the coauthor-citation median is lower for Cases although the mean is higher. These nuances support the specialization discussion.
- [ ] Remove the "All numerical values in this section come from the same analysis" sentence and similar process statements.

### Task B6: Write an interpretive Discussion

**Files:** `main.tex` Discussion

- [ ] Summary (one paragraph, no new numbers).
- [ ] Interpreting cohesion: alternative mechanisms that produce the same structure: narrow specialties and small fields; regional, national, or language-bound journal communities; stable teams and lab lineages; editorial and reviewer networks; strategic citation exchange. State which findings fit which mechanism and what the design cannot separate.
- [ ] Group mixing: explain that high within-group citation is partly expected because groups are defined by journal sets and authors cite their own journal communities; the informative part is the contrast with the expected share.
- [ ] Screen: the Case/Control flag-rate ratio is anchored by design (Controls set the 99th percentile, so the Control rate sits near 1% or lower); the finding is that Cases exceed a Control norm more often, not the size of the ratio.
- [ ] Field differences: interpret the clique subject breakdown (and A4 if run) in terms of field citation cultures.
- [ ] Relation to prior work: journal-level anomaly detection, author-level concentration indicators, coauthor citation studies.
- [ ] Implications for scientometric practice: cohesion as context in evaluation rather than a verdict; field and group normalization of cohesion indicators; screens as a way to prioritize human review; caution against using journal-impact group as a proxy for behavior.
- [ ] **Background and Discussion crosswalk (requested in both rounds).** Build a small table in the working notes: each Background theme maps to at least one Discussion paragraph that says whether the findings agree with, extend, or complicate that literature, with the citation repeated at that point. Every Discussion paragraph cites at least one Background source. Reviewers read this link as the test of whether the paper is a contribution or a technical exercise.

### Task B7: Reframe Limitations, conceptual first

**Files:** `main.tex` Limitations

- [ ] Conceptual: what a reciprocal clique is and is not (a structural pattern that legitimate communities also produce); specialization and topical proximity not measured (five broad subjects only); journal-impact group as a proxy that bundles language, region, and audience; matched-author graph boundary (peer measures describe links among matched authors, not the full network); association only.
- [ ] Data and measurement, shortened: ORCID coverage, LLM journal classification without human validation, 2020 to 2024 window and coauthor dating, operational screen without ground truth.

### Task B8: Abstract, title check, and Conclusion

- [ ] Abstract: question, design, main findings in plain words, main interpretive caveat. Title can stay.
- [ ] Conclusion: what a QSS reader should take away for indicator use and future work (finer topic data, collaboration-aware indicators, qualitative follow-up of flagged groups).

## Phase C: Response and submission package

### Task C1: Response to Reviewer 1

**Files:** `response_to_reviewer.tex`, `scholarone_response.txt`

- [ ] Point-by-point reply to the six reviewer points, each with the change made and its location (section and, through `xr-hyper`, line or section references).
- [ ] Thank the reviewer; acknowledge that the previous version was too technical; state what moved to the supplement and why.
- [ ] Frame the change as continuity across both rounds: the round-1 corrections (matching rule, named test, Control-percentile screen, sensitivity analyses, formal clique rule) are unchanged, are summarized in the main text, and are documented in full in the supplement. Include the old-to-new location table so the reviewer can find each round-1 fix.
- [ ] Keep the letter short and conceptual. The R1 letter was itself technical; a long technical reply would repeat the problem.
- [ ] Keep numbers in the letter sourced from macros.
- [ ] Plain-text version for the ScholarOne box.

### Task C2: Marked copy and final checks

- [ ] Produce the marked manuscript with `latexdiff` against the submitted R1 source (commit `1d828e1`, matching `QSS-2026-0077-R1-marked.pdf`; tag it `qss-r1` before starting so the baseline is fixed) (the journal requires highlighted changes). Because of the restructure, also give a short section-mapping table in the response (old section to new location).
- [ ] Compile `main.tex`, `supplement.tex`, and `response_to_reviewer.tex` with `biber`; no undefined references or citations.
- [ ] `make verify`; confirm no em-dash characters (`grep -rnP "\x{2014}"` on all changed files).
- [ ] Check that no causal or accusatory wording entered (search for "manipulat", "cartel" used about the cohort, "misconduct", "cause").
- [ ] Update the Zenodo record only when the new archive version is actually created.

## Suggested order

1. Phase 0 decisions.
2. In parallel: A1 to A7 code changes with tests; B0 (residue purge), B1 (move material), B2, B3 (literature search), B4 drafting.
3. A8 regeneration on the build server.
4. B5 to B8 with final numbers.
5. Phase C.

## Status (2026-11-06)

Done (not committed):
- Phase A code (A1, A2, A3, A5, A7 macros) in `citation_analysis.py` with tests; `make verify` passes (49 tests). A4 and A6 deferred per Decision 1.
- Phase B: `main.tex` restructured (B0 to B8), technical material in Appendices A to H after the references (tables numbered by appendix, e.g. Table D1), 24 verified references added to `references.bib`.
- Diagrams: Figure 1 (study design), the reciprocal-clique concept (Section 3.4), and competing explanations (Discussion) are TikZ in `main.tex`, using the journal-neutral layout and colours first drafted with the diagram-design skill; they compile with the manuscript and use the body font.
- Plots: matplotlib uses Computer Modern (bundled `cmr10`, mathtext numbers) at 9 to 10 pt; the paired-effects plot is black with Primary and Secondary headings, and the clique plot separates the overall bars from the subject bars and wraps subject names.
- Phase C1: `response_to_reviewer.tex` and `scholarone_response.txt` rewritten for R2.
- Preview compile (scratch copy, tables re-rendered from existing CSVs): main 37 pages including appendices, letter 5; no undefined or multiply defined references.

Remaining, in order:
1. Fill `SUBJECT_LABELS` in `citation_analysis.py` with the confirmed code-to-name mapping (codes 1 to 5 are printed as numbers until then).
2. No full run. On the build server, against the existing scratch database: `make clique-check` (reads `build/revision-v1/rolap.db`, adds `tables/clique_summary_coauthor_excluded.csv` and `tables/matched_pair_h5.csv`). Copy those two files and `tables/tier_mixing_permutations.csv` into `results/revision-v1/tables/`, then run `make rerender` locally (tables, figures, and macros from saved tables; no estimate recomputed). `make reproduce` remains the full path for replication.
3. Resolve the two `TODO(after run)` comments in `main.tex` (Results 4.2 and Discussion, collaboration paragraph) using the coauthor-excluded clique estimate.
4. Tag the R1 baseline (`git tag qss-r1 1d828e1`) and produce the marked copy with `latexdiff`.
5. Recompile; rerun the AGENTS.md invariant checks.

Notes for the run:
- `make clique-check` runs only the second clique pass (prior-coauthor links removed); a full `make reproduce` runs both passes.
- A restructure this large makes `latexdiff` output mostly red. If it is unreadable, submit it anyway as the required marked copy and point the editor to the old-to-new mapping table in the response letter.
- The response letter states that subjects are named; this is true only after step 1.
