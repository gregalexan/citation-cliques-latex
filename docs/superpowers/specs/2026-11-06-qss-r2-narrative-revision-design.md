# QSS R2 Narrative Revision Design

## Context

QSS-2026-0077.R1 received a major-revision decision (due 05-Nov-2027). Reviewer 1
accepts the question and data but judges the manuscript unreadable for QSS: it
reads like software documentation, interpretation is thin, methodological
choices are unmotivated, scientometric context (fields, collaboration, teams,
topics) is missing, tables are hard to decode (Table 12 named), and limitations
are technical rather than conceptual.

## Lesson from the previous round

The August 2026 "reader-facing" pass (`plans/2026-08-06-qss-reader-facing-revision-plan.md`)
polished prose but kept the structure. The reviewer still sees documentation.
This revision is therefore structural: it moves material out of the main text
and rebuilds the argument, rather than rewording sentences in place.

## Two rounds, one reviewer

The same Reviewer 1 wrote both reports. In round 1 (QSS-2026-0077, August 2026)
they asked for clarity *and* rigour: plain-language definitions, methods before
results, a Discussion tied to the literature, a coherent matching rule with a
sort order, justified outlier detection with sensitivity analyses, a named
paired test, no unsupported trajectory or hub-and-spoke claims, and explicit
graph definitions. R1 fixed the substance but put every fix into the main text,
often as a sentence that answers the reviewer ("No additional feature weights
are applied", "not a state-transition burst-detection model", "The former
normalised burst-intensity feature is not an additional outcome", "No new venue
classification was undertaken for this revision"). Round 2 now reads that
material as software documentation.

Two consequences shape this revision:

1. **Keep the rigour, relocate the detail.** Each round-1 fix stays visible in
   the main text as one plain sentence; the implementation detail and audit
   evidence move to the supplement. Moving everything out would undo what the
   reviewer accepted in round 1.
2. **The literature link has been requested twice.** Round 1 already said the
   Discussion "fails to connect the empirical results back to the literature
   review". Round 2 repeats it. This is the point most likely to decide the
   paper, so the Background and Discussion are written as a pair (see the
   crosswalk task in the plan).

### Round-1 fixes: what stays in the main text

| Round-1 concern | Main text (one sentence each) | Supplement |
|---|---|---|
| Undefined terms, metrics table (old Table 5) | Plain-language measure table: what each measure captures and on which links it is computed | Formulas, notation, missing-value rules |
| Burst intensity and Kleinberg link | Surge share described in words as the largest one-year rise toward one recipient | Formula; remove the "not a burst model" disclaimer from the main text |
| Methods mixed with results; k-means | Methods precede Results (keep); k-means stays removed | |
| Discussion not linked to literature | Background and Discussion crosswalk | |
| Matching bucket inconsistency, sort order | "Pairs differ by at most three $h_5$ points; ties are broken in a fixed order; observed maximum difference was 2" | Full order, audit, caliper checks |
| Outlier weights, four-sigma cutoff, redundancy | "Thresholds are Control percentiles, so no normality is assumed; results are stable across thresholds, seeds and feature removal; the two screen parts overlap by design" | Specification, overlap, sensitivity and ablation tables |
| Paired test unnamed | Wilcoxon signed-rank test with bootstrap intervals, named in Methods | Sign-flip check, BH details |
| "Widening gap" claim | Claim stays removed; no disclaimer sentence needed in the main text | |
| 12 of 14 versus 13 features | Eight declared measures in two families (keep, without the history) | |
| Hub-and-spoke, Louvain, graph definitions | Formal clique rule replaces the topology claim; the measure table states the graph for each measure | $G_s$, $U_s$, three link views |

## Reviewer points and responses (round 2)

| # | Reviewer point | Response |
|---|----------------|----------|
| R1.1 | Overly technical, reads like software documentation | Collapse four methods sections into one compact Data and Methods section. Move equations, the three-view population taxonomy, zero/missing rules, matching audit, caliper-violation sentences, per-metric count checks, screen specification, and reproducibility checks to an online supplement. Target roughly half of the 51-page marked PDF. |
| R1.2 | No narrative or interpretation; Discussion repeats numbers | Rewrite the Introduction around a conceptual question (what citation cohesion can mean). Rebuild Discussion around competing mechanisms and implications, with numbers stated once in Results. |
| R1.3 | Methodological choices unmotivated, sensitivity unclear | Add one rationale sentence per choice in Methods and point to existing sensitivity evidence (achieved balance, exact-$h_5$ subset, 9-cell clique grid, screen percentile, seed, and ablation runs). |
| R1.4 | No link to scientometric practice (fields, collaboration, teams, topics) | Subject-level results (named fields), a collaboration-aware clique check, team-size context, and a Discussion subsection on practice. Topical proximity becomes a stated conceptual limitation. |
| R1.5 | Tables hard to read; denominators unclear (Table 12) | Every generated table states its denominator in a header or note; mixing table reports row shares; subjects are named, not numbered; sensitivity tables move to the supplement. |
| R1.6 | Limitations only technical | Lead Limitations with conceptual limits: meaning of a reciprocal clique, specialization, journal group as a proxy, graph boundary, and Control-referenced screen design. |

## Proposed main-text outline

1. Introduction: why author-level citation cohesion matters, why compare journal-impact groups, competing readings of cohesion, three questions, one-paragraph preview of findings.
2. Background: citing as normative and social practice; collaboration and self-citation; reciprocity, citation rings and cartels; invisible colleges and specialization; journal stratification. Ends with expectations tied to mechanisms.
3. Data and Methods (compact): data and groups; matching; measures (plain-language table, no formulas); reciprocal cliques; analysis (paired comparison, Control-referenced screen, group mixing), each with its rationale.
4. Results, one subsection per question. Each opens with a one-sentence takeaway, then a figure, then at most one compact table.
5. Discussion: summary; interpreting cohesion (alternative mechanisms); field differences; relation to prior work; implications for scientometric practice.
6. Limitations: conceptual first, then data and measurement in brief.
7. Conclusion.

Supplement: venue scores; construction and missing-value rules with formulas;
matching specification and audit; screen specification and sensitivity;
full clique sensitivity grid and subject breakdown; reproducibility checks;
journal classification.

## Constraints

- Association-first. More interpretation comes from mechanisms and alternative explanations, not stronger claims. No intent, misconduct, or causal language.
- All numbers come from `results/revision-v1/results_macros.tex` or generated tables. New magnitude words in prose ("about one in ten") need a macro or qualitative phrasing.
- Any new analysis follows AGENTS.md: subject-keyed joins, RDBUnit/pytest coverage, one regeneration via `make reproduce` on the build server (the scratch database is not on this machine).
- No em-dash characters anywhere, including response letters and this plan.
