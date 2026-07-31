# Citation-Network Cohesion Across Journal-Impact Strata

This directory contains the reproducible data and analysis pipeline for the
matched author study.  The analysis compares author--subject observations from
low- and high-impact journal strata; it does not infer intent or adjudicate
research misconduct.

The authoritative workflow is offline.  It performs no ORCID lookup and makes
no network or API request.  It reads the fixed 2020--2024 Crossref snapshot,
rebuilds the subject-keyed derived tables, runs tests and analysis, and compiles
the manuscript from generated LaTeX macros.

## Canonical workflow

Prerequisites are SQLite, `rdbunit`, Python 3.11 or later, the packages declared
in `pyproject.toml`, and a LaTeX installation with `biber`.  The raw Crossref
snapshot is `impact.db`.  The released `rolap.db` preserves the retained
cohort, its matching covariate, and legacy derived tables; the canonical builder
reads only `author_matched_pairs` and `author_subject_h5_index` from it.
Corrected tables are rebuilt from scratch in `build/revision-v1/rolap.db`.
The released database contains ORCID-linked author records and descriptive
legacy metrics, none of which constitutes a finding of misconduct.
Software is licensed under the [MIT License](LICENSE); the database and its
source-data conditions are documented in [DATA_LICENSE.md](DATA_LICENSE.md).

```sh
make reproduce
```

Paths and the fixed detector seed can be overridden without editing source:

```sh
make reproduce \
  MAINDB=/absolute/path/to/impact \
  COHORT_DB=/absolute/path/to/retained-rolap.db \
  ANALYSIS_DB=/absolute/path/to/scratch-rolap.db \
  RESULTS_DIR=results/revision-v1 \
  SEED=42
```

`MAINDB` is specified without the `.db` suffix.  `COHORT_DB` and `ANALYSIS_DB`
include their suffixes and must be different paths.  The analysis CLI accepts
any corrected derived-database path and can also be run directly:

```sh
.venv/bin/python citation_analysis.py \
  --database build/revision-v1/rolap.db \
  --output-dir results/revision-v1 \
  --seed 42
```

The scratch database alone can be rebuilt with:

```sh
.venv/bin/python rebuild_analysis_database.py \
  --raw-database impact.db \
  --cohort-database rolap.db \
  --output-database build/revision-v1/rolap.db \
  --force
```

The builder validates the retained cohort before copying it, writes to a
temporary `.building` database, validates edge keys and surge bounds, and moves
the database into place only after a successful run.

The stable output root is `results/revision-v1/`.  A completed run writes:

- `results_macros.tex`, the sole source for numbers stated in `main.tex`;
- `matching_audit.txt`, the immutable rematching equality and caliper audit;
- `author_features_final.csv`, containing the declared feature and screen fields;
- `tables/`, including paired estimates, matching balance, detector/confirmation
  overlap (`anomaly_overlap`), threshold/seed and leave-one-feature-out
  sensitivity (`anomaly_feature_ablation`), weighted mixing, and outlier
  components;
- `figures/`, containing only figures built from the same generated feature and
  flag tables; and
- `run_metadata.json`, recording the seed, source database, schema checks, and
  output version.

The directories `latest/`, `analysis_output/`, `analysis_results_v4/`,
`publication_figures_v4/`, and the repository-level `figures/` directory are
historical artifacts.  They are non-authoritative and are not read by the
canonical workflow or manuscript.

## Study design

The primary cohort contains 9,431 pairs matched within five subjects.  The
analysis key is always `(ORCID, subject)`.  An ORCID occurring in more than one
subject is therefore more than one observation, and every join that affects an
estimate includes the subject.

The retained venue strata are fixed inputs.  Their preserved implementation
uses a directed citing-journal-to-cited-journal calculation, removes
within-journal citations, normalizes outgoing citation columns, allocates
dangling mass and the 15% teleportation term by journal article counts, uses
0.85 damping, and normalizes received influence to sum to 100 within subject.
The intermediate journal-edge and article-count exports are unavailable, so the
current workflow consumes the fixed database scores rather than independently
reproducing or verifying their end-to-end calculation.

After journal scores are attached to eligible 2020--2024 works, the retained
25th and 75th percentile cutoffs are calculated over positive-score work rows
within subject.  The cutoffs are therefore publication weighted, not quartiles
over distinct journals.  Journals at or below the lower cutoff and at or above
the upper cutoff define the two venue strata.

The fixed design proceeds as follows:

1. Classify journals with subject-specific, publication-weighted Eigenfactor
   score cutoffs.
2. Classify an author--subject portfolio as Case when at least 70% of eligible
   works are in the lower stratum and as Control when at least 70% are in the
   upper stratum (minimum three works).
3. Restrict the retained matching pool to positive `h5`, then match Case and
   Control observations without replacement within subject under the inclusive
   hard caliper `|h5_case - h5_control| <= 3`.
4. Reconstruct fractional author citation edges for citing works that belong to
   the matched subject.
5. Compute the eight declared metrics on the appropriate graph population.
6. Perform paired inference and a Control-referenced anomaly screen.
7. Describe components induced by the single final flag and compare fractional
   tier mixing under within-pair label swaps.

Steps 1--3 define the retained input cohort; the revision rebuilds Steps 4--7
from the raw snapshot.  Exact-`h5` pairs form a sensitivity cohort, not a
replacement sample.

Here `h5` is the largest integer `h` for which at least `h` of an author's
2020--2024 works in the subject are each referenced at least `h` times in the
fixed snapshot; it can be zero.  The retained upstream table materializes only
positive values, excluding otherwise tier-eligible author--subjects with
`h5 = 0` before candidate generation.

Within that positive-`h5` pool, candidate pairs are ordered by subject,
ascending absolute `h5` distance, Case ORCID, and Control ORCID.  Greedy
traversal accepts a candidate only when neither author--subject has already
been used.  Before an existing cohort is retained, its keys and positive `h5`
values are joined back to the source profiles and validated against the same
no-replacement and hard-caliper rules.  The matching balance and caliper audit
are generated by the workflow rather than asserted as fixed prose values.

The retained cohort can be checked without modifying the source database:

```sh
.venv/bin/python match_authors.py --audit \
  --audit-output results/revision-v1/matching_audit.txt rolap.db
```

This immutable, read-only audit reconstructs the greedy match from profiles and
materialized positive `h5` values, compares pair order and membership, prints
both SHA-256 hashes and caliper diagnostics, and exits nonzero on disagreement.
It does not rematch the excluded zero-`h5` profiles.

## Citation construction

Each resolved citing-work/cited-work reference is deduplicated before author
expansion.  If the citing work has `n_c` author rows and the cited work has
`n_a` author rows, every identified author endpoint receives fractional weight

```text
1 / (n_c * n_a).
```

All author rows contribute to `n_c` and `n_a`, including rows without an ORCID.
Only identified endpoints become analytic edges; a missing ORCID can never
become a node.  Yearly edges are unique on
`(subject, citing_orcid, cited_orcid, citation_year)`.

A cited author is a prior coauthor only if the first joint publication year is
strictly earlier than the citation year.  The table also records a same-year
sensitivity flag using `first_collaboration_year <= citation_year`.

Three analytic populations are deliberately distinct:

- The **outgoing ego layer** contains citations from a matched author--subject
  observation to any identified author.  It is used for self-citation,
  prior-coauthor citation, outgoing concentration, and annual dyadic surge.
- The **subject-specific matched-author induced graph** retains edges whose two
  endpoints both belong to the matched cohort in that subject.  It is used for
  reciprocity, local clustering, within-sample flow balance, components, and
  tier mixing.
- The **deduplicated work-reference layer** precedes author expansion and is
  used for journal endogamy.

For subject `s`, let `w[i,j]` be cumulative fractional citation weight and
`V[s]` the matched authors.  The induced graph is directed, weighted, and
loopless, with edge `i -> j` exactly when `i != j` and `w[i,j] > 0`.  Its
binary undirected projection contains `{i,j}` exactly when
`w[i,j] + w[j,i] > 0`; direction and fractional weight are discarded only in
that projection.  Self-edges remain available in the ego layer for
self-citation and surge share.

## Public derived tables

### `citation_network_final`

The analytic edge interface is:

```text
subject
citing_orcid
cited_orcid
citation_year
raw_count
citation_weight
is_self_citation
is_coauthor_citation
is_coauthor_citation_same_year
```

Both endpoints and the subject are non-null, and a unique index enforces the
subject/endpoint/year key.

### `author_subject_temporal_metrics`

This neutral table replaces the former `citation_anomalies` output.  It is keyed
by `(orcid, subject)` and contains identified outgoing denominators plus
`annual_dyadic_surge_share`.  No compatibility view with the old name is
created, so stale consumers fail visibly.

### `author_behavior_metrics` and `author_venue_metrics`

Both are keyed by `(orcid, subject)`.  Their denominators are retained so that a
zero, an undefined ratio, and absence from the graph remain distinguishable.

## Declared measures

Only these eight author--subject measures enter the paper:

| Measure | Population and definition |
|---|---|
| Coauthor-citation rate | Outgoing ego; non-self weight to strictly prior coauthors divided by identified non-self outgoing weight. |
| Self-citation rate | Outgoing ego; self weight divided by all identified outgoing weight. |
| Reciprocity | Directed weighted induced graph; `sum_j min(w[i,j], w[j,i]) / sum_j w[i,j]` over non-self internal dyads. |
| Local clustering | Binary undirected projection of the subject-specific induced graph; structural zero for degree below two. |
| Outgoing Herfindahl--Hirschman index (HHI) | Outgoing ego; sum of squared non-self recipient shares. It equals one for a single recipient and decreases as weight is dispersed. |
| Journal endogamy | Share of deduplicated resolved references for which citing and cited ISSNs agree. |
| Within-sample citation balance | Induced graph; `(out - in) / (out + in)` when internal weight is positive. |
| Maximum annual dyadic surge share | Largest positive 2021--2024 year-to-year increase to one recipient, divided by 2020--2024 identified outgoing weight. |

For dyad-year weight `x[i,j,t]`, with absent years set to zero only inside the
surge calculation, the final measure is

```text
max over j and t=2021..2024 of max(x[i,j,t] - x[i,j,t-1], 0)
----------------------------------------------------------------
       sum over j and t=2020..2024 of x[i,j,t]
```

It is bounded by `[0, 1]` and is a single five-year summary, not a citation
trajectory model.  Authors with no identified outgoing weight remain
missing.  Missing values elsewhere are preserved through feature assembly;
structural zeros are assigned only where the definition explicitly calls for
one.

## Paired inference

Case and Control rows are joined on both ORCID and subject.  Complete-pair
deletion is performed independently for every outcome, so pair counts can
differ and none can exceed 9,431.

The primary family is prior-coauthor citation rate, reciprocity, local clustering,
and outgoing HHI.  The secondary family is self-citation rate, journal
endogamy, within-sample citation balance, and annual dyadic surge share.  Each
table reports Case and Control summaries, two-sided paired Wilcoxon signed-rank
tests with zero differences omitted, matched
rank-biserial effect sizes, paired bootstrap confidence intervals, and
Benjamini--Hochberg adjusted p-values within its four-outcome family.  The
randomization check flips the signs of observed within-pair differences; it
does not permute pooled observations.  Primary outcomes are repeated on the
exact-`h5` subset.  These eight outcomes are distinct from the five detector
inputs below.

## Control-referenced screen

Eligibility requires at least one identified non-self outgoing citation.  For
each subject, Control observations define median/IQR robust scaling for five
features: self-citation, prior-coauthor citation, reciprocity, clustering, and
outgoing HHI.  One Isolation Forest is fit to Controls only with
`n_estimators=200`, `max_samples=auto`, `contamination=auto`, and the requested
seed.  A nonfinite or zero Control IQR is replaced by `1` to avoid division by
zero, leaving the centered feature in its original scale.  No additional
feature weights are applied.  The five inputs represent
self-reference, prior-collaborator citation, mutual exchange, neighborhood
closure, and recipient concentration, respectively.  Journal endogamy is
excluded because it is defined on work references, balance because it is a
signed internal-flow position, and surge share because it is temporal.  This
is a transparent revision rule, not a claim that the original analysis
prespecified an optimal feature set.  The revised screen uses no feature
weights, four-sigma cutoff, or Cohesion Composite Score.

Isolation Forest recursively partitions feature space; profiles isolated in
fewer splits receive lower library scores.  The workflow negates
`score_samples`, so larger study scores indicate greater departure from
Controls.  The library's `contamination=auto` cutoff is not used.  The declared
99th Control percentile is a stringent empirical-tail operating point without
a Gaussian assumption, not a uniquely optimal threshold.

An eligible observation is flagged at the main 99th-percentile threshold only
when both conditions hold:

1. its detector score is above the subject's 99th Control percentile; and
2. at least two of the four cohesion measures (coauthor citation, reciprocity,
   clustering, and outgoing HHI) exceed their own subject-specific 99th Control
   percentiles.

All four measures in the second condition also enter the detector.  It is
therefore an overlapping, interpretable restriction on the multivariate screen,
not independent confirmation or corroboration.

Sensitivity output covers 97.5%, 99%, and 99.5% thresholds across ten fixed
seeds.  `tables/anomaly_overlap.csv` and `.tex` report the detector-by-restriction
2x2 table and the rank association between detector score and cohesion
exceedance count.  `tables/anomaly_feature_ablation.csv` and `.tex` report
leave-one-feature-out fits at the canonical threshold and seed, including final-
flag counts, Jaccard overlap, and Case/Control enrichment.  The resulting column
`final_flag` is the only flag used by tables, figures, component extraction, and
enrichment calculations.  Reported Case share and Case-versus-Control
enrichment describe association, not classifier precision and not evidence of
manipulation.

Outlier components use the directed weighted graph induced by the canonical
flag.  Membership is weak connectivity, so direction is discarded only while
finding connected sets.  Within a reported component, net flow is weighted
outflow minus inflow and directed betweenness uses edge length `1 / w`.
Flow roles are `net giver`, `net receiver`, or `balanced flow`; the
`highest-betweenness node` is identified separately.  Five nodes is a
reporting gate, not a topology threshold.  A component figure is omitted when
no component passes it, and synthetic replacement data are forbidden.

Tier mixing uses directed fractional citation weight with citing tiers in rows
and cited tiers in columns.  For the normalized matrix `e`, row marginals `a`,
and column marginals `b`, weighted nominal assortativity is
`(trace(e) - a·b) / (1 - a·b)`.  The two-sided label-swap p-value compares the
observed within-tier weight share, `trace(e)`, with 10,000 independent
within-pair Case/Control label swaps.  Assortativity is reported descriptively
and is not the permutation statistic.

## Tests and invariants

```sh
make sql-test
make python-test
make verify
```

SQL fixtures cover fractional `1/6` weighting with missing ORCIDs, exclusion of
null endpoints, reference deduplication in multi-matched-author works, subject
retention, and strict prior-coauthor timing.  Python tests cover weighted HHI,
reciprocity, clustering, cumulative dyads, surge examples, missingness,
subject-keyed pairing, sign flips, weighted mixing, and reuse of the final flag.
Matching tests cover hard-caliper eligibility, deterministic tie ordering,
input-order invariance, no replacement, and rejection of an invalid retained
cohort.  Detector tests cover the overlap-table identities, rank association,
and leave-one-feature-out population and final-flag invariants.

Every canonical run additionally asserts:

- no duplicate author--subject--tier observations;
- retained pairs have unique, non-null author--subject keys, positive `h5`
  values, and satisfy the inclusive `h5` caliper;
- no duplicate or null-key citation edges;
- every per-metric pair count is at most 9,431;
- every non-missing surge share lies in `[0, 1]`;
- all component and figure inputs use `final_flag`; and
- manuscript quantities come from generated LaTeX macros.

Failure of an invariant stops the run.  Corrected estimates are authoritative
even when an effect weakens, reverses, or is not statistically distinguishable
from zero, and the manuscript must reflect that output.

## Scope and interpretation

The snapshot, five subject categories, publication-weighted Eigenfactor
cutoffs, 70% portfolio rule, positive-`h5` matching pool, and matched cohort are
fixed.  Coverage is limited by Crossref reference completeness and ORCID
availability, and the unavailable upstream journal-edge and article-count
exports prevent end-to-end reproduction of the fixed Eigenfactor scores.
Topical proximity, geography, language, and collaboration structure remain
plausible alternative explanations for observed cohesion.  The screen is
useful for prioritizing records for contextual review; it is not a finding of
intent or misconduct.

Version 3.0.0 of the revised software, aggregate replication materials, and
retained-cohort database is archived at
<https://doi.org/10.5281/zenodo.21720914>.  The approximately 128 GB
`impact.db` raw snapshot is not included in that archive.
