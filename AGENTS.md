# Repository Instructions

## Scope

These instructions apply to this directory and all descendants. This project
rebuilds and analyzes a matched author--subject citation cohort. Keep claims
association-first: the anomaly screen prioritizes records for review and does
not establish intent, manipulation, or misconduct.

## Canonical Offline Workflow

- Use `make reproduce` as the supported end-to-end entry point.
- Use `make verify` for the SQL and Python test suites without rebuilding the
  full data product.
- The workflow must remain offline. Do not add ORCID/network/API identity
  lookups or any other remote dependency.
- The retained cohort and matching covariate come from `rolap.db`; citation
  tables are rebuilt from the fixed `impact.db` snapshot into a fresh scratch
  database.
- Keep `(ORCID, subject)` as the analysis key. Every estimate-affecting join
  must include `subject`.

## Data and Artifact Authority

- Treat `impact.db` and `rolap.db` as large, preserved inputs. Never edit,
  replace, truncate, vacuum, or delete them. Build into
  `build/revision-v1/rolap.db` or another explicitly supplied scratch path.
- Do not commit database files, temporary `.building` databases, LaTeX build
  products, caches, or generated bulk data.
- `results/revision-v1/` is the authoritative generated output root.
  `results_macros.tex` is the sole source of quantitative manuscript claims.
- `latest/`, `analysis_output/`, `analysis_results_v4/`,
  `publication_figures_v4/`, and repository-level `figures/` are historical
  and non-authoritative. Do not read them into the canonical pipeline or copy
  their values into the manuscript.
- Use corrected regenerated evidence even when findings weaken, reverse, or
  become non-significant.

## Editing Conventions

### SQL

- Preserve subject-specific construction and non-null analytic endpoints.
- Deduplicate resolved work references before author expansion, but count all
  author rows (including missing ORCIDs) in fractional denominators.
- Keep yearly analytic edges unique on
  `(subject, citing_orcid, cited_orcid, citation_year)`.
- Treat a coauthor as prior only when first collaboration precedes the citation
  year; keep same-year inclusion only as a named sensitivity analysis.
- Preserve undefined values. Introduce structural zeroes only where the metric
  definition explicitly requires them.
- Add or update RDBUnit fixtures for every construction or schema change.

### Python

- Keep `citation_analysis.py` deterministic under its `--seed` argument and
  usable with arbitrary `--database` and `--output-dir` paths.
- Maintain the declared eight metrics and one Control-referenced `final_flag`.
  All enrichment, component, table, and figure outputs must reuse that flag.
- Never generate substitute/synthetic flags, components, figures, or data when
  empirical eligibility conditions are not met.
- Preserve missingness through feature assembly and perform complete-pair
  deletion separately per metric.
- Add focused tests for changed metrics, inference, joins, flags, or artifact
  contracts.

### LaTeX

- Keep essential methods before Results and retain separate Results,
  Discussion, Limitations, and Conclusion sections.
- Source reported numbers from generated macros/tables; do not hand-copy
  estimates into `main.tex` or `response_to_reviewer.tex`.
- Use neutral labels and avoid accusatory role names or causal language.
- Compile with `biber` and require no undefined references or stale result
  claims.

## Verification Before Handoff

Run at minimum:

```sh
make verify
```

For changes affecting the pipeline, generated results, or manuscript, run the
relevant canonical targets (normally `make reproduce`) and confirm:

- no duplicate author--subject--tier rows or null/duplicate analytic edge keys;
- every per-metric pair count is at most 9,431;
- every non-missing annual dyadic surge share lies in `[0, 1]`;
- figures and components use only `final_flag`, with no synthetic fallback;
- the manuscript consumes current generated macros; and
- LaTeX logs contain no undefined references.

Full rebuilds are expensive. Do not rerun them merely to inspect code, and do
not overwrite a completed authoritative output unless the requested change
requires regeneration.
