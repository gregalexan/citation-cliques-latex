# Reviewer-Gap Release Design

## Goal

Close the remaining mismatches between the full-cohort manuscript and the
point-by-point reviewer response before publishing release v4.0.0.

## Scope

Make five focused corrections:

1. Name reciprocal-clique membership in the research questions.
2. Add the clique analysis to the reader-facing pipeline figure.
3. State the anomaly screen's Control-based median/IQR preprocessing and the
   rationale for including and excluding its measures.
4. Add the promised Discussion comparison with Kojaku et al., while preserving
   the distinction between structural association and misconduct inference.
5. Replace the generated phrase "4 of four comparisons" with clear prose.

Update the replication-package citation and data-availability statement for
v4.0.0. Do not change estimates, clique definitions, matching, source data, or
the completed full-cohort outputs.

## Implementation

Edit `main.tex` for the research question, figure, anomaly explanation,
literature comparison, and release wording. Update the existing result-macro
generator for the awkward generated sentence, then regenerate only the
affected text artifact from the completed analysis output or database. Update
the bibliography's package version and use the stable all-versions Zenodo DOI.

Keep `response_to_reviewer.tex` aligned with the manuscript. Its substantive
claims should describe only material present in the clean revision.

## Verification and Release

Add or adjust the smallest existing artifact-contract test for the generated
sentence, observing a failing test before the generator change. Run the full
`make verify` target, compile the clean manuscript, response letter, and marked
revision, and scan their logs for warnings and undefined references. Rebuild
and verify the archive, including verification from an extracted copy.

Review the final Git diff so only intended project and generated release files
are committed. Push the current branch without force. Upload the verified
v4.0.0 archive to the existing Zenodo record, then report the resulting record
URL and DOI. If credentials or network access prevent publication, stop after
the verified push and report the exact external blocker.

## Non-goals

No new statistical analysis, package dependency, graph null model, manuscript
restructure, source-database mutation, or broad prose rewrite.
