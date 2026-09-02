# Analysis Progress and Clique Runtime Design

## Goal

Make the canonical full-cohort analysis report useful live progress and remove
the clique-scoring bottleneck without changing any statistic or artifact.

## Design

Keep the existing single-process workflow. Print flushed elapsed-time messages
before and after each major analysis phase. During clique analysis, report each
subject's enumeration result and periodically report scored groups, percentage,
rate, and estimated remaining time.

Count directed clique dyads by testing the ordered member pairs against the
existing subject weight dictionary. This is mathematically identical to
scanning every subject dyad for every clique, but costs O(k^2) per clique rather
than O(E) for a clique of size k in a subject with E directed dyads.

Stop the currently running pre-change process only after the replacement passes
focused tests, then restart the canonical command in the existing detached
`citation-full-cohort` tmux session and retain its log path.

## Verification

- Add a regression test proving unrelated subject dyads do not affect clique
  directed-density or reciprocity results.
- Add a progress-output test using a small fixture.
- Run the focused analysis tests before restarting the full cohort.
- Confirm the restarted process is active and the log contains flushed progress.

No checkpoint framework, dependency, parallel clique engine, or output-format
change is included.
