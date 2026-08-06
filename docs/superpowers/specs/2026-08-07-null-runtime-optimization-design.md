# Null-Model Runtime Optimization Design

## Goal

Reduce the runtime of the formal clique null analysis without changing the
observed clique definition, null distributions, random seeds, or reported
statistics.

## Constraints

- Keep the canonical 500 graph null replicates and 500 label swaps.
- Preserve the existing conditional null: observed maximal clique memberships
  are rescored after degree-preserving subject rewiring.
- Preserve deterministic results for a fixed seed.
- Avoid a whole-pipeline migration to Polars unless a benchmark identifies
  table aggregation as the dominant cost.
- Keep the current one-file analysis entry point and existing public helpers.

## Design

The observed pass continues to use NetworkX to enumerate maximal cliques. The
null pass will prepare compact per-subject edge arrays and candidate-member
indices once. Each null replicate will rewire and score those arrays without
constructing Pandas frames or Python weight dictionaries. Independent replicate
summaries will run in worker processes with seeds derived from the replicate
index. The parent process will combine summaries in the existing configuration
order, so output columns and empirical p-values remain unchanged.

Isolation Forest will use all available workers only for its existing model fit
(`n_jobs=-1`). It is a secondary optimization and is not used to parallelize
the clique null loop.

## Verification

- Add tests that serial and worker scoring produce identical summaries for a
  fixed seed.
- Keep degree, weight, node-membership, and threshold invariants covered.
- Benchmark the current and optimized paths with 49 replicates before running
  the canonical 500-replicate analysis.
- Retain the optimization only if the benchmark shows a material wall-clock
  improvement without a material memory or result discrepancy.
