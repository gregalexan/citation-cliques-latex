# Formal Clique Analysis Design

## Goal

Add a reproducible structural analysis that tests for dense, reciprocal citation groups while keeping the term “clique” tied to an explicit graph definition rather than an inference about intent.

## Scope

The primary analysis uses the full subject-specific matched-author graph. The canonical final flag is retained only as a descriptive overlay, so the clique result does not inherit the detector's feature selection or threshold.

For subject `s`, use the existing loopless cumulative directed graph `G_s` and its binary undirected projection `U_s`. A clique is a maximal clique in `U_s` with at least four matched authors. A group is therefore complete in the undirected projection, while direction and weight are evaluated separately.

For each clique (C), report:

* directed density `d_C = m_C / (|C| (|C|-1))`, where `m_C` is the number of positive directed dyads between clique members;
* weighted reciprocity `r_C = sum(min(w_ij, w_ji)) / sum(max(w_ij, w_ji))` over unordered member pairs;
* the number and share of Case clique memberships; and
* the number of canonical flagged members, as a descriptive overlay only.

The primary reciprocal-clique rule is `r_C >= 0.50` and `d_C >= 0.75`. Sensitivity repeats the same analysis for minimum clique sizes 3, 4, and 5 and reciprocity thresholds 0.25, 0.50, and 0.75. The density threshold remains 0.75 so that the reported group is not merely an undirected clique with one-way edges.

## Null model

Within each subject, rewire the simple directed graph using directed edge swaps that preserve every node's in-degree and out-degree. Reassign the observed cumulative weights to the rewired dyads using a fixed seeded permutation, preserving the subject-level weight multiset. The graph null is conditional on the observed maximal-clique membership sets: each rewired graph rescores those candidate groups with the same density and reciprocity thresholds rather than re-enumerating a changing null clique population. We compute qualifying candidate-group counts, mean density, mean reciprocity, and Case membership share for 499 successful null replicates. The empirical p-value is `(1 + count(null >= observed)) / (1 + n_null)` for count statistics. Subjects without four nodes or three directed dyads do not contribute a null replicate.

Within-pair Case/Control label swaps provide a separate null for Case membership share. They preserve the graph and matching structure and only test whether clique memberships are concentrated in Cases.

## Outputs

The analysis writes one summary table for the primary rule, one sensitivity table for size and reciprocity thresholds, and generated macros for the manuscript. The tables report observed values, conditional-null means, empirical p-values, valid null counts, and Case membership share. No member identifiers are written into the manuscript tables.

## Manuscript changes

The Methods section will define the clique population, density, reciprocity, thresholds, and null models before Results. The Results section will report the observed structural pattern and its null comparison without calling a clique evidence of coordination or misconduct. Reviewer response Comment 8 will state that the former qualitative topology claim was replaced by this formal, auditable analysis. The paper title will be updated after the analysis and generated outputs are verified so it describes citation-network cohesion and reciprocal clique structure without asserting misconduct.

## Simplicity and reproducibility

Reuse the existing cumulative-dyad and membership helpers. Add one focused analysis dataclass and small helper functions in `citation_analysis.py`; do not add a dependency, database table, or separate pipeline. Use the existing seed argument for all rewiring and label swaps. Keep generated artifacts under the existing revision output root and do not read historical outputs.
