# Formal Clique Null-Model Figure Design

## Goal

Add one reader-facing figure that makes the two formal reciprocal-clique null-model comparisons visible without displaying an unreadable node-level network.

## Scope

The figure belongs in the main Results section under `Formal reciprocal-clique structure`, immediately after the primary clique-summary table and before the sensitivity table. It has two panels:

1. A histogram of the 500 conditional graph-null reciprocal-clique counts, with the observed count of 17 shown as a vertical reference line.
2. A histogram of the 500 within-pair label-swap Case-membership shares, with the observed share of 91.6% shown as a vertical reference line.

The caption identifies the graph null, the label-swap null, the observed values, and the add-one empirical p-values. The figure uses aggregate values only. It does not show ORCIDs, individual authors, or a network hairball, and it does not encode intent or misconduct.

## Data flow

The plotting script reads the canonical `clique_summary.csv` and `clique_sensitivity.csv` outputs only when summary values are needed. The replicate-level values are written by the analysis to a small figure-data CSV in the same versioned results directory. The plotting script produces a vector PDF in `results/revision-v1/figures/` and a PNG preview alongside it.

The observed values and p-values in the caption and manuscript text continue to come from `results_macros.tex`; the figure is descriptive and must not introduce a second calculation path.

## Visual rules

- Use the existing manuscript typography and a colorblind-safe two-color palette.
- Label the x-axis in count units for panel A and percentage points for panel B.
- Draw the observed reference line in a dark accent color and identify it in the legend.
- Keep both panels on a common visual scale without truncating the observed line.
- Use a short caption that explains what each null preserves and does not imply.
- Do not add a schematic clique or a node-level network layout to the main paper.

## Acceptance criteria

- The PDF builds with the figure present after the primary clique table.
- The figure contains exactly the two declared panels and the observed lines.
- Values agree with the canonical primary row: 17 reciprocal cliques, graph-null mean 0.73, 91.6% Case membership, and both empirical p-values 0.0020.
- Existing Python and SQL verification suites remain green.
- LaTeX logs contain no errors, undefined references, or overfull boxes.
