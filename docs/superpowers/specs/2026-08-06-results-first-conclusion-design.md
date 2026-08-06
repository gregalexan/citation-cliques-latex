# Results-First Conclusion Design

## Objective

Replace the current inventory-style conclusion with a direct statement of the
study's main empirical contribution. The conclusion will answer the research
question before discussing secondary results and will preserve the initial
directional hypothesis as a measured finding within the matched cohort.

## Evidence to foreground

The conclusion will report the four primary mean paired differences:

- coauthor-citation rate: 0.045;
- reciprocity: 0.129;
- local clustering: 0.026; and
- outgoing HHI: 0.097.

All four primary comparisons pass the declared Benjamini-Hochberg threshold.
The text will identify reciprocity and outgoing concentration as the strongest
parts of the paired pattern, with local clustering providing a complementary
structural signal. It will then connect the author-level result to the 93.91%
within-tier citation-weight share and its label-swap result below 0.001.

## Proposed conclusion

The revised conclusion will use the following argument:

1. Within the matched author-subject cohort, Cases show greater citation-network
   cohesion and concentration on the primary outcomes.
2. The four estimates will be stated explicitly so the result is substantive
   rather than a generic claim about network density.
3. The pattern will be interpreted as more reciprocal, locally clustered, and
   recipient-concentrated citation structure.
4. The within-tier share will be presented as a sample-level result that places
   the author-level pattern in a stratified citation environment.
5. The final sentence will state that the results support the initial
   directional hypothesis and identify citation-network organization as a
   distinguishing feature of the two venue strata.

The conclusion will not include a separate caveat paragraph or repeat the
screening limitations. Those details remain in the Results, Discussion, and
Limitations sections where they are already documented.

## Scope

- Edit the conclusion in `main.tex` only.
- Do not change the database, metrics, cohort, tables, or inference code.
- Rebuild the clean manuscript, the reviewer response PDF, and the marked
  manuscript package after the prose change.
- Check reviewer-response page references after compilation and update them
  only if pagination changes.

## Verification

Run the existing test and SQL fixture suite, compile all requested PDFs, check
each PDF with `qpdf --check`, inspect the rendered conclusion, and confirm that
the manuscript log has no undefined references or overfull boxes.
