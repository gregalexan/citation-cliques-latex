# QSS reader-facing revision design

## Goal

Make the manuscript readable for a broad QSS audience without changing the
analysis, generated results, or qualified directional interpretation.

## Scope

- Rewrite the abstract so it leads with the question and main result, then gives
  only the minimum design and qualification needed to interpret them.
- Add plain-language signposts before the technical methods. Keep exact
  definitions and reproducibility details available in the appendix or tables,
  but move implementation mechanics out of the narrative where possible.
- Keep the metric table operationally self-contained while shortening its
  reader-facing wording and separating intuition from notation.
- Add a short plain-language results lead before the generated tables.
- Rewrite the discussion and conclusion so they lead with the association and
  zero-inflated result rather than the analysis pipeline.
- Replace repeated wording about incomplete coverage with one neutral scope
  statement tied to the fixed snapshot and inclusion criteria. Do not remove
  factual scope boundaries or introduce stronger claims.
- Leave the external Zenodo record unchanged until its new version is created;
  update the manuscript version citation as part of that release rather than
  guessing a future version number.

## Files and artifacts

- `main.tex` is the source of the prose changes.
- `response_to_reviewer.tex` is updated only where its clarity claim would no
  longer match the manuscript.
- `main.pdf`, `response_to_reviewer.pdf`, and the marked manuscript PDF are
  regenerated after source changes.
- Existing generated tables, macros, data, and unrelated untracked files are
  not edited or staged unless compilation requires a tracked generated output.

## Acceptance criteria

1. The abstract states the question, matched design, primary directional result,
   zero-inflation qualification, and non-causal interpretation in plain
   language.
2. The main methods introduce each technical idea with an intuitive sentence
   before its formula or implementation detail.
3. The Results section has a plain-language takeaway before its tables.
4. The conclusion is association-first and does not repeat a pipeline inventory.
5. The manuscript contains no claim that exceeds the fixed snapshot and stated
   inclusion criteria.
6. The clean and marked PDFs compile, pass `qpdf --check`, and contain the new
   reader-facing wording.
7. Existing analysis tests remain passing and no unrelated untracked artifact
   is staged.
