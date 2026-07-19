-- Work metadata used by both cohort construction and citation resolution.
-- Keep the selected ISSN in the output so venue-based metrics do not need to
-- repeat (and potentially change) the print/electronic fallback rule.
DROP TABLE IF EXISTS rolap.works_enhanced;

CREATE TABLE rolap.works_enhanced AS
WITH normalized_works AS (
    SELECT
        w.id AS work_id,
        w.doi,
        w.published_year,
        COALESCE(
            NULLIF(TRIM(w.issn_print), ''),
            NULLIF(TRIM(w.issn_electronic), '')
        ) AS issn
    FROM works w
    WHERE w.doi IS NOT NULL
      AND w.published_year BETWEEN 2020 AND 2024
)
SELECT
    nw.work_id,
    nw.doi,
    nw.published_year,
    nw.issn,
    isub.subject,
    COALESCE(es.eigenfactor_score, 0) AS eigenfactor_score
FROM normalized_works nw
LEFT JOIN issn_subjects isub
  ON nw.issn = isub.issn
LEFT JOIN eigenfactor_scores es
  ON isub.issn = es.issn
 AND isub.subject = es.subject;

CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_we_work_subject ON works_enhanced(work_id, subject);
CREATE INDEX IF NOT EXISTS rolap.idx_we_doi ON works_enhanced(doi);
