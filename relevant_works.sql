-- A matched author's citing works are eligible only in the subject in which
-- that author was matched.  This is the subject-keyed outgoing ego layer.
DROP TABLE IF EXISTS rolap.relevant_works;

CREATE TABLE rolap.relevant_works AS
SELECT DISTINCT
    wa.work_id,
    ma.orcid,
    ma.subject,
    we.published_year
FROM rolap.matched_authors ma
JOIN work_authors wa
  ON wa.orcid = ma.orcid
JOIN rolap.works_enhanced we
  ON we.work_id = wa.work_id
 -- The matched cohort is imported through pandas as TEXT, whereas the
 -- Crossref subject table is commonly INTEGER-affined in SQLite.
 AND CAST(we.subject AS TEXT) = CAST(ma.subject AS TEXT)
WHERE ma.orcid IS NOT NULL
  AND TRIM(ma.orcid) <> ''
  AND ma.subject IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_rw_key ON relevant_works(subject, orcid, work_id);
CREATE INDEX IF NOT EXISTS rolap.idx_rw_work_id ON relevant_works(work_id);
