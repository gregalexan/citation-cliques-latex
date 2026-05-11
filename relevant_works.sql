-- Only look at works by our matched authors
CREATE TABLE rolap.relevant_works AS
SELECT DISTINCT wa.work_id, wa.orcid, we.published_year
FROM work_authors wa
JOIN rolap.matched_authors ma ON ma.orcid = wa.orcid
JOIN rolap.works_enhanced we ON we.work_id = wa.work_id;

CREATE INDEX IF NOT EXISTS rolap.idx_rw_work_id ON relevant_works(work_id);