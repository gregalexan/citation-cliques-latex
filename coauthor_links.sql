-- Identify co-authorship among matched authors with temporal tracking
-- FIX: Track first_collab_year to prevent temporal leakage in is_coauthor detection
CREATE TABLE rolap.coauthor_links AS
SELECT
  CASE WHEN wa1.orcid < wa2.orcid THEN wa1.orcid ELSE wa2.orcid END AS orcid1,
  CASE WHEN wa1.orcid < wa2.orcid THEN wa2.orcid ELSE wa1.orcid END AS orcid2,
  MIN(we.published_year) AS first_collab_year  -- Track when relationship started
FROM work_authors wa1
JOIN work_authors wa2 ON wa1.work_id = wa2.work_id
JOIN rolap.works_enhanced we ON we.work_id = wa1.work_id
WHERE wa1.orcid IN (SELECT orcid FROM rolap.matched_authors)
  AND wa2.orcid IN (SELECT orcid FROM rolap.matched_authors)
  AND wa1.orcid <> wa2.orcid
GROUP BY 1, 2;

CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_cl_pair ON coauthor_links(orcid1, orcid2);