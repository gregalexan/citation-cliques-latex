-- First observed collaboration between each matched author and any identified
-- coauthor.  The cited endpoint need not itself belong to the matched cohort:
-- coauthor-citation rates are computed on the outgoing ego layer.
DROP TABLE IF EXISTS rolap.coauthor_links;

CREATE TABLE rolap.coauthor_links AS
WITH matched_orcids AS (
    SELECT DISTINCT orcid
    FROM rolap.matched_authors
)
SELECT
    CASE WHEN wa1.orcid < wa2.orcid THEN wa1.orcid ELSE wa2.orcid END AS orcid1,
    CASE WHEN wa1.orcid < wa2.orcid THEN wa2.orcid ELSE wa1.orcid END AS orcid2,
    MIN(w.published_year) AS first_collaboration_year
FROM matched_orcids ma
-- Keep the small matched set outermost when the fresh scratch database has no
-- planner statistics; work_authors has an ORCID index in the raw snapshot.
CROSS JOIN work_authors wa1
JOIN work_authors wa2
  ON wa2.work_id = wa1.work_id
JOIN works w
  ON w.id = wa1.work_id
WHERE wa1.orcid = ma.orcid
  AND wa1.orcid IS NOT NULL
  AND TRIM(wa1.orcid) <> ''
  AND wa2.orcid IS NOT NULL
  AND TRIM(wa2.orcid) <> ''
  AND wa1.orcid <> wa2.orcid
  AND w.published_year IS NOT NULL
GROUP BY 1, 2;

CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_cl_pair ON coauthor_links(orcid1, orcid2);
