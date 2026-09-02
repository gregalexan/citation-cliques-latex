-- First observed collaboration for each author pair used by an analytic edge.
-- The cited endpoint need not itself belong to the matched cohort.
DROP TABLE IF EXISTS rolap.coauthor_links;

CREATE TABLE rolap.coauthor_links AS
WITH candidate_pairs AS (
    SELECT DISTINCT
        CASE WHEN citing_orcid < cited_orcid
             THEN citing_orcid ELSE cited_orcid END AS orcid1,
        CASE WHEN citing_orcid < cited_orcid
             THEN cited_orcid ELSE citing_orcid END AS orcid2
    FROM rolap.micro_edges
    WHERE citing_orcid <> cited_orcid
)
SELECT
    pairs.orcid1,
    pairs.orcid2,
    MIN(w.published_year) AS first_collaboration_year
FROM candidate_pairs pairs
CROSS JOIN work_authors wa1
JOIN work_authors wa2
  ON wa2.work_id = wa1.work_id
 AND wa2.orcid = pairs.orcid2
JOIN works w
  ON w.id = wa1.work_id
WHERE wa1.orcid = pairs.orcid1
  AND w.published_year IS NOT NULL
GROUP BY pairs.orcid1, pairs.orcid2;

CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_cl_pair ON coauthor_links(orcid1, orcid2);
