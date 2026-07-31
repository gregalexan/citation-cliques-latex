-- Fractional denominators count author rows, not just identified ORCIDs.
-- COUNT(*) deliberately includes rows whose ORCID is NULL.
DROP TABLE IF EXISTS rolap.citing_authors_counts;

CREATE TABLE rolap.citing_authors_counts AS
SELECT work_id, COUNT(*) AS n_citing_authors
FROM work_authors
WHERE work_id IN (SELECT citing_work_id FROM rolap.resolved_refs)
GROUP BY work_id;

CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_cac_work ON citing_authors_counts(work_id);
