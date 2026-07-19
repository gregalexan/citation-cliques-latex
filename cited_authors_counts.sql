-- Fractional denominators count author rows, not just identified ORCIDs.
-- COUNT(*) deliberately includes rows whose ORCID is NULL.
DROP TABLE IF EXISTS rolap.cited_authors_counts;

CREATE TABLE rolap.cited_authors_counts AS
SELECT work_id AS cited_work_id, COUNT(*) AS n_cited_authors
FROM work_authors
WHERE work_id IN (SELECT cited_work_id FROM rolap.resolved_refs)
GROUP BY work_id;

CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_zac_work ON cited_authors_counts(cited_work_id);
