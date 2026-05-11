-- Count ALL authors on citing papers (for 1/N weight)
CREATE TABLE rolap.citing_authors_counts AS
SELECT work_id, COUNT(DISTINCT orcid) AS n_citing_authors
FROM work_authors
WHERE work_id IN (SELECT citing_work_id FROM rolap.resolved_refs)
GROUP BY work_id;