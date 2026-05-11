-- Count ALL authors on cited papers (for 1/N weight)
CREATE TABLE rolap.cited_authors_counts AS
SELECT work_id as cited_work_id, COUNT(DISTINCT orcid) AS n_cited_authors
FROM work_authors
WHERE work_id IN (SELECT cited_work_id FROM rolap.resolved_refs)
GROUP BY work_id;