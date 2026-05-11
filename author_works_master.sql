-- The master log of what every author published, where, and its impact
CREATE TABLE rolap.author_works_master AS
SELECT
    wa.orcid, we.subject, we.eigenfactor_score,
    COALESCE(wc.citations_number, 0) as citations
FROM work_authors wa
JOIN rolap.works_enhanced we ON wa.work_id = we.work_id
JOIN rolap.filtered_authors fa ON wa.orcid = fa.orcid
LEFT JOIN rolap.work_citations wc ON we.doi = wc.doi;

CREATE INDEX IF NOT EXISTS rolap.idx_awm_os ON author_works_master(orcid, subject);