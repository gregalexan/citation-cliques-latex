-- Pre-calculate global citation counts per DOI
CREATE TABLE rolap.work_citations AS
SELECT wr.doi, COUNT(*) AS citations_number
FROM work_references wr
WHERE wr.doi IN (SELECT doi FROM rolap.works_enhanced)
GROUP BY wr.doi;

CREATE INDEX IF NOT EXISTS rolap.idx_wc_doi ON work_citations(doi);