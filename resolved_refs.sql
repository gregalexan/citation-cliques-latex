-- Resolve references to Work IDs using the map
CREATE TABLE rolap.resolved_refs AS
SELECT rr.work_id AS citing_work_id, wdm.work_id AS cited_work_id
FROM (
    SELECT wr.work_id, 
           LOWER(REPLACE(REPLACE(wr.doi,'https://doi.org/',''),'http://doi.org/','')) as doi_norm
    FROM work_references wr
    JOIN rolap.relevant_works rw ON rw.work_id = wr.work_id
    WHERE wr.doi IS NOT NULL
) rr
JOIN rolap.works_doi_map wdm ON wdm.doi_norm = rr.doi_norm;

CREATE INDEX IF NOT EXISTS rolap.idx_rr_citing ON resolved_refs(citing_work_id);
CREATE INDEX IF NOT EXISTS rolap.idx_rr_cited ON resolved_refs(cited_work_id);