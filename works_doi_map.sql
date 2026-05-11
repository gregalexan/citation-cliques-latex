-- Performance: Normalize DOIs once
CREATE TABLE rolap.works_doi_map AS
SELECT work_id,
  LOWER(REPLACE(REPLACE(doi,'https://doi.org/',''),'http://doi.org/','')) AS doi_norm
FROM rolap.works_enhanced WHERE doi IS NOT NULL;

CREATE INDEX IF NOT EXISTS rolap.idx_wdm_doi ON works_doi_map(doi_norm);