CREATE TABLE rolap.works_enhanced AS
SELECT
    w.id as work_id, 
    w.doi, 
    w.published_year,
    isub.subject, -- Changed alias from 'is' to 'isub'
    COALESCE(es.eigenfactor_score, 0) as eigenfactor_score
FROM works w
LEFT JOIN issn_subjects isub ON COALESCE(w.issn_print, w.issn_electronic) = isub.issn
LEFT JOIN eigenfactor_scores es ON isub.issn = es.issn AND isub.subject = es.subject
WHERE w.doi IS NOT NULL AND w.published_year BETWEEN 2020 AND 2024;

CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_we_work_id ON works_enhanced(work_id);
CREATE INDEX IF NOT EXISTS rolap.idx_we_doi ON works_enhanced(doi);