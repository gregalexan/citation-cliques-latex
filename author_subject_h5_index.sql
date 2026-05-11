-- Calculate H5 Index for every author/subject pair
CREATE TABLE rolap.author_subject_h5_index AS
WITH ranked AS (
    SELECT orcid, subject, citations,
           ROW_NUMBER() OVER (PARTITION BY orcid, subject ORDER BY citations DESC) as r
    FROM rolap.author_works_master
)
SELECT orcid, subject, MAX(r) as h5_index
FROM ranked WHERE r <= citations
GROUP BY orcid, subject;

CREATE INDEX IF NOT EXISTS rolap.idx_ashi_os ON author_subject_h5_index(orcid, subject);