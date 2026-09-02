-- Legacy compatibility table: retain every identified author.
CREATE TABLE rolap.filtered_authors AS
SELECT DISTINCT orcid
FROM work_authors
WHERE orcid IS NOT NULL
  AND TRIM(orcid) <> '';

CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_fa_orcid ON filtered_authors(orcid);
