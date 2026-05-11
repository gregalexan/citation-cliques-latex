-- Filter to ~10% of authors for performance (ending in '0')
CREATE TABLE rolap.filtered_authors AS
SELECT DISTINCT orcid
FROM work_authors
WHERE substr(orcid, -1) = '0';

CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_fa_orcid ON filtered_authors(orcid);