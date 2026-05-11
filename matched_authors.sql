CREATE TABLE rolap.matched_authors AS
SELECT case_orcid AS orcid FROM rolap.author_matched_pairs
UNION
SELECT control_orcid FROM rolap.author_matched_pairs;

CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_ma_orcid ON matched_authors(orcid);