-- Normalize each DOI once and resolve duplicate metadata records
-- deterministically to one work identifier.
DROP TABLE IF EXISTS rolap.works_doi_map;

CREATE TABLE rolap.works_doi_map AS
WITH normalized AS (
    SELECT
        work_id,
        TRIM(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(LOWER(doi), 'https://doi.org/', ''),
                        'http://doi.org/', ''
                    ),
                    'https://dx.doi.org/', ''
                ),
                'http://dx.doi.org/', ''
            )
        ) AS doi_norm
    FROM rolap.works_enhanced
    WHERE doi IS NOT NULL
)
SELECT MIN(work_id) AS work_id, doi_norm
FROM normalized
WHERE doi_norm <> ''
GROUP BY doi_norm;

CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_wdm_doi ON works_doi_map(doi_norm);
