-- Resolve and deduplicate work-to-work references before expanding either
-- endpoint to authors.  In particular, multiple matched coauthors on one
-- citing work must not multiply the underlying resolved reference.
DROP TABLE IF EXISTS rolap.resolved_refs;

CREATE TABLE rolap.resolved_refs AS
WITH relevant_work_ids AS (
    SELECT DISTINCT work_id
    FROM rolap.relevant_works
),
normalized_refs AS (
    SELECT DISTINCT
        wr.work_id AS citing_work_id,
        TRIM(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(LOWER(wr.doi), 'https://doi.org/', ''),
                        'http://doi.org/', ''
                    ),
                    'https://dx.doi.org/', ''
                ),
                'http://dx.doi.org/', ''
            )
        ) AS doi_norm
    FROM relevant_work_ids rw
    -- CROSS JOIN fixes the small relevant-work set as the outer loop; SQLite
    -- can then seek work_references through its work_id index instead of
    -- choosing a whole-snapshot scan when scratch-table statistics are absent.
    CROSS JOIN work_references wr
    WHERE wr.doi IS NOT NULL
      AND wr.work_id = rw.work_id
)
SELECT DISTINCT
    nr.citing_work_id,
    wdm.work_id AS cited_work_id
FROM normalized_refs nr
JOIN rolap.works_doi_map wdm
  ON wdm.doi_norm = nr.doi_norm;

CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_rr_pair ON resolved_refs(citing_work_id, cited_work_id);
CREATE INDEX IF NOT EXISTS rolap.idx_rr_cited ON resolved_refs(cited_work_id);
