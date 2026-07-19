-- Expand each unique work-to-work reference to identified author endpoints.
-- Team-size denominators still include unidentified author rows; NULL/blank
-- ORCIDs are simply not emitted as graph nodes.  Self edges remain available
-- for the self-citation measure.
DROP TABLE IF EXISTS rolap.micro_edges;

CREATE TABLE rolap.micro_edges AS
WITH identified_cited_authors AS (
    SELECT DISTINCT work_id, orcid
    FROM work_authors
    WHERE orcid IS NOT NULL
      AND TRIM(orcid) <> ''
)
SELECT
    rw.subject,
    rw.orcid AS citing_orcid,
    ica.orcid AS cited_orcid,
    rw.published_year AS citation_year,
    1.0 / (cac.n_citing_authors * zac.n_cited_authors) AS w
FROM rolap.resolved_refs rr
JOIN rolap.relevant_works rw
  ON rw.work_id = rr.citing_work_id
JOIN identified_cited_authors ica
  ON ica.work_id = rr.cited_work_id
JOIN rolap.citing_authors_counts cac
  ON cac.work_id = rr.citing_work_id
JOIN rolap.cited_authors_counts zac
  ON zac.cited_work_id = rr.cited_work_id
WHERE rw.subject IS NOT NULL
  AND rw.orcid IS NOT NULL
  AND TRIM(rw.orcid) <> ''
  AND cac.n_citing_authors > 0
  AND zac.n_cited_authors > 0;

CREATE INDEX IF NOT EXISTS rolap.idx_me_key ON micro_edges(subject, citing_orcid, cited_orcid, citation_year);
