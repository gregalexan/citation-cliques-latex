-- Build the raw graph. CRITICAL FIX: Includes self-citations so we can flag them later.
CREATE TABLE rolap.micro_edges AS
SELECT
  rw.orcid              AS citing_orcid,
  wa.orcid              AS cited_orcid,
  rw.published_year     AS citation_year,
  1.0 / (cac.n_citing_authors * zac.n_cited_authors) AS w
FROM rolap.resolved_refs rr
JOIN rolap.relevant_works rw ON rw.work_id = rr.citing_work_id
JOIN work_authors wa ON wa.work_id = rr.cited_work_id
JOIN rolap.citing_authors_counts cac ON cac.work_id = rr.citing_work_id
JOIN rolap.cited_authors_counts zac ON zac.cited_work_id = rr.cited_work_id;
-- Note: citing <> cited check is intentionally removed here to allow "Self Citation" metric

CREATE INDEX IF NOT EXISTS rolap.idx_me_pair ON micro_edges(citing_orcid, cited_orcid);