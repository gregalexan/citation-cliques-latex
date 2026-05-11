-- Aggregate edges, flagging self and co-author citations
-- FIX: is_coauthor now checks that co-authorship existed BEFORE the citation year
CREATE TABLE rolap.citation_network_final AS
SELECT
  me.citing_orcid,
  me.cited_orcid,
  me.citation_year,
  COUNT(*) as raw_count,
  SUM(me.w) as citation_weight,
  CASE WHEN me.citing_orcid = me.cited_orcid THEN 1 ELSE 0 END as is_self,
  CASE 
    WHEN EXISTS (
      SELECT 1 FROM rolap.coauthor_links cl 
      WHERE cl.orcid1 = MIN(me.citing_orcid, me.cited_orcid)
        AND cl.orcid2 = MAX(me.citing_orcid, me.cited_orcid)
        AND cl.first_collab_year <= me.citation_year  -- TEMPORAL CONSTRAINT: only count if they were already co-authors
    ) THEN 1 
    ELSE 0 
  END as is_coauthor
FROM rolap.micro_edges me
GROUP BY me.citing_orcid, me.cited_orcid, me.citation_year;

CREATE INDEX IF NOT EXISTS rolap.idx_cnf_citing ON citation_network_final(citing_orcid);
CREATE INDEX IF NOT EXISTS rolap.idx_cnf_cited ON citation_network_final(cited_orcid);  -- Added for groupby performance