-- Calculate per-author metrics: Self-citation rate & Co-author citation rate
CREATE TABLE rolap.author_behavior_metrics AS
WITH sums AS (
  SELECT 
    citing_orcid as orcid,
    SUM(citation_weight) as total_weight,
    SUM(CASE WHEN is_self = 1 THEN citation_weight ELSE 0 END) as self_weight,
    SUM(CASE WHEN is_self = 0 AND is_coauthor = 1 THEN citation_weight ELSE 0 END) as co_weight,
    SUM(CASE WHEN is_self = 0 THEN citation_weight ELSE 0 END) as nonself_weight
  FROM rolap.citation_network_final
  GROUP BY citing_orcid
)
SELECT 
  s.orcid,
  p.subject,
  s.total_weight as total_outgoing_citations,
  (s.self_weight / NULLIF(s.total_weight, 0)) as self_citation_rate,
  COALESCE((s.co_weight / NULLIF(s.nonself_weight, 0)), 0) as coauthor_citation_rate
FROM sums s
JOIN rolap.author_profiles p ON s.orcid = p.orcid; -- Re-attach subject