-- Subject-keyed outgoing ego metrics.  A zero numerator with a positive
-- denominator is a structural zero; an unavailable denominator remains NULL.
DROP TABLE IF EXISTS rolap.author_behavior_metrics;

CREATE TABLE rolap.author_behavior_metrics AS
WITH sums AS (
    SELECT
        subject,
        citing_orcid AS orcid,
        SUM(citation_weight) AS total_weight,
        SUM(CASE
            WHEN is_self_citation = 1 THEN citation_weight ELSE 0.0 END
        ) AS self_weight,
        SUM(CASE
            WHEN is_self_citation = 0 THEN citation_weight ELSE 0.0 END
        ) AS nonself_weight,
        SUM(CASE
            WHEN is_self_citation = 0
             AND is_coauthor_citation = 1
            THEN citation_weight ELSE 0.0 END
        ) AS strict_prior_coauthor_weight,
        SUM(CASE
            WHEN is_self_citation = 0
             AND is_coauthor_citation_same_year = 1
            THEN citation_weight ELSE 0.0 END
        ) AS same_year_coauthor_weight
    FROM rolap.citation_network_final
    GROUP BY subject, citing_orcid
)
SELECT
    ma.orcid,
    ma.subject,
    COALESCE(s.total_weight, 0.0) AS total_outgoing_citations,
    COALESCE(s.nonself_weight, 0.0) AS nonself_outgoing_citations,
    s.self_weight / NULLIF(s.total_weight, 0.0) AS self_citation_rate,
    s.strict_prior_coauthor_weight / NULLIF(s.nonself_weight, 0.0)
        AS coauthor_citation_rate,
    s.same_year_coauthor_weight / NULLIF(s.nonself_weight, 0.0)
        AS coauthor_citation_rate_same_year
FROM rolap.matched_authors ma
LEFT JOIN sums s
  ON s.orcid = ma.orcid
 AND s.subject = ma.subject;

CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_abm_key ON author_behavior_metrics(orcid, subject);
