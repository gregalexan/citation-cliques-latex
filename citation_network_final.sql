-- Subject-keyed annual analytic edges.  Strict-prior collaboration is the
-- primary definition; the <= flag supports the prespecified same-year
-- sensitivity analysis without rebuilding the graph.
DROP TABLE IF EXISTS rolap.citation_network_final;

CREATE TABLE rolap.citation_network_final (
    subject TEXT NOT NULL,
    citing_orcid TEXT NOT NULL,
    cited_orcid TEXT NOT NULL,
    citation_year INTEGER NOT NULL,
    raw_count INTEGER NOT NULL CHECK (raw_count > 0),
    citation_weight REAL NOT NULL CHECK (citation_weight >= 0),
    is_self_citation INTEGER NOT NULL CHECK (is_self_citation IN (0, 1)),
    is_coauthor_citation INTEGER NOT NULL CHECK (is_coauthor_citation IN (0, 1)),
    is_coauthor_citation_same_year INTEGER NOT NULL
        CHECK (is_coauthor_citation_same_year IN (0, 1)),
    PRIMARY KEY (subject, citing_orcid, cited_orcid, citation_year)
);

INSERT INTO rolap.citation_network_final (
    subject,
    citing_orcid,
    cited_orcid,
    citation_year,
    raw_count,
    citation_weight,
    is_self_citation,
    is_coauthor_citation,
    is_coauthor_citation_same_year
)
WITH aggregated AS (
    SELECT
        subject,
        citing_orcid,
        cited_orcid,
        citation_year,
        COUNT(*) AS raw_count,
        SUM(w) AS citation_weight
    FROM rolap.micro_edges
    WHERE subject IS NOT NULL
      AND citing_orcid IS NOT NULL
      AND TRIM(citing_orcid) <> ''
      AND cited_orcid IS NOT NULL
      AND TRIM(cited_orcid) <> ''
      AND citation_year IS NOT NULL
    GROUP BY subject, citing_orcid, cited_orcid, citation_year
)
SELECT
    a.subject,
    a.citing_orcid,
    a.cited_orcid,
    a.citation_year,
    a.raw_count,
    a.citation_weight,
    CASE WHEN a.citing_orcid = a.cited_orcid THEN 1 ELSE 0 END,
    CASE
        WHEN a.citing_orcid <> a.cited_orcid
         AND cl.first_collaboration_year < a.citation_year
        THEN 1 ELSE 0
    END,
    CASE
        WHEN a.citing_orcid <> a.cited_orcid
         AND cl.first_collaboration_year <= a.citation_year
        THEN 1 ELSE 0
    END
FROM aggregated a
LEFT JOIN rolap.coauthor_links cl
  ON cl.orcid1 = CASE
        WHEN a.citing_orcid < a.cited_orcid
        THEN a.citing_orcid ELSE a.cited_orcid END
 AND cl.orcid2 = CASE
        WHEN a.citing_orcid < a.cited_orcid
        THEN a.cited_orcid ELSE a.citing_orcid END;

CREATE INDEX IF NOT EXISTS rolap.idx_cnf_citing_subject ON citation_network_final(subject, citing_orcid);
CREATE INDEX IF NOT EXISTS rolap.idx_cnf_cited_subject ON citation_network_final(subject, cited_orcid);
