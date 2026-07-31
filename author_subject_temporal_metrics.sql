-- Neutral author-subject temporal metrics for the 2020--2024 observation
-- window.  For author i, cited author j, and year t, x_ijt is the fractional
-- citation weight in citation_network_final.  Missing dyad-years are zero.
--
-- annual_dyadic_surge_share =
--   max_{j,t=2021,...,2024} [x_ijt - x_ij(t-1)]_+
--   / sum_{j,t=2020,...,2024} x_ijt
--
-- Authors without identified outgoing weight retain a NULL surge value.
DROP TABLE IF EXISTS rolap.author_subject_temporal_metrics;

CREATE TABLE rolap.author_subject_temporal_metrics (
    orcid TEXT NOT NULL,
    subject TEXT NOT NULL,
    identified_outgoing_weight REAL NOT NULL
        CHECK (identified_outgoing_weight >= 0),
    nonself_identified_outgoing_weight REAL NOT NULL
        CHECK (nonself_identified_outgoing_weight >= 0),
    annual_dyadic_surge_share REAL
        CHECK (
            annual_dyadic_surge_share IS NULL
            OR annual_dyadic_surge_share BETWEEN 0.0 AND 1.0
        ),
    PRIMARY KEY (orcid, subject)
);

INSERT INTO rolap.author_subject_temporal_metrics (
    orcid,
    subject,
    identified_outgoing_weight,
    nonself_identified_outgoing_weight,
    annual_dyadic_surge_share
)
WITH years(citation_year) AS (
    SELECT 2020
    UNION ALL SELECT 2021
    UNION ALL SELECT 2022
    UNION ALL SELECT 2023
    UNION ALL SELECT 2024
),
dyads AS (
    SELECT DISTINCT subject, citing_orcid, cited_orcid
    FROM rolap.citation_network_final
    WHERE citation_year BETWEEN 2020 AND 2024
),
filled_dyad_years AS (
    SELECT
        d.subject,
        d.citing_orcid,
        d.cited_orcid,
        y.citation_year,
        COALESCE(cnf.citation_weight, 0.0) AS x_ijt
    FROM dyads d
    CROSS JOIN years y
    LEFT JOIN rolap.citation_network_final cnf
      ON cnf.subject = d.subject
     AND cnf.citing_orcid = d.citing_orcid
     AND cnf.cited_orcid = d.cited_orcid
     AND cnf.citation_year = y.citation_year
),
dyad_deltas AS (
    SELECT
        subject,
        citing_orcid,
        cited_orcid,
        citation_year,
        x_ijt - LAG(x_ijt) OVER (
            PARTITION BY subject, citing_orcid, cited_orcid
            ORDER BY citation_year
        ) AS annual_change
    FROM filled_dyad_years
),
surges AS (
    SELECT
        subject,
        citing_orcid AS orcid,
        MAX(CASE
            WHEN citation_year BETWEEN 2021 AND 2024
             AND annual_change > 0.0
            THEN annual_change ELSE 0.0
        END) AS max_positive_annual_dyadic_change
    FROM dyad_deltas
    GROUP BY subject, citing_orcid
),
totals AS (
    SELECT
        subject,
        citing_orcid AS orcid,
        SUM(citation_weight) AS identified_outgoing_weight,
        SUM(CASE
            WHEN is_self_citation = 0 THEN citation_weight ELSE 0.0 END
        ) AS nonself_identified_outgoing_weight
    FROM rolap.citation_network_final
    WHERE citation_year BETWEEN 2020 AND 2024
    GROUP BY subject, citing_orcid
)
SELECT
    ma.orcid,
    ma.subject,
    COALESCE(t.identified_outgoing_weight, 0.0),
    COALESCE(t.nonself_identified_outgoing_weight, 0.0),
    CASE
        WHEN t.identified_outgoing_weight IS NULL
          OR t.identified_outgoing_weight <= 0.0
        THEN NULL
        WHEN s.max_positive_annual_dyadic_change <= 0.0
        THEN 0.0
        WHEN s.max_positive_annual_dyadic_change
             >= t.identified_outgoing_weight
        THEN 1.0
        ELSE s.max_positive_annual_dyadic_change
             / t.identified_outgoing_weight
    END
FROM rolap.matched_authors ma
LEFT JOIN totals t
  ON t.orcid = ma.orcid
 AND t.subject = ma.subject
LEFT JOIN surges s
  ON s.orcid = ma.orcid
 AND s.subject = ma.subject;

CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_astm_key ON author_subject_temporal_metrics(orcid, subject);
