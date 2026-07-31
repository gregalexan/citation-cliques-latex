-- Join Bottom vs Top authors within the inclusive three-point h5 caliper.
DROP TABLE IF EXISTS rolap.author_matched_candidates;

CREATE TABLE rolap.author_matched_candidates AS
WITH enriched AS (
    SELECT ap.orcid, ap.subject, ap.author_tier, h.h5_index
    FROM rolap.author_profiles ap
    JOIN rolap.author_subject_h5_index h ON ap.orcid=h.orcid AND ap.subject=h.subject
    WHERE ap.author_tier IN ('Bottom Tier', 'Top Tier')
      AND ap.orcid IS NOT NULL
      AND TRIM(ap.orcid) <> ''
      AND h.h5_index > 0
),
controls AS (SELECT * FROM enriched WHERE author_tier = 'Top Tier'),
cases AS (SELECT * FROM enriched WHERE author_tier = 'Bottom Tier')
SELECT
  b.orcid AS case_orcid, c.orcid AS control_orcid, b.subject,
  ABS(b.h5_index - c.h5_index) AS score
FROM cases b
JOIN controls c ON b.subject = c.subject
AND ABS(b.h5_index - c.h5_index) <= 3;
