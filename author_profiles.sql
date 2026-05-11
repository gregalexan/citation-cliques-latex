-- Classify authors into Tiers (Top, Bottom) based on journal prestige
CREATE TABLE rolap.author_profiles AS
WITH stats AS (
    SELECT orcid, ep.subject,
       COUNT(*) as papers,
       SUM(CASE WHEN eigenfactor_score <= ep.p25 THEN 1 ELSE 0 END) as bottom,
       SUM(CASE WHEN eigenfactor_score >= ep.p75 THEN 1 ELSE 0 END) as top
    FROM rolap.author_works_master ap
    JOIN rolap.eigenfactor_percentiles ep ON ap.subject = ep.subject
    GROUP BY orcid, ep.subject
)
SELECT orcid, subject,
    CASE
        WHEN CAST(bottom AS REAL)/papers >= 0.7 AND papers >= 3 THEN 'Bottom Tier'
        WHEN CAST(top AS REAL)/papers >= 0.7 AND papers >= 3 THEN 'Top Tier'
        ELSE 'Other'
    END as author_tier
FROM stats;

CREATE INDEX IF NOT EXISTS rolap.idx_ap_os ON author_profiles(orcid, subject);