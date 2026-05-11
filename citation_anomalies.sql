-- Detect Bursts and Asymmetry
CREATE TABLE rolap.citation_anomalies AS
WITH pairs AS (
    SELECT citing_orcid, cited_orcid, SUM(citation_weight) as w,
           MAX(citation_year) - MIN(citation_year) as duration
    FROM rolap.citation_network_final
    WHERE is_self = 0
    GROUP BY citing_orcid, cited_orcid
),
stats AS (
    -- Calculate Asymmetry (Metric: How much more do I give than take?)
    SELECT p1.citing_orcid, 
           AVG(CASE WHEN p1.w > COALESCE(p2.w, 0) THEN p1.w - COALESCE(p2.w, 0) ELSE 0 END) as avg_asymmetry,
           MAX(CASE WHEN p1.w > COALESCE(p2.w, 0) THEN p1.w - COALESCE(p2.w, 0) ELSE 0 END) as max_asymmetry
    FROM pairs p1
    LEFT JOIN pairs p2 ON p1.citing_orcid = p2.cited_orcid AND p1.cited_orcid = p2.citing_orcid
    GROUP BY p1.citing_orcid
),
bursts AS (
    -- Pairwise Burst: Max citations given to a single person in a single year
    -- This detects "Spamming" or "Cartel Service" behavior
    SELECT citing_orcid, 
           MAX(citation_weight) as max_burst, 
           AVG(citation_weight) as avg_velocity
    FROM rolap.citation_network_final
    WHERE is_self = 0
    GROUP BY citing_orcid
)
SELECT s.citing_orcid as orcid, 
       s.avg_asymmetry, s.max_asymmetry, 
       b.avg_velocity, b.max_burst
FROM stats s
JOIN bursts b ON s.citing_orcid = b.citing_orcid;