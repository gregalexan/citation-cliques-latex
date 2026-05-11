-- Calculate p25 and p75 Eigenfactor thresholds per subject
-- FIX: Use MAX(1, ...) to ensure r25/r75 never go below row 1
CREATE TABLE rolap.eigenfactor_percentiles AS
WITH ranked AS (
  SELECT subject, eigenfactor_score,
         ROW_NUMBER() OVER (PARTITION BY subject ORDER BY eigenfactor_score) AS rn,
         COUNT(*) OVER (PARTITION BY subject) AS n
  FROM rolap.works_enhanced
  WHERE eigenfactor_score > 0
),
q AS (
  SELECT subject,
         MAX(1, CAST(ROUND(n * 0.25) AS INT)) AS r25,  -- Never go below row 1
         MAX(1, CAST(ROUND(n * 0.75) AS INT)) AS r75   -- Never go below row 1
  FROM ranked GROUP BY subject HAVING MAX(n) >= 50
)
SELECT r.subject,
       (SELECT eigenfactor_score FROM ranked WHERE subject=r.subject AND rn = q.r25) AS p25,
       (SELECT eigenfactor_score FROM ranked WHERE subject=r.subject AND rn = q.r75) AS p75
FROM (SELECT DISTINCT subject FROM ranked) r
JOIN q ON q.subject = r.subject;