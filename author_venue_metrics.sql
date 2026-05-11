-- Calculate "Journal Endogamy" (Venue Self-Citation Rate)
-- Measures how often an author cites papers from the SAME journal they are publishing in.
-- FIX: Deduplicate reference pairs to avoid overcounting for multi-author papers
CREATE TABLE rolap.author_venue_metrics AS
WITH normalized_works AS (
    SELECT 
        id,
        -- Robust ISSN cleaning: 
        -- 1. Coalesce Print/Electronic (take whichever exists)
        -- 2. Remove hyphens ('-') and spaces to ensure matching across formats
        REPLACE(REPLACE(COALESCE(issn_print, issn_electronic, ''), '-', ''), ' ', '') as issn_clean
    FROM works
),
-- Deduplicate: Each (orcid, citing_work, cited_work) tuple counts only once
refs_dedupe AS (
    SELECT DISTINCT 
        wa.orcid, 
        rr.citing_work_id, 
        rr.cited_work_id
    FROM rolap.resolved_refs rr
    JOIN work_authors wa ON rr.citing_work_id = wa.work_id
    WHERE wa.orcid IN (SELECT orcid FROM rolap.matched_authors)
)
SELECT
    rd.orcid,
    COUNT(*) as total_refs,
    SUM(CASE 
        WHEN w1.issn_clean != '' 
             AND w1.issn_clean = w2.issn_clean
        THEN 1 
        ELSE 0 
    END) as same_journal_refs
FROM refs_dedupe rd
JOIN normalized_works w1 ON rd.citing_work_id = w1.id
JOIN normalized_works w2 ON rd.cited_work_id = w2.id
GROUP BY rd.orcid;

-- Calculate the rate column
ALTER TABLE rolap.author_venue_metrics ADD COLUMN journal_endogamy_rate REAL;

UPDATE rolap.author_venue_metrics 
SET journal_endogamy_rate = CAST(same_journal_refs AS REAL) / NULLIF(total_refs, 0);

CREATE INDEX IF NOT EXISTS rolap.idx_avm_orcid ON author_venue_metrics(orcid);