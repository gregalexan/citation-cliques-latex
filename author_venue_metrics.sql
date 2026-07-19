-- Journal endogamy is calculated only from a matched author's works in the
-- matched subject.  Each resolved work-to-work reference contributes once.
DROP TABLE IF EXISTS rolap.author_venue_metrics;

CREATE TABLE rolap.author_venue_metrics AS
WITH normalized_works AS (
    SELECT
        id,
        REPLACE(
            REPLACE(
                COALESCE(
                    NULLIF(TRIM(issn_print), ''),
                    NULLIF(TRIM(issn_electronic), ''),
                    ''
                ),
                '-', ''
            ),
            ' ', ''
        ) AS issn_clean
    FROM works
),
refs_deduped AS (
    SELECT DISTINCT
        rw.orcid,
        rw.subject,
        rr.citing_work_id,
        rr.cited_work_id
    FROM rolap.resolved_refs rr
    JOIN rolap.relevant_works rw
      ON rw.work_id = rr.citing_work_id
)
SELECT
    rd.orcid,
    rd.subject,
    COUNT(*) AS total_refs,
    SUM(CASE
        WHEN citing_work.issn_clean <> ''
         AND citing_work.issn_clean = cited_work.issn_clean
        THEN 1 ELSE 0
    END) AS same_journal_refs,
    CAST(SUM(CASE
        WHEN citing_work.issn_clean <> ''
         AND citing_work.issn_clean = cited_work.issn_clean
        THEN 1 ELSE 0
    END) AS REAL) / NULLIF(COUNT(*), 0) AS journal_endogamy_rate
FROM refs_deduped rd
JOIN normalized_works citing_work
  ON citing_work.id = rd.citing_work_id
JOIN normalized_works cited_work
  ON cited_work.id = rd.cited_work_id
GROUP BY rd.orcid, rd.subject;

CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_avm_key ON author_venue_metrics(orcid, subject);
