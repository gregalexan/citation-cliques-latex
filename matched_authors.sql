-- Author membership is subject keyed throughout the analysis.  The primary
-- key also makes a contradictory Case/Control assignment within a subject a
-- hard failure instead of silently multiplying downstream rows.
DROP TABLE IF EXISTS rolap.matched_authors;

CREATE TABLE rolap.matched_authors (
    orcid TEXT NOT NULL,
    subject TEXT NOT NULL,
    tier_type TEXT NOT NULL CHECK (tier_type IN ('Case', 'Control')),
    PRIMARY KEY (orcid, subject)
);

INSERT INTO rolap.matched_authors (orcid, subject, tier_type)
SELECT case_orcid, subject, 'Case'
FROM rolap.author_matched_pairs
WHERE case_orcid IS NOT NULL
  AND TRIM(case_orcid) <> ''
  AND subject IS NOT NULL
UNION
SELECT control_orcid, subject, 'Control'
FROM rolap.author_matched_pairs
WHERE control_orcid IS NOT NULL
  AND TRIM(control_orcid) <> ''
  AND subject IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_ma_author_subject_tier ON matched_authors(orcid, subject, tier_type);
