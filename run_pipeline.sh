#!/bin/bash
# Run the modularized SQL pipeline for author matching

DB="rolap.db"

echo "Running Step 0: Filter Authors (10% Sample)..."
sqlite3 "$DB" < filtered_authors.sql

echo "Running Step 0.5: Create Works DOI Map..."
sqlite3 "$DB" < works_doi_map.sql

echo "Running Step 1: Author Works Master (Consolidated)..."
sqlite3 "$DB" < author_works_master.sql

echo "Running Step 2: Author Subject Stats..."
sqlite3 "$DB" < author_subject_stats.sql

echo "Running Step 3: Author Profiles..."
sqlite3 "$DB" < author_profiles.sql

# Step 4 removed (consolidated into Step 1)

echo "Running Step 5: Ranked Author Papers..."
sqlite3 "$DB" < ranked_author_papers.sql

echo "Running Step 6: Author Subject H5 Index..."
sqlite3 "$DB" < author_subject_h5_index.sql

echo "Running Step 11: Authors Enriched..."
sqlite3 "$DB" < authors_enriched.sql

echo "Running Step 12: Control Authors Bucketed..."
sqlite3 "$DB" < control_authors_bucketed.sql

echo "Running Step 13: Control Counts..."
sqlite3 "$DB" < ctrl_counts.sql

echo "Running Step 14: Bottom Authors Sampled..."
sqlite3 "$DB" < bottom_authors_sampled.sql

echo "Running Step 15: Raw Candidates..."
echo "Running Step 16: Author Matched Candidates (Ranked & Filtered)..."
sqlite3 "$DB" < author_matched_candidates.sql

echo "Running Step 17: Ordered Pairs (Pre-calculation)..."
sqlite3 "$DB" < ordered_pairs.sql

echo "Running Step 18: Greedy Matching (SQL)..."
sqlite3 "$DB" < author_matched_pairs.sql

echo "Running Step 19: Author Primary Subject Ranked..."
sqlite3 "$DB" < author_primary_subject_ranked.sql

echo "Running Step 20: Author Primary Subject..."
sqlite3 "$DB" < author_primary_subject.sql

echo "Running Step 21: Citation Network Agg..."
sqlite3 "$DB" < citation_network_agg.sql

echo "Running Step 22: Citation Network Final..."
sqlite3 "$DB" < citation_network_final.sql

echo "Running Step 23: Author Behavior Metrics RAM..."
sqlite3 "$DB" < author_behavior_metrics_ram.sql

echo "Running Step 24: Author Behavior Metrics..."
sqlite3 "$DB" < author_behavior_metrics.sql

echo "Running Step 25: Citation Anomalies Enriched..."
sqlite3 "$DB" < citation_anomalies_enriched.sql

echo "Running Step 26: Citation Anomalies Metrics..."
sqlite3 "$DB" < citation_anomalies_metrics.sql

echo "Running Step 27: Citation Anomalies..."
sqlite3 "$DB" < citation_anomalies.sql

echo "Pipeline completed successfully."
