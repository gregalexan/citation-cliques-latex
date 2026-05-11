#
# Calculate impact factor of authors but only take account works published by popular journals with impact factor >=25
# from the last 2 years.
# (e.g., Nature, Science, PNAS, etc.)
# based on each work subject of the author.
#

export MAINDB?=$(shell pwd)/impact
export DEPENDENCIES=populate journal-names

include ../common/Makefile

# Populate database with required details for past five years
populate: $(CROSSREF_DIR)
	# Populate database with DOIs of works and their references
	$(TIME) $(A3K) --debug progress populate "$(MAINDB).db" crossref "$(CROSSREF_DIR)" \
	  --columns works.id works.doi works.published_year works.page \
	    work_references.doi work_references.work_id work_references.year \
		works.issn_print works.issn_electronic \
	    work_authors.work_id work_authors.orcid \
	  --row-selection 'works.published_year BETWEEN 2020 AND 2024'
	touch $@


tables/author_matched_pairs: tables/author_matched_candidates match_authors.py
	@echo "--- PYTHON MATCHING ---"
	# The script creates 'author_matched_pairs' in the DB
	uv run match_authors.py "$(ROLAPDB).db"
	
	# We create the timestamp file so Make knows this step succeeded
	mkdir -p tables
	touch $@

# -----------------------------------------------------------------------------
# 3. Final Target
# -----------------------------------------------------------------------------
pipeline: tables/citation_anomalies
	@echo "--- PIPELINE COMPLETE ---"

# 4. Analysis Script
analysis: pipeline
	@echo "--- RUNNING ANALYSIS SCRIPT ---"
	uv run analyze_results.py