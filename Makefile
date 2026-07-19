# Canonical offline workflow for the matched citation-cohesion study.

export MAINDB ?= $(CURDIR)/impact
# SQL files use `rolap` as their attached SQLite schema.  Keep this identifier
# separate from the analysis CLI, which accepts an arbitrary database path.
override export ROLAPDB := rolap
export DEPENDENCIES :=

RESULTS_DIR ?= results/revision-v1
COHORT_DB ?= rolap.db
ANALYSIS_DB ?= build/revision-v1/rolap.db
SEED ?= 42
PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
LATEX ?= pdflatex
BIBER ?= biber
LATEX_FLAGS ?= -interaction=nonstopmode -halt-on-error
SQL_TESTS := $(wildcard tests/*.rdbu)
export UNIT ?= $(SQL_TESTS)

include ../common/Makefile

.PHONY: check-inputs pipeline analysis sql-test python-test verify manuscript \
	reviewer-response reproduce

check-inputs:
	@test -r "$(MAINDB).db" || { echo "Missing raw snapshot: $(MAINDB).db" >&2; exit 2; }
	@test -r "$(COHORT_DB)" || { echo "Missing retained cohort database: $(COHORT_DB)" >&2; exit 2; }

# Matching is deterministic and materialized by Python after SQL candidate
# generation.  The analysis key in this table is (ORCID, subject).
tables/author_matched_pairs: tables/author_matched_candidates match_authors.py | check-inputs
	@echo "[Create deterministic author matches]"
	$(PYTHON) match_authors.py "$(ROLAPDB).db"
	mkdir -p tables
	touch $@

# Rebuild the citation-facing analysis tables in a genuinely fresh database.
# The journal classification and 9,431-pair cohort are copied as fixed inputs;
# the citation construction and every downstream table are reconstructed.
pipeline: check-inputs rebuild_analysis_database.py
	@echo "[Build fresh subject-keyed analysis database]"
	$(PYTHON) rebuild_analysis_database.py \
		--raw-database "$(MAINDB).db" \
		--cohort-database "$(COHORT_DB)" \
		--output-database "$(ANALYSIS_DB)" \
		--force

analysis: pipeline
	@echo "[Generate versioned analysis artifacts]"
	$(PYTHON) citation_analysis.py \
		--database "$(ANALYSIS_DB)" \
		--output-dir "$(RESULTS_DIR)" \
		--seed "$(SEED)"

sql-test:
	@echo "[Run SQL fixtures]"
	$(MAKE) --no-print-directory UNIT="$(SQL_TESTS)" test

python-test:
	@echo "[Run Python tests]"
	$(PYTHON) -m unittest discover -s tests -p 'test_*.py' -v

verify: sql-test python-test

manuscript: analysis
	@echo "[Compile manuscript from generated macros]"
	$(LATEX) $(LATEX_FLAGS) main.tex
	$(BIBER) main
	$(LATEX) $(LATEX_FLAGS) main.tex
	$(LATEX) $(LATEX_FLAGS) main.tex
	@! grep -Eq "undefined references|Citation .* undefined|Reference .* undefined" main.log

reviewer-response: manuscript
	$(LATEX) $(LATEX_FLAGS) response_to_reviewer.tex
	$(LATEX) $(LATEX_FLAGS) response_to_reviewer.tex

# One supported, offline entry point.  No identity lookup or network request is
# performed by any prerequisite of this target.
reproduce: verify pipeline analysis manuscript reviewer-response
	@echo "[Reproduction complete: $(RESULTS_DIR)]"
