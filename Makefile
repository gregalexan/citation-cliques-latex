# Canonical offline workflow for the matched citation-cohesion study.

export MAINDB ?= $(CURDIR)/impact

RESULTS_DIR ?= results/revision-v1
COHORT_DB ?= rolap.db
ANALYSIS_DB ?= build/revision-v1/rolap.db
SEED ?= 42
PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
RDBUNIT ?= rdbunit
SQLITE3 ?= sqlite3
LATEX ?= pdflatex
BIBER ?= biber
LATEX_FLAGS ?= -interaction=nonstopmode -halt-on-error
SQL_TESTS := $(wildcard tests/*.rdbu)

.PHONY: check-inputs match-audit pipeline analysis sql-test python-test verify \
	manuscript reviewer-response reproduce

check-inputs:
	@test -r "$(MAINDB).db" || { echo "Missing raw snapshot: $(MAINDB).db" >&2; exit 2; }
	@test -r "$(COHORT_DB)" || { echo "Missing retained cohort database: $(COHORT_DB)" >&2; exit 2; }

match-audit: check-inputs match_authors.py author_matched_candidates.sql
	@echo "[Audit deterministic hard-caliper matching without modifying the cohort]"
	$(PYTHON) match_authors.py --audit \
		--audit-output "$(RESULTS_DIR)/matching_audit.txt" "$(COHORT_DB)"

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
	@set -eu; tmp=$$(mktemp -d); trap 'rm -r "$$tmp"' EXIT; \
	for test in $(SQL_TESTS); do \
		echo "[Test $$test]"; \
		$(RDBUNIT) --database=sqlite "$$test" >"$$tmp/test.sql"; \
		$(SQLITE3) <"$$tmp/test.sql" >"$$tmp/test.out"; \
		sed '/^$$/d' "$$tmp/test.out"; \
		if grep -Ev -e '^ *ok [0-9]+' -e '^ *[0-9]+\.\.[0-9]+.?$$' \
			-e '^ *$$' "$$tmp/test.out" >/dev/null; then \
			echo "The test $$test failed or produced extraneous output" >&2; \
			exit 1; \
		fi; \
	done

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
reproduce: verify match-audit pipeline analysis manuscript reviewer-response
	@echo "[Reproduction complete: $(RESULTS_DIR)]"
