"""Rewrite tables, figures, and macros from a completed run's saved tables.

No estimate is recomputed and no database is needed.  The script reads the
CSV files of a completed run and writes the LaTeX tables, figures, and
results_macros.tex with the same writers as citation_analysis.py.  It requires
tables/matched_pair_h5.csv and tables/tier_mixing_permutations.csv (see
clique_check.py) and uses tables/clique_summary_coauthor_excluded.csv when
present.
"""

import argparse
from pathlib import Path

import citation_analysis as analysis


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output-dir", type=Path, default=analysis.DEFAULT_OUTPUT_DIRECTORY,
                        help="The completed run's results directory.")
    args = parser.parse_args()
    analysis.rerender_results(args.output_dir)
    print(f"Re-rendered {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
