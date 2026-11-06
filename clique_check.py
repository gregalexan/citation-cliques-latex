"""Add the prior-coauthor-excluded clique check to a completed analysis run.

Run on the machine that holds the run's analysis database.  It reads the
database without rebuilding it, repeats only the reciprocal-clique analysis
after removing prior-coauthor links, and writes two files into the run's
tables directory:

  clique_summary_coauthor_excluded.csv   the new check
  matched_pair_h5.csv                    pair h5 values needed by rerender_results.py

Copy those two files and tables/tier_mixing_permutations.csv back with the
rest of the results directory, then run rerender_results.py.
"""

import argparse
from pathlib import Path

import citation_analysis as analysis


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--database", type=Path, required=True,
                        help="The completed run's analysis database (read only).")
    parser.add_argument("--output-dir", type=Path, default=analysis.DEFAULT_OUTPUT_DIRECTORY,
                        help="The completed run's results directory.")
    args = parser.parse_args()
    analysis.run_coauthor_clique_check(args.database, args.output_dir)


if __name__ == "__main__":
    main()
