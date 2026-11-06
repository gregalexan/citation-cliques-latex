# Server step for the R2 revision: prior-coauthor clique check

This step adds the one new estimate in the R2 revision, the reciprocal-clique
comparison with prior-coauthor links removed, to the completed `revision-v1`
run. It does not rebuild the database or rerun the rest of the analysis.

What it does:

- reads the existing scratch database `build/revision-v1/rolap.db` (read only);
- runs one clique pass on the citation links without prior-coauthor links;
- writes two new files into `results/revision-v1/tables/`:
  - `clique_summary_coauthor_excluded.csv` (the new check);
  - `matched_pair_h5.csv` (pair ID, subject, and both h5 values; no ORCIDs).

It changes no existing file. Run time is about one clique pass plus loading
the data, so expect roughly the time the clique phase took in the original run
(see that run's log).

## 0. Check the server state

The original run used this database
(from `results/revision-v1/run_metadata.json`):

```
/home/panspan/final_indexes/citation_manipulation/build/revision-v1/rolap.db
```

On the server:

```sh
cd /home/panspan/final_indexes/citation_manipulation
ls -lh build/revision-v1/rolap.db
ls results/revision-v1/run_metadata.json \
   results/revision-v1/tables/tier_mixing_permutations.csv \
   results/revision-v1/tables/clique_summary.csv
grep matched_pairs results/revision-v1/run_metadata.json   # expect 102626
```

All three files must exist. If `rolap.db` is gone, this shortcut is not
possible; run `make reproduce` instead.

## 1. Get the new code onto the server

The server needs the revised `citation_analysis.py`, the new
`clique_check.py`, and the revised `Makefile` from branch `qss-r2-revision`.
Choose one option.

**Option A: through GitHub** (the branch must be pushed first, from the laptop):

```sh
# laptop
git push origin qss-r2-revision

# server
cd /home/panspan/final_indexes/citation_manipulation
git status                      # commit or stash local edits first
git fetch origin
git switch qss-r2-revision
```

The branch does not change any file under `results/`, so the switch leaves the
completed run untouched.

**Option B: copy the three files** (nothing pushed):

```sh
# laptop, from the repository root
scp citation_analysis.py clique_check.py Makefile \
    panspan@SERVER:/home/panspan/final_indexes/citation_manipulation/
```

Replace `SERVER` with the server's host name.

## 2. Confirm the command is the shortcut, not the full pipeline

```sh
cd /home/panspan/final_indexes/citation_manipulation
make -n clique-check
```

It must print exactly one command, similar to:

```
.venv/bin/python clique_check.py --database "build/revision-v1/rolap.db" --output-dir "results/revision-v1"
```

If it prints `rebuild_analysis_database.py` or anything else, stop: the
Makefile is not the revised one.

The Makefile uses `.venv/bin/python` when it exists, otherwise `python3`. If
the original run used another environment, pass it explicitly, for example
`make clique-check PYTHON=/path/to/python`.

## 3. Run it in a session that survives logout

```sh
tmux new -s cliquecheck
cd /home/panspan/final_indexes/citation_manipulation
make clique-check 2>&1 | tee clique-check.log
# detach with Ctrl-b then d; reattach with: tmux attach -t cliquecheck
```

Progress lines look like `[   12.3 min] Clique subject 1: ...`. The last line
should read:

```
[ ... min] Wrote clique_summary_coauthor_excluded.csv and matched_pair_h5.csv in .../results/revision-v1/tables
```

Without tmux: `nohup make clique-check > clique-check.log 2>&1 &` and follow
with `tail -f clique-check.log`.

## 4. Check the output

```sh
cd /home/panspan/final_indexes/citation_manipulation/results/revision-v1/tables
wc -l matched_pair_h5.csv                     # 102627 (header + 102,626 pairs)
cat clique_summary_coauthor_excluded.csv      # header + one row
```

In the summary row, `matched_pair_count` must be 102626. The Case and Control
membership rates are expected to be lower than the all-links values in
`clique_summary.csv`, but a higher rate is not an error: removing one-way
links can raise a group's reciprocity and let it pass the threshold.

## 5. Copy three files back to the laptop

From the laptop, at the repository root:

```sh
scp 'panspan@SERVER:/home/panspan/final_indexes/citation_manipulation/results/revision-v1/tables/{clique_summary_coauthor_excluded.csv,matched_pair_h5.csv,tier_mixing_permutations.csv}' \
    results/revision-v1/tables/
```

`tier_mixing_permutations.csv` already existed on the server; the laptop copy
needs it to recompute the label-swap p-value. It is ignored by git.

Then, on the laptop, the remaining steps are `make rerender`, resolving the
two `TODO(after run)` comments in `main.tex`, `make marked`, and the final
compile and checks.

## Troubleshooting

| Message | Meaning and fix |
|---|---|
| `... has N matched pairs but the completed run reported 102,626` | The database is not the one from the completed run. Point `ANALYSIS_DB=` to the right file. |
| `completed-run file is missing: .../tier_mixing_permutations.csv` | The results directory is not the completed run's. Check `RESULTS_DIR=`. |
| `no completed run found: .../run_metadata.json is missing` | Same as above. |
| `ModuleNotFoundError` | Wrong Python environment; pass `PYTHON=` as in step 2. |
| Process killed without a message | Probably out of memory; check `dmesg` and rerun when the machine is idle. |
