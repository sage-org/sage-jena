#!/bin/bash

NBRUNS=(1 2 3)
NBCLIENTS=(1 2 3 4 5 6 7 8 9 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 95 100)

for run in ${NBRUNS[@]}; do
  RESDIR="/home/minier/watdiv-ref-load/run${run}"
  mkdir -p $RESDIR
  for nb in ${NBCLIENTS[@]}; do
    python3 scripts/run_load.py watDivQueries/ watDivQueries/query_10175.rq $RESDIR /home/minier/watdiv-reference/results/ $nb ref
    mv $RESDIR/execution_times_ref.csv $RESDIR/execution_times_${nb}c.csv
  done
done
