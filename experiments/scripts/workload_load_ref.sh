#!/bin/bash

NBRUNS=(1 2 3)
NBCLIENTS=(1 5 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 90 95 100)

for run in ${NBRUNS[@]}; do
  RESDIR="/home/minier/www19-watdiv-tpf/run${run}"
  mkdir -p $RESDIR
  for nb in ${NBCLIENTS[@]}; do
    python3 scripts/run_load.py watDivQueries/ load_watdiv.sparql $RESDIR /home/minier/watdiv-reference/results/ $nb ref
    mv $RESDIR/execution_times_tpf.csv $RESDIR/execution_times_${nb}c.csv
  done
done
