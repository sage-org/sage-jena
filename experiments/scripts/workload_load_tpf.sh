#!/bin/bash

NBRUNS=(1 2 3)
NBCLIENTS=(1 5 10 15 20 25 30 35 40 45 50)

for run in ${NBRUNS[@]}; do
  RESDIR="/home/minier/www19-watdiv-tpf/run${run}"
  mkdir -p $RESDIR
  for nb in ${NBCLIENTS[@]}; do
    python3 scripts/run_load.py watdiv_queries/ $RESDIR $nb tpf
  done
done
