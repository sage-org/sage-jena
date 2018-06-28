#!/usr/bin/env bash
#!/bin/bash
# Run execution time + completeness/soundness experiment for BrTPF

QUERIES=$1 # i.e. a folder that contains SPARQL queries to execute
OUTPUT=$2
REF=$3 # directory where reference files are stored, for completeness & soundness computation
cpt=1

if [ "$#" -ne 3 ]; then
  echo "Illegal number of parameters."
  echo "Usage: ./run_brtpf.sh <queries-directory> <output-folder> <reference-folder>"
  exit
fi

# SERVER="http://172.16.8.50:8000/brtpf/watdiv10m"
SERVER="http://localhost:8000/brtpf/watdiv10m"

mkdir -p $OUTPUT/results/
mkdir -p $OUTPUT/errors/

RESFILE="${OUTPUT}/execution_times_brtpf.csv"

# init results file with headers
echo "query,time,httpCalls,completeness,soundness,errors" > $RESFILE

for qfile in $QUERIES/*; do
  x=`basename $qfile`
  qname="${x%.*}"
  echo -n "${qname}," >> $RESFILE
  # execution time
  ./bin/brtpf-client.js $SERVER -f $qfile -m $RESFILE > $OUTPUT/results/$qname.log 2> $OUTPUT/errors/$qname.err
  echo -n "," >> $RESFILE
  # completeness
  echo -n `./scripts/completeness.sh ${REF}/$qname.log ${OUTPUT}/results/$qname.log compl` >> $RESFILE
  echo -n "," >> $RESFILE
  # soundness
  echo -n `./scripts/completeness.sh ${REF}/$qname.log ${OUTPUT}/results/$qname.log sound` >> $RESFILE
  echo -n "," >> $RESFILE
  # nb errors during query processing
  echo `wc -l ${OUTPUT}/errors/${qname}.err | awk '{print $1}'` >> $RESFILE
done

# remove tmp folders
rm -rf $OUTPUT/errors/ $OUTPUT/results/