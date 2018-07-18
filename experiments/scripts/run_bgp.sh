#!/usr/bin/env bash
#!/bin/bash
# Run execution time + completeness/soundness experiment for BGP LDF

QUERIES=$1 # i.e. a folder that contains SPARQL queries to execute
OUTPUT=$2
REF=$3 # directory where reference files are stored, for completeness & soundness computation
cpt=1

if [ "$#" -ne 3 ]; then
  echo "Illegal number of parameters."
  echo "Usage: ./run_bgp.sh <queries-directory> <output-folder> <reference-folder>"
  exit
fi

# SERVER="http://172.16.8.50:8000/sparql/bsbm1k"
SERVER="http://172.16.8.50:8000/sparql/watdiv"

mkdir -p $OUTPUT/results/
mkdir -p $OUTPUT/errors/

RESFILE="${OUTPUT}/execution_times_bgp.csv"

# init results file with headers
echo "query,time,httpCalls,serverTime,importTime,exportTime,errors" > $RESFILE

for qfile in $QUERIES/*; do
  x=`basename $qfile`
  qname="${x%.*}"
  echo -n "${qname}," >> $RESFILE
  # execution time
  bin/sage-jena-1.0-SNAPSHOT/bin/sage-jena -u $SERVER -f $qfile -m $RESFILE > $OUTPUT/results/$qname.log 2> ${OUTPUT}/errors/${qname}.err
  echo -n "," >> $RESFILE
  # # completeness
  # echo -n `./scripts/completeness.sh ${REF}/$qname.log ${OUTPUT}/results/$qname.log compl` >> $RESFILE
  # echo -n "," >> $RESFILE
  # # soundness
  # echo -n `./scripts/completeness.sh ${REF}/$qname.log ${OUTPUT}/results/$qname.log sound` >> $RESFILE
  # echo -n "," >> $RESFILE
  # # nb errors during query processing
  echo `wc -l ${OUTPUT}/errors/${qname}.err | awk '{print $1}'` >> $RESFILE
done

# remove tmp folders
rm -rf $OUTPUT/errors/ $OUTPUT/results/
