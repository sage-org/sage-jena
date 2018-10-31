#!/usr/bin/env bash
#!/bin/bash
# Run experiment for BrTPF

QUERIES=$1 # i.e. a folder that contains SPARQL queries to execute
OUTPUT=$2
cpt=1

if [ "$#" -ne 2 ]; then
  echo "Illegal number of parameters."
  echo "Usage: ./run_brtpf.sh <queries-directory> <output-folder>"
  exit
fi

SERVER="http://172.16.8.50:8001/watdiv10M"

mkdir -p $OUTPUT/results/
mkdir -p $OUTPUT/errors/

RESFILE="${OUTPUT}/execution_times_brtpf.csv"

# init results file with headers
echo "query,time,httpCalls,serverTime,errors" > $RESFILE

for qfile in $QUERIES/*; do
  x=`basename $qfile`
  qname="${x%.*}"
  echo -n "${qname}," >> $RESFILE
  # execution time
  /home/minier/Client.js-brTPF/bin/brTPF-client $SERVER $qfile -o $RESFILE > $OUTPUT/results/$qname.log 2> $OUTPUT/errors/$qname.err
  echo -n "," >> $RESFILE
  # nb errors during query processing
  echo `wc -l ${OUTPUT}/errors/${qname}.err | awk '{print $1}'` >> $RESFILE
done

# remove tmp folders
rm -rf $OUTPUT/errors/ $OUTPUT/results/
