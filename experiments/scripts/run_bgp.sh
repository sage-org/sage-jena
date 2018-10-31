#!/usr/bin/env bash
#!/bin/bash
# Run experiments for Sage

QUERIES=$1 # i.e. a folder that contains SPARQL queries to execute
OUTPUT=$2
cpt=1

if [ "$#" -ne 2 ]; then
  echo "Illegal number of parameters."
  echo "Usage: ./run_bgp.sh <queries-directory> <output-folder>"
  exit
fi

# SERVER="http://172.16.8.50:8000/sparql/bsbm1k"
SERVER="http://172.16.8.50:8000/sparql/watdiv"

mkdir -p $OUTPUT
mkdir -p $OUTPUT/results/
mkdir -p $OUTPUT/errors/

RESFILE="${OUTPUT}/execution_times_sage.csv"

# init results file with headers
echo "query,time,httpCalls,serverTime,importTime,exportTime,errors" > $RESFILE

for qfile in $QUERIES/*; do
  x=`basename $qfile`
  qname="${x%.*}"
  # query name
  echo -n "${qname}," >> $RESFILE
  # execution time
  ./bin/sage-jena-1.0-SNAPSHOT/bin/sage-jena -u $SERVER -f $qfile -m $RESFILE > /dev/null 2> ${OUTPUT}/errors/${qname}.err
  echo -n "," >> $RESFILE
  # nb errors during query processing
  echo `wc -l ${OUTPUT}/errors/${qname}.err | awk '{print $1}'` >> $RESFILE
done

# remove tmp folders
rm -rf $OUTPUT/errors/ $OUTPUT/results/
