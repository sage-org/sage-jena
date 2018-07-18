#!/usr/bin/env bash
#!/bin/bash
# Run execution time experiment for reference (TPF)

QUERIES=$1 # i.e. query used to generate load

if [ "$#" -ne 1 ]; then
  echo "Illegal number of parameters."
  echo "Usage: ./run_brtpf_load.sh <queries>"
  exit
fi

# SERVER="http://172.16.8.50:8000/tpf/watdiv10m"
SERVER="http://localhost:8000/brtpf/watdiv10m"

while true; do
  #for qfile in `ls $QUERIES/* | sort -R`; do
    #x=`basename $qfile`
    #qname="${x%.*}"
    bin/sage-jena-1.0-SNAPSHOT/bin/sage-jena -u $SERVER -f $QUERIES > /dev/null 2> /dev/null
  #done
done
