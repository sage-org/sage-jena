#!/usr/bin/env bash
#!/bin/bash
# Run execution time experiment for reference (TPF)

QUERIES=$1 # i.e. query used to generate load

if [ "$#" -ne 1 ]; then
  echo "Illegal number of parameters."
  echo "Usage: ./run_reference_load.sh <queries>"
  exit
fi

# SERVER="http://172.16.8.50:8000/tpf/watdiv10m"
SERVER="http://172.16.8.50:5000/watdiv10M"

while true; do
  for qfile in `ls $QUERIES/* | sort -R`; do
    x=`basename $qfile`
    qname="${x%.*}"
    ./bin/reference.js $SERVER -f $qfile -s > /dev/null 2> /dev/null
  done
done
