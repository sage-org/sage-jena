#!/usr/bin/env bash
#!/bin/bash
# Run execution time experiment for virtuoso

QUERIES=$1 # i.e. query used to generate load

if [ "$#" -ne 1 ]; then
  echo "Illegal number of parameters."
  echo "Usage: ./run_virtuoso_load.sh <queries>"
  exit
fi

# SERVER="http://172.16.8.50:8000/tpf/watdiv10m"
SERVER="http://localhost:8000/sparql"

while true; do
#  for qfile in `ls $QUERIES/* | sort -R`; do
  ./bin/virtuoso.js $SERVER -f $QUERIES -s > /dev/null 2> /dev/null
#  done
done
