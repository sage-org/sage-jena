#!/bin/bash
# Compute completeness between two files which contains results

reference=$1
results=$2
mode=$3

if [ "$#" -ne 3 ]; then
  echo "Illegal number of parameters."
  echo "Usage: ./completeness.sh <reference-file> <results> <mode>"
  exit
fi

sort $reference > tempRef
sort $results > tempRes
resultsSize=`wc -l tempRes | sed 's/^[ ^t]*//' | cut -d' ' -f1`
# compute soundness rather than completeness
if [[ "$mode" = "sound" ]]; then
  groundTruth=`wc -l tempRes | sed 's/^[ ^t]*//' | cut -d' ' -f1`
else
  groundTruth=`wc -l tempRef | sed 's/^[ ^t]*//' | cut -d' ' -f1`
fi
commons=`comm -12 tempRef tempRes | wc -l`
if [[ $groundTruth -eq 0 && $resultsSize -eq 0 ]]; then
  completeness="1.00"
elif [[ $groundTruth -eq 0 && $resultsSize -gt 0 ]]; then
  completeness="0.00"
else
  completeness=`echo "scale=2; $commons/$groundTruth" | bc`
fi
echo $completeness

rm -f tempRef tempRes
