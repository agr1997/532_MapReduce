#!/bin/bash

appName=$1
ourOutput=$2
benchmarkOutput=$3

printf "\n\nComparing output of $appName application to expected output.\n"
# Sort the lines of each file and compare.
$(cmp -s <(sort $ourOutput) <(sort $benchmarkOutput))
exitStatus=$?
if [[ $exitStatus -eq 0 ]]; then
    echo "[PASSED] Outputs match!"
else
    echo "[FAILED] Outputs do not match."
fi
echo
