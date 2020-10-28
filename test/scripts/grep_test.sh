#!/bin/bash

### GRADER may modify to the following parameters to test the MR library
### $N 		: Arbitrary
### $killNum 		: Exact worker number to kill (range: 0, ..., N-1)
### $killType		: Type of worker to kill ("mapper"/"reducer")
### $killSection	: When to kill- BEFORE (0) or AFTER (1) socket connection to worker

N=2
killNum=1
killType="mapper"
killSection=1

appName="Grep"
dirName="./out/grep-${N}-output"
combinedName="combined_grep_${N}_out.txt"
sleepLength=200

# Clean up old stuff
rm -r out/grep-$N-output/
rm $combinedName

echo
echo "[EXECUTING GREP APPLICATION]"
if [[ $killSection -eq 0 ]]; then
  order="before"
else
  order="after"
fi
echo "We will be testing fault tolerance by killing the ${killNum}-th $killType $order the worker establishes a socket connection."

echo

# Execute the resulting jar.
java -jar ./test/grepapp.jar "grep_cfg_n${N}.txt" $killNum $killType $sleepLength $killSection 

# MR produces N output files. Concatenate them into single file.
cat ${dirName}/final/* > ${combinedName}

./test/scripts/compare_outputs.sh "$appName" $combinedName ./test/benchmark_grep_out.txt;

./test/scripts/check_N_procs.sh "$appName" $N $dirName/log;
