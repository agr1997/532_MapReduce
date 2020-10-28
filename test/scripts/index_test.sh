#!/bin/bash

### GRADER may modify to the following parameters to test the MR library
### $N 		: Arbitrary
### $killNum 		: Exact worker number to kill (range: 0, ..., N-1)
### $killType		: Type of worker to kill ("mapper"/"reducer")
### $killSection	: When to kill- BEFORE (0) or AFTER (1) socket connection to worker

N=1
killNum=0
killType="reducer"
killSection=1

appName="Inverted Index"
dirName="./out/inverted-index-${N}-output"
combinedName="combined_index_${N}_out.txt"
sleepLength=200

rm -r out/inverted-index-$N-output/
rm $combinedName

echo
echo "[EXECUTING INVERTED-INDEX APPLICATION WITH N=$N]"
if [[ $killSection -eq 0 ]]; then
  order="before"
else
  order="after"
fi
echo "We will be testing fault tolerance by killing the ${killNum}-th $killType $order the worker establishes a socket connection."
echo

# Execute the resulting jar.
java -jar ./test/indexapp.jar "invertedindex_cfg_n${N}.txt" $killNum $killType $sleepLength $killSection

# MR produces N output files. Concatenate them into single file.
cat ${dirName}/final/* > ${combinedName}

./test/scripts/compare_outputs.sh "$appName" $combinedName ./test/benchmark_index_out.txt;

./test/scripts/check_N_procs.sh "$appName" $N $dirName/log;

########################################################

# Now with N=4
N=4
dirName="./out/inverted-index-${N}-output"
combinedName="combined_index_${N}_out.txt"
appName="Inverted Index"
killNum=2
killType="mapper"
sleepLength=200
killSection=0

# Remove previous output files
rm -r out/inverted-index-$N-output/
rm $combinedName

echo
echo "[EXECUTING INVERTED-INDEX APPLICATION WITH N=$N]"
if [[ $killSection -eq 0 ]]; then
  order="before"
else
  order="after"
fi
echo "We will be testing fault tolerance by killing the ${killNum}-th $killType $order the worker establishes a socket connection."
echo

# Execute the resulting jar.
java -jar ./test/indexapp.jar "invertedindex_cfg_n${N}.txt" $killNum $killType $sleepLength $killSection 

# MR produces N output files. Concatenate them into single file.
cat ${dirName}/final/* > ${combinedName}

./test/scripts/compare_outputs.sh "$appName" $combinedName ./test/benchmark_index_out.txt;

./test/scripts/check_N_procs.sh "$appName" $N $dirName/log;
