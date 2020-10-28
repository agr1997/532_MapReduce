!/bin/bash

### GRADER may modify to the following parameters to test the MR library
### $N 		: Arbitrary
### $killNum 		: Exact worker number to kill (range: 0, ..., N-1)
### $killType		: Type of worker to kill ("mapper"/"reducer")
### $killSection	: When to kill- BEFORE (0) or AFTER (1) socket connection to worker

N=7
killNum=5
killType="reducer"
killSection=1

appName="Word Count"
sleepLength=200

# Clean up old stuff
rm -r out/word-count-$N-output/
rm combined_wc_out.txt # Proof we are not cheating

echo
echo "[EXECUTING WORD COUNT APPLICATION]"
if [[ $killSection -eq 0 ]]; then
  order="before"
else
  order="after"
fi
echo "We will be testing fault tolerance by killing the ${killNum}-th $killType $order the worker establishes a socket connection."
echo

java -jar ./test/wordcountapp.jar wordcount_cfg_n$N.txt $killNum $killType $sleepLength $killSection

# MR produces N output files. Concatenate them into single file.
cat ./out/word-count-$N-output/final/* > combined_wc_out.txt

./test/scripts/compare_outputs.sh "$appName" combined_wc_out.txt ./test/benchmark_wc_out.txt;

./test/scripts/check_N_procs.sh "$appName" $N ./out/word-count-$N-output/log;
