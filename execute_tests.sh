#!/bin/bash

rm -r target/
rm -r word-count-app/

# Compile the Word Count test application with our library using Ant.
ant -buildfile build.xml
ant build-jar

# Execute the resulting jar.
java -jar ./test/wordcountapp.jar

# MR produces N output files. Concatenate them into single file.
cat ./word-count-output/final/* > combined_wc_out.txt

# Sort the lines of each file and compare.
$(cmp -s <(sort combined_wc_out.txt) <(sort ./test/benchmark_wc_out.txt))
exitStatus=$?
if [[ $exitStatus -eq 0 ]]; then
    echo "Word Count test case passed."
else
    echo "Word Count test case failed."
fi
