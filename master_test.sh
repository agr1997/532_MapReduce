#!/bin/bash

# Compile the code, and build jars for the applications
rm -r target/
rm -r out/
rm test/wordcountapp.jar
rm test/indexapp.jar
rm test/grepapp.jar
ant -buildfile build.xml
ant build-wc-jar
ant build-index-jar
ant build-grep-jar

scriptsDir=./test/scripts
logFile=all_tests_log.txt
rm $logFile

$scriptsDir/wc_test.sh | tee -a $logFile;
$scriptsDir/index_test.sh | tee -a $logFile;
$scriptsDir/grep_test.sh | tee -a $logFile;
