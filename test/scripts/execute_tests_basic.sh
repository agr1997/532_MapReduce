#!/bin/bash

rm -r target/
rm -r out/

# Compile the Word Count test application with our library using Ant.
ant -buildfile build.xml
#ant build-mapper-jar
ant build-wc-jar

N=1

#cp -r test/ target/classes/

# Execute the resulting jar.
java -jar ./test/wordcountapp.jar "wordcount_cfg_n1.txt"

#dir_name="./word-count-$N_wc-output/final"
dir_name="./out/word-count-${N}-output"
combined_name="combined_wc_${N}_out.txt"

# MR produces N output files. Concatenate them into single file.
#cat ./word-count-output/final/* > combined_wc_out.txt
#cat $dir_name/* > combined_wc_out.txt
cat ${dir_name}/final/* > ${dir_name}/${combined_name}

printf "\n\nComparing output of Word Count application to expected output.\n"
# Sort the lines of each file and compare.
#$(cmp -s <(sort combined_wc_out.txt) <(sort ./test/benchmark_wc_out.txt))
$(cmp -s <(sort "${dir_name}/${combined_name}") <(sort ./test/benchmark_wc_out.txt))
exitStatus=$?
if [[ $exitStatus -eq 0 ]]; then
    echo "Word Count test case with N=$N passed."
else
    echo "Word Count test case with N=$N failed."
fi
echo

expected=`expr 2 \* $N` # i.e. N_wc mappers and N_wc reducers
num_pid=$(cat $dir_name/log/*.txt | grep "PID" | uniq | wc -l)
printf "Number of distinct process IDs associated with Word Count application: $num_pid\nExpected at least: $expected\n"



N=4

#cp -r test/ target/classes/

# Execute the resulting jar.
java -jar ./test/wordcountapp.jar "wordcount_cfg_n4.txt"

#dir_name="./word-count-$N_wc-output/final"
dir_name="./out/word-count-${N}-output"
combined_name="combined_wc_${N}_out.txt"

# MR produces N output files. Concatenate them into single file.
#cat ./word-count-output/final/* > combined_wc_out.txt
#cat $dir_name/* > combined_wc_out.txt
cat ${dir_name}/final/* > ${dir_name}/${combined_name}

printf "\n\nComparing output of Word Count application to expected output.\n"
# Sort the lines of each file and compare.
#$(cmp -s <(sort combined_wc_out.txt) <(sort ./test/benchmark_wc_out.txt))
$(cmp -s <(sort "${dir_name}/${combined_name}") <(sort ./test/benchmark_wc_out.txt))
exitStatus=$?
if [[ $exitStatus -eq 0 ]]; then
    echo "Word Count test case with N=$N passed."
else
    echo "Word Count test case with N=$N failed."
fi
echo

expected=`expr 2 \* $N` # i.e. N_wc mappers and N_wc reducers
num_pid=$(cat $dir_name/log/*.txt | grep "PID" | uniq | wc -l)
printf "Number of distinct process IDs associated with Word Count application: $num_pid\nExpected at least: $expected\n"


N=7

# Execute the resulting jar.
java -jar ./test/wordcountapp.jar "wordcount_cfg_n7.txt"

dir_name="./out/word-count-${N}-output"
combined_name="combined_wc_${N}_out.txt"

# MR produces N output files. Concatenate them into single file.
#cat ./word-count-output/final/* > combined_wc_out.txt
#cat $dir_name/* > combined_wc_out.txt
cat ${dir_name}/final/* > ${dir_name}/${combined_name}

printf "\n\nComparing output of Word Count application to expected output.\n"
# Sort the lines of each file and compare.
$(cmp -s <(sort "${dir_name}/${combined_name}") <(sort ./test/benchmark_wc_out.txt))
exitStatus=$?
if [[ $exitStatus -eq 0 ]]; then
    echo "Word Count test case with N=$N passed."
else
    echo "Word Count test case with N=$N failed."
fi
echo

expected=`expr 2 \* $N` # i.e. N_wc mappers and N_wc reducers
num_pid=$(cat $dir_name/log/*.txt | grep "PID" | uniq | wc -l)
printf "Number of distinct process IDs associated with Word Count application: $num_pid\nExpected at least: $expected\n"

####################

ant build-index-jar
N=4

# Execute the resulting jar.
java -jar ./test/indexapp.jar "invertedindex_cfg_n${N}.txt"

dir_name="./out/inverted-index-${N}-output"
combined_name="combined_index_${N}_out.txt"
app_name="Inverted Index"

# MR produces N output files. Concatenate them into single file.
cat ${dir_name}/final/* > ${dir_name}/${combined_name}

printf "\n\nComparing output of ${app_name} application to expected output.\n"
# Sort the lines of each file and compare.
$(cmp -s <(sort "${dir_name}/${combined_name}") <(sort ./test/benchmark_index_out.txt))
exitStatus=$?
if [[ $exitStatus -eq 0 ]]; then
    echo "${app_name} test case with N=$N passed."
else
    echo "${app_name} test case with N=$N failed."
fi
echo

expected=`expr 2 \* $N` # i.e. N mappers and N reducers
num_pid=$(cat $dir_name/log/*.txt | grep "PID" | uniq | wc -l)
printf "Number of distinct process IDs associated with ${app_name} application: $num_pid\nExpected at least: $expected\n"


#############

ant build-grep-jar
N=4

# Execute the resulting jar.
java -jar ./test/grepapp.jar "grep_cfg_n${N}.txt"

dir_name="./out/grep-${N}-output"
combined_name="combined_grep_${N}_out.txt"
app_name="Grep"

# MR produces N output files. Concatenate them into single file.
cat ${dir_name}/final/* > ${dir_name}/${combined_name}

printf "\n\nComparing output of ${app_name} application to expected output.\n"
# Sort the lines of each file and compare.
$(cmp -s <(sort "${dir_name}/${combined_name}") <(sort ./test/benchmark_grep_out.txt))
exitStatus=$?
if [[ $exitStatus -eq 0 ]]; then
    echo "${app_name} test case with N=$N passed."
else
    echo "${app_name} test case with N=$N failed."
fi
echo

expected=`expr 2 \* $N` # i.e. N mappers and N reducers
num_pid=$(cat $dir_name/log/*.txt | grep "PID" | uniq | wc -l)
printf "Number of distinct process IDs associated with ${app_name} application: $num_pid\nExpected at least: $expected\n"
