#!/bin/bash

appName=$1
N=$2
logFolder=$3

printf "Testing to see that N mappers and reducers were spawned in the $appName application.\n"
expected=`expr 2 \* $N` # i.e. N mappers and N reducers
num_pid=$(cat $logFolder/*.txt | grep "PID" | uniq | wc -l)
printf "The application spawned this many worker processes: $num_pid. Expected: $expected\n"
if [[ $num_pid -eq $expected ]]; then
  echo "[PASSED] Spawned N mappers and reducers."
else
  echo "[FAILED] Spawned fewer than N mappers and/or reducers."
fi
echo

