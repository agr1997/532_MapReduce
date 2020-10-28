# mapreduce-agr1997
mapreduce-agr1997 created by GitHub Classroom

## First Deliverable Instructions:
This project is part of a group submission. Grader may refer to any of Dylan Finkbeiner, Kathyrn Ricci or my code.  
Our development git repo: https://github.com/dylanfinkbeiner/532-mapreduce

To run project:
-Ensure machine can run ant commands to build the jar file:  
  >sudo apt-get install ant

-Then execute the bash script:  
  >./execute_tests.sh

Note: The presence of %&% as a separator is a conscious design choice until we can find a way to format better.

Note: If build path is incomplete, please add JRE library to the build_path.xml. We have shipped the MR library only with the externalsort library it uses and have left it to the user to run any instance of JRE over version 8 as they so please.  

## Second Deliverable Instructions:
Same as before. Run bash script:
  >./execute_tests.sh
  
## FINAL SUBMISSION

*Refer to Dylan Finkbeiner's repo for group submission.*

[This is the one official submission that will be shared by 
Dylan Finkbeiner, Kathryn Ricci, and Arvind Govindaraja.]

NOTE: Our design document can be found in the "docs" folder.

### INTRODUCTION

To run our tests, just execute the command "./master_test.sh" from
the command line. You will need Apache Ant, which can be obtained by running 
"sudo apt-get install ant". The master test executes other tests found in the
scripts directory, "test/scripts/".

This script will compile our code by calling Ant on our "build.xml" build file
inside the "532_mapreduce/" folder. Then it executes our test applications,
first executing the Word Count application, then it executes the Inverted Index 
application twice with different values of N, and finally the Grep application. 

The outputs from these tests are written both to standard out and to a file 
named "all_tests_log.txt" which will be located in the "docs/" folder.


### INTERPRETING THE OUTPUT

#### FAULT TOLERANCE

You will see messages in the output indicating where execution of an
application begins and ends. Each application will be executed with some
command line arguments specifying the nature of how fault tolerance will
be tested. In the output, there will be lines starting with 
"[FAULT TOLERANCE]" that explain in plain english how fault tolerance
will be tested. During execution, our library code will write messages to 
standard out indicating when workers are started or restarted, as well
as messages like "Master timed out waiting for a response from worker 1"
that come from the ClientHandlers. These messages will help you to see,
immediately, that the fault tolerance is being tested in the way
described. You may also see the fault tolerance tested in ways not 
explicitly described, due to the random timing of your OS's context
switching, but you should still see the other tests of correctness pass.

These outputs are just meant to show that the fault tolerance is being
tested, but the true test of their success should be judged by the
results of the other tests, testing correctness of output and that N mapper
and reducer workers were created.

Under the hood, the execute() function of MapReduce.java is
interwoven with code that ensures that the fault tolerance mechanisms
are tested as stated, by killing worker processes and letting our
fault tolerance mechanisms kick in. We integrated the tests at this
level of the code to ensure that the fault tolerance mechanisms
are tested reliably each time the tests are run, leaving nothing to
chance at runtime.


#### CORRECTNESS OF OUTPUT

You will see messages in the output indicating the beginning and
end of so-called "OUTPUT COMPARISON TEST"s. These tests compare the 
outputs produced by each test application to gold-standard outputs produced 
by carefully designed Python scripts. Comparison is done using unix's 
'sort' and 'cmp' functions to compare the two files, sorted, byte-by-byte.
If the files are identical, you will see "[PASSED] Outputs match!". See the
"compare_outputs.sh" script in the scripts directory for more details.


#### CORRECTLY SPAWNING N WORKER AND REDUCER PROCESSES

You will see messages in the output indicating the beginning and end of
"N PROCESSES CREATED TEST"s. These tests are designed to show that
in fact N distinct worker and reducer processes were created during
execution of a test application. You can see that at the start of the 
main functions of the Mapper and Reducer classes, they log their process
IDs and worker type (mapper/reducer) out to a log file. 
So this test counts the number of unique PIDs found among these log files, 
and number of each type of worker and uses unix commands to make sure that
these numbers matches the expected numbers, based on N. See
"check_N_procs.sh" in the scripts directory for more details.

### ADDITIONAL TESTING

If the grader needs to test further, the master_test.sh refers to scripts in the test/scripts directory. There, you will find shell scripts for index, grep and wordcount (wc). Instructions to run arbitrary tests are avaiable in those shell scripts. 
