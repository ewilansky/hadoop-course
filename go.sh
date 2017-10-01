#!/bin/sh

printf "Removing  hdfs dir /tst if present\n"
hadoop fs -rm -R /tst

printf "\nDemonstrating no directory named /tst in HDFS:\n"
hadoop fs -ls /tst

printf "\nShowing files to be copied and compressed from local (including uncompressed file sizes:\n"
stat -c "%s %n" /home/hdadmin/StreamFiles/*

printf "\nRunning program with 3 threads copying and compressing from a local file system into HDFS:\n"
./run.sh

printf "\nResulting compressed files in HDFS:\n"
hadoop fs -ls /tst

