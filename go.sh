#!/bin/sh
echo "Showing no directory named /tst in HDFS"
hadoop fs -ls /tst
echo "Showing files to be copied and compressed from local (including uncompressed sizes"
stat -c "%s %n" /home/hdadmin/StreamFiles/*
echo "Running program with 3 threads copying and compressing from a local file system into HDFS"
java -jar /home/hdadmin/code/hadoop-course/dist/ParallelLocalToHdfsCopy.jar /home/hdadmin/StreamFiles /tst 3
echo "Showing compressed files in HDFS"
hadoop fs -ls /tst

