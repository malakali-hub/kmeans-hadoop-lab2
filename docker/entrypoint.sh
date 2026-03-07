#!/bin/bash

start-dfs.sh
start-yarn.sh


hdfs dfs -mkdir -p /user/lab2/input


hdfs dfs -put -f iris_features.csv /user/lab2/input/


echo "5.1,3.5,1.4,0.2" > centroids.txt
echo "4.9,3.0,1.4,0.2" >> centroids.txt
echo "4.7,3.2,1.3,0.2" >> centroids.txt

hdfs dfs -mkdir -p /user/lab2/centroids
hdfs dfs -put -f centroids.txt /user/lab2/centroids/

hadoop jar kmeans.jar KMeansDriver /user/lab2/input /user/lab2/centroids /user/lab2/output 3 100


hdfs dfs -cat /user/lab2/output/*


tail -f /dev/null