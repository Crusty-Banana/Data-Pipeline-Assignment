#!/bin/bash

# Start the HDFS NameNode
hdfs namenode &

# Wait for the NameNode to start
sleep 10

# Run the HDFS commands
hdfs dfs -mkdir -p /raw_zone/fact/activity
hdfs dfs -chown nifi:supergroup /raw_zone/fact/activity
hdfs dfs -chmod 777 /raw_zone/fact/activity
hdfs dfs -mkdir -p /output_zone
hdfs dfs -chown spark:supergroup /output_zone
hdfs dfs -chmod 777 /output_zone
hdfs dfs -put /Hadoop/danh_sach_sv_de.csv hdfs://namenode/raw_zone/fact/student_list

# Keep the container running
tail -f /dev/null
