#!/bin/bash

hdfs dfs -rm -r output

spark-submit ./src/main.py hdfs://localhost:9000/user/root/input/ hdfs://localhost:9000/user/root/output/