#!/bin/bash

# server
# dir="/hdd/code/yuhansun"
# dataset="Patents_100_random_20"
# data_dir="${dir}/data/${dataset}"
# code_dir="${dir}/code"

# local test setup
dir="/Users/zhouyang/Google_Drive/Projects/tmp/risotree"
dataset="Yelp"
data_dir="${dir}/${dataset}"
code_dir="/Users/zhouyang/Google_Drive/Projects/github_code"

db_path="${data_dir}/neo4j-community-3.1.1/data/databases/graph.db_Gleene_1.0"
graph_path="${data_dir}/graph.txt"
entity_path="${data_dir}/entity.txt"
label_path="${data_dir}/label.txt"
containID_path="${data_dir}/containID.txt"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"
echo "java -Xmx100g -jar ${jar_path} -h"
java -Xmx100g -jar ${jar_path} -h

echo "java -Xmx100g -jar ${jar_path} -f containID -dp ${db_path} -d ${dataset} -c ${containID_path}"
java -Xmx100g -jar ${jar_path} -f containID -dp ${db_path} -d ${dataset} -c ${containID_path}