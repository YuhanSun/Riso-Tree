#!/bin/bash

tree_type="Gleene_1.0"

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

db_path="${data_dir}/neo4j-community-3.1.1/data/databases/graph.db_${tree_type}"
graph_path="${data_dir}/graph.txt"
entity_path="${data_dir}/entity.txt"
label_path="${data_dir}/label.txt"
containID_path="${data_dir}/containID.txt"
map_path="${data_dir}/node_map_RTree.txt"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

# echo "java -Xmx100g -jar ${jar_path} -h"
# java -Xmx100g -jar ${jar_path} -h

###### Load ######
MAX_HOPNUM="2"
PNPathAndPreffix="${data_dir}/PathNeighbors_${tree_type}"
java -Xmx100g -jar ${jar_path} -f LoadAll -ep ${entity_path} -dp ${db_path} -c ${containID_path} -gp ${graph_path} -lp ${label_path} -MAX_HOPNUM ${MAX_HOPNUM} -PNPreffix ${PNPathAndPreffix} -mapPath ${map_path} -d ${dataset}
