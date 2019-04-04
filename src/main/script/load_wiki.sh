#!/bin/bash
./package.sh
# tree_type="Gleene_1.0"

dataset="wikidata"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/${dataset}"
code_dir="${dir}/code"

# local test setup
# dir="/Users/zhouyang/Google_Drive/Projects/tmp/risotree"
# dataset="Yelp"
# data_dir="${dir}/${dataset}"
# code_dir="/Users/zhouyang/Google_Drive/Projects/github_code"

# 407CD setup
# data_dir="D:/Project_Data/wikidata-20180308-truthy-BETA.nt"
# code_dir="D:/Google_Drive/Projects/github_code"
# db_path="D:/Neo4jData/neo4jDatabases/database-0c3f32e4-025a-4a22-a4cd-a9b979a9adf8/installation-3.4.9/data/databases/graph.db"


# server setup
db_path="${data_dir}/neo4j-community-3.4.12/data/databases/graph.db"

# common setup
# graph_path="${data_dir}/graph.txt"
entity_path="${data_dir}/entity.txt"
# label_path="${data_dir}/label.txt"
containID_path="${data_dir}/containID.txt"
map_path="${data_dir}/node_map_RTree.txt"
jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"


java -Xmx100g -jar ${jar_path} -h

###### Load ######
# MAX_HOPNUM="2"
# PNPathAndPreffix="${data_dir}/PathNeighbors_${tree_type}"
# java -Xmx100g -jar ${jar_path} -f LoadAll -ep ${entity_path} -dp ${db_path} -c ${containID_path} -gp ${graph_path} -lp ${label_path} -MAX_HOPNUM ${MAX_HOPNUM} -PNPreffix ${PNPathAndPreffix} -mapPath ${map_path} -d ${dataset}
java -Xmx100g -jar ${jar_path} -f wikiConstructRTree -dp ${db_path} -d ${dataset} -ep ${entity_path}

