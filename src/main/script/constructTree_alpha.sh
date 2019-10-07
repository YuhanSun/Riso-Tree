#!/bin/bash
./package.sh

dataset="Yelp_100"

# server
dir="/hdd/code/yuhansun"
db_dir="${dir}/data/${dataset}/alpha"
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
graph_path="${data_dir}/graph.txt"
entity_path="${data_dir}/entity.txt"
label_path="${data_dir}/graph_label.txt"
labelStrMapPath="${data_dir}/entity_string_label.txt"
spatialNodePNPath="${data_dir}/spatialNodesZeroOneHopPN.txt"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

split_mode="Gleenes"
maxPNSize="-1"

# for alpha in 0 0.25 0.5 0.75 1.0
# for alpha in 0
for alpha in 0.55 0.6 0.65 0.7 0.8 0.85 0.9 0.95
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"

	db_path="${db_dir}/neo4j-community-3.4.12_${suffix}/data/databases/graph.db"
	containID_path="${db_dir}/containID_${suffix}.txt"
	PNPathAndPrefix="${db_dir}/PathNeighbors_${suffix}"

	# Construct the tree structure
	java -Xmx100g -jar ${jar_path} \
	-f wikiConstructRTree \
	-dp ${db_path} \
	-d ${dataset} \
	-ep ${entity_path} \
	-spatialNodePNPath ${spatialNodePNPath} \
	-alpha ${alpha} \
	-maxPNSize ${maxPNSize}

	# Generate the leaf contain spatial node file
	java -Xmx100g -jar ${jar_path} -f wikiGenerateContainSpatialID \
	-dp ${db_path} \
	-d ${dataset} \
	-c ${containID_path}

	# # 0-hop
	# java -Xmx100g -jar ${jar_path} -f wikiConstructPNTimeSingleHop \
	# -dp ${db_path} -c ${containID_path} -gp ${graph_path} -labelStrMapPath ${labelStrMapPath}\
	# -lp ${label_path} -hop 0 -PNPrefix ${PNPathAndPrefix} -maxPNSize ${maxPNSize}

	# java -Xmx100g -jar ${jar_path} -f wikiLoadPN \
	# -dp ${db_path} -c ${containID_path} -gp ${graph_path} -labelStrMapPath ${labelStrMapPath}\
	# -lp ${label_path} -hop 0 -PNPrefix ${PNPathAndPrefix} -maxPNSize ${maxPNSize}

	# # # 1-hop
	# java -Xmx100g -jar ${jar_path} -f wikiConstructPNTimeSingleHop \
	# -dp ${db_path} -c ${containID_path} -gp ${graph_path} -labelStrMapPath ${labelStrMapPath}\
	# -lp ${label_path} -hop 1 -PNPrefix ${PNPathAndPrefix} -maxPNSize ${maxPNSize}

	# java -Xmx100g -jar ${jar_path} -f wikiLoadPN \
	# -dp ${db_path} -c ${containID_path} -gp ${graph_path} -labelStrMapPath ${labelStrMapPath}\
	# -lp ${label_path} -hop 1 -PNPrefix ${PNPathAndPrefix} -maxPNSize ${maxPNSize}
done