#!/bin/bash

dataset="Yelp_100"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/${dataset}"
db_dir="${dir}/data/${dataset}/alpha"

graph_path="${data_dir}/graph.txt"
entity_path="${data_dir}/entity.txt"
label_path="${data_dir}/graph_label.txt"
labelStrMapPath="${data_dir}/entity_string_label.txt"
spatialNodePNPath="${data_dir}/spatialNodesZeroOneHopPN.txt"

split_mode="Gleenes"
maxPNSize="-1"

# for alpha in 0 0.25 0.5 0.75 1.0
# for alpha in 0
# for alpha in 0.55 0.6 0.65 0.7 0.8 0.85 0.9 0.95
for alpha in 0.999999
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
	echo ${suffix}

	source_path="${data_dir}/neo4j-community-3.4.12_node_edge/"
	target_path="${db_dir}/neo4j-community-3.4.12_${suffix}"
	rm -r ${target_path}
	cp -a ${source_path} ${target_path}

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

	# 0-hop
	java -Xmx100g -jar ${jar_path} -f wikiConstructPNTimeSingleHopNoGraphDb \
	-c ${containID_path} -gp ${graph_path} -labelStrMapPath ${labelStrMapPath}\
	-lp ${label_path} -hop 0 -PNPrefix ${PNPathAndPrefix} -maxPNSize ${maxPNSize}

	# 1-hop
	java -Xmx100g -jar ${jar_path} -f wikiConstructPNTimeSingleHopNoGraphDb \
	-c ${containID_path} -gp ${graph_path} -labelStrMapPath ${labelStrMapPath}\
	-lp ${label_path} -hop 1 -PNPrefix ${PNPathAndPrefix} -maxPNSize ${maxPNSize}

	# 2-hop
	java -Xmx100g -jar ${jar_path} -f wikiConstructPNTimeSingleHopNoGraphDb \
	-c ${containID_path} -gp ${graph_path} -labelStrMapPath ${labelStrMapPath}\
	-lp ${label_path} -hop 2 -PNPrefix ${PNPathAndPrefix} -maxPNSize ${maxPNSize}

done