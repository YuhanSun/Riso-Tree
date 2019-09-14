#!/bin/bash
./package.sh

dataset="foursquare_100"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/${dataset}"
code_dir="${dir}/code"

# server setup
graph_path="${data_dir}/graph.txt"
entity_path="${data_dir}/entity.txt"
label_path="${data_dir}/graph_label.txt"
labelStrMapPath="${data_dir}/entity_string_label.txt"
spatialNodePNPath="${data_dir}/spatialNodesZeroOneHopPN_-1.txt"
entityStringLabelMapPath="${data_dir}/entity_string_label.txt"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

split_mode="Gleenes"
maxPNSize="-1"

for alpha in 1.0
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"

	db_path="${data_dir}/neo4j-community-3.4.12_${suffix}/data/databases/graph.db"
	containID_path="${data_dir}/containID_${suffix}.txt"
	PNPathAndPrefix="${data_dir}/PathNeighbors_${suffix}"

	# Generate the 0-1 hop pn for spatial nodes
	# java -Xmx100g -jar ${jar_path} \
	# -f wikigenerateZeroOneHopPNForSpatialNodes \
	# -gp ${graph_path} \
	# -lp ${label_path} \
	# -ep ${entity_path} \
	# -entityStringLabelMapPath ${entityStringLabelMapPath} \
	# -maxPNSize -1 \
	# -outputPath ${spatialNodePNPath}

	# Load graph nodes.
	java -Xmx100g -jar ${jar_path} -f wikidataLoadGraph \
		-ep ${entity_path} \
		-lp ${label_path} \
		-entityStringLabelMapPath ${entityStringLabelMapPath} \
		-dp ${db_path}

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
	java -Xmx100g -jar ${jar_path} \
		-f wikiGenerateContainSpatialID \
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