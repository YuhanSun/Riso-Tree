#!/bin/bash
./package.sh

# dataset="wikidata"
# AX_HOPNUM=1
# hopListStr="0 1"

dataset="Gowalla_100"
# dataset="foursquare_100"
MAX_HOPNUM=2
hopListStr="0 1 2"

####################
split_mode="Gleenes"
maxPNSize="-1"
alpha=1.0

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/${dataset}"
db_dir="${dir}/data/${dataset}/selectivity"
code_dir="${dir}/code"

mkdir -p $db_dir

graph_path="${data_dir}/graph.txt"
entity_path="${data_dir}/entity.txt"
label_path="${data_dir}/graph_label.txt"
labelStrMapPath="${data_dir}/entity_string_label.txt"
entityStringLabelMapPath="${data_dir}/entity_string_label.txt"
spatialNodePNPath="${data_dir}/spatialNodesZeroOneHopPN_-1.txt"
jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

node_edges_db_dir="${data_dir}/neo4j-community-3.4.12_node_edges"
if [ ! -d "$node_edges_db_dir" ];	then
	node_edges_db_path="$node_edges_db_dir/data/databases/graph.db"
	cp -a ${dir}/data/neo4j_versions/neo4j-community-3.4.12_ip_modified $node_edges_db_dir

	java -Xmx100g -jar ${jar_path} -f wikidataLoadGraph \
		-ep ${entity_path} \
		-lp ${label_path} \
		-entityStringLabelMapPath ${entityStringLabelMapPath} \
		-dp ${node_edges_db_path}

	java -Xmx100g -jar ${jar_path} -f loadGraphEdgesNoMap \
			-dp ${node_edges_db_path}	\
			-gp ${graph_path}
fi

if [ ! -f "$spatialNodePNPath" ];	then
	java -Xmx100g -jar ${jar_path} \
	-f wikigenerateZeroOneHopPNForSpatialNodes \
	-gp ${graph_path} \
	-lp ${label_path} \
	-ep ${entity_path} \
	-entityStringLabelMapPath ${entityStringLabelMapPath} \
	-maxPNSize -1 \
	-MAX_HOPNUM 1 \
	-outputPath ${spatialNodePNPath}
fi

suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
echo ${suffix}
db_path="${db_dir}/neo4j-community-3.4.12_${suffix}/data/databases/graph.db"

source_path="${data_dir}/neo4j-community-3.4.12_node_edges/"
target_path="${db_dir}/neo4j-community-3.4.12_${suffix}"
if [ ! -d "$target_path" ];	then
	# Copy the node edges db dir to ./selectivity.
	cp -a ${source_path} ${target_path}
	touch ${target_path}
fi


containID_path="${db_dir}/containID_${suffix}.txt"
if [ ! -f "$containID_path" ];	then
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
fi

# modify
PNPathAndPrefix="${db_dir}/PathNeighbors_${suffix}"
if [ ! -f "${PNPathAndPrefix}_${MAX_HOPNUM}.txt" ];	then
	for hop in $hopListStr
	do
		# Always keep all 0-hop path neighbors
		if [ $hop == 0 ];	then
			java -Xmx100g -jar ${jar_path} -f wikiConstructPNTimeSingleHopNoGraphDb \
			-c ${containID_path} -gp ${graph_path} -labelStrMapPath ${labelStrMapPath}\
			-lp ${label_path} -hop ${hop} -PNPrefix ${PNPathAndPrefix} -maxPNSize -1
		else
			java -Xmx100g -jar ${jar_path} -f wikiConstructPNTimeSingleHopNoGraphDb \
				-c ${containID_path} -gp ${graph_path} -labelStrMapPath ${labelStrMapPath}\
				-lp ${label_path} -hop ${hop} -PNPrefix ${PNPathAndPrefix} -maxPNSize ${maxPNSize}
		fi
	done

	java -Xmx100g -jar ${jar_path} \
		-f wikiLoadAllHopPN \
		-PNPrefix ${PNPathAndPrefix} \
		-hopListStr $hopListStr \
		-dp ${db_path} \
		-c ${containID_path}
fi


