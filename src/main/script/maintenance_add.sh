#!/bin/bash
./package.sh
source ./utility.sh

dataset="wikidata"
# dataset="Gowalla_100"

MAX_HOPNUM=$(get_MAX_HOPNUM $dataset)

run_safe=1
run_naive=0

cur_dir="/hdd/code/yuhansun/data/${dataset}/add"
backup_dir="${cur_dir}/backup"

split_mode="Gleenes"
maxPNSize=-1
alpha=1.0
suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
db_dir="${cur_dir}/neo4j-community-3.4.12_${suffix}"
db_path="${db_dir}/data/databases/graph.db"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/${dataset}"
code_dir="${dir}/code"
result_dir="${dir}/result/Riso-Tree/add"
mkdir -p $result_dir

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

# server setup
graph_path="${data_dir}/graph.txt"
entity_path="${data_dir}/entity.txt"
label_path="${data_dir}/graph_label.txt"
labelStrMapPath="${data_dir}/entity_string_label.txt"
spatialNodePNPath="${data_dir}/spatialNodesZeroOneHopPN.txt"
graph_property_edge_path="${data_dir}/graph_property_edge.txt"

add_edge_path="${cur_dir}/edges.txt"

test_count=1000

##### No safe nodes strategy #####
if [ $run_naive == 1 ]; then
	safe_nodes_path="${cur_dir}/safeNodes.txt"
	output_path="${result_dir}/${dataset}"

	# remove current db_dir and use the backup to restore
	rm -r ${db_dir}
	cp -a ${backup_dir}/neo4j-community-3.4.12_${suffix}_after_pn $db_dir

	rm $safe_nodes_path
	cp ${backup_dir}/safeNodes.txt $safe_nodes_path

	java -Xmx100g -jar ${jar_path} \
		-f addEdgeExperiment \
		-dp ${db_path}	\
		-MAX_HOPNUM ${MAX_HOPNUM}	\
		-maxPNSize ${maxPNSize}	\
		-edgePath ${add_edge_path}	\
		-queryCount ${test_count}	\
		-safeNodesPath ""	\
		-outputPath ${output_path}
fi

###### Safe nodes strategy used #######
if [ $run_safe == 1 ]; then
	output_path="${result_dir}/${dataset}_with_safe"

	# remove current db_dir and use the backup to restore
	rm -r ${db_dir}
	cp -a ${backup_dir}/neo4j-community-3.4.12_${suffix}_after_pn $db_dir

	java -Xmx100g -jar ${jar_path} \
		-f addEdgeExperiment \
		-dp ${db_path}	\
		-MAX_HOPNUM ${MAX_HOPNUM}	\
		-maxPNSize ${maxPNSize}	\
		-edgePath ${add_edge_path}	\
		-queryCount ${test_count}	\
		-safeNodesPath $safe_nodes_path	\
		-outputPath ${output_path}
fi


