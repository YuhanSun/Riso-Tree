#!/bin/bash
./package.sh

# server
dir="/hdd/code/yuhansun"
code_dir="${dir}/code"
jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

for dataset in Yelp_100 Patents_100_random_20 Gowalla_100
do
	data_dir="${dir}/data/${dataset}"

	# server setup
	# graph_path="${data_dir}/graph.txt"
	# entity_path="${data_dir}/entity.txt"
	label_path="${data_dir}/label.txt"
	label_graph_path="${data_dir}/graph_label.txt"
	labelStrMapPath="${data_dir}/entity_string_label.txt"
	# spatialNodePNPath="${data_dir}/spatialNodesZeroOneHopPN.txt"

	# split_mode="Gleenes"
	# alpha="1.0"
	# containID_suffix="${split_mode}_${alpha}_new_version"
	# containID_path="${data_dir}/containID_${containID_suffix}.txt"

	java -Xmx100g -jar ${jar_path} 
	-f singleLabelListToLabelGraph \
	-labelStrMapPath ${labelStrMapPath}\
	-lp ${label_path} \
	-outputPath ${label_graph_path}
done