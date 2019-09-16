#!/bin/bash
./package.sh

dir="/hdd/code/yuhansun"
# outputPath="${dir}/data/degreeSD.csv"
outputPath="${dir}/data/degreeAvg.csv"

for dataset in "Yelp_100" "Gowalla_100" "Patents_100_random_20" "foursquare_100"
do

	data_dir="${dir}/data/${dataset}"
	code_dir="${dir}/code"

	graph_path="${data_dir}/graph.txt"
	entity_path="${data_dir}/entity.txt"
	label_path="${data_dir}/graph_label.txt"
	labelStrMapPath="${data_dir}/entity_string_label.txt"

	jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

	split_mode="Gleenes"
	alpha="1.0"
	# containID_suffix="${split_mode}_${alpha}_new_version"
	containID_path="${data_dir}/containID.txt"

	java -Xmx100g -jar ${jar_path} \
		-f degreeAvg \
		-gp ${graph_path} \
		-ep ${entity_path} \
		-outputPath ${outputPath}

	# java -Xmx100g -jar ${jar_path} \
	# 	-f degreeSD \
	# 	-gp ${graph_path} \
	# 	-outputPath ${outputPath}

done