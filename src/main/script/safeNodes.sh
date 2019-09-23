#!/bin/bash
./package.sh

dir="/hdd/code/yuhansun"

# for dataset in "Yelp_100" "Gowalla_100" "foursquare_100"
for dataset in "wikidata"
do

	data_dir="${dir}/data/${dataset}"
	code_dir="${dir}/code"
	result_dir="${dir}/result"

	# alpha=1.0
	# split_mode="Gleenes"
	# suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"

	# graph_path="${data_dir}/graph.txt"
	# entity_path="${data_dir}/entity.txt"
	# label_path="${data_dir}/graph_label.txt"
	# labelStrMapPath="${data_dir}/entity_string_label.txt"

	PNPathAndPrefix="${data_dir}/PathNeighbors_Gleenes_1.0_-1_new_version"
	hopListStr="0,1"
	output_path="${data_dir}/safeNodes.txt"

	jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

	# split_mode="Gleenes"
	# alpha="1.0"
	# containID_suffix="${split_mode}_${alpha}_new_version"
	# containID_path="${data_dir}/containID.txt"

	java -Xmx100g -jar ${jar_path}	\
		-f generateSafeNodes	\
		-PNPrefix ${PNPathAndPrefix}	\
		-hopListStr ${hopListStr}	\
		-nodeCount 47116657
		-outputPath ${output_path}
done