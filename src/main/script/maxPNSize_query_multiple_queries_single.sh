#!/bin/bash
./package.sh

dir="/hdd/code/yuhansun"
result_dir="${dir}/result"
maxPNSize_result_dir="${result_dir}/Riso-Tree/maxPNSizeRisoTreeQuery"
code_dir="${dir}/code"
password="syhSYH.19910205"

clear_cache="true"
clear_cache_method="DOUBLE"

source ./utility.sh

# for dataset in "Yelp_100" "Gowalla_100" "foursquare_100"
for dataset in "foursquare_100"
do
	data_dir="${dir}/data/${dataset}"
	output_dir="${maxPNSize_result_dir}/${dataset}"
	mkdir -p ${output_dir}
	time=$(get_time)
	log_path="${output_dir}/${dataset}_log_${time}.txt"
	avg_path="${output_dir}/RISOTREE_avg.tsv"
	detail_path="${output_dir}/RISOTREE_detail.txt"
	echo "${time}" >> ${avg_path}
	echo "${time}" >> ${detail_path}

	# alpha=1.0
	# split_mode="Gleenes"
	# suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
	db_path="${data_dir}/neo4j-community-3.4.12_Gleenes_1.0_10_new_version/data/databases/graph.db"
	db_path+=",${data_dir}/neo4j-community-3.4.12_Gleenes_1.0_40_new_version/data/databases/graph.db"
	db_path+=",${data_dir}/neo4j-community-3.4.12_Gleenes_1.0_160_new_version/data/databases/graph.db"
	db_path+=",${data_dir}/neo4j-community-3.4.12_Gleenes_1.0_640_new_version/data/databases/graph.db"
	db_path+=",${data_dir}/neo4j-community-3.4.12_Gleenes_1.0_-1_new_version/data/databases/graph.db"

	# graph_path="${data_dir}/graph.txt"
	# entity_path="${data_dir}/entity.txt"
	# label_path="${data_dir}/graph_label.txt"
	# labelStrMapPath="${data_dir}/entity_string_label.txt"

	query_dir="${dir}/result/query/${dataset}"
	node_count=3
	query_count=50
	# for selectivity in 0.000001 0.00001 0.0001 0.001 0.01
	# for selectivity in 0.01 0.001 0.0001 0.00001 0.000001
	for selectivity in 0.001
	do
		query_path="${query_dir}/${node_count}_${selectivity}"
		jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

		# split_mode="Gleenes"
		# alpha="1.0"
		# containID_suffix="${split_mode}_${alpha}_new_version"
		# containID_path="${data_dir}/containID.txt"

		java -Xmx100g -jar ${jar_path} \
			-f maxPNSizeRisoTreeQueryMultiple \
			-dp ${db_path} \
			-d ${dataset} \
			-MAX_HOPNUM 2 \
			-queryPath ${query_path} \
			-queryCount ${query_count} \
			-password ${password} \
			-clearCache ${clear_cache} \
			-clearCacheMethod ${clear_cache_method} \
			-outputPath ${output_dir} \
			>> ${log_path}
	done
done