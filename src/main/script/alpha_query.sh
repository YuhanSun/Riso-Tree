#!/bin/bash
./package.sh

dir="/hdd/code/yuhansun"

dataset="Yelp_100"
MAX_HOPNUM=2

data_dir="${dir}/data/${dataset}"
db_dir="${data_dir}/alpha"
code_dir="${dir}/code"
result_dir="${dir}/result/Riso-Tree/alpha"
query_dir="${dir}/result/query/${dataset}"

query_size=2
query_count=100
query_path="${query_dir}/{query_size}_0.0001"
query_path+=",{query_dir}/{query_size}_0.001"
query_path+=",{query_dir}/{query_size}_0.01"
query_path+=",{query_dir}/{query_size}_0.1"

for alpha in 0 0.9 0.99 0.999 0.9999 1.0
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
	db_path="${db_dir}/neo4j-community-3.4.12_${suffix}/data/databases/graph.db"
	
	java -Xmx100g -jar ${jar_path} \
		-f alphaExperiment \
		-dp ${db_path} \
		-d ${dataset} \
		-MAX_HOPNUM ${MAX_HOPNUM} \
		-queryPath ${query_path} \
		-queryCount ${query_count} \
		-outputPath ${output_path}
done
db_path="${data_dir}/neo4j-community-3.4.12_Gleenes_1.0_10_new_version/data/databases/graph.db"
db_path+=",${data_dir}/neo4j-community-3.4.12_Gleenes_1.0_40_new_version/data/databases/graph.db"
db_path+=",${data_dir}/neo4j-community-3.4.12_Gleenes_1.0_160_new_version/data/databases/graph.db"
db_path+=",${data_dir}/neo4j-community-3.4.12_Gleenes_1.0_640_new_version/data/databases/graph.db"

# graph_path="${data_dir}/graph.txt"
# entity_path="${data_dir}/entity.txt"
# label_path="${data_dir}/graph_label.txt"
# labelStrMapPath="${data_dir}/entity_string_label.txt"

query_dir="${dir}/result/query/${dataset}"
node_count=3
query_count=50
for selectivity in 0.000001 0.00001 0.0001 0.001
do
	query_path="${query_dir}/${node_count}_${selectivity}"

	output_path="${result_dir}/Riso-Tree/maxPNSizeRisoTreeQuery"

	jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

	# split_mode="Gleenes"
	# alpha="1.0"
	# containID_suffix="${split_mode}_${alpha}_new_version"
	# containID_path="${data_dir}/containID.txt"

	
done