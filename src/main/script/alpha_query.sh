#!/bin/bash
./package.sh

dir="/hdd/code/yuhansun"

dataset="Yelp_100"
MAX_HOPNUM=2
password="syhSYH.19910205"

data_dir="${dir}/data/${dataset}"
db_dir="${data_dir}/alpha"
code_dir="${dir}/code"
result_dir="${dir}/result/Riso-Tree/alpha"
query_dir="${dir}/result/query/${dataset}"

query_size=2
query_count=50

split_mode="Gleenes"
maxPNSize="-1"
db_path=""
for alpha in 0 0.9 0.99 0.999 0.9999 1.0
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
	if [-n $db_path]
	then
		db_path="${db_dir}/neo4j-community-3.4.12_${suffix}/data/databases/graph.db"
	else
		db_path+=",${db_dir}/neo4j-community-3.4.12_${suffix}/data/databases/graph.db"
	fi
done

output_path="${result_dir}/${dataset}_result.txt"
log_path="${result_dir}/${dataset}_log.txt"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

for selectivity in 0.0001 0.001 0.01 0.1
do
	
	query_path="${query_dir}/${query_size}_${selectivity}"

	java -Xmx100g -jar ${jar_path} \
		-f alphaExperiment \
		-dp ${db_path} \
		-d ${dataset} \
		-MAX_HOPNUM ${MAX_HOPNUM} \
		-queryPath ${query_path} \
		-queryCount ${query_count} \
		-password ${password}	\
		-clearCache "true"	\
		-clearCacheMethod "DOUBLE" \
		-outputPath ${output_path} \
		>> ${log_path}
done
printf "\n" >> ${output_path}