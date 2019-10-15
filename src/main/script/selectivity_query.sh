#!/bin/bash
./package.sh

dir="/hdd/code/yuhansun"

dataset="Yelp_100"
MAX_HOPNUM=2
password="syhSYH.19910205"

data_dir="${dir}/data/${dataset}"
db_dir="${data_dir}/alpha"
code_dir="${dir}/code"
result_dir="${dir}/result/Riso-Tree/selectivity"
query_dir="${dir}/result/query/${dataset}"

query_size=2
query_count=50

split_mode="Gleenes"
maxPNSize="-1"
query_path=""
for selectivity in 0.0001 0.001 0.01 0.1
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"

	if [ -z "$query_path" ];	then
		query_path="${query_dir}/${query_size}_${selectivity}"
	else
		query_path+=",${query_dir}/${query_size}_${selectivity}"
	fi
done

output_path="${result_dir}"
log_path="${result_dir}/${dataset}_log.txt"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

java -Xmx100g -jar ${jar_path} \
	-f selectivityExperiment \
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