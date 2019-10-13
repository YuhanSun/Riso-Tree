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
query_count=100
query_path="${query_dir}/{query_size}_0.0001"
query_path+=",{query_dir}/{query_size}_0.001"
query_path+=",{query_dir}/{query_size}_0.01"
query_path+=",{query_dir}/{query_size}_0.1"

outputPath="${result_dir}/result.txt"
log_path="${result_dir}/log.txt"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

for alpha in 0 0.9 0.99 0.999 0.9999 1.0
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
	db_path="${db_dir}/neo4j-community-3.4.12_${suffix}/data/databases/graph.db"
	
	java -Xmx100g -jar ${jar_path} \
		-f alphaExperiment \
		-dp ${db_path} \
		-d ${dataset} \
		-method RISOTREE	\
		-MAX_HOPNUM ${MAX_HOPNUM} \
		-queryPath ${query_path} \
		-queryCount ${query_count} \
		-password ${password}	\
		-clearCache "true"	\
		-clearCacheMethod	"DOUBLE"\
		-outputPath ${output_path}	\
		>> ${log_path}
done