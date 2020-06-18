#!/bin/bash
./package.sh

dir="/hdd/code/yuhansun"

# dataset="Yelp_100"
# selectivity=0.0001

# dataset="Gowalla_100"
dataset="foursquare_100"
selectivity=0.000001
joinDistance="0.001,0.002,0.003,0.004"

MAX_HOPNUM=2

#dataset="wikidata"
#MAX_HOPNUM=1

run_spatial=1
run_risotree=1
run_naive=1

split_mode="Gleenes"
maxPNSize="-1"
alpha=1.0

query_size=3
query_count=10

password="syhSYH.19910205"
data_dir="${dir}/data/${dataset}"
db_dir="${data_dir}/selectivity"
code_dir="${dir}/code"
result_dir="${dir}/result/Riso-Tree/join_distance/${dataset}"
query_dir="${dir}/result/query/${dataset}"
mkdir -p $result_dir

query_path="${query_dir}/${query_size}_${selectivity}"

output_path="${result_dir}"
log_path="${result_dir}/${dataset}_log.txt"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"
suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
db_path="${db_dir}/neo4j-community-3.4.12_${suffix}/data/databases/graph.db"

source ./utility.sh
time=$(get_time)

##### NAIVE ######
log_path="${result_dir}/${dataset}_naive_log_${time}.txt"
if [ $run_naive == 1 ];	then
	java -Xmx100g -jar ${jar_path} \
		-f joinDistanceSingleMethod \
		-method "NAIVE"	\
		-dp ${db_path} \
		-d ${dataset} \
		-MAX_HOPNUM ${MAX_HOPNUM} \
		-joinDistance ${joinDistance}	\
		-queryPath ${query_path} \
		-queryCount ${query_count} \
		-password ${password}	\
		-clearCache "false"	\
		-clearCacheMethod "NULL" \
		-outputPath ${output_path} \
		>> ${log_path}
fi


##### SPATIAL_FIRST ######
log_path="${result_dir}/${dataset}_spatialfirst_log_${time}.txt"
if [ $run_spatial == 1 ];	then
	java -Xmx100g -jar ${jar_path} \
		-f joinDistanceSingleMethod \
		-method "SPATIAL_FIRST"	\
		-dp ${db_path} \
		-d ${dataset} \
		-MAX_HOPNUM ${MAX_HOPNUM} \
		-joinDistance ${joinDistance}	\
		-queryPath ${query_path} \
		-queryCount ${query_count} \
		-password ${password}	\
		-clearCache "false"	\
		-clearCacheMethod "NULL" \
		-outputPath ${output_path} \
		>> ${log_path}
fi

##### RISOTREE ######
log_path="${result_dir}/${dataset}_risotree_log_${time}.txt"
if [ $run_risotree == 1 ];	then
	java -Xmx100g -jar ${jar_path} \
		-f joinDistanceSingleMethod \
		-method "RISOTREE"	\
		-dp ${db_path} \
		-d ${dataset} \
		-MAX_HOPNUM ${MAX_HOPNUM} \
		-joinDistance ${joinDistance}	\
		-queryPath ${query_path} \
		-queryCount ${query_count} \
		-password ${password}	\
		-clearCache "false"	\
		-clearCacheMethod "NULL" \
		-outputPath ${output_path} \
		>> ${log_path}
fi
