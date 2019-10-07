#!/bin/bash
dataset="Yelp_100"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/${dataset}"
db_dir="${dir}/data/${dataset}/alpha"

split_mode="Gleenes"
maxPNSize="-1"

# for alpha in 0 0.25 0.5 0.75 1.0
# for alpha in 0
for alpha in 0.55 0.6 0.65 0.7 0.8 0.85 0.9 0.95
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
	echo ${suffix}

	source_path="${data_dir}/neo4j-community-3.4.12_node_edge/"
	target_path="${db_dir}/neo4j-community-3.4.12_${suffix}"
	rm -r ${target_path}
	cp -a ${source_path} ${target_path}
done