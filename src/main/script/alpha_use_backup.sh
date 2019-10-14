#!/bin/bash
dataset="Yelp_100"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/${dataset}"
db_dir="${dir}/data/${dataset}/alpha"
backup_dir="${db_dir}/backup"

split_mode="Gleenes"
maxPNSize="-1"

# for alpha in 0 0.25 0.5 0.75 1.0
# for alpha in 0
for alpha in 0 0.9 0.99 0.999 0.9999 1.0
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
	echo ${suffix}

	source_path="${db_dir}/neo4j-community-3.4.12_${suffix}/"
	target_path="${backup_dir}/neo4j-community-3.4.12_${suffix}"
	rm -r ${source_path}
	cp -a ${target_path} ${source_path}
done