#!/bin/bash
dataset="wikidata"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/wikidata_risotree"

split_mode="Gleenes"
alpha="1.0"

for maxPNSize in 10 40 160 640
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
	echo ${suffix}

	# source_path="${data_dir}/neo4j-community-3.4.12_node_edges/"
	source_path="$data_dir/neo4j-community-3.4.12_${suffix}"
	target_path="${data_dir}/backup/neo4j-community-3.4.12_${suffix}"

	echo ${source_path}
	echo ${target_path}
	cp -a ${source_path} ${target_path}
done