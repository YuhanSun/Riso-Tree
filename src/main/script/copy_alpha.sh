#!/bin/bash
dataset="wikidata"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/wikidata_risotree"

split_mode="Gleenes"
maxPNSize="100"

for alpha in 0 0.25 0.5 0.75 1.0
# for alpha in 0
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
	echo ${suffix}

	source_path="${data_dir}/neo4j-community-3.4.12_node_edges/"
	target_path="${data_dir}/neo4j-community-3.4.12_${suffix}"
	cp -a ${source_path} ${target_path}
done