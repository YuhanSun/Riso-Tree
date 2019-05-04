#!/bin/bash

dataset="wikidata"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/wikidata_risotree"
code_dir="${dir}/code"

workingDbPath="${data_dir}/neo4j-community-3.4.12"

# modify this line most of the time

backupDbPath="${data_dir}/neo4j-community-3.4.12_node_edges"
rm -r ${workingDbPath}
cp -a ${backupDbPath} ${workingDbPath}