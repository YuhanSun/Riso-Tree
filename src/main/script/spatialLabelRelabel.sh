#!/bin/bash

./package.sh

dataset="Yelp_110"
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/${dataset}"
code_dir="${dir}/code"
jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

input_path="/hdd/code/yuhansun/data/Yelp_100/graph_label.txt"
output_path="${data_dir}/graph_label.txt"

java -Xmx100g -jar ${jar_path} \
	-f wikiConstructRTree \
	-input_path $input_path \
	-output_path $output_path \
	-newlabelListString "102,103,104,105,106,107,108,109,110,111"