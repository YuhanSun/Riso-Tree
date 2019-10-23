#!/bin/bash
./package.sh

# dataset="Yelp_100"
dataset="wikidata"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/${dataset}/alpha"
code_dir="${dir}/code"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

split_mode="Gleenes"
maxPNSize="-1"

for alpha in 0 0.25 0.5 0.75 1.0
# for alpha in 0.55 0.6 0.65 0.7 0.8 0.85 0.9 0.95
# for alpha in 0
# for alpha in 0.99 0.999 0.9999 0.99999 0.999999
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
	db_path="${data_dir}/neo4j-community-3.4.12_${suffix}/data/databases/graph.db"
	outputPath="${data_dir}/leaf_vis_${alpha}.jpg"

	java -Xmx100g -jar ${jar_path} \
	-f visualizeLeafNodes \
	-dp $db_path
	-d $dataset
	-input1 "(-180,-90,180,90)"	\
	-input2 "(0,0,1000,1000)"	\
	-outputPath ${outputPath}
done
printf "\n" >> ${outputPath}
