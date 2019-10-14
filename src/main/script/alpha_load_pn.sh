#!/bin/bash
./package.sh

dataset="Yelp_100"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/${dataset}"
db_dir="${data_dir}/alpha"
code_dir="${dir}/code"
jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

maxPNSize=-1
split_mode="Gleenes"

for alpha in 0 0.9 0.99 0.999 0.9999 1.0
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
	containId_suffix="${split_mode}_${alpha}_new_version"
	containID_path="${data_dir}/containID_${containId_suffix}.txt"
	db_path="${db_dir}/neo4j-community-3.4.12_${suffix}/data/databases/graph.db"
	PNPathAndPrefix="${data_dir}/PathNeighbors_${suffix}"
	java -Xmx100g -jar ${jar_path} \
		-f wikiLoadAllHopPN \
		-PNPrefix ${PNPathAndPrefix} \
		-hopListStr 0,1,2 \
		-dp ${db_path} \
		-c ${containID_path}		
done