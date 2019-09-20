#!/bin/bash
./package.sh

# for dataset in "Yelp_100" "Gowalla_100" "Patents_100_random_20"
for dataset in "Yelp_100"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/${dataset}"
code_dir="${dir}/code"
jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

alpha=1.0
split_mode="Gleenes"
containId_suffix="${split_mode}_${alpha}_-1_new_version"
containID_path="${data_dir}/containID_${containId_suffix}.txt"

for maxPNSize in 10 40 160 640 -1
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
	db_path="${data_dir}/neo4j-community-3.4.12_${suffix}/data/databases/graph.db"
	PNPathAndPrefix="${data_dir}/PathNeighbors_${suffix}"
	java -Xmx100g -jar ${jar_path} \
		-f wikiLoadAllHopPN \
		-PNPrefix ${PNPathAndPrefix} \
		-hopListStr 0,1,2 \
		-dp ${db_path} \
		-c ${containID_path}
done