#!/bin/bash
./package.sh

dataset="Yelp_100"

# server
dir="/hdd/code/yuhansun"
db_dir="${dir}/data/${dataset}/alpha"
data_dir="${dir}/data/${dataset}"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"
outputPath="${db_dir}/area_allnodes_analysis.csv"

split_mode="Gleenes"
maxPNSize="-1"

for alpha in 0 0.25 0.5 0.75 1.0
# for alpha in 0.9999 1.0
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
	db_path="${db_dir}/neo4j-community-3.4.12_${suffix}/data/databases/graph.db"

	java -Xmx100g -jar ${jar_path} \
	-f treeNodesAvgArea \
	-dp ${db_path} \
	-d ${dataset} \
	-outputPath ${outputPath}
done

printf "\n" >> ${outputPath}
