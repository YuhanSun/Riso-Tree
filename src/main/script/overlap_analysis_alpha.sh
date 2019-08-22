#!/bin/bash
./package.sh

dataset="wikidata"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/wikidata_risotree"
code_dir="${dir}/code"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"
outputPath="${data_dir}/overlap_analysis.csv"

split_mode="Gleenes"
maxPNSize="100"

for alpha in 0 0.25 0.5 0.75 1.0
# for alpha in 0
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_test"
	db_path="${data_dir}/neo4j-community-3.4.12_${suffix}/data/databases/graph.db"

	java -Xmx100g -jar ${jar_path} \
	-f overlapAnalysis \
	-dp ${db_path} \
	-d ${dataset} \
	-outputPath ${outputPath}
done
