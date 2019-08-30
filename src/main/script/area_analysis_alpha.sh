#!/bin/bash
./package.sh

dataset="wikidata"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/wikidata_risotree"
code_dir="${dir}/code"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"
outputPath="${data_dir}/area_analysis.csv"

header="db_path\ttotal_area\taverage_area\n"
printf ${header} >> ${outputPath}

split_mode="Gleenes"
maxPNSize="100"

for alpha in 0 0.25 0.5 0.75 1.0
# for alpha in 0
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
	db_path="${data_dir}/neo4j-community-3.4.12_${suffix}/data/databases/graph.db"

	java -Xmx100g -jar ${jar_path} \
	-f areaAnalysis \
	-dp ${db_path} \
	-d ${dataset} \
	-outputPath ${outputPath}
done

printf "\n" >> ${outputPath}
