#!/bin/bash
./package.sh

dataset="wikidata"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/wikidata_risotree"
code_dir="${dir}/code"

# server setup
graph_path="${data_dir}/graph.txt"
entity_path="${data_dir}/entity.txt"
label_path="${data_dir}/graph_label.txt"
labelStrMapPath="${data_dir}/entity_string_label.txt"
spatialNodePNPath="${data_dir}/spatialNodesZeroOneHopPN.txt"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"
selectivitiesStr="0.000001,0.00001,0.0001,0.001,0.01,0.1"
queryCount=10
outputDir="/hdd/code/yuhansun/result/query/wikidata"

for nodeCount in 2 3 4 5
do
	java -Xmx100g -jar ${jar_path} \
		-f generateQuery \
		-gp ${graph_path} \
		-ep ${entity_path} \
		-lp ${label_path} \
		-labelStrMapPath ${labelStrMapPath} \
		-selectivitiesStr ${selectivitiesStr} \
		-nodeCount ${nodeCount} \
		-queryCount ${queryCount} \
		-outputPath ${outputDir}
done