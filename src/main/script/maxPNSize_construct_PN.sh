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

split_mode="Gleenes"
alpha="1.0"
containID_suffix="${split_mode}_${alpha}_new_version"
containID_path="${data_dir}/containID_${containID_suffix}.txt"

for maxPNSize in 10 20 40 80 160 320 640 1280 2560
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
	PNPathAndPrefix="${data_dir}/PathNeighbors_${suffix}"

	# # 0-hop
	# java -Xmx100g -jar ${jar_path} -f wikiConstructPNTimeSingleHopNoGraphDb \
	# -c ${containID_path} -gp ${graph_path} -labelStrMapPath ${labelStrMapPath}\
	# -lp ${label_path} -hop 0 -PNPrefix ${PNPathAndPrefix} -maxPNSize ${maxPNSize}

	# # 1-hop
	# java -Xmx100g -jar ${jar_path} -f wikiConstructPNTimeSingleHopNoGraphDb \
	# -c ${containID_path} -gp ${graph_path} -labelStrMapPath ${labelStrMapPath}\
	# -lp ${label_path} -hop 1 -PNPrefix ${PNPathAndPrefix} -maxPNSize ${maxPNSize}

	# 2-hop
	java -Xmx100g -jar ${jar_path} -f wikiConstructPNTimeSingleHopNoGraphDb \
	-c ${containID_path} -gp ${graph_path} -labelStrMapPath ${labelStrMapPath}\
	-lp ${label_path} -hop 2 -PNPrefix ${PNPathAndPrefix} -maxPNSize ${maxPNSize}

done