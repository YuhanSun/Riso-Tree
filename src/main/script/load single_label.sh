#!/bin/bash
./package.sh

dataset="foursquare_100"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/${dataset}"
code_dir="${dir}/code"

# server setup
graph_path="${data_dir}/graph.txt"
entity_path="${data_dir}/entity.txt"
label_path="${data_dir}/graph_label.txt"
labelStrMapPath="${data_dir}/entity_string_label.txt"
spatialNodePNPath="${data_dir}/spatialNodesZeroOneHopPN_-1.txt"
entityStringLabelMapPath="${data_dir}/entity_string_label.txt"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

split_mode="Gleenes"
maxPNSize="-1"
alpha=1.0

db_path="${data_dir}/neo4j-community-3.4.12_${split_mode}_${alpha}_${maxPNSize}/data/databases/graph.db"
containID_path="${data_dir}/containID_${split_mode}_${alpha}_${maxPNSize}.txt"
PNPathAndPrefix="${data_dir}/PathNeighbors_${split_mode}_${alpha}_${maxPNSize}"

# construct
java -Xmx100g -jar ${jar_path} \
	-f wikiConstructRTree \
	-dp ${db_path} \
	-d ${dataset} \
	-ep ${entity_path} \
	-spatialNodePNPath ${spatialNodePNPath}
	-alpha ${alpha}
	-maxPNSize ${maxPNSize}


# java -Xmx100g -jar ${jar_path} \
# -f LoadAll \
# -ep ${entity_path} \
# -dp ${db_path} \
# -c ${containID_path} \
# -gp ${graph_path} \
# -lp ${label_path} \
# -MAX_HOPNUM ${MAX_HOPNUM} \
# -PNPreffix ${PNPathAndPreffix} \
# -mapPath ${map_path} \
# -d ${dataset}
