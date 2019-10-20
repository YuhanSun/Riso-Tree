#!/bin/bash
./package.sh

dataset="wikidata"
# always hard code this dir
# because a rm -r will happen here
cur_dir="/hdd/code/yuhansun/data/${dataset}/add"
backup_dir="${cur_dir}/backup"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/${dataset}"
code_dir="${dir}/code"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

# server setup
graph_path="${data_dir}/graph.txt"
entity_path="${data_dir}/entity.txt"
label_path="${data_dir}/graph_label.txt"
labelStrMapPath="${data_dir}/entity_string_label.txt"
spatialNodePNPath="${data_dir}/spatialNodesZeroOneHopPN.txt"
graph_property_edge_path="${data_dir}/graph_property_edge.txt"

db_folder_name="neo4j-community-3.4.12_node_edges"
# cp -a ${data_dir}/${db_folder_name} ${cur_dir}/${db_folder_name}
# do it mannually

add_edge_path="${cur_dir}/edges.txt"
graph_after_removal_path="${cur_dir}/graph.txt"
db_after_removal_path="${cur_dir}/neo4j-community-3.4.12_node_edges/data/databases/graph.db"

# java -Xmx100g -jar ${jar_path} \
# 	-f sampleFile \
# 	-inputPath ${graph_property_edge_path}	\
# 	-ratio 0.01	\
# 	-outputPath ${add_edge_path}

# java -Xmx100g -jar ${jar_path} \
# 	-f removeEdgesFromDb \
# 	-dp ${db_after_removal_path}	\
# 	-edgePath ${add_edge_path}

# java -Xmx100g -jar ${jar_path} \
# 	-f removeEdgesFromGraphFile \
# 	-gp ${graph_path}	\
# 	-edgePath ${add_edge_path}	\
# 	-outputPath ${graph_after_removal_path}

# Back up the new node_edges graph db.
# cp -a ${cur_dir}/${db_folder_name} ${backup_dir}/${db_folder_name}

# Rename db dir with suffix
split_mode="Gleenes"
alpha="1.0"
maxPNSize=-1
suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
db_dir="${cur_dir}/neo4j-community-3.4.12_${suffix}"
db_path="${db_dir}/data/databases/graph.db"
mv ${cur_dir}/neo4j-community-3.4.12_node_edges $db_dir

# Construct the tree structure
java -Xmx100g -jar ${jar_path} \
	-f wikiConstructRTree \
	-dp ${db_path} \
	-d ${dataset} \
	-ep ${entity_path} \
	-spatialNodePNPath ${spatialNodePNPath} \
	-alpha ${alpha} \
	-maxPNSize ${maxPNSize}

# Backup the db after tree construction.
after_tree="${backup_dir}/neo4j-community-3.4.12_${suffix}_after_tree"
cp -a $db_dir $after_tree

containID_path="${cur_dir}/containID_${suffix}.txt"
PNPathAndPrefix="${cur_dir}/PathNeighbors_${suffix}"

# Generate the leaf contain spatial node file
java -Xmx100g -jar ${jar_path} -f wikiGenerateContainSpatialID \
-dp ${db_path} \
-d ${dataset} \
-c ${containID_path}

# modify
for hop in 0 1
do
	java -Xmx100g -jar ${jar_path} -f wikiConstructPNTimeSingleHopNoGraphDb \
		-c ${containID_path} -gp ${db_path} -labelStrMapPath ${labelStrMapPath}\
		-lp ${label_path} -hop ${hop} -PNPrefix ${PNPathAndPrefix} -maxPNSize ${maxPNSize}
done

java -Xmx100g -jar ${jar_path} \
		-f wikiLoadAllHopPN \
		-PNPrefix ${PNPathAndPrefix} \
		-hopListStr 0,1 \
		-dp ${db_path} \
		-c ${containID_path}	

# Backup the db after pn load.
cp -a $db_dir ${backup_dir}/neo4j-community-3.4.12_${suffix}_after_pn


