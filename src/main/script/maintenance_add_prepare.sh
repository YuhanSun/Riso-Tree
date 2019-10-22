#!/bin/bash
./package.sh

# dataset="wikidata"
dataset="Gowalla_100"
hopListStr="0,1,2"
# always hard code this dir
# because a rm -r will happen here
cur_dir="/hdd/code/yuhansun/data/${dataset}/add"
backup_dir="${cur_dir}/backup"
mkdir -p $cur_dir
mkdir -p $backup_dir

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
src_db_dir="${data_dir}/${db_folder_name}"
cur_db_dir="${cur_dir}/${db_folder_name}"
if [ ! -d "$cur_db_dir" ]; then
	rm -r $cur_db_dir
fi
cp -a $src_db_dir $cur_db_dir

add_edge_path="${cur_dir}/edges.txt"
graph_after_removal_path="${cur_dir}/graph.txt"
db_after_removal_path="${cur_dir}/neo4j-community-3.4.12_node_edges/data/databases/graph.db"

if [ ! -f "$graph_property_edge_path" ]; then
	java -Xmx100g -jar ${jar_path} \
		-f convertGraphToEdgeFormat \
		-gp ${graph_path}	\
		-ratio 0.01	\
		-graphPropertyEdgePath ${graph_property_edge_path}
fi

if [ ! -f "$add_edge_path" ]; then
	java -Xmx100g -jar ${jar_path} \
		-f sampleFile \
		-inputPath ${graph_property_edge_path}	\
		-ratio 0.01	\
		-outputPath ${add_edge_path}
fi

java -Xmx100g -jar ${jar_path} \
	-f removeEdgesFromDb \
	-dp ${db_after_removal_path}	\
	-edgePath ${add_edge_path}

if [ ! -f "$graph_after_removal_path" ]; then
	java -Xmx100g -jar ${jar_path} \
		-f removeEdgesFromGraphFile \
		-gp ${graph_path}	\
		-edgePath ${add_edge_path}	\
		-outputPath ${graph_after_removal_path}
fi

# Back up the new node_edges graph db.
backup_db_dir="${backup_dir}/${db_folder_name}"
if [ ! -d "$backup_db_dir" ]; then
	rm -r $backup_db_dir
fi
cp -a $cur_db_dir $backup_db_dir

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
if [ -d "$after_tree" ]; then
	rm -r $after_tree
fi
cp -a $db_dir $after_tree

containID_path="${cur_dir}/containID_${suffix}.txt"
PNPathAndPrefix="${cur_dir}/PathNeighbors_${suffix}"

# Generate the leaf contain spatial node file
java -Xmx100g -jar ${jar_path} \
	-f wikiGenerateContainSpatialID \
	-dp ${db_path} \
	-d ${dataset} \
	-c ${containID_path}

# modify
for hop in 0 1 2
do
	java -Xmx100g -jar ${jar_path} -f wikiConstructPNTimeSingleHopNoGraphDb \
		-c ${containID_path} -gp ${graph_path} -labelStrMapPath ${labelStrMapPath}\
		-lp ${label_path} -hop ${hop} -PNPrefix ${PNPathAndPrefix} -maxPNSize ${maxPNSize}
done

java -Xmx100g -jar ${jar_path} \
		-f wikiLoadAllHopPN \
		-PNPrefix ${PNPathAndPrefix} \
		-hopListStr $hopListStr \
		-dp ${db_path} \
		-c ${containID_path}	

# Backup the db after pn load.
after_pn_db_dir="${backup_dir}/neo4j-community-3.4.12_${suffix}_after_pn"
if [ ! -d "$after_pn_db_dir" ]; then
	rm -r $after_pn_db_dir
fi
cp -a $db_dir $after_pn_db_dir

# Safe nodes generation
PNPathAndPrefix="${cur_dir}/PathNeighbors_Gleenes_1.0_-1_new_version"
output_path="${backup_dir}/safeNodes.txt"
jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

java -Xmx100g -jar ${jar_path}	\
	-f generateSafeNodes	\
	-PNPrefix ${PNPathAndPrefix}	\
	-hopListStr ${hopListStr}	\
	-ep $entity_path	\
	-outputPath ${output_path}



