#!/bin/bash
./package.sh

dataset="wikidata"
# always hard code this dir
# because a rm -r will happen here
db_dir="/hdd/code/yuhansun/data/wikidata/add"

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

# db_folder_name="neo4j-community-3.4.12_node_edges"
# cp -a ${data_dir}/${db_folder_name} ${db_dir}/${db_folder_name}
# do it mannually

add_edge_path="${db_dir}/edges.txt"
graph_after_removal_path="${db_dir}/graph.txt"
db_after_removal_path="${db_dir}/neo4j-community-3.4.12_node_edges/data/databases/graph.db"

java -Xmx100g -jar ${jar_path} \
	-f sampleFile \
	-inputPath ${graph_property_edge_path}	\
	-ratio 0.01	\
	-outputPath ${add_edge_path}

java -Xmx100g -jar ${jar_path} \
	-f removeEdgesFromDb \
	-dp ${graph_path}	\
	-edgePath ${add_edge_path}

java -Xmx100g -jar ${jar_path} \
	-f removeEdgesFromGraphFile \
	-gp ${graph_path}	\
	-edgePath ${add_edge_path}	\
	-outputPath ${graph_after_removal_path}


