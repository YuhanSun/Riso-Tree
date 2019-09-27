#!/bin/bash
./package.sh

dataset="wikidata"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/wikidata_source_backup"
code_dir="${dir}/code"

# local test setup
# dir="/Users/zhouyang/Google_Drive/Projects/tmp/risotree"
# dataset="Yelp"
# data_dir="${dir}/${dataset}"
# code_dir="/Users/zhouyang/Google_Drive/Projects/github_code"

# 407CD setup
# data_dir="D:/Project_Data/wikidata-20180308-truthy-BETA.nt"
# code_dir="D:/Google_Drive/Projects/github_code"
# db_path="D:/Neo4jData/neo4jDatabases/database-0c3f32e4-025a-4a22-a4cd-a9b979a9adf8/installation-3.4.9/data/databases/graph.db"


# server setup
# db_path="${data_dir}/neo4j-community-3.4.12_0.5/data/databases/graph.db"

# graph_path="${data_dir}/graph.txt"
single_graph_path="${data_dir}/graph_single.txt"
# entity_path="${data_dir}/entity.txt"
# label_path="${data_dir}/graph_label.txt"
# entityStringLabelMapPath="${data_dir}/entity_string_label.txt"
graphPropertyEdgePath="${data_dir}/graph_property_edge.txt"
# propertyMapPath="${data_dir}/property_map.txt"
# spatialNodePNPath="${data_dir}/spatialNodesZeroOneHopPN.txt"

# containID_path="${data_dir}/containID.txt"
# map_path="${data_dir}/node_map_RTree.txt"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

graph_property_edge_before="${data_dir}/graph_property_edge_no_instance_before_refine.txt"

java -Xmx100g -jar ${jar_path} \
	-f refineGraphPropertyEdge \
	-inputPath ${graph_property_edge_before}	\
	-outputpath ${graphPropertyEdgePath}