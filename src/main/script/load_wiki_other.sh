#!/bin/bash
# This script is used to load other datasets except for wikidata. The difference is that edges do not have label. 
# So the edges are loaded using a different function. graphPropertyEdgePath is not used here. 

./package.sh

dataset="Yelp_110"
MAX_HOPNUM=2
db_dir_name="neo4j-community-3.4.12_node_edges"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/${dataset}"
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

# copy the empty neo4j db folder to the target location
source_dir="/hdd/code/yuhansun/data/neo4j_versions/neo4j-community-3.4.12_ip_modified/"
target_dir="${data_dir}/${db_dir_name}"
[ ! -d $source_dir ] && echo "Copy failed!" && echo "$source_dir does not exist!" && exit 1
[ -d $target_dir ] && echo "Copy failed!" && echo "$target_dir already exists!" && exit 1
cp -a $source_dir $target_dir

# server setup
db_path="${data_dir}/${db_dir_name}/data/databases/graph.db"


graph_path="${data_dir}/graph.txt"
entity_path="${data_dir}/entity.txt"
label_path="${data_dir}/graph_label.txt"
entityStringLabelMapPath="${data_dir}/entity_string_label.txt"
# graphPropertyEdgePath="${data_dir}/graph_property_edge.txt"
# propertyMapPath="${data_dir}/property_map.txt"
# spatialNodePNPath="${data_dir}/spatialNodesZeroOneHopPN.txt"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"
java -Xmx100g -jar ${jar_path} -h

###### Load ######
# Load graph nodes, edges and spatial attributes.
java -Xmx100g -jar ${jar_path} -f wikidataLoadGraph \
	-ep ${entity_path} \
	-lp ${label_path} \
	-entityStringLabelMapPath ${entityStringLabelMapPath} \
	-dp ${db_path}

# Load graph edges
java -Xmx100g -jar ${jar_path} -f loadGraphEdgesNoMap \
		-dp ${db_path}	\
		-gp ${graph_path}

# MAX_HOPNUM="2"
# PNPathAndPreffix="${data_dir}/PathNeighbors_${tree_type}"
# java -Xmx100g -jar ${jar_path} -f LoadAll -ep ${entity_path} -dp ${db_path} -c ${containID_path} -gp ${graph_path} -lp ${label_path} -MAX_HOPNUM ${MAX_HOPNUM} -PNPreffix ${PNPathAndPreffix} -mapPath ${map_path} -d ${dataset}

####### Not used for efficiency reason ####
# java -Xmx100g -jar ${jar_path} \
#  -f wikisetZeroOneHopPNForSpatialNodes \
#  -dp ${db_path} \
#  -gp ${graph_path} \
#  -lp ${label_path} \
#  -entityStringLabelMapPath ${entityStringLabelMapPath} \
#  -maxPNSize 100

# output_path="${data_dir}/spatialNodesZeroOneHopPN_-1_${MAX_HOPNUM}.txt"
# java -Xmx100g -jar ${jar_path} \
# 	-f wikigenerateZeroOneHopPNForSpatialNodes \
# 	-gp ${graph_path} \
# 	-lp ${label_path} \
# 	-ep ${entity_path} \
# 	-entityStringLabelMapPath ${entityStringLabelMapPath} \
# 	-maxPNSize -1 \
# 	-MAX_HOPNUM $MAX_HOPNUM	\
# 	-outputPath ${output_path}

# java -Xmx100g -jar ${jar_path} \
# 	-f wikiConstructRTree \
# 	-dp ${db_path} \
# 	-d ${dataset} \
# 	-ep ${entity_path} \
# 	-spatialNodePNPath ${spatialNodePNPath}

