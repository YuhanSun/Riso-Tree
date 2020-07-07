#!/bin/bash
# for all datasets except for real wikidata.
./package.sh

# dataset="foursquare_100"
# dataset="Gowalla_100"
dataset="Yelp_110"

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

suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
db_dir_name="neo4j-community-3.4.12_${suffix}"

db_path="${data_dir}/${db_dir_name}/data/databases/graph.db"
containID_path="${data_dir}/containID_${suffix}.txt"
PNPathAndPrefix="${data_dir}/PathNeighbors_${suffix}"

# copy the empty neo4j db folder to the target location
source_dir="/hdd/code/yuhansun/data/neo4j_versions/neo4j-community-3.4.12_ip_modified/"
target_dir="${data_dir}/${db_dir_name}"
[ ! -d $source_dir ] && echo "Copy failed!" && echo "$source_dir does not exist!" && exit 1
[ -d $target_dir ] && echo "Copy failed!" && echo "$target_dir already exists!" && exit 1
cp -a $source_dir $target_dir

# Generate the 0-1 hop pn for spatial nodes
java -Xmx100g -jar ${jar_path} \
-f wikigenerateZeroOneHopPNForSpatialNodes \
-gp ${graph_path} \
-lp ${label_path} \
-ep ${entity_path} \
-entityStringLabelMapPath ${entityStringLabelMapPath} \
-maxPNSize -1 \
-outputPath ${spatialNodePNPath}

# Load graph nodes.
java -Xmx100g -jar ${jar_path} -f wikidataLoadGraph \
	-ep ${entity_path} \
	-lp ${label_path} \
	-entityStringLabelMapPath ${entityStringLabelMapPath} \
	-dp ${db_path}

# Load graph edges
java -Xmx100g -jar ${jar_path} -f loadGraphEdgesNoMap \
	-dp ${db_path}	\
	-gp ${graph_path}

# Construct the tree structure
java -Xmx100g -jar ${jar_path} \
-f wikiConstructRTree \
-dp ${db_path} \
-d ${dataset} \
-ep ${entity_path} \
-spatialNodePNPath ${spatialNodePNPath} \
-alpha ${alpha} \
-maxPNSize ${maxPNSize}

# Generate the leaf contain spatial node file
java -Xmx100g -jar ${jar_path} \
	-f wikiGenerateContainSpatialID \
	-dp ${db_path} \
	-d ${dataset} \
	-c ${containID_path}

# # 0-hop
# java -Xmx100g -jar ${jar_path} -f wikiConstructPNTimeSingleHop \
# -dp ${db_path} -c ${containID_path} -gp ${graph_path} -labelStrMapPath ${labelStrMapPath}\
# -lp ${label_path} -hop 0 -PNPrefix ${PNPathAndPrefix} -maxPNSize ${maxPNSize}

# java -Xmx100g -jar ${jar_path} -f wikiLoadPN \
# -dp ${db_path} -c ${containID_path} -gp ${graph_path} -labelStrMapPath ${labelStrMapPath}\
# -lp ${label_path} -hop 0 -PNPrefix ${PNPathAndPrefix} -maxPNSize ${maxPNSize}

# # # 1-hop
# java -Xmx100g -jar ${jar_path} -f wikiConstructPNTimeSingleHop \
# -dp ${db_path} -c ${containID_path} -gp ${graph_path} -labelStrMapPath ${labelStrMapPath}\
# -lp ${label_path} -hop 1 -PNPrefix ${PNPathAndPrefix} -maxPNSize ${maxPNSize}

# java -Xmx100g -jar ${jar_path} -f wikiLoadPN \
# -dp ${db_path} -c ${containID_path} -gp ${graph_path} -labelStrMapPath ${labelStrMapPath}\
# -lp ${label_path} -hop 1 -PNPrefix ${PNPathAndPrefix} -maxPNSize ${maxPNSize}