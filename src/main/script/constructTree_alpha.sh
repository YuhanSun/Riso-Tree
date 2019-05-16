#!/bin/bash
./package.sh

dataset="wikidata"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/wikidata_risotree"
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
graph_path="${data_dir}/graph.txt"
entity_path="${data_dir}/entity.txt"
label_path="${data_dir}/graph_label.txt"
labelStrMapPath="${data_dir}/entity_string_label.txt"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

split_mode="Gleenes"
maxPNSize="100"

for alpha in 0,0.25,0.75
do
	db_path="${data_dir}/neo4j-community-3.4.12_${split_mode}_${alpha}_${maxPNSize}/data/databases/graph.db"
	containID_path="${data_dir}/containID_${split_mode}_${alpha}_${maxPNSize}.txt"
	PNPathAndPrefix="${data_dir}/PathNeighbors_${split_mode}_${alpha}_${maxPNSize}"

	java -Xmx100g -jar ${jar_path} \
	-f wikiConstructRTree \
	-dp ${db_path} \
	-d ${dataset} \
	-ep ${entity_path} \
	-spatialNodePNPath ${spatialNodePNPath} \
	-alpha ${alpha} \
	-maxPNSize ${maxPNSize}
done




# java -Xmx100g -jar ${jar_path} \
#  -f wikisetZeroOneHopPNForSpatialNodes \
#  -dp ${db_path} \
#  -gp ${graph_path} \
#  -lp ${label_path} \
#  -entityStringLabelMapPath ${entityStringLabelMapPath} \
#  -maxPNSize 100

# java -Xmx100g -jar ${jar_path} \
# 	-f wikigenerateZeroOneHopPNForSpatialNodes \
# 	-gp ${graph_path} \
# 	-lp ${label_path} \
# 	-ep ${entity_path} \
# 	-entityStringLabelMapPath ${entityStringLabelMapPath} \
# 	-maxPNSize 100 \
# 	-outputPath ${data_dir}/spatialNodesZeroOneHopPN.txt