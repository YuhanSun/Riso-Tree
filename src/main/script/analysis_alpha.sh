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
# graph_path="${data_dir}/graph.txt"
# entity_path="${data_dir}/entity.txt"
# label_path="${data_dir}/graph_label.txt"
# labelStrMapPath="${data_dir}/entity_string_label.txt"
# spatialNodePNPath="${data_dir}/spatialNodesZeroOneHopPN.txt"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"
outputPath="${data_dir}/PNNonEmptyCount.csv"

split_mode="Gleenes"
maxPNSize="100"

for alpha in 0 0.25 0.5 0.75 1.0
# for alpha in 0
do
	# suffix="_${split_mode}_${alpha}_${maxPNSize}_test"
	suffix="${split_mode}_${alpha}_${maxPNSize}"

	# db_path="${data_dir}/neo4j-community-3.4.12_${split_mode}_${alpha}_${maxPNSize}${suffix}/data/databases/graph.db"
	# containID_path="${data_dir}/containID_${split_mode}_${alpha}_${maxPNSize}${suffix}.txt"
	# PNPathAndPrefix="${data_dir}/PathNeighbors_${split_mode}_${alpha}_${maxPNSize}${suffix}"

	inputPath="${data_dir}/PathNeighbors_${suffix}_0.txt"

	java -Xmx100g -jar ${jar_path} \
	-f getPNNonEmptyCount \
	-inputPath ${inputPath} \
	-outputPath ${outputPath}
done

for alpha in 0 0.25 0.5 0.75 1.0
do
	suffix="${split_mode}_${alpha}_${maxPNSize}"
	inputPath="${data_dir}/PathNeighbors_${suffix}_1.txt"

	java -Xmx100g -jar ${jar_path} \
	-f getPNNonEmptyCount \
	-inputPath ${inputPath} \
	-outputPath ${outputPath}
done