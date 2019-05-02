#!/bin/bash
./package.sh

# server
# dir="/hdd/code/yuhansun"
# data_dir="${dir}/data/wikidata_risotree"
# code_dir="${dir}/code"
# db_path="${data_dir}/neo4j-community-3.4.12/data/databases/graph.db"

# 407CD setup
data_dir="D:/Project_Data/wikidata-20180308-truthy-BETA.nt"
code_dir="D:/Google_Drive/Projects/github_code"
db_path="D:/Project_Data/wikidata-20180308-truthy-BETA.nt/neo4j-community-3.4.12_risotree/data/databases/graph.db"

entity_path="${data_dir}/entity.txt"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"

java -Xmx100g -jar ${jar_path} -f constructRTreeWikidata -dp ${db_path} -d ${dataset} -ep ${entity_path}