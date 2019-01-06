dir="/hdd/code/yuhansun"
data_dir="${dir}/data"

dataset="Patents\_100\_random\_20"
dataset_dir="${data_dir}"
db_path="${dataset_dir}/neo4j-community-3.1.1/data/databases/graph.db_Gleene_1.0"
graph_path="${dataset_dir}/graph.txt"
entity_path="${dataset_dir}/entity.txt"
label_path="${dataset_dir}/label.txt"

code_dir="${dir}/code"
jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"
echo "java -Xmx100g -cp ${jar_path} Driver -h"
java -Xmx100g -cp ${jar_path} Driver -h

echo "java -Xmx100g -cp ${jar_path} Driver -f tree -dp ${db_path} -d ${dataset} -gp ${graph_path} -ep ${entity_path} -lp ${label_path}"
java -Xmx100g -cp ${jar_path} Driver -f tree -dp ${db_path} -d ${dataset} -gp ${graph_path} -ep ${entity_path} -lp ${label_path}
