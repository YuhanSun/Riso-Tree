dir="/hdd/code/yuhansun"
data_dir="${dir}/data"

dataset=""
dataset_dir="${data_dir}"
graph_path="${dataset_dir}/graph.txt"
entity_path="${dataset_dir}/graph.txt"
label_path="${dataset_dir}/label.txt"

code_dir="${dir}/code"
jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"
echo "java -Xmx100g -cp ${jar_path} Driver -h"
java -Xmx100g -cp ${jar_path} Driver -h
