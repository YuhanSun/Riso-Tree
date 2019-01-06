dir="/hdd/code/yuhansun/data"
code_dir="${dir}/code"
data_dir="${dir}/data"

dataset_dir="${data_dir}"
graph_path="${dataset_dir}/graph.txt"
entity_path="${dataset_dir}/graph.txt"
label_path="${dataset_dir}/label.txt"

jar_path="{code_dir}/Riso-Tree-0.0.1-SNAPSHOT.jar"
java -Xmx100g -cp jar_path Driver -h