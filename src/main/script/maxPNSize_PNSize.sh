#!/bin/bash
./package.sh

dataset="wikidata"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/wikidata_risotree"
code_dir="${dir}/code"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"
outputPath="${data_dir}/maxPNSize_PNSize_analysis.csv"

header="filepath\tPNPropertyCount\tPNNeighborCount\tPNPropertySize\tPNNeighborSize\ttotal_size\n"
printf ${header} >> ${outputPath}

split_mode="Gleenes"
alpha="1.0"

for maxPNSize in 10 20 40 80 160
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"

	inputPath="${data_dir}/PathNeighbors_${suffix}_0.txt"

	java -Xmx100g -jar ${jar_path} \
	-f getPNNonEmptyCount \
	-inputPath ${inputPath} \
	-outputPath ${outputPath}
done

printf "\n" >> ${outputPath}
printf ${header} >> ${outputPath}

for maxPNSize in 10 20 40 80 160
do
	suffix="${split_mode}_${alpha}_${maxPNSize}_test"
	inputPath="${data_dir}/PathNeighbors_${suffix}_1.txt"

	java -Xmx100g -jar ${jar_path} \
	-f getPNNonEmptyCount \
	-inputPath ${inputPath} \
	-outputPath ${outputPath}
done

printf "\n" >> ${outputPath}