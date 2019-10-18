#!/bin/bash
./package.sh

# dataset="Yelp_100"
dataset="Gowalla_100"

# server
dir="/hdd/code/yuhansun"
data_dir="${dir}/data/${dataset}/alpha"
code_dir="${dir}/code"


jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"
outputPath="${data_dir}/PNNonEmptyCount.csv"

header="filepath\tPNPropertyCount\tnonEmptyPNCount\tPNNeighborCount\tPNPropertySize\tPNNeighborSize\ttotal_size\n"

split_mode="Gleenes"
maxPNSize="-1"

for hop in 0 1 2
do
	printf ${header} >> ${outputPath}
	# for alpha in 0 0.25 0.5 0.75 1.0
	# for alpha in 0.55 0.6 0.65 0.7 0.8 0.85 0.9 0.95
	# for alpha in 0
	for alpha in 0.99 0.999 0.9999 0.99999 0.999999
	do
		suffix="${split_mode}_${alpha}_${maxPNSize}_new_version"
		# suffix="${split_mode}_${alpha}_${maxPNSize}"

		inputPath="${data_dir}/PathNeighbors_${suffix}_${hop}.txt"

		java -Xmx100g -jar ${jar_path} \
		-f getPNNonEmptyCount \
		-inputPath ${inputPath} \
		-outputPath ${outputPath}
	done
	printf "\n" >> ${outputPath}
done