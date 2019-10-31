#!/bin/bash

function get_time() {
	current_time=$(date "+%Y.%m.%d-%H.%M.%S")
	echo $current_time
}

function attach_with_time() {
	echo ${current_time}_$(get_time)
}

# current_time=$(date "+%Y.%m.%d-%H.%M.%S")
# echo $current_time
 
# file_name=test_files.txt
 

 
# new_fileName=$file_name.$current_time
# echo "New FileName: " "$new_fileName"

