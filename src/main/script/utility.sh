#!/bin/bash

function get_time() {
	current_time=$(date "+%Y.%m.%d-%H.%M.%S")
	echo $current_time
}

function get_MAX_HOPNUM() {
	if [ $1 == "wikidata" ]; then
		echo 1
	else
		echo 2
	fi
}
