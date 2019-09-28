#!/bin/bash

while true
do
	sleep 1
	sync; echo 3 > /proc/sys/vm/drop_caches 
done