#!/bin/bash

while [[ $# > 0 ]]; do
	if [[ $1 == "*w*" ]]
	then
		(gconvert -i $1 -o "$1.bin" -w "$1.w" && louvain "$1.bin" -l -1 -q 0 -w "$1.w" > "$1.graph" 2> "$1.mod" && hierarchy -l `hierarchy "$1.graph" | tail -1 | cut -d: -f1 | sed "s/level //"` > "$1.comm" && gzip "$1.comm")
	else
		(gconvert -i $1 -o "$1.bin" && louvain "$1.bin" -l -1 -q 0 > "$1.graph" 2> "$1.mod" && hierarchy -l `hierarchy "$1.graph" | tail -1 | cut -d: -f1 | sed "s/level //"` > "$1.comm" && gzip "$1.comm")
	fi
	shift
done
