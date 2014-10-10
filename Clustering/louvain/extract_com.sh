#!/bin/bash

while [[ $# > 0 ]]; do
    hierarchy "$1.graph" -l `hierarchy "$1.graph" | tail -1 | cut -d: -f1 | sed "s/level //"` > "$1.comm" && gzip "$1.comm"
    shift
done
