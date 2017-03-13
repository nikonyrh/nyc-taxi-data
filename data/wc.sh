#!/bin/bash
BASE_PATH="$(dirname "$(realpath "$0")")";

if [ "$1" = "" ]; then
	time echo *.gz | xargs -n1 echo | shuf | xargs -n1 -P8 "$BASE_PATH/wc.sh" | tee out.txt
	octave --eval 'sum(load("out.txt"), 1) * 1e-9'
else
	cat $1 | gunzip | wc -l -c
fi

