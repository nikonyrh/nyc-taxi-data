#!/bin/bash
if [ "$1" = "" ]; then
	time echo *.gz | xargs -n1 echo | shuf | xargs -n1 -P8 ./wc.sh | tee out.txt
else
	cat $1 | gunzip | wc -l -c
fi
