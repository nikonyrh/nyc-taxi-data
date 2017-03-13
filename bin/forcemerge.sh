#!/bin/bash
curl -XPOST -d '{}' "localhost:9200/taxicab-*/_forcemerge?max_num_segments=$1"
