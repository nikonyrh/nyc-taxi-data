#!/bin/bash
# Note that starting from July 2016 the TLC no longer provides pickup and dropoff coordinates.
# Instead, each trip comes with taxi zone pickup and dropoff location IDs. The original toddwschneider's
# code used a location ID lookup to get approximate locations but this project does not support those.

# rm data/*_2016-0{7,8,9}*.csv.gz data/*_2016-1*.csv.gz

cd "$(dirname "$(realpath "$0")")";
set -e

if [ "$1" = "" ]; then
	# You could also use raw_uber_data_urls.txt here
	# but this Clojure code doesn't support its schema.
	# FHV's schema is not supported either.
	time cat raw_data_urls.txt | grep -v fhv_tripdata | \
		xargs -n1 -P8 ./download_raw_data.sh
	
	# Unifying Uber zip files to gzip compression
	if stat --printf='' data/*.zip 2>/dev/null; then
		cd data
		unzip -q -o *.zip
		for fname in uber-raw-*.csv; do
			cat $fname | gzip > "$fname.gz"
			rm $fname
		done
		rm -fr __MACOSX *.zip
		cd ..
	fi
	
	exit 0
fi

fname=`echo "$1" | sed -r 's_.+/__'`

if [[ "$fname" == *.zip ]]; then
	TRANSFORM=cat
	fname="data/$fname"
else
	TRANSFORM=gzip
	fname="data/$fname.gz"
fi

if [ ! -f $fname ]; then
	tmp=`mktemp`
	echo "$1 => $tmp => $fname"
	curl -s "$1" | $TRANSFORM > $tmp && mv $tmp $fname
fi

