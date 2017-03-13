#!/bin/bash
# Polls Elasticsearch every 30 seconds, fetching the doc count and
# printing how many documents were ingested on average per second. 

if [ "$ELASTICSEARCH_URL" = "" ]; then
    ELASTICSEARCH_URL="http://localhost:9200"
fi

ndocs=`curl -s "$ELASTICSEARCH_URL/taxicab-*/_count" | jq .count`
tsleep=30

while true; do
  sleep $tsleep
  ndocs_prev=$ndocs
  ndocs=`curl -s "$ELASTICSEARCH_URL/taxicab-*/_count" | jq .count`
  echo `date` "delta $ndocs_prev vs. $ndocs:" `echo "($ndocs - $ndocs_prev) / $tsleep" | bc` 'docs / second'
done

