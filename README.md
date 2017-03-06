# Unified New York City Taxi and Uber data (Now in Clojure!)

Forked from https://github.com/toddwschneider/nyc-taxi-data, but instead of Bash and PostgreSQL
this project uses Clojure and Elasticsearch.

# Set up (works on my machine...)

`zfs create /data1/volume1`
`zfs set atime=off     data1/volume1`
`zfs set recordsize=8K data1/volume1`
`~/projects/docker-scripts/startElasticsearchContainer.sh 5 16 --data /data1/volume1/es5`

