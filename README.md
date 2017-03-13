# Unified New York City Taxi and Uber data (now in Clojure!)

Forked from https://github.com/toddwschneider/nyc-taxi-data, but instead of Bash and PostgreSQL
this project uses Clojure and Elasticsearch. At this point only supports Yellow and Green datasets
as they have the most trips, have detailed pickup/dropoff coordinates 

# Set up (works on my machine...)

Starting Elasticsearch 5 with 16 GB of RAM, storing files to main EXT4 SSD:

```bash
~/projects/docker-scripts/startElasticsearchContainer.sh 5 16 --data /data0/es5
```

Starting Elasticsearch 5 with 16 GB of RAM, storing files to mirrored SSDs on ZFS on Linux:

```bash
zfs zpool create data1 /dev/disk/by-id/ata-Samsung_SSD_750_EVO_500GB_XXX /dev/disk/by-id/ata-Samsung_SSD_750_EVO_500GB_YYY

# I had terrible 40 MB/s write troughput without this hack... Not sure why :( Root partition is EXT4
truncate -s 8g /data1_zil.dat && zpool add data1 log /data1_zil.dat

zfs create /data1/volume1

# These might give us a performance boost
zfs set atime=off data1/volume1
zfs set recordsize=8K data1/volume1

~/projects/docker-scripts/startElasticsearchContainer.sh 5 16 --data /data1/volume1/es5
```

Starting Kibana:

```bash
~/projects/docker-scripts/startKibanaContainer.sh 5
```

# Useful commands

I cannot guarantee these instructions will be up to date, but this is how this project works at the moment

```bash
$ git clone git@github.com:nikonyrh/nyc-taxi-data.git
$ cd nyc-taxi-data

# Downloading raw CSVs
$ ./download_raw_data.sh

# Checking what we've got, apparently 148.5 GB of CSVs (879.2 million rows) compressed to 31 GB
$ cd data
$ du -hc * | tail -n1
31G	total

$ ./wc.sh
ans =
     0.87915   148.52923

# Building the JAR
$ cd ../taxi-rides-clj
$ lein uberjar

# You need Java 8 or newer to run this project as dates are parsed by java.time
$ java -version
java version "1.8.0_121"
Java(TM) SE Runtime Environment (build 1.8.0_121-b13)
Java HotSpot(TM) 64-Bit Server VM (build 25.121-b13, mixed mode)

# Do less parallel work if you run out of memory or want to use the computer for other work as well.
$ N_PARALLEL=`nproc`
$ JAR=target/taxi-rides-clj-0.0.1-SNAPSHOT-standalone.jar

# Parsing, removing duplicates, merging with weather data and writing to local Elasticsearch.
# Destination can be overridden by ES_SERVER=10.0.2.100:9201 env variable if needed.
# I could index on average about 20k docs / minute on Core i7 6700K, resulting in 873.3
# million docs taking 331.3 GB of storage. _all was disabled but _source was not.
$ time ls ../data/*.gz | shuf | xargs java -jar $JAR insert $N_PARALLEL

# Parsing, removing duplicates, merging with weather data and writing out to .csv.gz files.
# On Core i7 6700K this took 143 core-hours! There might be room for optimization, but then again
# it produced 45.4 gigabytes of compressed output and 189.4 gigabytes (873.3 million lines) of raw CSV.
# It should be easy to bulk-insert to other database systems such as Redshift or just MS SQL Server.
$ mkdir data_out
$ time ls ../data/*.gz | shuf | xargs java -jar $JAR extract $N_PARALLEL
```

