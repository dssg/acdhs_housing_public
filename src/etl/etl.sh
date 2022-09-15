#!/bin/bash

ETLFILES="src/etl"

export $(xargs < ../../.env)

# pg restore
echo "-- pg restore snap project database dump --"
FILENAME=/mnt/data/projects/acdhs-housing/data/20220606-snap/acdhs_snap_20220606.dmp
pg_restore \
    -h "$PGHOST" \
    -p "$PGPORT" \
    -d "$PGDATABASE" \
    -U "$PGUSER" \
    -O -j 8 $FILENAME
    
# TODO: pg restore the db dump from the snap project: /mnt/data/projects/acdhs-housing/data/20220606-snap/acdhs_snap_20220606.dmp
psql -c 'set role "acdhs-housing-role";'

# dump data to database in raw format
echo "-- dumping data --"
for i in $ETLFILES/dump_data/*.sql; do
    echo "processing $i"
    psql -f $i
done

echo "-- fix, clean, and dump address feed csv files --"
sh ./$ETLFILES/dump_data/dump_address_feeds.sh

echo "-- cleaning data --"
# clean the raw data and dump tables in new schema called 'clean'
for i in $ETLFILES/clean_data/*.sql; do
    echo "processing $i"
    psql -f $i
done

echo "-- creating helper tables --"
# creating helper tables
for i in $ETLFILES/create_helper_tables/*.py; do
    echo "processing $i"
    python $i
done


echo "-- etl is done :) --"
