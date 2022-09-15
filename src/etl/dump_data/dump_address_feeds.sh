#!/bin/bash


DATADUMP="/mnt/data/projects/acdhs-housing/data/20220606-snap/address_feed_csvs"

# address feed files contain rows that wrap addresses with {"" & ""}, 
# so it sees it as two empty strings than a wrapping of an address
# We need to fix that
# Steps:
#     1. Only some rows have this problem, let's split the files into "good" and "tofix"


echo "Creating the raw table..." 


psql -c 'set role "acdhs-housing-role";'
psql -c 'drop table if exists raw.address_feed;;'
psql -c "create table raw.address_feed(client_hash varchar, address_line_1 varchar, address_line_2 varchar, city varchar, state varchar, zip_cd varchar, eff_date varchar, complete_address text);"


mkdir $DATADUMP/modified

for f in $DATADUMP/*
do
    echo "processing $f "

    mod=${f%.*}

    awk -F ',' '{if (NF > 7) print}' $f > "${mod}_tofix.csv"

    awk -F ',' '{if (NF == 7) print}' $f > "${mod}_good.csv"

    mv "${mod}_tofix.csv" $DATADUMP/modified

    mv "${mod}_good.csv" $DATADUMP/modified

done


rm -r $DATADUMP/modified/modified* # Two files with the directory name gets 

for f in $DATADUMP/modified/*_good.csv
do
    echo copying $f
    psql -c "\copy raw.address_feed(client_hash, address_line_1, address_line_2, city, state, zip_cd, eff_date) from $f CSV HEADER ENCODING 'ISO88591' QUOTE E'\u0007';"
done


## We end up with errors for three files 
# "CMU_SNAP_ADDRESS_Feed_2011_BETWEEN_1_AND_12_20220525_good.csv"
# "CMU_SNAP_ADDRESS_Feed_2018_BETWEEN_1_AND_12_20220525_good.csv"
# "CMU_SNAP_ADDRESS_Feed_2019_BETWEEN_7_AND_12_20220525_good.csv"

echo "Fixing the NUL byte errors in the three errored out files"

f1=/mnt/data/projects/acdhs-housing/data/20220606-snap/address_feed_csvs/modified/CMU_SNAP_ADDRESS_Feed_2011_BETWEEN_1_AND_12_20220525_good.csv
f2=/mnt/data/projects/acdhs-housing/data/20220606-snap/address_feed_csvs/modified/CMU_SNAP_ADDRESS_Feed_2018_BETWEEN_1_AND_12_20220525_good.csv
f3=/mnt/data/projects/acdhs-housing/data/20220606-snap/address_feed_csvs/modified/CMU_SNAP_ADDRESS_Feed_2019_BETWEEN_7_AND_12_20220525_good.csv

sed -i 's/\x0//g' $f1
sed -i 's/\x0//g' $f2
sed -i 's/\x0//g' $f3

echo "copying ${f1}"
psql -c "\copy raw.address_feed(client_hash, address_line_1, address_line_2, city, state, zip_cd, eff_date) FROM ${f1} CSV HEADER ENCODING 'ISO88591' QUOTE E'\u0007';"

echo "copying ${f2}"

psql -c "\copy raw.address_feed(client_hash, address_line_1, address_line_2, city, state, zip_cd, eff_date) FROM ${f2} CSV HEADER ENCODING 'ISO88591' QUOTE E'\u0007';"

echo "copying ${f3}"

psql -c "\copy raw.address_feed(client_hash, address_line_1, address_line_2, city, state, zip_cd, eff_date) FROM ${f3} CSV HEADER ENCODING 'ISO88591' QUOTE E'\u0007';"


echo "fixing the files with quote errors..."

python fix_address_feed_files.py

echo "files fixed, copying to the DB..."

for f in $DATADUMP/modified/*_fixed.csv
do
    echo copying $f
    psql -c "\copy raw.address_feed from $f CSV DELIMITER E'\t' ENCODING 'ISO88591';"
done

echo "copying from raw to raw..."

q="create table raw.address_feed as (
select 
	client_hash,
	address_line_1,
	address_line_2,
	city, 
	state, 
	zip_cd, 
	eff_date,
	case
	when complete_address is null 
	then concat_ws(', ', address_line_1, address_line_2, city, state, zip_cd)
	else complete_address
	end as complete_address
from raw.address_feed af
);"
