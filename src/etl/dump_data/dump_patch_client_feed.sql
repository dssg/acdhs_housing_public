
-- this file contains sql code to dump the cliend feed data into the database
-- run file with: psql -f src/etl/db_dump_patch_client_feed.sql

-- statements created with, e.g.: head -n 1000 /mnt/data/projects/acdhs-housing/data/20220703/SNP_Client_Feed_2011_2020_1_12_20220630.csv | tr [:upper:] [:lower:] | tr ' ' '_' | sed 's/#/num/' | csvsql -i postgresql --db-schema raw --tables snp_client_feed_2011_2020

set role "acdhs-housing-role";
create schema if not exists raw;

-- drop existing raw tables if they exist already
-- this includes old client_feed files from pg_restore of SNAP project dump
drop table if exists raw.snp_client_feed_2011_2020;
drop table if exists raw.snp_client_feed_2021_2021;

-- snap client feed until 2020

CREATE TABLE raw.snp_client_feed_2011_2020(
        client_hash VARCHAR, 
        dob VARCHAR, 
        dod VARCHAR, 
        legal_sex VARCHAR, 
        gender VARCHAR, 
        race VARCHAR, 
        ethnic_desc VARCHAR, 
        living_arangt_common_desc VARCHAR, 
        empt_sts_common_desc VARCHAR, 
        martl_sts_common_desc VARCHAR, 
        ed_lvl_common_desc VARCHAR, 
        address_key VARCHAR, 
        kml_allegheny_council VARCHAR, 
        kml_pa_congress_district VARCHAR, 
        kml_pa_state_house_district VARCHAR, 
        kml_pa_state_senate_district VARCHAR, 
        kml_allegheny_municipality VARCHAR, 
        kml_pittsburgh_neighborhood VARCHAR, 
        std_neighborhood VARCHAR, 
        std_township VARCHAR, 
        kml_allegheny_school VARCHAR, 
        kml_pa_census_tract VARCHAR, 
        std_street_number VARCHAR, 
        std_route VARCHAR, 
        std_city VARCHAR, 
        std_state VARCHAR, 
        std_zip VARCHAR
);

\COPY raw.snp_client_feed_2011_2020 from program 'iconv -f LATIN1 -t UTF-8//TRANSLIT /mnt/data/projects/acdhs-housing/data/20220703/SNP_Client_Feed_2011_2020_1_12_20220630.csv | sed -E -e '' /^\s*$/d;''' WITH CSV HEADER;

-- snap client feed in 2021

CREATE TABLE raw.snp_client_feed_2021_2021(
        client_hash VARCHAR, 
        dob VARCHAR, 
        dod VARCHAR, 
        legal_sex VARCHAR, 
        gender VARCHAR, 
        race VARCHAR, 
        ethnic_desc VARCHAR, 
        living_arangt_common_desc VARCHAR, 
        empt_sts_common_desc VARCHAR, 
        martl_sts_common_desc VARCHAR, 
        ed_lvl_common_desc VARCHAR, 
        address_key VARCHAR, 
        kml_allegheny_council VARCHAR, 
        kml_pa_congress_district VARCHAR, 
        kml_pa_state_house_district VARCHAR, 
        kml_pa_state_senate_district VARCHAR, 
        kml_allegheny_municipality VARCHAR, 
        kml_pittsburgh_neighborhood VARCHAR, 
        std_neighborhood VARCHAR, 
        std_township VARCHAR, 
        kml_allegheny_school VARCHAR, 
        kml_pa_census_tract VARCHAR, 
        std_street_number VARCHAR, 
        std_route VARCHAR, 
        std_city VARCHAR, 
        std_state VARCHAR, 
        std_zip VARCHAR
);

\COPY raw.snp_client_feed_2021_2021 from program 'iconv -f LATIN1 -t UTF-8//TRANSLIT /mnt/data/projects/acdhs-housing/data/20220703/SNP_Client_Feed_2021_2021_1_12_20220630.csv | sed -E -e '' /^\s*$/d;''' WITH CSV HEADER;