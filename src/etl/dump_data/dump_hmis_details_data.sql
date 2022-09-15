-- this file contains sql code to dump data on HMIS Programs into the sql database

-- run this file with: psql -f src/etl/db_dump_hmis_details_data.sql

-- Content: 
    -- Data on client enrollment in various HMIS programs: 
    -- Information on program type 
    -- Information on time between enrollment and move in date
    -- Information on client's situation at the time of the move-in, including prior living arrangement and reasons for homelessness
-- statements created with, e.g.: head -n 1000 /mnt/data/projects/acdhs-housing/data/20220714/HMIS_Details_CMU.csv | tr [:upper:] [:lower:] | tr ' ' '_' | sed 's/#/num/' | csvsql -i postgresql --db-schema raw --tables hmis_details



set role 'acdhs-housing-role';

create schema if not exists raw;


-- drop existing tables if they exist already
drop table if exists raw.hmis_details;

CREATE TABLE raw.hmis_details (
        hashed_mci_uniq_id VARCHAR, 
        is_head_of_household VARCHAR,
        relation_to_hoh VARCHAR, 
        household_type VARCHAR, 
        household_size VARCHAR, 
        hud_project_type_id VARCHAR, 
        hud_project_type_desc VARCHAR, 
        uniq_prog_nm VARCHAR, 
        prog_hh_type VARCHAR, 
        winter_shelter VARCHAR,  
        enrollment_start_dt VARCHAR, 
        enrollment_end_dt VARCHAR, 
        move_in_dt VARCHAR, 
        is_domestic_violence_survivor VARCHAR, 
        when_dv_experience_occured VARCHAR, 
        reltn_between_perp_and_victim VARCHAR, 
        prior_living_arrangement VARCHAR, 
        reason_for_homelessness VARCHAR, 
        is_clnt_disqual_frm_pub_hsing VARCHAR,  
        exit_destination_type VARCHAR, 
        client_exiting_to VARCHAR
);

\copy raw.hmis_details from program 'iconv -f LATIN1 -t UTF-8//TRANSLIT /mnt/data/projects/acdhs-housing/data/20220714/HMIS_Details_CMU.csv | sed -E -e ''s/[^[:print:]]/-/g; /^\s*$/d; ''' with CSV HEADER;



