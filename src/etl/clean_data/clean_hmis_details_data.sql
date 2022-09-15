-- this file contains sql code to clean HMIS details data and move it to the clean schema
-- run this file with: psql -f src/etl/db_clean_hmis_details_data.sql

-- statements created with, e.g.: head -n 1000 /mnt/data/projects/acdhs-housing/data/20220714/HMIS_Details_CMU.csv | tr [:upper:] [:lower:] | tr ' ' '_' | sed 's/#/num/' | csvsql -i postgresql --db-schema raw --tables hmis_details

set role 'acdhs-housing-role';



-- create clean schema if it doesnt yet exists
create schema if not exists clean;


-- drop clean tables if they already exist
drop table if exists clean.hmis_details;

CREATE TABLE clean.hmis_details as (
    select
        hashed_mci_uniq_id::VARCHAR, 
        is_head_of_household::BOOLEAN, 
        relation_to_hoh::VARCHAR, 
        household_type::VARCHAR, 
        household_size::SMALLINT, 
        hud_project_type_id::SMALLINT, 
        hud_project_type_desc::VARCHAR, 
        uniq_prog_nm::VARCHAR, 
        prog_hh_type::VARCHAR, 
        winter_shelter::BOOLEAN, 
        enrollment_start_dt::DATE, 
        enrollment_end_dt::DATE, 
        move_in_dt::DATE, 
        is_domestic_violence_survivor::VARCHAR, 
        when_dv_experience_occured::VARCHAR, 
        reltn_between_perp_and_victim::VARCHAR, 
        prior_living_arrangement::VARCHAR, 
        reason_for_homelessness::VARCHAR, 
        is_clnt_disqual_frm_pub_hsing::VARCHAR, 
        exit_destination_type::VARCHAR, 
        client_exiting_to::VARCHAR
    from raw.hmis_details
);

create index on clean.hmis_details(hashed_mci_uniq_id);
create index on clean.hmis_details(enrollment_start_dt);
create index on clean.hmis_details(enrollment_end_dt);
create index on clean.hmis_details(move_in_dt);
