-- this file contains sql code to dump data on Public Housing Assistance data into the sql database

-- run this file with: psql -f src/etl/ph_data_db_dump.sql

-- Content: 
    -- All data from two of AC three housing authorities: 
        -- Allegheny County Housing Authority (ACHA)
        -- Housing Authority of the city of Pittsburgh (HACP)
    -- Data includes two types of information:
        -- Activity: Information on residents: move dates, demographics, info on rental and info on previous place of residence (zip)
        -- Action: Informatino on bais of annual or interrim reexamination to adjust income determination and amount of rent and utility assistance. Includes info on rent, tenat rent 

-- statements created with, e.g.: head -n 1000 /mnt/data/projects/acdhs-housing/data/20220621/ACHA\ Res\ Action.csv | tr [:upper:] [:lower:] | tr ' ' '_' | sed 's/#/num/' | csvsql -i postgresql --db-schema raw --tables acha_res_action




set role 'acdhs-housing-role';

create schema if not exists raw;


-- drop existing tables if they exist already
drop table if exists raw.acha_res_action;
drop table if exists raw.acha_res_activity;
drop table if exists raw.acha_s8_action;
drop table if exists raw.acha_s8_activity;
drop table if exists raw.hacp_res_action;
drop table if exists raw.hacp_res_activity;
drop table if exists raw.hacp_s8_action;
drop table if exists raw.hacp_s8_activity;


-- RES ACTION
CREATE TABLE raw.acha_res_action
(
    hashed_household_id VARCHAR,
    program VARCHAR,
    moveindate VARCHAR,
    actiontype VARCHAR,
    effectivedate VARCHAR,
    iscorrection VARCHAR,
    correctionreason VARCHAR,
    totaladjustedannualincome VARCHAR,
    utilityallowance VARCHAR,
    renttocharge VARCHAR,
    typeofrentselected VARCHAR
);

\copy raw.acha_res_action from '/mnt/data/projects/acdhs-housing/data/20220621/ACHA Res Action.csv' WITH CSV HEADER;


-- RES ACTIVITY
CREATE TABLE raw.acha_res_activity
(
    hashed_household_id VARCHAR,
    hashed_mci_uniq_id VARCHAR,
    program VARCHAR,
    dateofbirth VARCHAR,
    gender VARCHAR,
    ethnicitydesc VARCHAR,
    race VARCHAR,
    primarystreet VARCHAR,
    addr2 VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip VARCHAR,
    moveindate VARCHAR,
    moveoutdate VARCHAR,
    relationshipdesc VARCHAR,
    fssstatus VARCHAR,
    community VARCHAR,
    dateoforiginalapplication VARCHAR,
    zipofparticipantwhenadmitted VARCHAR,
    homelessatadmission VARCHAR,
    isaccessableunit VARCHAR
);

\copy raw.acha_res_activity from '/mnt/data/projects/acdhs-housing/data/20220621/ACHA Res Activity.csv' WITH CSV HEADER;


-- ACHS S8 ACTION
CREATE TABLE raw.acha_s8_action
(
    hashed_household_id VARCHAR,
    program VARCHAR,
    moveindate VARCHAR,
    actiontype VARCHAR,
    effectivedate VARCHAR,
    iscorrection VARCHAR,
    correctionreason VARCHAR,
    totaladjustedannualincome VARCHAR,
    vouchertype VARCHAR,
    utilityallowance VARCHAR,
    tenantrenttoowner VARCHAR,
    haptoowner VARCHAR
);

\copy raw.acha_s8_action from '/mnt/data/projects/acdhs-housing/data/20220621/ACHA S8 Action.csv' WITH CSV HEADER;


-- ACHA S8 ACTIVITY
CREATE TABLE raw.acha_s8_activity
(
    hashed_household_id VARCHAR,
    hashed_mci_uniq_id VARCHAR,
    program VARCHAR,
    dateofbirth VARCHAR,
    gender VARCHAR,
    ethnicity VARCHAR,
    race VARCHAR,
    primarystreet VARCHAR,
    secondarystreet VARCHAR,
    suite VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip VARCHAR,
    moveindate VARCHAR,
    moveoutdate VARCHAR,
    relationship VARCHAR,
    bedrooms VARCHAR,
    numberofdependants VARCHAR,
    dateenteredwaitinglist VARCHAR,
    zipofparticipantwhenadmitted VARCHAR,
    homelessatadmission VARCHAR
);

\copy raw.acha_s8_activity from '/mnt/data/projects/acdhs-housing/data/20220621/ACHA S8 Activity.csv' WITH CSV HEADER;


-- HACP Res Action 
CREATE TABLE raw.hacp_res_action
(
    hashed_household_id VARCHAR,
    program VARCHAR,
    moveindate VARCHAR,
    actiontype VARCHAR,
    effectivedate VARCHAR,
    iscorrection VARCHAR,
    correctionreason VARCHAR,
    totaladjustedannualincome VARCHAR,
    utilityallowance VARCHAR,
    renttocharge VARCHAR,
    typeofrentselected VARCHAR
);

\copy raw.hacp_res_action from '/mnt/data/projects/acdhs-housing/data/20220621/HACP Res Action.csv' WITH CSV HEADER;


-- HACP Res Activity
CREATE TABLE raw.hacp_res_activity
(
    hashed_household_id VARCHAR,
    hashed_mci_uniq_id VARCHAR,
    program VARCHAR,
    dateofbirth VARCHAR,
    gender VARCHAR,
    ethnicitydesc VARCHAR,
    race VARCHAR,
    primarystreet VARCHAR,
    addr2 VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip VARCHAR,
    moveindate VARCHAR,
    moveoutdate VARCHAR,
    relationshipdesc VARCHAR,
    fssstatus VARCHAR,
    community VARCHAR,
    dateoforiginalapplication VARCHAR,
    zipofparticipantwhenadmitted VARCHAR,
    homelessatadmission VARCHAR,
    isaccessableunit VARCHAR
);

\copy raw.hacp_res_activity from '/mnt/data/projects/acdhs-housing/data/20220621/HACP Res Activity.csv' WITH CSV HEADER;


-- HACP S8 Action
CREATE TABLE raw.hacp_s8_action
(
    hashed_household_id VARCHAR,
    program VARCHAR,
    moveindate VARCHAR,
    actiontype VARCHAR,
    effectivedate VARCHAR,
    iscorrection VARCHAR,
    correctionreason VARCHAR,
    totaladjustedannualincome VARCHAR,
    vouchertype VARCHAR,
    utilityallowance VARCHAR,
    tenantrenttoowner VARCHAR,
    hacptoowner VARCHAR
);

\copy raw.hacp_s8_action from '/mnt/data/projects/acdhs-housing/data/20220621/HACP S8 Action.csv' WITH CSV HEADER;


-- HACP S8 Activity
CREATE TABLE raw.hacp_s8_activity
(
    hashed_household_id VARCHAR,
    hashed_mci_uniq_id VARCHAR,
    program VARCHAR,
    dateofbirth VARCHAR,
    gender VARCHAR,
    ethnicity VARCHAR,
    race VARCHAR,
    primarystreet VARCHAR,
    secondarystreet VARCHAR,
    suite VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip VARCHAR,
    moveindate VARCHAR,
    moveoutdate VARCHAR,
    relationship VARCHAR,
    bedrooms VARCHAR,
    numberofdependants VARCHAR,
    dateenteredwaitinglist VARCHAR,
    zipofparticipantwhenadmitted VARCHAR,
    homelessatadmission VARCHAR
);

\copy raw.hacp_s8_activity from '/mnt/data/projects/acdhs-housing/data/20220621/HACP S8 Activity.csv' with CSV HEADER;