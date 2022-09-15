-- this file contains sql code to dump the eviction data into the database
-- run file with: psql -f src/etl/db_dump_eviction_data.sql

-- statements created with, e.g.: head -n 1000 /mnt/data/projects/acdhs-housing/data/20220614/Allegheny\ County\ Eviction\ Data\ 2012_2018.csv | tr [:upper:] [:lower:] | tr ' ' '_' | sed 's/#/num/' | csvsql -i postgresql --db-schema raw --tables eviction20122018

set role "acdhs-housing-role";
create schema if not exists raw;

-- drop existing raw tables if they exist already
drop table if exists raw.eviction20122018;
drop table if exists raw.eviction20192022;
drop table if exists raw.eviction_landlords;
drop table if exists raw.eviction_client_matches;

-- eviction data 2012-2018

CREATE TABLE raw.eviction20122018 (
        matter_id VARCHAR,
        docketno VARCHAR,
        filingdt VARCHAR,
        filing_yr VARCHAR,
        filing_month VARCHAR,
        dispositiondt VARCHAR,
        casestatus VARCHAR,
        districtcourtno VARCHAR,
        city_of_pgh_flag VARCHAR,
        grantspossession VARCHAR,
        grantpossessionjudgmentnotsat VARCHAR,
        order_for_possession VARCHAR,
        ofp_issue_dt VARCHAR,
        judgement_for_landlord VARCHAR,
        judgement_for_tenant VARCHAR,
        settled VARCHAR,
        withdrawn VARCHAR,
        dismissed VARCHAR,
        possession_appeal_by_tenant VARCHAR,
        possession_appeal_by_ll VARCHAR,
        monetary_appeal_by_tenant VARCHAR,
        monetary_appeal_by_ll VARCHAR,
        monetary_appeal_successful VARCHAR,
        possession_appeal_successful VARCHAR,
        claimamount VARCHAR,
        totaljudgmentamount VARCHAR,
        civiljudgmentcomponenttype VARCHAR,
        component_amount VARCHAR,
        satisfaction_entered VARCHAR,
        monthlyrentamount VARCHAR,
        tenanthasrepresentation VARCHAR,
        landlordhasrepresentation VARCHAR
);

\COPY raw.eviction20122018 from '/mnt/data/projects/acdhs-housing/data/20220614/Allegheny County Eviction Data 2012_2018.csv' WITH CSV HEADER;

-- eviction data 2019-2022

CREATE TABLE raw.eviction20192022 (
        matter_id VARCHAR,
        docketno VARCHAR,
        filingdt VARCHAR,
        filing_yr VARCHAR,
        filing_month VARCHAR,
        dispositiondt VARCHAR,
        casestatus VARCHAR,
        districtcourtno VARCHAR,
        city_of_pgh_flag VARCHAR,
        grantspossession VARCHAR,
        grantpossessionjudgmentnotsat VARCHAR,
        order_for_possession VARCHAR,
        ofp_issue_dt VARCHAR,
        judgement_for_landlord VARCHAR,
        judgement_for_tenant VARCHAR,
        settled VARCHAR,
        withdrawn VARCHAR,
        dismissed VARCHAR,
        possession_appeal_by_tenant VARCHAR,
        possession_appeal_by_ll VARCHAR,
        monetary_appeal_by_tenant VARCHAR,
        monetary_appeal_by_ll VARCHAR,
        monetary_appeal_successful VARCHAR,
        possession_appeal_successful VARCHAR,
        claimamount VARCHAR,
        totaljudgmentamount VARCHAR,
        civiljudgmentcomponenttype VARCHAR,
        component_amount VARCHAR,
        satisfaction_entered VARCHAR,
        monthlyrentamount VARCHAR,
        tenanthasrepresentation VARCHAR,
        landlordhasrepresentation VARCHAR
);

\COPY raw.eviction20192022 from '/mnt/data/projects/acdhs-housing/data/20220614/Allegheny County Eviction Data 2019_2022.06.13.csv' WITH CSV HEADER;

-- eviction data landlords

CREATE TABLE raw.eviction_landlords (
        matter_id VARCHAR,
        shortcaption VARCHAR,
        caseparticipantrole VARCHAR,
        participantcategory VARCHAR,
        participanttype VARCHAR,
        participant_id VARCHAR,
        primarydisplaynm VARCHAR,
        unique_displaynm VARCHAR,
        current_best_uniqnm VARCHAR,
        ha_owned_managed VARCHAR,
        ha_owned_pvt_mgmt VARCHAR,
        filingyr VARCHAR
);

\COPY raw.eviction_landlords from '/mnt/data/projects/acdhs-housing/data/20220614/AC Eviction Landlords 2012 to June_2022.csv' WITH CSV HEADER;

-- eviction data client matches

CREATE TABLE raw.eviction_client_matches (
        hashed_mci_uniq_id VARCHAR,
        caseparticipant_id VARCHAR,
        participant_id VARCHAR,
        caseparticipantrole VARCHAR,
        agency VARCHAR,
        docketno VARCHAR,
        docketyr VARCHAR,
        matter_id VARCHAR,
        shortcaption VARCHAR,
        address_line_1 VARCHAR,
        address_line_2 VARCHAR,
        city VARCHAR,
        state VARCHAR,
        zip_cd VARCHAR,
        initiationdt VARCHAR,
        dispositiondt VARCHAR,
        participanttype VARCHAR,
        address_key VARCHAR,
        rule_id VARCHAR
);


\COPY raw.eviction_client_matches from '/mnt/data/projects/acdhs-housing/data/20220614/Eviction Tenant_DHS Client Matches.csv' WITH CSV HEADER;
