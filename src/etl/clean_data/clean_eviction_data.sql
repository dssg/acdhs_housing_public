-- this file contains sql code to clean the eviction dataset
-- run file with: psql -f src/etl/db_clean_eviction_data.sql
set role "acdhs-housing-role";

create schema if not exists clean;

-- drop existing clean tables if they exist already
drop table if exists clean.eviction20122018;
drop table if exists clean.eviction20192022;
drop table if exists clean.eviction_landlords;
drop table if exists clean.eviction_client_matches;

-- eviction20122018 and eviction20192022
create table clean.eviction as (
    select 
    matter_id::BIGINT , 
    docketno::VARCHAR , 
    filingdt::DATE , 
    filing_yr::SMALLINT,
    filing_month::SMALLINT,
    dispositiondt::DATE, 
    casestatus::VARCHAR , 
    districtcourtno::SMALLINT , 
    city_of_pgh_flag::BOOLEAN , 
    grantspossession::BOOLEAN, 
    grantpossessionjudgmentnotsat::BOOLEAN, 
    order_for_possession::BOOLEAN , 
    ofp_issue_dt::DATE, 
    judgement_for_landlord::BOOLEAN , 
    judgement_for_tenant::BOOLEAN , 
    settled::BOOLEAN , 
    withdrawn::BOOLEAN , 
    dismissed::BOOLEAN , 
    possession_appeal_by_tenant::BOOLEAN , 
    possession_appeal_by_ll::BOOLEAN , 
    monetary_appeal_by_tenant::BOOLEAN , 
    monetary_appeal_by_ll::BOOLEAN , 
    monetary_appeal_successful::BOOLEAN , 
    possession_appeal_successful::BOOLEAN , 
    claimamount::DECIMAL , 
    totaljudgmentamount::DECIMAL, 
    civiljudgmentcomponenttype::VARCHAR, 
    component_amount::DECIMAL, 
    satisfaction_entered::BOOLEAN , 
    monthlyrentamount::DECIMAL, 
    tenanthasrepresentation::BOOLEAN , 
    landlordhasrepresentation::BOOLEAN 
    from (
        select * from raw.eviction20122018
        union all
        select * from raw.eviction20192022
    ) as evictiondatacombined
);

create index on clean.eviction(matter_id);
create index on clean.eviction(filing_yr);
create index on clean.eviction(filing_month);
create index on clean.eviction(docketno);
create index on clean.eviction(dispositiondt);
create index on clean.eviction(districtcourtno);
create index on clean.eviction(ofp_issue_dt);
create index on clean.eviction(claimamount);
create index on clean.eviction(monthlyrentamount);

-- eviction_landlords
create table clean.eviction_landlords as (
    select 
    matter_id::BIGINT, 
    shortcaption::VARCHAR, 
    caseparticipantrole::VARCHAR, 
    participantcategory::VARCHAR, 
    participanttype::VARCHAR, 
    participant_id::BIGINT, 
    primarydisplaynm::VARCHAR, 
    unique_displaynm::VARCHAR, 
    current_best_uniqnm::VARCHAR, 
    ha_owned_managed::BOOLEAN, 
    ha_owned_pvt_mgmt::BOOLEAN, 
    to_date(filingyr, 'YYYY') as filing_yr
    from raw.eviction_landlords
);

create index on clean.eviction_landlords(matter_id);
create index on clean.eviction_landlords(participant_id);
create index on clean.eviction_landlords(filing_yr);
create index on clean.eviction_landlords(current_best_uniqnm);

-- eviction_client_matches
create table clean.eviction_client_matches as (
    select
    hashed_mci_uniq_id::VARCHAR, 
    caseparticipant_id::BIGINT, 
    participant_id::BIGINT, 
    caseparticipantrole::VARCHAR, 
    agency::VARCHAR, 
    docketno::VARCHAR, 
    to_date(docketyr, 'YYYY') as docketyr,
    matter_id::BIGINT, 
    shortcaption::VARCHAR, 
    address_line_1::VARCHAR, 
    address_line_2::VARCHAR, 
    city::VARCHAR, 
    state::VARCHAR, 
    zip_cd::VARCHAR, 
    initiationdt::VARCHAR, 
    dispositiondt::DATE, 
    participanttype::VARCHAR, 
    address_key::INTEGER, 
    rule_id::VARCHAR
    from raw.eviction_client_matches
);

create index on clean.eviction_client_matches(hashed_mci_uniq_id);
create index on clean.eviction_client_matches(caseparticipant_id);
create index on clean.eviction_client_matches(participant_id);
create index on clean.eviction_client_matches(docketno);
create index on clean.eviction_client_matches(docketyr);
create index on clean.eviction_client_matches(matter_id);
create index on clean.eviction_client_matches(city);
create index on clean.eviction_client_matches(zip_cd);
