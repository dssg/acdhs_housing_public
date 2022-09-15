-- this file contains sql code to clean HMIS details data and move it to the clean schema
-- run this file with: psql -f src/etl/db_clean_parent_child.sql

set role 'acdhs-housing-role';

-- create clean schema if it doesnt yet exists
create schema if not exists clean;

-- drop clean tables if they already exist
drop table if exists clean.parent_child;

CREATE TABLE clean.parent_child as (
    select
        hashed_mci_uniq_id::VARCHAR,
        hashed_mci_uniq_child::VARCHAR,
        relationship::VARCHAR
    from raw.parent_child
);

create index on clean.parent_child(hashed_mci_uniq_id);
create index on clean.parent_child(hashed_mci_uniq_child);

