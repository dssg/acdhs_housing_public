set role 'acdhs-housing-role';

create schema if not exists raw;

-- drop existing tables if they exist already
drop table if exists raw.parent_child;

--parent child
CREATE TABLE raw.parent_child
(
    hashed_mci_uniq_id VARCHAR,
    hashed_mci_uniq_child VARCHAR,
    relationship VARCHAR
    
);

\copy raw.parent_child from '/mnt/data/projects/acdhs-housing/data/20220622/Parent Child.csv' WITH CSV HEADER;
