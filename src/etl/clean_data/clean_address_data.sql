set role "acdhs-housing-role";
create schema if not exists clean;

-- drop existing raw tables if they exist already
drop table if exists clean.address_feed;

-- address_feed data
create table clean.address_feed as (
    select
    client_hash::VARCHAR, 
    address_line_1::VARCHAR,
    address_line_2::VARCHAR,
    city::VARCHAR,
    state::VARCHAR,
    zip_cd::VARCHAR,
    eff_date::DATE
    from raw.address_feed af 
);

create index on clean.address_feed(zip_cd);
create index on clean.address_feed(client_hash);

