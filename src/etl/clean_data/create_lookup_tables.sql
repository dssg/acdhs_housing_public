set role "acdhs-housing-role";

-- create lookup schema
create schema if not exists lookup;

-- drop existing tables to reload them
drop table if exists lookup.race_codes, lookup.county_codes, lookup.program_codes, lookup.program_feed  ;


-- Race codes
create table lookup.race_codes (
    race_cd smallint,
    "desc" text
);

insert into lookup.race_codes (race_cd, "desc") values 
(1, 'Black or African American'),
(3, 'American Indian or Alaska Native'),
(4, 'Asian'),
(5, 'White'),
(6, 'Other'),
(7, 'Native Hawaiian or Other Pacific Islander'),
(8, 'Unknown');



-- Program feed 
-- This looks like the programs offered by the County DHS and 
-- connects with the involvement_feed table
create table lookup.program_feed as (
	select
	program_key::smallint,
	program_name::text,
	program_desc::text,
	field_name_active::text
	from raw.program_feed
);


-- County codes
-- Serves as a lookup for the county codes in the clients table
create table lookup.county_codes as (
	select 
	 case 
		 when county_code='#' then -2 
		 when county_code='?' then -1
		 else county_code::int 
	end as county_code,
	county_name::text 
	from raw.county_code_crosswalk ccc
	group by 1, 2
	order by 1
);


-- Program codes
-- Connects to the eligibility table
create table lookup.program_codes as (
	select 
	po_status_key::smallint,
	scu_key::int,
	dw_category::varchar,
	po_status_description::varchar,
	phdhs_category_code::varchar,
	padhs_program_status::int
	from raw.program_code_crosswalk pcc
);

