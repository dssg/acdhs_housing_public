set role "acdhs-housing-role";

create schema if not exists clean;

drop table if exists clean.clients, clean.client_feed, clean.eligibility, clean.involvement_feed ;


-- Clients data
create table clean.clients as(
	select
	client_hash::varchar,
	sent_client_hash::varchar, 
	padhs_client_hash::varchar, 
	county_code::float::smallint, 
	dpw_record_nbr::float::int,
	elig_member_count::float::smallint, 
	sent_ind::boolean,
	dob::date, 
	address_line_1::text, 
	address_line_2::text, 
	city::text, 
	state::text,
	zip::varchar, 
	zip_ext::varchar, 
	living_arangt_cd::varchar, -- # seems to indicate missing, but no lookup table. Need to verify
	living_arrangement_desc::text,
	ed_lvl_cd::varchar, 
	education_level_desc::text,
	martl_sts_cd::varchar,
	marital_status_desc::text, 
	(case when race_cd::text ='?' then null else race_cd::float::smallint end) as race_cd, 
	gender_cd::varchar,
	training_cd::varchar,
	load_date::date,
	active_ind::float::smallint ,
	address_key::float::bigint,
	change_date::date,
	to_date(extract_period_first, 'YYYYMM') as extract_period_first,
	to_date(extract_period_last, 'YYYYMM') as extract_period_last 
	from raw.clients c
);
	
create index on clean.clients(client_hash);
create index on clean.clients(dpw_record_nbr);


-- Elibility

create table clean.eligibility as (
	select
		eligibility_hash::varchar, 
		padhs_client_hash::varchar, 
		(case when dpw_record_nbr::text='' then null else dpw_record_nbr::float::int end) as dpw_record_nbr,
		(case when elig_member_count::text='' then null else elig_member_count::float::int end) as elig_member_count,
		category::varchar, 
		program_status_cd::smallint ,
		dpw_scu_cd::varchar,
		(case when elig_begin_date::text='' then null else elig_begin_date end)::date,
		(case when elig_end_date::text='' then null else elig_end_date end)::date,
		load_date::date,
		(case when change_date::text='' then null else change_date end)::date,
		to_date(extract_period_first, 'YYYYMM') as extract_period_first,
		to_date(extract_period_last, 'YYYYMM') as extract_period_last	
	from raw.eligibility e);

create index on clean.eligibility(padhs_client_hash);
create index on clean.eligibility(elig_begin_date);
create index on clean.eligibility(elig_end_date);



-- Client Feed

create table clean.client_feed as (
	select distinct on (client_hash)
		client_hash::varchar,
		(case when dob::text='' then null else dob::date end) as dob, 
		(case when dod::text='' then null else dod::date end) as dod, 
		legal_sex::varchar, -- sex has 99 unknown
		gender::varchar, --  gender has 99 unknown
		race::varchar, -- race has null and no data, so not explicitly coded
		(case when ethnic_desc='No Data' then null else ethnic_desc::varchar end) as ethnic_desc,
		living_arangt_common_desc::varchar,
		empt_sts_common_desc::varchar,
		martl_sts_common_desc::varchar,
		ed_lvl_common_desc::varchar,
		address_key::int, -- adresss_key has two negative values, -1 and -2, but no lookup table available 
		(case when kml_allegheny_council='' then null else kml_allegheny_council::varchar end) as kml_allegheny_council,
		(case when kml_pa_congress_district='' then null else kml_pa_congress_district::varchar end) as kml_pa_congress_district,
		(case when kml_pa_state_house_district::text='' then null else kml_pa_state_house_district::float::int end) as kml_pa_state_house_district,
		(case when kml_pa_state_senate_district::text='' then null else kml_pa_state_senate_district::float::int end) as kml_pa_state_senate_district,
		(case when kml_allegheny_municipality='' then null else kml_allegheny_municipality::varchar end) as kml_allegheny_municipality,
		(case when kml_pittsburgh_neighborhood='' then null else kml_pittsburgh_neighborhood::varchar end) as kml_pittsburgh_neighborhood,
		(case when std_neighborhood='' then null else std_neighborhood::varchar end) as std_neighborhood,
		(case when std_township ='' then null else std_township::varchar end) as std_township,
		(case when kml_allegheny_school  ='' then null else kml_allegheny_school::varchar end) as kml_allegheny_school,
		(case when kml_pa_census_tract='' then null else kml_pa_census_tract::varchar end) as kml_pa_census_tract, -- this variable is a bigint or float, but stored as varchar for now
		(case when std_street_number='' then null else std_street_number::varchar end) as std_street_number,
		(case when std_route='' then null else std_route::varchar end) as std_route,
		(case when std_city='' then null else std_city::varchar end) as std_city,
		(case when std_state='' then null else std_state::varchar end) as std_state,
		(case when std_zip='' then null else std_zip::varchar end) as std_zip
	from (
        select * from raw.snp_client_feed_2011_2020
        union all
        select * from raw.snp_client_feed_2021_2021
    ) as snp_client_feed_combined
);

create index on clean.client_feed(client_hash);
create index on clean.client_feed(dob);
create index on clean.client_feed(dod);


-- Involvement Feed
create table clean.involvement_feed as (
	select 
	client_hash::varchar,
	to_date(prog_dt, 'YYYY-MM') as prog_dt,
	program_key::smallint
	from raw.snp_involvement_feed
);

create index on clean.involvement_feed(prog_dt);
create index on clean.involvement_feed(program_key);
create index on clean.involvement_feed(client_hash);
