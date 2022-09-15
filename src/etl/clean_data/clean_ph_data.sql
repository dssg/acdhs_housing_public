-- this file contains sql code to clean Public Housing Assistance data and move it to the clean schema
-- run this file with: psql -f src/etl/db_clean_ph_data.sql

-- Content: 
    -- All data from two of AC three housing authorities: 
        -- Allegheny County Housing Authority (ACHA)
        -- Housing Authority of the city of Pittsburgh (HACP)
    -- Data includes two types of information:
        -- Activity: Information on residents: move dates, demographics, info on rental and info on previous place of residence (zip)
        -- Action: Information on bais of annual or interrim reexamination to adjust income determination and amount of rent and utility assistance. Includes info on rent, tenat rent 

-- statements created with, e.g.: head -n 1000 /mnt/data/projects/acdhs-housing/data/20220621/ACHA\ Res\ Action.csv | tr [:upper:] [:lower:] | tr ' ' '_' | sed 's/#/num/' | csvsql -i postgresql --db-schema raw --tables acha_res_action
set role 'acdhs-housing-role';



-- create clean schema if it doesnt yet exists
create schema if not exists clean;


-- drop clean tables if they already exist
drop table if exists clean.res_action;
drop table if exists clean.s8_action;
drop table if exists clean.res_activity;
drop table if exists clean.s8_activity;


-- RES Action
create table clean.res_action as (

	-- ACHA Res Action	
	select
		hashed_household_id::varchar,
		program::varchar,
		moveindate::date, 
		actiontype::varchar,
		effectivedate::date,
		(case when trim(iscorrection)='Yes' then 1 when trim(iscorrection)='No' then 0 else null end) as iscorrection,
		(case when correctionreason='NULL' then null else correctionreason::smallint end) as correctionreason, 
		totaladjustedannualincome::float,
		utilityallowance::float,
		renttocharge::float,
		typeofrentselected::varchar -- this should only include I and F,  all others are mistakes. 
	from raw.acha_res_action ara
	
	union all

	-- HACP Res Action
	select
		hashed_household_id::varchar,
		program::varchar,
		moveindate::date, 
		actiontype::varchar,
		effectivedate::date,
		(case when trim(iscorrection)='Yes' then 1 when trim(iscorrection)='No' then 0 else null end) as iscorrection,
		(case when correctionreason='NULL' then null else correctionreason::smallint end) as correctionreason, 
		totaladjustedannualincome::float,
		utilityallowance::float,
		renttocharge::float,
		typeofrentselected::varchar -- this should only include I and F,  all others are mistakes. 
	from raw.hacp_res_action hra
	);

create index on clean.res_action(hashed_household_id);
create index on clean.res_action(program);
create index on clean.res_action(moveindate);


-- S8 Action
-- Note: raw.acha_s8_action has bad hashed_household_id
-- Note: raw.acha_s8_action likely has spelling error: hapoowner->hacptoowner

create table clean.s8_action as (

	-- ACHA S8 Action
	select 
		hashed_household_id::varchar, -- NOTE: This is an integer, which doesnt correspond with the hash in other datasets
		program::varchar,
		moveindate::date,
		actiontype::varchar,
		effectivedate::date,
		(case when trim(iscorrection)='Yes' then 1 when trim(iscorrection)='No' then 0 else null end) as iscorrection,
		(case when correctionreason='NULL' then null else correctionreason::varchar end) as correctionreason, 
		totaladjustedannualincome::float,
		vouchertype::varchar, -- this is a mix of code and descriptions
		utilityallowance::float,
		tenantrenttoowner::float,
		asa.haptoowner::float as hacptoowner -- this seems like a naming error, check w Rachel
	from raw.acha_s8_action asa 
	
	union all
	
	-- HACP S8 ACTION
	select 
		hashed_household_id::varchar, 
		program::varchar, 
		moveindate::date,
		actiontype::varchar,
		effectivedate::date,
		(case when trim(iscorrection)='Yes' then 1 when trim(iscorrection)='No' then 0 else null end) as iscorrection,
		(case when correctionreason='NULL' then null else correctionreason::varchar end) as correctionreason, 
		totaladjustedannualincome::float,
		vouchertype::varchar, -- this is a mix of code and descriptions
		utilityallowance::float,
		tenantrenttoowner::float,
		hsa.hacptoowner::float
	from raw.hacp_s8_action hsa 
);

create index on clean.s8_action(hashed_household_id);
create index on clean.s8_action(program);
create index on clean.s8_action(moveindate);


-- Res Activity
create table clean.res_activity as(

	-- ACHA Res Activity
	select
		hashed_household_id::varchar,
		hashed_mci_uniq_id::varchar, 
		program::varchar, 
		dateofbirth::date, 
		gender::varchar, 
		(case when trim(ethnicitydesc)='Hispanic' then 'Hispanic or Latino' 
			when trim(ethnicitydesc)='Hispanic or Latino' then 'Hispanic or Latino' 
			when trim(ethnicitydesc)='Not Hispanic or Latino' then 'Not Hispanic or Latino' 
			when trim(ethnicitydesc)='Non-Hispanic' then 'Not Hispanic or Latino' 
			else ethnicitydesc end) as ethnicitydesc,
		(case when trim(race)='NULL' then null else race::varchar end) as race, 
		primarystreet::varchar, 
		addr2::varchar, 
		city::varchar, 
		state::varchar,
		zip::smallint, 
		moveindate::date, 
		moveoutdate::date,
		relationshipdesc::varchar, 
		(case when fssstatus='NULL' then null else fssstatus::varchar end) as fssstatus, 
		(case when community='NULL' then null else community::varchar end) as community, 
		dateoforiginalapplication::date, 
		zipofparticipantwhenadmitted::varchar, 
		(case when trim(homelessatadmission)='Yes' then 1
		when trim(homelessatadmission)='No' then 0 else null end) as homelessatadmission,
		(case when trim(isaccessableunit)='Yes' then 1 when trim(isaccessableunit)='No' then 0 else null end) as isaccessableunit
	from raw.acha_res_activity ara
	
	union all
	
	-- HACP Res Activity
	select
		hashed_household_id::varchar,
		hashed_mci_uniq_id::varchar, 
		program::varchar, 
		dateofbirth::date, 
		gender::varchar, 
		(case when trim(ethnicitydesc)='Hispanic' then 'Hispanic or Latino' 
			when trim(ethnicitydesc)='Hispanic or Latino' then 'Hispanic or Latino' 
			when trim(ethnicitydesc)='Not Hispanic or Latino' then 'Not Hispanic or Latino' 
			when trim(ethnicitydesc)='Non-Hispanic' then 'Not Hispanic or Latino' 
			else ethnicitydesc end) as ethnicitydesc,
		(case when trim(race)='NULL' then null else race::varchar end) as race, 
		primarystreet::varchar, 
		addr2::varchar, 
		city::varchar, 
		state::varchar,
		zip::smallint, 
		moveindate::date, 
		moveoutdate::date,
		relationshipdesc::varchar, 
		(case when fssstatus='NULL' then null else fssstatus::varchar end) as fssstatus, 
		(case when community='NULL' then null else community::varchar end) as community, 
		dateoforiginalapplication::date, 
		zipofparticipantwhenadmitted::varchar, 
		(case when trim(homelessatadmission)='Yes' then 1
		when trim(homelessatadmission)='No' then 0 else null end) as homelessatadmission,
		(case when trim(isaccessableunit)='Yes' then 1 when trim(isaccessableunit)='No' then 0 else null end) as isaccessableunit
	from raw.hacp_res_activity ara 
	);

create index on clean.res_activity(hashed_household_id);
create index on clean.res_activity(hashed_mci_uniq_id);
create index on clean.res_activity(program);
create index on clean.res_activity(moveindate);


-- S8 Activity
create table clean.s8_activity as(

	-- ACHA S8 Activity
	select
		hashed_household_id::varchar,
		hashed_mci_uniq_id::varchar, 
		program::varchar, 
		dateofbirth::date, 
		gender::varchar, 
		(case when trim(ethnicity)='Hispanic' then 'Hispanic or Latino' 
			when trim(ethnicity)='Hispanic or Latino' then 'Hispanic or Latino' 
			when trim(ethnicity)='Not Hispanic or Latino' then 'Not Hispanic or Latino' 
			when trim(ethnicity)='Non-Hispanic' then 'Not Hispanic or Latino' 
			else ethnicity end) as ethnicity,
		(case when trim(race)='NULL' then null else race::varchar end) as race, 
		primarystreet::varchar, 
		secondarystreet::varchar, 
		suite::varchar,
		city::varchar, 
		state::varchar,
		zip::int, 
		moveindate::date, 
		moveoutdate::date,
		relationship::varchar, 
		bedrooms::smallint, 
		numberofdependants::smallint,
		dateenteredwaitinglist::date, 
		zipofparticipantwhenadmitted::varchar, 
		(case when trim(homelessatadmission)='Yes' then true 
		when trim(homelessatadmission)='No' then false else null end) as homelessatadmission
	from raw.acha_s8_activity 
	
	union all
	
	-- HACP S8 Activity
	
	select
		hashed_household_id::varchar,
		hashed_mci_uniq_id::varchar, 
		program::varchar, 
		dateofbirth::date, 
		gender::varchar, 
		(case when trim(ethnicity)='Hispanic' then 'Hispanic or Latino' 
			when trim(ethnicity)='Hispanic or Latino' then 'Hispanic or Latino' 
			when trim(ethnicity)='Not Hispanic or Latino' then 'Not Hispanic or Latino' 
			when trim(ethnicity)='Non-Hispanic' then 'Not Hispanic or Latino' 
			else ethnicity end) as ethnicity,
		(case when trim(race)='NULL' then null else race::varchar end) as race, 
		primarystreet::varchar, 
		secondarystreet::varchar, 
		suite::varchar,
		city::varchar, 
		state::varchar,
		zip::int, 
		moveindate::date, 
		moveoutdate::date,
		relationship::varchar, 
		bedrooms::smallint, 
		numberofdependants::smallint,
		dateenteredwaitinglist::date, 
		zipofparticipantwhenadmitted::varchar, 
		(case when trim(homelessatadmission)='Yes' then true 
		when trim(homelessatadmission)='No' then false else null end) as homelessatadmission
	from raw.hacp_s8_activity  
	);


create index on clean.s8_activity(hashed_household_id);
create index on clean.s8_activity(hashed_mci_uniq_id);
create index on clean.s8_activity(program);
create index on clean.s8_activity(moveindate);