------------------
--- data story ---
------------------
-- this file contains a template to generate feature-label-pairs. To be run (and extended) with the python script feature_generator.py
------------------

set role "acdhs-housing-role";

create schema if not exists modelling;
create table if not exists modelling.{table_name} as 
	with e_stats as (
		select distinct matter_id, claimamount, totaljudgmentamount, monthlyrentamount, filingdt -- stats on all matters before current_date
		from clean.eviction
		where filingdt < {current_date}
	),
	-- info on all evictions per individual before the specified date
	e as (
		select hashed_mci_uniq_id as client_hash, count(*) as number_of_evicition_filings, sum(claimamount) as sum_claimamount, sum(totaljudgmentamount) as sum_totaljudgmentamount, sum(monthlyrentamount) as sum_monthlyrentamount
		from clean.eviction_client_matches ecm
		left join e_stats using(matter_id)
		where filingdt < {current_date}
		and hashed_mci_uniq_id notnull -- only look at individuals whose client hash is known in eviction_client_matches
		group by hashed_mci_uniq_id
	),
	-- date of last eviction per individual before the specified date
	e2 as (
		select
			distinct on (hashed_mci_uniq_id)
			hashed_mci_uniq_id as client_hash, filingdt as date_of_last_eviction, DATE_PART('day', {current_date}::timestamp - filingdt::timestamp) as days_since_last_eviction
		from clean.eviction_client_matches ecm
		left join e_stats using(matter_id)
		where filingdt < {current_date}
		order by hashed_mci_uniq_id, date_of_last_eviction, days_since_last_eviction desc
	),
	-- info on all evictions per individual in a specified time interval
	e3 as (
		select hashed_mci_uniq_id as client_hash, count(*) as number_of_evicition_filings_last_year, sum(claimamount) as sum_claimamount_last_year, sum(totaljudgmentamount) as sum_totaljudgmentamount_last_year, sum(monthlyrentamount) as sum_monthlyrentamount_last_year
		from clean.eviction_client_matches ecm
		left join e_stats using(matter_id)
		where filingdt between ({current_date} - {eviction_in_last_x_years}::interval) and {current_date} -- nr of eviction filings in the year before current_date
		--and hashed_mci_uniq_id notnull -- only look at individuals whose client hash is known in eviction_client_matches
		group by hashed_mci_uniq_id
	),
	-- client feed not used anymore because clients has several individuals with same client_hash
	--cf as (
	--	select distinct client_hash, legal_sex, gender, race, dob, dod, living_arangt_common_desc, empt_sts_common_desc, martl_sts_common_desc, ed_lvl_common_desc -- TODO: how to handle duplicate entries for client_hashes
	--	from clean.client_feed
	--), 
	c as (
		select
			distinct on (client_hash, padhs_client_hash) -- select last entry before current_date for each client, which is specified by distinct client hash and padhs client hash
			client_hash, padhs_client_hash, DATE_PART('year', {current_date}) - DATE_PART('year', dob) as age, city, state, zip, ed_lvl_cd, martl_sts_cd, race_cd, gender_cd, load_date
		from clean.clients
		where load_date < {current_date}
		order by client_hash, padhs_client_hash, load_date desc
	),
	--- list of all individuals who participated in homeless housing services (i.e., program key 263, 29, 32, 33, 264, 103) within y months from now ---
	hl as (
		select client_hash, count(*) as nr_of_months_in_program_homeless
		from clean.involvement_feed if2
		where program_key in {homelessness_programs}
		and prog_dt between {current_date} and ({current_date} + {hl_in_next_y_months}::interval)
		group by client_hash
	),
	--- list of all individuals who participated in homeless housing services (i.e., program key 263, 29, 32, 33, 264, 103) within y months from now ---
	housing_support_programs as (
		select client_hash, count(*) as nr_of_months_in_housing_support_programs
		from clean.involvement_feed if2
		where program_key in (27, 262, 297, 296, 114, 29, 298, 263, 31, 264, 30, 103, 32, 28, 33)
		and prog_dt between {current_date} and ({current_date} + {hl_in_next_y_months}::interval)
		group by client_hash
	) {add_features_for_program_keys1}
	select
		*,
		case
			when nr_of_months_in_program_homeless > 0 then 1 else 0 -- make sure that nulls are labeled as 0 for those who are not homeless within y months from now
		end is_currently_homeless,
		case
			when nr_of_months_in_housing_support_programs > 0 then 1 else 0 -- make sure that nulls are labeled as 0 for those who are not homeless_or_at_risk within y months from now
		end is_currently_in_housing_support_program {add_features_for_program_keys2}
	from
		e
		--left join cf using(client_hash)
		left join e2 using(client_hash)
		left join e3 using(client_hash)
		left join c using(client_hash)
		left join hl using(client_hash)
		left join housing_support_programs using(client_hash) {add_features_for_program_keys3}
	--where e.hashed_mci_uniq_id = 'AA5E65FD59'
		--c.client_hash notnull -- only consider individuals who are known in the clients table --> we do not use this where clause. otherwise we would exclude all individuals who are not yet in the clients table at the current point in time (irrespective of whether they will ever be in the clients table in the future).
		--and cf.client_hash notnull -- only consider individuals who are known in the client_feed table
	--order by is_currently_homeless asc
;

-----------------------------------------------
--- additional information to run the query ---
-----------------------------------------------

-- the dbeaver parameters can, for example, be specified as follows:
-- current_date = '2019-01-01'::date
-- eviction_in_last_x_years = '1 years'
-- hl_in_next_y_months = '6 months'

--- labels ---
-- homeless = part of any of the programs: 263, 29, 32, 33, 264, 103
-- homeless_or_at_risk = part of any of the programs: 27, 262, 297, 296, 114, 29, 298, 263, 31, 264, 30, 103, 32, 28, 33