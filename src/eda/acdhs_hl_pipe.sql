-----------------------------------------------
-- GENERATING SQL QUERY FOR HOMELESSNESS EDA --
-----------------------------------------------

-- params
/*
 TABLE_NAME = acdhs_12months
 DATE_ANALYSIS = '2019-01-01' 	-- date of analysis
 MONTHS_COHORT = 12				-- specifies number of months included in cohort
 MONTHS_CURRENT_HL = 6			-- individual has to have been homeless within MONTHS_CURRENT_HL and DATE_ANALYSIS to be considered currently homeless
 MONTHS_FUTURE_HL = 12			-- OUTCOME VAR: individual has to become homeless between DATE_ANALYSIS and MONTHS_FUTURE_HL to be considered homeless in future
 MONTHS_EVICT_RANGE = 12		-- specifies a month range to count recent evictions.
 HL_PROGRAM_KEY = (263)			-- specifies definition of homelessness
*/


-- set roles
set role "acdhs-housing-role";

create schema if not exists modelling;

--drop table if exists modelling.{TABLE_NAME} ;

create table if not exists MODELLING.{TABLE_NAME} as 

	-- COHORT: select clients in period of analysis 
	-- also generates number of prior program interactions
	with cohort as (
		select 
			client_hash,
			sum(case when program_key=27 then 0 else 1 end) as nr_programs_not_housing,
			sum(case when program_key=27 then 1 else 0 end) as nr_programs_housing,
			count(distinct case when program_key=27 then null else program_key end) as nr_distinct_programs_not_housing, 
			count(distinct case when program_key=27 then program_key else null end) as nr_distinct_programs_housing 
			from clean.involvement_feed if2
		where prog_dt 
			between('{DATE_ANALYSIS}'::date - '{MONTHS_COHORT} months'::interval) 
			and ('{DATE_ANALYSIS}'::date - '1 day'::interval)
		group by client_hash
		),

	
	-- HOMELESS: information on homeless individuals in past, present and future
	
	-- PAST HOMELESSNESS: individuals who have been homeless in the past (up until specified time)
	hl_past as (
		select 
			client_hash,
			count(*) as past_months_in_hl, 
			min(prog_dt) as past_hl_first_date,
			max(prog_dt) as past_hl_last_date,
	        DATE_PART('day', '{DATE_ANALYSIS}'::date::timestamp - max(prog_dt)::timestamp) as days_since_last_hl
		from clean.involvement_feed if3 
		where program_key in {HL_PROGRAM_KEY}
		and prog_dt < '{DATE_ANALYSIS}'::date
		group by client_hash
		),
	
		
	-- CURRENT HOMELESSNESS (homelessness at specified date or in last 12 months)
	hl_current as (
		select
			client_hash,
			count(*) as current_months_in_hl,
			min(prog_dt) as current_hl_first_date,
			max(prog_dt) as current_hl_last_date
		from clean.involvement_feed if2 
		where program_key in {HL_PROGRAM_KEY}
		and prog_dt 
			between ('{DATE_ANALYSIS}'::date - '{MONTHS_CURRENT_HL} months'::interval) 
			and ('{DATE_ANALYSIS}'::date - '1 day'::interval)
		group by client_hash
		),
		
		
	-- FUTURE HOMELESSNESS  (OUTCOME VAR): future homelessness within next XX months
	hl_future as (
		select
			client_hash,
			count(*) as future_months_in_hl,
			min(prog_dt) as future_hl_first_date,
			max(prog_dt) as future_hl_last_date
		from clean.involvement_feed if2 
		where program_key in {HL_PROGRAM_KEY}
		and prog_dt 
			between '{DATE_ANALYSIS}'::date 
			and ('{DATE_ANALYSIS}'::date + '{MONTHS_FUTURE_HL} months'::interval)
		group by client_hash
		),
	
		
	-- CLIENTS
	
	-- Demographic information
	-- NOTE: client_feed only includes most recently available info:
		-- TODO: for variable vars, go into clients table to see if variables used to be different. If so, update
	client_dem as ( 
		select 
			distinct on (client_hash)
			client_hash, 
			DATE_PART('year', '{DATE_ANALYSIS}'::date) - DATE_PART('year', dob) as age, 
			(case when dod < '{DATE_ANALYSIS}'::date then 1 else 0 end) as is_dead,
			legal_sex, 
			race, 
			(case when race in 
				('Other', 'Asian', 'Two or More Races', 'Other Single Race', 
				'Native Hawaiian/Pacific Islander', 'No Data')
				then 'Other/no data' else race end) as race_grp,
			ethnic_desc, 
			empt_sts_common_desc, 
			(case when empt_sts_common_desc in 
				('1~Employed - Full time', '6~Self-employed - Full time')
				then 'Full-time'  
				when empt_sts_common_desc in ('99~Unknown') then 'Unknown'
				else 'Not full-time' end) as empt_grp, -- Note: this classifies
			martl_sts_common_desc, 
			ed_lvl_common_desc, 
			(case when ed_lvl_common_desc in 
				('UNDERGRAD~Some College', 'GRAD COMP~College Degree', 'GRAD~Graduate Degree') then 'College'
				when ed_lvl_common_desc in ('99~Unknown') then 'Unknown'
				else 'No college' end) as ed_grp, 
			kml_pa_census_tract, 
			std_city, 
			std_zip
		from clean.client_feed cf
		),
	
	-- NOTE: CLIENTS DOES NOT HAVE INFORMATION ON ALL CLIENTS, JUST THOSE WHERE DATA HAS BEEN UPDATED
	/*
	client_dem as (
		select
			distinct on (client_hash, padhs_client_hash) -- select last entry before current_date for each client, which is specified by distinct client hash and padhs client hash
			client_hash, 
			padhs_client_hash, 
			DATE_PART('year', '2019-01-01'::date) - DATE_PART('year', dob) as age, 
			city, 
			state, 
			zip, 
			ed_lvl_cd, 
			martl_sts_cd, 
			race_cd, 
			gender_cd, 
			load_date
		from clean.clients
		where load_date < '2019-01-01'::date
		order by client_hash, padhs_client_hash, load_date desc
	),
	*/
	
		
		
	-- EVICTIONS
	
	-- eviction stats (prior to specified date)
	evict_stats as (
		select 
	        distinct matter_id, 
	        claimamount,   
	        totaljudgmentamount, 
	        monthlyrentamount, 
	        filingdt
		from clean.eviction
	    where filingdt < '{DATE_ANALYSIS}'::date
	),
	
	-- info on all evictions per individual  (prior to specified date)
	evict as (
		select 
	        hashed_mci_uniq_id as client_hash, 
	        count(*) as number_of_evict_filings, 
	        avg(claimamount) as mn_claimamount, 
	        avg(totaljudgmentamount) as mn_totaljudgmentamount, 
	        avg(monthlyrentamount) as mn_monthlyrentamount
		from clean.eviction_client_matches ecm
		left join evict_stats using(matter_id)
		where filingdt < '{DATE_ANALYSIS}'::date
		and hashed_mci_uniq_id notnull -- only look at individuals whose client hash is known in eviction_client_matches
		group by hashed_mci_uniq_id
	),
	
	-- date of last eviction per individual before the specified date
	evict_last as (
		select
			distinct on (hashed_mci_uniq_id)
			hashed_mci_uniq_id as client_hash, 
	        filingdt as date_last_evict, 
	        claimamount as claim_last_evict,   
	        totaljudgmentamount judgement_last_evict, 
	        DATE_PART('day', '2019-01-01'::date::timestamp - filingdt::timestamp) as days_since_last_evict
		from clean.eviction_client_matches ecm
		left join evict_stats using(matter_id)
		where filingdt < '{DATE_ANALYSIS}'::date
		order by hashed_mci_uniq_id, date_last_evict, days_since_last_evict desc
	),
	
	-- info on most all evictions in given range (prior to specified date)
	evict_range as (
		select 
			hashed_mci_uniq_id as client_hash, 
			count(*) as number_of_evict_range, 
			avg(claimamount) as mn_claimamount_range, 
			avg(totaljudgmentamount) as mn_totaljudgmentamount_range, 
			avg(monthlyrentamount) as mn_monthlyrentamount_range
		from clean.eviction_client_matches ecm
		left join evict_stats using(matter_id)
		where filingdt 
			between ('{DATE_ANALYSIS}'::date - '{MONTHS_EVICT_RANGE}'::interval) 
			and ('{DATE_ANALYSIS}'::date - '1 day'::interval) -- nr of eviction filings in the year before current_date
		--and hashed_mci_uniq_id notnull -- only look at individuals whose client hash is known in eviction_client_matches
		group by hashed_mci_uniq_id
	),

	
	-- GENERATE JOINED TABLE 
	
	sample as (	
		select 
			*
		from cohort
		left join client_dem using(client_hash)
		left join hl_past using(client_hash)
		left join hl_current using(client_hash)
		left join hl_future using(client_hash)
		left join evict using(client_hash)
		left join evict_last using(client_hash)
		left join evict_range using(client_hash)
	)

	-- Replace null values with 0s for dummies
	select 
		*,
		(case when number_of_evict_filings > 0 then 1 else 0 end) as had_eviction, 
		(case when number_of_evict_range > 0 then 1 else 0 end) as had_eviction_range, 
		(case when past_months_in_hl > 0 then 1 else 0 end) as past_hl, 
		(case when current_months_in_hl > 0 then 1 else 0 end) as current_hl, 
		(case when future_months_in_hl > 0 then 1 else 0 end) as future_hl
	from sample 
;
