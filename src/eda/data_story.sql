------------------
--- data story ---
------------------
-- this file includes a first version of a possible data story. Details must be adjusted to match a specific analytical formulation.
-- To be run in dbeaver.
-- The py script feature_generator.py contains an updated version of this query, which stores the result in a new table in the db.
------------------


with e_stats as (
	select distinct matter_id, claimamount, totaljudgmentamount, monthlyrentamount, filingdt -- stats on all matters before current_date
	from clean.eviction
),
-- info on all evictions per individual before the specified date
e as (
	select hashed_mci_uniq_id, count(*) as number_of_evictions, sum(claimamount) as sum_claimamount, sum(totaljudgmentamount) as sum_totaljudgmentamount, sum(monthlyrentamount) as sum_monthlyrentamount
	from clean.eviction_client_matches ecm
	left join e_stats using(matter_id)
	where filingdt < :current_date
	and hashed_mci_uniq_id notnull -- only look at individuals whose client hash is known in eviction_client_matches
	group by hashed_mci_uniq_id
),
-- date of last eviction per individual before the specified date
e2 as (
	select
		distinct on (hashed_mci_uniq_id)
		hashed_mci_uniq_id, filingdt as date_of_last_eviction
	from clean.eviction_client_matches ecm
	where filingdt < :current_date
	left join e_stats using(matter_id)
	order by hashed_mci_uniq_id, date_of_last_eviction desc
),
-- info on all evictions per individual in a specified time interval
e3 as (
	select hashed_mci_uniq_id, count(*) as number_of_evictions_last_year, sum(claimamount) as sum_claimamount_last_year, sum(totaljudgmentamount) as sum_totaljudgmentamount_last_year, sum(monthlyrentamount) as sum_monthlyrentamount_last_year
	from clean.eviction_client_matches ecm
	left join e_stats using(matter_id)
	where filingdt between (:current_date - :eviction_in_last_x_years::interval) and :current_date -- nr of eviction filings in the year before current_date
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
		client_hash, padhs_client_hash, dob, city, state, zip, ed_lvl_cd, martl_sts_cd, race_cd, gender_cd, load_date
	from clean.clients
	where load_date < :current_date
	order by client_hash, padhs_client_hash, load_date desc
),
--- list of all individuals who participated in homeless housing services (i.e., program key 263, 29, 32, 33, 264, 103) within y months from now ---
hl as (
	select client_hash, count(*) as homeless_program_participations
	from clean.involvement_feed if2
	where program_key in (263, 29, 32, 33, 264, 103)
	and prog_dt between :current_date and (:current_date + :hl_in_next_y_months::interval)
	group by client_hash
),
--- list of all individuals who participated in homeless housing services (i.e., program key 263, 29, 32, 33, 264, 103) within y months from now ---
hl_or_at_risk as (
	select client_hash, count(*) as homeless_or_at_risk_program_participations
	from clean.involvement_feed if2
	where program_key in (27, 262, 297, 296, 114, 29, 298, 263, 31, 264, 30, 103, 32, 28, 33)
	and prog_dt between :current_date and (:current_date + :hl_in_next_y_months::interval)
	group by client_hash
)
select
	*,
	case
		when homeless_program_participations > 0 then 1 else 0 -- make sure that nulls are labeled as 0 for those who are not homeless within y months from now
	end is_homeless,
	case
		when homeless_or_at_risk_program_participations > 0 then 1 else 0 -- make sure that nulls are labeled as 0 for those who are not homeless_or_at_risk within y months from now
	end is_homeless_or_at_risk
from
	e
	--left join cf on e.hashed_mci_uniq_id = cf.client_hash
	left join e2 using(hashed_mci_uniq_id)
	left join e3 using(hashed_mci_uniq_id)
	left join c on e.hashed_mci_uniq_id = c.client_hash
	left join hl on e.hashed_mci_uniq_id = hl.client_hash
	left join hl_or_at_risk on e.hashed_mci_uniq_id = hl_or_at_risk.client_hash
--where e.hashed_mci_uniq_id = 'AA5E65FD59'
	--c.client_hash notnull -- only consider individuals who are known in the clients table --> we do not use this where clause. otherwise we would exclude all individuals who are not yet in the clients table at the current point in time (irrespective of whether they will ever be in the clients table in the future).
	--and cf.client_hash notnull -- only consider individuals who are known in the client_feed table
--order by is_homeless asc
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