-- returns a list of all distinct client hashes that are considered currently homeless
with currently_hl as (
    select distinct client_hash
    from {schema_name}.hl_table ht
    -- individuals who started utilising hl services in last 4 months
    where (program_start_dt >= ('{as_of_date}'::date - '{currently_hl_timespan}'::interval) and program_start_dt < '{as_of_date}'::date)
    -- individuals who stopped utilising hl services in last 4 months
    or (program_end_dt >= ('{as_of_date}'::date - '{currently_hl_timespan}'::interval) and program_end_dt < '{as_of_date}'::date)
    -- individuals who are still using hl services at time of analysis (potentially for a long time)
    or (program_start_dt < '{as_of_date}'::date and (program_end_dt is null or program_end_dt > '{as_of_date}'::date))
),
previously_hl as (
    select distinct client_hash
    from {schema_name}.hl_table ht
    -- individuals who ever utilised hl services
    where program_start_dt < '{as_of_date}'::date
)
-- cohort returns distinct list of clients who 
-- have had an eviction filed against them and who have not had a disposition by as_of_date
-- or who lost an eviction case 
-- and who have been homeless in the past
select 
    distinct hashed_mci_uniq_id as client_hash,
    '{as_of_date}'::date as as_of_date
    from clean.eviction_client_matches ecm 
    left join clean.eviction using(matter_id)
	where (
		filingdt between('{as_of_date}'::date - '{cohort_evict_timespan}'::interval) and ('{as_of_date}'::date)
            and (ecm.dispositiondt is null or not(ecm.dispositiondt < '{as_of_date}'::date))
		or (ecm.dispositiondt between('{as_of_date}'::date - '{cohort_evict_timespan}'::interval) and ('{as_of_date}'::date)
	        and judgement_for_landlord is true)
        )
        and hashed_mci_uniq_id not in (select client_hash from currently_hl)
        and hashed_mci_uniq_id in (select client_hash from previously_hl)
        and hashed_mci_uniq_id is not null
        and hashed_mci_uniq_id not like '%%##%%'

       