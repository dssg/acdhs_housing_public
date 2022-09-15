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
eviction_data as (
    select
        distinct on (hashed_mci_uniq_id)
        hashed_mci_uniq_id as client_hash,
        e.filingdt
    from clean.eviction_client_matches ecm
    left join clean.eviction e using(matter_id)
)
-- cohort returns distinct list of clients who interacted with acdhs in the last year
-- based on three tables: involvement feed (monthly level), hmis details, and hmis current
-- and who are not currently homeless
select
    distinct pic.client_hash,
    '{as_of_date}'::date as as_of_date
    from clean.program_involvement_consolidated pic
    full outer join eviction_data ed
        on pic.client_hash = ed.client_hash
        -- individuals who had an eviction filed against them in the last x years
        and (tsrange('{as_of_date}'::date::timestamp - interval '{cohort_large_interaction_with_acdhs_timespan}', '{as_of_date}'::date::timestamp) @> ed.filingdt::timestamp)
    -- select individuals who have interacted with adhs in the last x years
    -- these are individuals whose program involvement timespan overlaps with the last x years
    where (
        (tsrange('{as_of_date}'::date::timestamp - interval '{cohort_large_interaction_with_acdhs_timespan}', '{as_of_date}'::date::timestamp) @> program_start_dt::timestamp)
        or
        (tsrange('{as_of_date}'::date::timestamp - interval '{cohort_large_interaction_with_acdhs_timespan}', '{as_of_date}'::date::timestamp) @> program_end_dt::timestamp)
        or
        (program_start_dt < '{as_of_date}'::date and not program_end_dt < '{as_of_date}'::date)
        )
    -- exclude currently homeless individuals
    and pic.client_hash not in (select client_hash from currently_hl)
    and pic.client_hash is not null
    and pic.client_hash not like '%%##%%'
       