-- Cohort: individuals who have had an eviction filing or a disposition in the four months prior to the prediction date
-- Label: whether an individual interacts with homelessness services at least once within [label_timespan] of the prediction date
-- The label and cohort definitions come from the DSSG summer 2022 project
select 
    ecm.client_id as entity_id, bool_or(hir.program_start_dt is not null)::int as outcome 
from pretriage.eviction_client_matches_id ecm left join pretriage.homelessness_id hil 
on ecm.client_id = hil.client_id  
and ( -- Filtering out currently homeless inviduals
    (hil.program_start_dt < '{as_of_date}'::date and hil.program_start_dt >= '{as_of_date}'::date - interval '4 months')
    or 
    (hil.program_end_dt < '{as_of_date}'::date and hil.program_end_dt >= '{as_of_date}'::date - interval '4 months')
    or 
    (program_start_dt < '{as_of_date}'::date and (program_end_dt is null or program_end_dt > '{as_of_date}'::date))
)
    left join pretriage.homelessness_id hir -- joining to get the label
    on ecm.client_id = hir.client_id
    and hir.program_start_dt > '{as_of_date}'::date 
    and hir.program_start_dt < '{as_of_date}'::date + '{label_timespan}'::interval
    left join pretriage.client_feed_dod cfd on ecm.client_id = cfd.client_id and cfd.dod < '{as_of_date}'::date -- joining to get dod
where 
    (
    ((filingdt >= '{as_of_date}'::date - '4 months'::interval and filingdt < '{as_of_date}'::date) and (dispositiondt is null or not(dispositiondt < '{as_of_date}'::date)))
    or
    ((dispositiondt >= '{as_of_date}'::date - '4 months'::interval and dispositiondt < '{as_of_date}'::date) and judgement_for_landlord is true)
)
group by 1 
-- Filtering out currently homeless people and people with a date of death before as_of_date
having not bool_or(hil.program_start_dt is not null) and not bool_or(cfd.dod is not null)