-- This takes the same evictions cohort we considered in the summer and builds a first time homelessness risk prediction mdoels
-- i.e., we exclude people with any homelessness history from the eviction cohort
select 
    ecm.client_id as entity_id, bool_or(hir.program_start_dt is not null)::int as outcome 
from pretriage.eviction_client_matches_id ecm left join pretriage.homelessness_id hil 
on ecm.client_id = hil.client_id 
and hil.program_start_dt < '{as_of_date}'::date
    left join pretriage.homelessness_id hir -- joining to get the label
    on ecm.client_id = hir.client_id
    and hir.program_start_dt > '{as_of_date}'::date 
    and hir.program_start_dt < '{as_of_date}'::date + '{label_timespan}'::interval
where (
    ((filingdt >= '{as_of_date}'::date - '4 months'::interval and filingdt < '{as_of_date}'::date) and (dispositiondt is null or not(dispositiondt < '{as_of_date}'::date)))
    or
    ((dispositiondt >= '{as_of_date}'::date - '4 months'::interval and dispositiondt < '{as_of_date}'::date) and judgement_for_landlord is true)
)
group by 1 
having not bool_or(hil.program_start_dt is not null)