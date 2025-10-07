-- This takes the same evictions cohort we considered in the summer and builds repeat homelessness risk prediction mdoels
-- i.e., we exclude people without homelessness history from the eviction cohort
select 
    ecm.client_id as entity_id, bool_or(hir.program_start_dt is not null)::int as outcome 
from pretriage.eviction_client_matches_id ecm left join pretriage.homelessness_id hil 
on ecm.client_id = hil.client_id 
and hil.program_start_dt < '{as_of_date}'::date
-- if the homelessness spell overlaps with our eviction lookback period (4 months), we consider them currently homeless and exclude from our cohort
and not ((hil.program_start_dt, hil.program_end_dt) overlaps ('{as_of_date}'::date - '4 months'::interval, '{as_of_date}'::date)) 
-- making sure the hl span doesn't overlap the as_of_date
and not (hil.program_end_dt > '{as_of_date}'::date or hil.program_end_dt is null)
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
having bool_or(hil.program_start_dt is not null)