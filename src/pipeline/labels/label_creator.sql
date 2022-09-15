select
    distinct cohort.client_hash,
    cohort.as_of_date,
    case when count(hl.client_hash) > 0 then 1 else 0
	end homelessness_label
from 
    {schema_name}.{cohort_table} as cohort
    left join {schema_name}.hl_table hl
        on cohort.client_hash = hl.client_hash
        and hl.program_start_dt between cohort.as_of_date and (cohort.as_of_date + '{label_timespan}'::interval)
group by cohort.client_hash, cohort.as_of_date 


