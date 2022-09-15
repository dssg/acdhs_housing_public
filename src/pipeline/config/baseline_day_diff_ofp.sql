with evictions as (
	select 
		matter_id,
		hashed_mci_uniq_id as client_hash,
		filingdt, 
		ofp_issue_dt
	from clean.eviction_client_matches ecm
	left join clean.eviction using(matter_id)
	group by 1, 2, 3, 4
	order by matter_id
)
select distinct on (ch.client_hash, ch.as_of_date)
	ch.client_hash,
	ch.as_of_date,
	case
		when (
				(e.ofp_issue_dt is not null)
				and
				(DATE_PART('day', ch.as_of_date::date::timestamp - e.ofp_issue_dt::timestamp)::int >= 0)
			) then DATE_PART('day', ch.as_of_date::date::timestamp - e.ofp_issue_dt::timestamp)::int
			else 99999
		end day_diff_ofp,
	case
		when (
				(e.ofp_issue_dt is not null)
				and
				(DATE_PART('day', ch.as_of_date::date::timestamp - e.ofp_issue_dt::timestamp)::int >= 0)
			) then 0
			else 1
		end day_diff_ofp_imputation_flag
from {cohort} as ch
left join evictions e
	on ch.client_hash = e.client_hash
	and e.filingdt < ch.as_of_date
order by ch.client_hash, ch.as_of_date, filingdt desc