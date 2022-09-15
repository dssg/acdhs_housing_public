
select 
client_hash, category, elig_begin_date, elig_end_date, count(distinct eligibility_hash) as nr_of_unique_eligibilities
from clean.eligibility e 
left join clean.clients using(padhs_client_hash)
group by 1, 2, 3, 4