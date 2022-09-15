select 
	ch2.client_hash, 
	f.*
from {cohort} ch2
left join (
	select
	    ch.as_of_date,
	    count(*) filter (where hl.program_start_dt between ch.as_of_date - interval '1month' and ch.as_of_date) "total_hl_use,month1",
	    count(*) filter (where hl.program_start_dt between ch.as_of_date - interval '2month' and ch.as_of_date - interval '1month') "total_hl_use,month2",
	    count(*) filter (where hl.program_start_dt between ch.as_of_date - interval '3month' and ch.as_of_date - interval '2month') "total_hl_use,month3",
	    count(*) filter (where hl.program_start_dt between ch.as_of_date - interval '4month' and ch.as_of_date - interval '3month') "total_hl_use,month4",
	    count(*) filter (where hl.program_start_dt between ch.as_of_date - interval '5month' and ch.as_of_date - interval '4month') "total_hl_use,month5",
	    count(*) filter (where hl.program_start_dt between ch.as_of_date - interval '6month' and ch.as_of_date - interval '5month') "total_hl_use,month6",
	    count(*) filter (where hl.program_start_dt between ch.as_of_date - interval '7month' and ch.as_of_date - interval '6month') "total_hl_use,month7",
	    count(*) filter (where hl.program_start_dt between ch.as_of_date - interval '8month' and ch.as_of_date - interval '7month') "total_hl_use,month8",
	    count(*) filter (where hl.program_start_dt between ch.as_of_date - interval '9month' and ch.as_of_date - interval '8month') "total_hl_use,month9",
	    count(*) filter (where hl.program_start_dt between ch.as_of_date - interval '10month' and ch.as_of_date - interval '9month') "total_hl_use,month10",
	    count(*) filter (where hl.program_start_dt between ch.as_of_date - interval '11month' and ch.as_of_date - interval '10month') "total_hl_use,month11",
	    count(*) filter (where hl.program_start_dt between ch.as_of_date - interval '12month' and ch.as_of_date - interval '11month') "total_hl_use,month12",
	    count(distinct hl.client_hash) filter (where hl.program_start_dt between ch.as_of_date - interval '1month' and ch.as_of_date) "total_hl_users,month1",
	    count(distinct hl.client_hash) filter (where hl.program_start_dt between ch.as_of_date - interval '2month' and ch.as_of_date - interval '1month') "total_hl_users,month2",
	    count(distinct hl.client_hash) filter (where hl.program_start_dt between ch.as_of_date - interval '3month' and ch.as_of_date - interval '2month') "total_hl_users,month3",
	    count(distinct hl.client_hash)  filter (where hl.program_start_dt between ch.as_of_date - interval '4month' and ch.as_of_date - interval '3month') "total_hl_users,month4",
	    count(distinct hl.client_hash)  filter (where hl.program_start_dt between ch.as_of_date - interval '5month' and ch.as_of_date - interval '4month') "total_hl_users,month5",
	    count(distinct hl.client_hash)  filter (where hl.program_start_dt between ch.as_of_date - interval '6month' and ch.as_of_date - interval '5month') "total_hl_users,month6",
	    count(distinct hl.client_hash)  filter (where hl.program_start_dt between ch.as_of_date - interval '7month' and ch.as_of_date - interval '6month') "total_hl_users,month7",
	    count(distinct hl.client_hash)  filter (where hl.program_start_dt between ch.as_of_date - interval '8month' and ch.as_of_date - interval '7month') "total_hl_users,month8",
	    count(distinct hl.client_hash)  filter (where hl.program_start_dt between ch.as_of_date - interval '9month' and ch.as_of_date - interval '8month') "total_hl_users,month9",
	    count(distinct hl.client_hash)  filter (where hl.program_start_dt between ch.as_of_date - interval '10month' and ch.as_of_date - interval '9month') "total_hl_users,month10",
	    count(distinct hl.client_hash)  filter (where hl.program_start_dt between ch.as_of_date - interval '11month' and ch.as_of_date - interval '10month') "total_hl_users,month11",
	    count(distinct hl.client_hash)  filter (where hl.program_start_dt between ch.as_of_date - interval '12month' and ch.as_of_date - interval '11month') "total_hl_users,month12"
    from (select distinct as_of_date from {cohort}) ch
    left join {schema_name}.hl_table hl on hl.program_start_dt < ch.as_of_date 
    group by ch.as_of_date
    ) f 
using(as_of_date)

