select 
	ch2.client_hash, 
	f.*
from {cohort} ch2
left join (
	select
	    ch.as_of_date,
	    count(*) filter (where ev.filingdt between ch.as_of_date - interval '1month' and ch.as_of_date) "total_filings,month1",
	    count(*) filter (where ev.filingdt between ch.as_of_date - interval '2month' and ch.as_of_date - interval '1month') "total_filings,month2",
	    count(*) filter (where ev.filingdt between ch.as_of_date - interval '3month' and ch.as_of_date - interval '2month') "total_filings,month3",
	    count(*) filter (where ev.filingdt between ch.as_of_date - interval '4month' and ch.as_of_date - interval '3month') "total_filings,month4",
	    count(*) filter (where ev.filingdt between ch.as_of_date - interval '5month' and ch.as_of_date - interval '4month') "total_filings,month5",
	    count(*) filter (where ev.filingdt between ch.as_of_date - interval '6month' and ch.as_of_date - interval '5month') "total_filings,month6",
	    count(*) filter (where ev.filingdt between ch.as_of_date - interval '7month' and ch.as_of_date - interval '6month') "total_filings,month7",
	    count(*) filter (where ev.filingdt between ch.as_of_date - interval '8month' and ch.as_of_date - interval '7month') "total_filings,month8",
	    count(*) filter (where ev.filingdt between ch.as_of_date - interval '9month' and ch.as_of_date - interval '8month') "total_filings,month9",
	    count(*) filter (where ev.filingdt between ch.as_of_date - interval '10month' and ch.as_of_date - interval '9month') "total_filings,month10",
	    count(*) filter (where ev.filingdt between ch.as_of_date - interval '11month' and ch.as_of_date - interval '10month') "total_filings,month11",
	    count(*) filter (where ev.filingdt between ch.as_of_date - interval '12month' and ch.as_of_date - interval '11month') "total_filings,month12"
    from (select distinct as_of_date from  {cohort}) ch
    left join (
		select distinct on (matter_id) * 
		from clean.eviction e 
		left join clean.eviction_client_matches ecm 
		using(matter_id) 
	) ev on ev.filingdt < ch.as_of_date 
    group by ch.as_of_date
    ) f 
using(as_of_date)
