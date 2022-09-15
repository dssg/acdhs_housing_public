set role "acdhs-housing-role";
create table if not exists modelling.entry_into_homelessness_final as
with hl_pop as (
	select *
	from clean.involvement_feed if1
	where program_key = 263
	--limit 10000
),
hl_pop2 as (
	select
	client_hash,
	prog_dt,
	(
		select max(prog_dt)
		from hl_pop as hl_pop3
		where hl_pop3.client_hash = hl_pop.client_hash
		and hl_pop3.prog_dt < hl_pop.prog_dt
	) as last_hl
	from hl_pop
)
select *, (prog_dt - last_hl) as days_since_last_hl
from hl_pop2
;