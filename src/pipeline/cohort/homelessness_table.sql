-- generates a table with a lag homeless date and a row number
with hl_lag_table as (
    select
        client_hash,
        row_number() over(partition by client_hash order by prog_dt) as rn,
        prog_dt,
        lag(prog_dt,1) over (partition by client_hash order by prog_dt) as lag_prog_dt
    from
        clean.involvement_feed if2
    where program_key in {program_keys}
    order by client_hash, prog_dt
),
-- generates a table with a unique identifier for each homelessness spell
hl_id_table as (
    select
        *,
        -- generates a unique id for each new homelessness spell (i.e. a homelessness spell that isn't consecutive)
        sum(case when (hl_lag_table.lag_prog_dt + '1 month'::interval) < prog_dt then 1 else 0 end) 
        over (partition by client_hash order by hl_lag_table.rn) as hl_spell_id
    from hl_lag_table
),
-- this generates a table that includes the start date and end date of each new homelessness spell
hl_feed as (
    select
    	client_hash,
    	hl_spell_id + 1 as hl_spell_id, -- this ensures that the spell id starts from 1
    	min(prog_dt) as program_start_dt,
        (max(prog_dt) + '{hl_imputed_duration}'::interval - '1 day'::interval)::date as program_end_dt, -- this imputes some days (e.g. 1 month) to end date in absence of overlap. 
        'feed' as data_type
    from hl_id_table
    group by client_hash, hl_spell_id
),

-- this selects the relevant information from the HMIS data
hl_hmis as (
    select 
    	mci_uniq_id as client_hash,
    	row_number() over(partition by mci_uniq_id order by program_start_dt) as hl_spell_id,
    	program_start_dt, 
    	exit_dt as program_end_dt,
    	'hmis' as data_type
    from clean.cmu_hmis_current_prm chcp 
    where project_type in {hmis_programs}
    group by client_hash, program_start_dt, program_end_dt
),
-- this selects the information from HMIS details 
-- defines all those who are enrolled in permanent housing as homeless at time of enrollment
-- if move in time differs from time of enrollment, then homeless end date is time of move in 
-- otherwise homeless spell lasts 1 day
hl_hmis_ph as (
    select 
    hashed_mci_uniq_id as client_hash, 
	row_number() over(partition by hashed_mci_uniq_id order by enrollment_start_dt) as hl_spell_id,
    enrollment_start_dt as program_start_dt, 
    (case when move_in_dt is not null then move_in_dt else enrollment_start_dt end) as program_end_dt,
    'hmis_ph' as data_type
    from clean.hmis_details hd 
    where hud_project_type_desc in {hmis_ph_programs}
    group by hashed_mci_uniq_id, enrollment_start_dt, move_in_dt 
),
-- this combines all datasets
hl_all as (
		(select * 
		from hl_feed)
	union all 
		(select *
		from hl_hmis)
	union all 
		(select * 
		from hl_hmis_ph)
	order by client_hash, program_start_dt
	)
-- this selects the relevant rows only by data typw
select 
    client_hash, 
    program_start_dt, 
    program_end_dt, 
    data_type 
from hl_all
where data_type in {data_type}