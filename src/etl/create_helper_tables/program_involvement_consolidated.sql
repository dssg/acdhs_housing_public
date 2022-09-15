-- generates a table with a lag program participation date and a row number
with if_lag_table as (
    select
        client_hash,
        row_number() over(partition by client_hash, program_key order by prog_dt) as rn,
        program_key::varchar as project_type,
        prog_dt,
        lag(prog_dt,1) over (partition by client_hash, program_key order by prog_dt) as lag_prog_dt
    from
        clean.involvement_feed if2
    order by client_hash, program_key, prog_dt
),
-- generates a table with a unique identifier for each program participation
program_participation_id_table as (
    select
        *,
        -- generates a unique id for each new program participation (i.e. a participations in programs that are not consecutive)
        sum(case when (if_lag_table.lag_prog_dt + '1 month'::interval) < prog_dt then 1 else 0 end) 
        over (partition by client_hash, project_type order by if_lag_table.rn) as program_participation_id
    from if_lag_table
),
-- this generates a table that includes the start date and end date of each new program participation
if_consolidated as (
    select
    	client_hash,
    	project_type,
    	program_participation_id + 1 as program_participation_id, -- this ensures that the participation id starts from 1
    	min(prog_dt) as program_start_dt,
        (max(prog_dt) + '{if_imputed_duration}'::interval - '1 day'::interval)::date as program_end_dt, -- this imputes some days (e.g. 1 month) to end date in absence of overlap. 
        'feed' as data_type
    from program_participation_id_table
    group by client_hash, project_type, program_participation_id
),
-- this selects the relevant information from the HMIS data
if_hmis as (
select 
    	mci_uniq_id as client_hash,
    	project_type_id::varchar as project_type,
    	row_number() over(partition by mci_uniq_id order by program_start_dt) as program_participation_id,
    	program_start_dt, 
    	case when exit_dt is null then program_start_dt else exit_dt end program_end_dt,
    	'hmis' as data_type
    from clean.cmu_hmis_current_prm chcp
    group by client_hash, project_type_id, program_start_dt, program_end_dt
),
-- this selects the waiting list information from HMIS details 
-- if move in time differs from time of enrollment, then end date is time of move in 
-- otherwise program participation lasts 1 day
if_hmis_ph as (
    select 
	    hashed_mci_uniq_id as client_hash,
	    hud_project_type_id::varchar as project_type,
		row_number() over(partition by hashed_mci_uniq_id order by enrollment_start_dt) as program_participation_id,
	    enrollment_start_dt as program_start_dt, 
	    (case when (enrollment_end_dt is not null) then enrollment_end_dt when (enrollment_end_dt is null and move_in_dt is not null) then move_in_dt else enrollment_start_dt end) as program_end_dt,
	    'hmis_ph_waiting_list' as data_type
    from clean.hmis_details hd
    group by hashed_mci_uniq_id, hud_project_type_id, enrollment_start_dt, enrollment_end_dt, move_in_dt
),
-- this combines all datasets
if_all as (
		(select * 
		from if_consolidated)
	union all 
		(select *
		from if_hmis)
	union all 
		(select * 
		from if_hmis_ph)
	order by client_hash, project_type, program_start_dt
	)
-- this selects the relevant rows only by data type
select 
    client_hash,
    project_type,
    program_start_dt,
    program_end_dt,
    data_type
from if_all
where data_type in {data_type}
