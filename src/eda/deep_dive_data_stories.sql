-- This files consists of queries to look through all the data we have on one specific ACDHS client.


---------------------------------
-- DATA STORIES -----------------
---------------------------------
-- data story 1 -----------------
-- return homeless, been evicted
-- client_hash: A929126982
---------------------------------
-- data story 2 -----------------
-- rental assistance and not homeless
-- client_hash: 020EEB6462
---------------------------------


---------------------------------
-- BACKUP data stories ----------
---------------------------------
-- data story backup 1 --
-- old client_hash: 5C4C43E755
---------------------------------
-- data story backup 2 ----------
-- client_hash: AFDEF7D859
---------------------------------



-- client feed info
select * from clean.clients c 
where client_hash = :client;

select * from clean.client_feed cf 
where client_hash = :client;


-- eviction matters per client
select * from clean.eviction_client_matches ecm 
where hashed_mci_uniq_id = :client;

-- information on the evictions
select * from clean.eviction e 
inner join (
	select * from clean.eviction_client_matches ecm 
	where hashed_mci_uniq_id = :client
) as temp using(matter_id);

-- get all the information on all the eviction matters this client has been involved in
select * from clean.eviction_landlords el
where matter_id in (
	select distinct matter_id
	from clean.eviction_client_matches ecm 
	where hashed_mci_uniq_id = :client
);

-- info on homelessness programs
select * from clean.involvement_feed if2
left join lookup.program_feed pf using(program_key)
where client_hash = :client
and program_key in (263, 29, 32, 33, 114);

-- info on rental assistance
select * from clean.involvement_feed if2
left join lookup.program_feed pf using(program_key)
where client_hash = :client
and program_key in (297, 262, 296, 298, 30);


-- nr of months per program
with xx as (
	select program_key, count(*) as nr_of_months_in_program from clean.involvement_feed if2
	where client_hash = :client
	group by program_key)
select *
from xx
left join lookup.program_feed pf using(program_key);

-- info on public housing
select * from clean.s8_activity sa 
where hashed_mci_uniq_id = :client;

select * from clean.s8_action sa  
inner join (
	select * from clean.s8_activity sa2 
	where hashed_mci_uniq_id = :client
) as temp2 using(hashed_household_id);

select * from clean.res_activity ra  
where hashed_mci_uniq_id = :client;

select * from clean.res_action ra 
inner join (
	select * from clean.res_activity ra2  
	where hashed_mci_uniq_id = :client
) as temp2 using(hashed_household_id);


-- get aging data
select * from raw.cmu_aging_prm
where mci_uniq_id = :client;

-- get allegation data
select * from raw.cmu_allegation_prm
where mci_uniq_id = :client;

-- get behavior_health data
select * from raw.cmu_behavior_health_prm
where mci_uniq_id = :client;

-- get cyf_case data
select * from raw.cmu_cyf_case_prm
where mci_uniq_id = :client;

-- get cyf_referral data
select * from raw.cmu_cyf_referral_prm
where mci_uniq_id = :client;

-- get findings data
select * from raw.cmu_findings_prm
where child_mci_uniq_id = :client
or perp_mci_uniq_id = :client;

-- get hmis_current data
select * from raw.cmu_hmis_current_prm
where mci_uniq_id = :client;

-- get hmis_old data
select * from raw.cmu_hmis_old_prm
where mci_uniq_id = :client;

-- get mh data
select * from raw.cmu_mh_prm
where mci_uniq_id = :client;

-- get outpatient_health data
select * from raw.cmu_outpatient_health_prm
where mci_uniq_id = :client;

-- get physical_health data
select * from raw.cmu_physical_health_prm
where mci_uniq_id = :client;

-- get placement data
select * from raw.cmu_placement_prm
where mci_uniq_id = :client;

-- get removal data
select * from raw.cmu_removal_prm
where mci_uniq_id = :client;
