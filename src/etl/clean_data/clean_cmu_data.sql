-- this file contains sql code to dump the eviction data into the database
-- run file with: psql -f src/etl/db_clean_cmu_data.sql

-- statements created with, e.g.: head -n 1000 /mnt/data/projects/acdhs-housing/data/20220623/CMU_AGING_PRM_20220623.csv | tr [:upper:] [:lower:] | tr ' ' '_' | sed 's/#/num/' | csvsql -i postgresql --db-schema raw --tables cmu_aging_prm

set role "acdhs-housing-role";
create schema if not exists clean;

-- drop existing raw tables if they exist already
drop table if exists clean.cmu_aging_prm;
drop table if exists clean.cmu_allegation_prm;
drop table if exists clean.cmu_behavior_health_prm;
drop table if exists clean.cmu_cyf_case_prm;
drop table if exists clean.cmu_cyf_referral_prm;
drop table if exists clean.cmu_findings_prm;
drop table if exists clean.cmu_hmis_current_prm;
drop table if exists clean.cmu_hmis_old_prm;
drop table if exists clean.cmu_mh_prm;
drop table if exists clean.cmu_outpatient_health_prm;
drop table if exists clean.cmu_physical_health_prm;
drop table if exists clean.cmu_placement_prm;
drop table if exists clean.cmu_removal_prm;

-- aging program

create table clean.cmu_aging_prm as (
    select
    mci_uniq_id::VARCHAR, 
    session_date::DATE, 
    ps_worker_ron_receipt_date::DATE, 
    emot_mental_conditions::VARCHAR, 
    life_threatening::VARCHAR, 
    ref_to_other_entity::VARCHAR, 
    intake_worker_ron_compdate::DATE, 
    age_range_if_no_dob::VARCHAR, 
    sub_abuse::VARCHAR, 
    ron_receipt_date::DATE, 
    mandated_report::VARCHAR, 
    mr_type::VARCHAR, 
    disabilities::VARCHAR, 
    ps_agency_remind_date::VARCHAR, 
    conditions_desc_in_records::VARCHAR, 
    mr_advised_reporting::VARCHAR, 
    res_county::VARCHAR, 
    comm_assist_type::VARCHAR, 
    ron_receipt_time::VARCHAR, 
    primary_language::VARCHAR, 
    ron_cat_at_intake::VARCHAR, 
    does_legal_or_financial_help::VARCHAR, 
    phys_health_reported::VARCHAR, 
    ps_receipt_mr_report::VARCHAR, 
    allegations_by_reporter::VARCHAR, 
    res_muni::VARCHAR, 
    sup_approv_ron_date::DATE, 
    vr_type::VARCHAR, 
    facility::VARCHAR, 
    residence_when_reported::VARCHAR, 
    mr_org::VARCHAR, 
    ps_worker_ron_receipt_time::VARCHAR, 
    ron_cat_if_changed::VARCHAR, 
    note_dangers::VARCHAR, 
    ron_cat_changed_or_confirmed::VARCHAR, 
    has_legal_or_financial_help::VARCHAR, 
    why_no_need_cat::VARCHAR, 
    curr_living_arr::VARCHAR, 
    incident_county::VARCHAR, 
    orgs_id_after_ron_reviewed::VARCHAR, 
    abuse_type_reported::VARCHAR, 
    ps_forward_to_state::BOOLEAN, 
    phys_env_prob::VARCHAR, 
    perp_has_access::VARCHAR, 
    director_approv_ron_date::VARCHAR, 
    incident_date::VARCHAR, 
    fin_prob_reported::VARCHAR, 
    ps_agency_remind_time::VARCHAR, 
    curr_location::VARCHAR, 
    ron_rcptdate_cw_or_invest::DATE, 
    agency_uuid::BOOLEAN, 
    agency::VARCHAR, 
    provider::VARCHAR, 
    subprovider::VARCHAR, 
    create_datetime::VARCHAR, 
    update_datetime::VARCHAR, 
    creation_dt::DATE
    from raw.cmu_aging_prm
);

create index on clean.cmu_aging_prm(mci_uniq_id);
create index on clean.cmu_aging_prm(session_date);

-- allegation program

create table clean.cmu_allegation_prm as (
    select
	refer_id::VARCHAR, 
	mci_uniq_id::VARCHAR, 
	alleg_seq_nbr::SMALLINT, 
	allegation_id::VARCHAR, 
	abus_nglct_typ_cde::INTEGER, 
	abus_high_lvl_name::VARCHAR, 
	cps_flag::BOOLEAN, 
	gps_flag::BOOLEAN, 
	creation_dt::DATE
    from raw.cmu_allegation_prm
);

create index on clean.cmu_allegation_prm(mci_uniq_id);
create index on clean.cmu_allegation_prm(creation_dt);

-- behavioral health program

create table clean.cmu_behavior_health_prm as (
    select
	mci_uniq_id::VARCHAR, 
	event_type::VARCHAR, 
	event_sequence::SMALLINT, 
	event_beg_date::DATE, 
	event_end_date::DATE, 
	event_duration::SMALLINT, 
	event_active_days::SMALLINT, 
	src_load_dt::DATE, 
	creation_dt::DATE, 
	prvdr_cd::VARCHAR, 
	diagnosis_code::VARCHAR, 
	diagnosis_sub_category::VARCHAR
    from raw.cmu_behavior_health_prm
);

create index on clean.cmu_behavior_health_prm(mci_uniq_id);
create index on clean.cmu_behavior_health_prm(event_beg_date);
create index on clean.cmu_behavior_health_prm(event_end_date);
create index on clean.cmu_behavior_health_prm(diagnosis_code);

-- CYF case program

create table clean.cmu_cyf_case_prm as (
    select
	mci_uniq_id::VARCHAR, 
	case_id::VARCHAR, 
	type::VARCHAR, 
	sequence::SMALLINT, 
	start_date::DATE, 
	end_date::DATE, 
	duration::SMALLINT, 
	days_to_next_reentry::SMALLINT, 
	days_from_last_exit::SMALLINT, 
	first_start_date::VARCHAR, 
	days_since_first_start_date::SMALLINT, 
	days_since_last_end_date::SMALLINT, 
	age_at_first_start_date::SMALLINT, 
	had_prior::BOOLEAN, 
	data_as_of_date::DATE, 
	load_dt::DATE
    from raw.cmu_cyf_case_prm
);

create index on clean.cmu_cyf_case_prm(case_id);
create index on clean.cmu_cyf_case_prm(start_date);
create index on clean.cmu_cyf_case_prm(end_date);

-- CYF referral program

create table clean.cmu_cyf_referral_prm as (
    select
	refer_id::VARCHAR, 
	intake_dt::DATE, 
	intake_time::VARCHAR, 
	mci_uniq_id::VARCHAR, 
	pri_ref_role::VARCHAR, 
	in_househld_cd::BOOLEAN, 
	call_scrn_outcome::VARCHAR, 
	service_decision::VARCHAR, 
	service_decision_dt::DATE, 
	creation_dt::DATE
    from raw.cmu_cyf_referral_prm
);

create index on clean.cmu_cyf_referral_prm(mci_uniq_id);
create index on clean.cmu_cyf_referral_prm(intake_dt);

-- findings program

create table clean.cmu_findings_prm as (
    select
	refer_id::VARCHAR, 
	refer_dt::DATE, 
	allegation_id::VARCHAR, 
	alleg_seq_nbr::SMALLINT, 
	child_mci_uniq_id::VARCHAR, 
	perp_mci_uniq_id::VARCHAR, 
	finding_long::VARCHAR, 
	finding_short::VARCHAR, 
	creation_dt::DATE
    from raw.cmu_findings_prm
);

create index on clean.cmu_findings_prm(child_mci_uniq_id);
create index on clean.cmu_findings_prm(perp_mci_uniq_id);
create index on clean.cmu_findings_prm(refer_dt);
create index on clean.cmu_findings_prm(finding_long);

-- HMIS current program

create table clean.cmu_hmis_current_prm as (
    select
	assessment_id::VARCHAR, 
	project_type_id::VARCHAR, 
	hmis_id::VARCHAR, 
	mci_uniq_id::VARCHAR, 
	program_start_dt::DATE, 
	exit_dt::DATE, 
	assessment_date::DATE, 
	project_type::VARCHAR, 
	src_load_dt::DATE, 
	creation_dt::DATE
    from raw.cmu_hmis_current_prm
);

create index on clean.cmu_hmis_current_prm(mci_uniq_id);
create index on clean.cmu_hmis_current_prm(program_start_dt);
create index on clean.cmu_hmis_current_prm(exit_dt);

-- HMIS old program

create table clean.cmu_hmis_old_prm as (
    select
	assessment_id::VARCHAR, 
	mci_uniq_id::VARCHAR, 
	program_start_dt::DATE, 
	program_end_dt::DATE, 
	assessment_dt::DATE, 
	svc_name::VARCHAR, 
	data_load_dt::DATE
    from raw.cmu_hmis_old_prm
);

create index on clean.cmu_hmis_old_prm(mci_uniq_id);
create index on clean.cmu_hmis_old_prm(program_start_dt);
create index on clean.cmu_hmis_old_prm(program_end_dt);

-- mental health program

create table clean.cmu_mh_prm as (
    select
	mci_uniq_id::VARCHAR, 
	event_type::VARCHAR, 
	event_sequence::SMALLINT, 
	event_start_date::DATE, 
	event_end_date::DATE, 
	event_duration::SMALLINT, 
	event_active_days::SMALLINT, 
	event_days_to_reentry::SMALLINT, 
	event_days_from_prev_reentry::SMALLINT, 
	first_event_date::DATE, 
	days_since_first_event::SMALLINT, 
	days_since_last_event::SMALLINT, 
	age_at_first_event::DECIMAL, 
	event_los::BOOLEAN, 
	current_event::BOOLEAN, 
	had_prior::BOOLEAN, 
	data_as_of_date::DATE, 
	view_build_date::DATE, 
	expunged::BOOLEAN, 
	prvdr_cd::VARCHAR, 
	almh_ind::BOOLEAN, 
	ccbho_ind::BOOLEAN, 
	da_diagnosis_flag::BOOLEAN, 
	mh_diagnosis_flag::BOOLEAN, 
	smi_dx::BOOLEAN, 
	service::VARCHAR, 
	service_count::SMALLINT
    from raw.cmu_mh_prm
);

create index on clean.cmu_mh_prm(mci_uniq_id);
create index on clean.cmu_mh_prm(event_type);
create index on clean.cmu_mh_prm(event_start_date);
create index on clean.cmu_mh_prm(event_end_date);

-- outpatient health program

create table clean.cmu_outpatient_health_prm as (
    select
	mci_uniq_id::VARCHAR, 
	svc_start_dt::DATE, 
	svc_end_dt::DATE, 
	service_category::VARCHAR
    from raw.cmu_outpatient_health_prm
);

create index on clean.cmu_outpatient_health_prm(mci_uniq_id);
create index on clean.cmu_outpatient_health_prm(svc_start_dt);
create index on clean.cmu_outpatient_health_prm(svc_end_dt);

-- physical health program

create table clean.cmu_physical_health_prm as (
    select
	source::VARCHAR, 
	claim_nbr::INTEGER, 
	mci_uniq_id::VARCHAR, 
	svc_start_dt::DATE, 
	svc_end_dt::DATE, 
	svc_cat_grp_nbr::SMALLINT, 
	scu_cd::VARCHAR, 
	source_load_date::DATE, 
	creation_dt::DATE
    from (
        select * from raw.cmu_physical_health_prm_1
        union all
        select * from raw.cmu_physical_health_prm_2
        union all
        select * from raw.cmu_physical_health_prm_3
        union all
        select * from raw.cmu_physical_health_prm_4
        union all
        select * from raw.cmu_physical_health_prm_5
        union all
        select * from raw.cmu_physical_health_prm_6
        union all
        select * from raw.cmu_physical_health_prm_7
        union all
        select * from raw.cmu_physical_health_prm_8
        union all
        select * from raw.cmu_physical_health_prm_9
        union all
        select * from raw.cmu_physical_health_prm_10
        union all
        select * from raw.cmu_physical_health_prm_11
        union all
        select * from raw.cmu_physical_health_prm_12
        union all
        select * from raw.cmu_physical_health_prm_13
        union all
        select * from raw.cmu_physical_health_prm_14
        union all
        select * from raw.cmu_physical_health_prm_15
        union all
        select * from raw.cmu_physical_health_prm_16
    ) as cmu_physical_health_combined
);

create index on clean.cmu_physical_health_prm(mci_uniq_id);
create index on clean.cmu_physical_health_prm(svc_start_dt);
create index on clean.cmu_physical_health_prm(svc_end_dt);

-- placement program

create table clean.cmu_placement_prm as (
    select
	plcmnt_seq_nbr::INTEGER, 
	plcmnt_entry_date::DATE, 
	plcmnt_end_date::DATE, 
	mci_uniq_id::VARCHAR, 
	srvc_key::SMALLINT, 
	srvc_group_1::VARCHAR, 
	srvc_group_2::VARCHAR, 
	plcmnt_type_desc::VARCHAR, 
	source_load_date::DATE, 
	create_ts::VARCHAR
    from raw.cmu_placement_prm
);

create index on clean.cmu_placement_prm(mci_uniq_id);
create index on clean.cmu_placement_prm(plcmnt_entry_date);
create index on clean.cmu_placement_prm(plcmnt_end_date);
create index on clean.cmu_placement_prm(srvc_key);

-- removal program

create table clean.cmu_removal_prm as (
    select
	mci_uniq_id::VARCHAR, 
	rmvl_type_amend::VARCHAR, 
	rmvl_seq_nbr::SMALLINT, 
	rmvl_entry_dt::DATE, 
	rmvl_entry_age::SMALLINT, 
	rmvl_entry_mo::SMALLINT, 
	rmvl_entry_yr::SMALLINT, 
	rmvl_exit_dt::DATE, 
	rmvl_rtrn_rsn::VARCHAR, 
	rmvl_rtrn_cde::VARCHAR, 
	perm_status::SMALLINT, 
	rmvl_exit_age::SMALLINT, 
	rmvl_exit_mo::SMALLINT, 
	rmvl_exit_yr::SMALLINT, 
	rmvl_in_care::BOOLEAN, 
	rmvl_los_days::SMALLINT, 
	cyf_rmvl_reentry_time::SMALLINT, 
	rem_prnt_bh::BOOLEAN, 
	rem_prnt_chld_conf::BOOLEAN, 
	rem_prnt_cope::BOOLEAN, 
	rem_prnt_skls::BOOLEAN, 
	rem_relinq::BOOLEAN, 
	rem_resump::BOOLEAN, 
	rem_sex_abus::BOOLEAN, 
	rem_truancy::BOOLEAN, 
	rem_unk::BOOLEAN, 
	view_build_date::DATE, 
	create_date::DATE
    from raw.cmu_removal_prm
);

create index on clean.cmu_removal_prm(mci_uniq_id);
create index on clean.cmu_removal_prm(rmvl_entry_dt);
create index on clean.cmu_removal_prm(rmvl_exit_dt);
create index on clean.cmu_removal_prm(rmvl_rtrn_cde);