-- this file contains sql code to dump the eviction data into the database
-- run file with: psql -f src/etl/db_dump_cmu_data.sql

-- statements created with, e.g.: head -n 1000 /mnt/data/projects/acdhs-housing/data/20220623/CMU_AGING_PRM_20220623.csv | tr [:upper:] [:lower:] | tr ' ' '_' | sed 's/#/num/' | csvsql -i postgresql --db-schema raw --tables cmu_aging_prm

set role "acdhs-housing-role";
create schema if not exists raw;

-- drop existing raw tables if they exist already
drop table if exists raw.cmu_aging_prm;
drop table if exists raw.cmu_allegation_prm;
drop table if exists raw.cmu_behavior_health_prm;
drop table if exists raw.cmu_cyf_case_prm;
drop table if exists raw.cmu_cyf_referral_prm;
drop table if exists raw.cmu_findings_prm;
drop table if exists raw.cmu_hmis_current_prm;
drop table if exists raw.cmu_hmis_old_prm;
drop table if exists raw.cmu_mh_prm;
drop table if exists raw.cmu_outpatient_health_prm;
drop table if exists raw.cmu_physical_health_prm_1;
drop table if exists raw.cmu_physical_health_prm_2;
drop table if exists raw.cmu_physical_health_prm_3;
drop table if exists raw.cmu_physical_health_prm_4;
drop table if exists raw.cmu_physical_health_prm_5;
drop table if exists raw.cmu_physical_health_prm_6;
drop table if exists raw.cmu_physical_health_prm_7;
drop table if exists raw.cmu_physical_health_prm_8;
drop table if exists raw.cmu_physical_health_prm_9;
drop table if exists raw.cmu_physical_health_prm_10;
drop table if exists raw.cmu_physical_health_prm_11;
drop table if exists raw.cmu_physical_health_prm_12;
drop table if exists raw.cmu_physical_health_prm_13;
drop table if exists raw.cmu_physical_health_prm_14;
drop table if exists raw.cmu_physical_health_prm_15;
drop table if exists raw.cmu_physical_health_prm_16;
drop table if exists raw.cmu_placement_prm;
drop table if exists raw.cmu_removal_prm;

-- aging program

create table raw.cmu_aging_prm (
        mci_uniq_id VARCHAR, 
        session_date VARCHAR, 
        ps_worker_ron_receipt_date VARCHAR, 
        emot_mental_conditions VARCHAR, 
        life_threatening VARCHAR, 
        ref_to_other_entity VARCHAR, 
        intake_worker_ron_compdate VARCHAR, 
        age_range_if_no_dob VARCHAR, 
        sub_abuse VARCHAR, 
        ron_receipt_date VARCHAR, 
        mandated_report VARCHAR, 
        mr_type VARCHAR, 
        disabilities VARCHAR, 
        ps_agency_remind_date VARCHAR, 
        conditions_desc_in_records VARCHAR, 
        mr_advised_reporting VARCHAR, 
        res_county VARCHAR, 
        comm_assist_type VARCHAR, 
        ron_receipt_time VARCHAR, 
        primary_language VARCHAR, 
        ron_cat_at_intake VARCHAR, 
        does_legal_or_financial_help VARCHAR, 
        phys_health_reported VARCHAR, 
        ps_receipt_mr_report VARCHAR, 
        allegations_by_reporter VARCHAR, 
        res_muni VARCHAR, 
        sup_approv_ron_date VARCHAR, 
        vr_type VARCHAR, 
        facility VARCHAR, 
        residence_when_reported VARCHAR, 
        mr_org VARCHAR, 
        ps_worker_ron_receipt_time VARCHAR, 
        ron_cat_if_changed VARCHAR, 
        note_dangers VARCHAR, 
        ron_cat_changed_or_confirmed VARCHAR, 
        has_legal_or_financial_help VARCHAR, 
        why_no_need_cat VARCHAR, 
        curr_living_arr VARCHAR, 
        incident_county VARCHAR, 
        orgs_id_after_ron_reviewed VARCHAR, 
        abuse_type_reported VARCHAR, 
        ps_forward_to_state VARCHAR, 
        phys_env_prob VARCHAR, 
        perp_has_access VARCHAR, 
        director_approv_ron_date VARCHAR, 
        incident_date VARCHAR, 
        fin_prob_reported VARCHAR, 
        ps_agency_remind_time VARCHAR, 
        curr_location VARCHAR, 
        ron_rcptdate_cw_or_invest VARCHAR, 
        agency_uuid VARCHAR, 
        agency VARCHAR, 
        provider VARCHAR, 
        subprovider VARCHAR, 
        create_datetime VARCHAR, 
        update_datetime VARCHAR, 
        creation_dt VARCHAR
);

\COPY raw.cmu_aging_prm from program 'iconv -f LATIN1 -t UTF-8//TRANSLIT /mnt/data/projects/acdhs-housing/data/20220623/CMU_AGING_PRM_20220623.csv | sed -E -e ''s/[^[:print:]]/-/g; /^\s*$/d; s/3"15 PM/3:15 PM/''' with CSV HEADER;

-- allegation program

create table raw.cmu_allegation_prm (
	refer_id VARCHAR, 
	mci_uniq_id VARCHAR, 
	alleg_seq_nbr VARCHAR, 
	allegation_id VARCHAR, 
	abus_nglct_typ_cde VARCHAR, 
	abus_high_lvl_name VARCHAR, 
	cps_flag VARCHAR, 
	gps_flag VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_allegation_prm from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220623/CMU_ALLEGATION_PRM_20220623.csv' with CSV HEADER;

-- behavioral health program

create table raw.cmu_behavior_health_prm (
	mci_uniq_id VARCHAR, 
	event_type VARCHAR, 
	event_sequence VARCHAR, 
	event_beg_date VARCHAR, 
	event_end_date VARCHAR, 
	event_duration VARCHAR, 
	event_active_days VARCHAR, 
	src_load_dt VARCHAR, 
	creation_dt VARCHAR, 
	prvdr_cd VARCHAR, 
	diagnosis_code VARCHAR, 
	diagnosis_sub_category VARCHAR
);

\COPY raw.cmu_behavior_health_prm from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220623/CMU_BEHAVIOR_HEALTH_PRM_20220623.csv' with CSV HEADER;

-- CYF case program

create table raw.cmu_cyf_case_prm (
	mci_uniq_id VARCHAR, 
	case_id VARCHAR, 
	type VARCHAR, 
	sequence VARCHAR, 
	start_date VARCHAR, 
	end_date VARCHAR, 
	duration VARCHAR, 
	days_to_next_reentry VARCHAR, 
	days_from_last_exit VARCHAR, 
	first_start_date VARCHAR, 
	days_since_first_start_date VARCHAR, 
	days_since_last_end_date VARCHAR, 
	age_at_first_start_date VARCHAR, 
	had_prior VARCHAR, 
	data_as_of_date VARCHAR, 
	load_dt VARCHAR
);

\COPY raw.cmu_cyf_case_prm from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220623/CMU_CYF_CASE_PRM_20220623.csv' with CSV HEADER;

-- CYF referral program

create table raw.cmu_cyf_referral_prm (
	refer_id VARCHAR, 
	intake_dt VARCHAR, 
	intake_time VARCHAR, 
	mci_uniq_id VARCHAR, 
	pri_ref_role VARCHAR, 
	in_househld_cd VARCHAR, 
	call_scrn_outcome VARCHAR, 
	service_decision VARCHAR, 
	service_decision_dt VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_cyf_referral_prm from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220623/CMU_CYF_REFERRAL_PRM_20220623.csv' with CSV HEADER;

-- findings program

create table raw.cmu_findings_prm (
	refer_id VARCHAR, 
	refer_dt VARCHAR, 
	allegation_id VARCHAR, 
	alleg_seq_nbr VARCHAR, 
	child_mci_uniq_id VARCHAR, 
	perp_mci_uniq_id VARCHAR, 
	finding_long VARCHAR, 
	finding_short VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_findings_prm from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220623/CMU_FINDINGS_PRM_20220623.csv' with CSV HEADER;

-- HMIS current program

create table raw.cmu_hmis_current_prm (
	assessment_id VARCHAR, 
	project_type_id VARCHAR, 
	hmis_id VARCHAR, 
	mci_uniq_id VARCHAR, 
	program_start_dt VARCHAR, 
	exit_dt VARCHAR, 
	assessment_date VARCHAR, 
	project_type VARCHAR, 
	src_load_dt VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_hmis_current_prm from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220623/CMU_HMIS_CURRENT_PRM_20220623.csv' with CSV HEADER;

-- HMIS old program

create table raw.cmu_hmis_old_prm (
	assessment_id VARCHAR, 
	mci_uniq_id VARCHAR, 
	program_start_dt VARCHAR, 
	program_end_dt VARCHAR, 
	assessment_dt VARCHAR, 
	svc_name VARCHAR, 
	data_load_dt VARCHAR
);

\COPY raw.cmu_hmis_old_prm from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220623/CMU_HMIS_OLD_PRM_20220623.csv' with CSV HEADER;

-- mental health program

create table raw.cmu_mh_prm (
	mci_uniq_id VARCHAR, 
	event_type VARCHAR, 
	event_sequence VARCHAR, 
	event_start_date VARCHAR, 
	event_end_date VARCHAR, 
	event_duration VARCHAR, 
	event_active_days VARCHAR, 
	event_days_to_reentry VARCHAR, 
	event_days_from_prev_reentry VARCHAR, 
	first_event_date VARCHAR, 
	days_since_first_event VARCHAR, 
	days_since_last_event VARCHAR, 
	age_at_first_event VARCHAR, 
	event_los VARCHAR, 
	current_event VARCHAR, 
	had_prior VARCHAR, 
	data_as_of_date VARCHAR, 
	view_build_date VARCHAR, 
	expunged VARCHAR, 
	prvdr_cd VARCHAR, 
	almh_ind VARCHAR, 
	ccbho_ind VARCHAR, 
	da_diagnosis_flag VARCHAR, 
	mh_diagnosis_flag VARCHAR, 
	smi_dx VARCHAR, 
	service VARCHAR, 
	service_count VARCHAR
);

\COPY raw.cmu_mh_prm from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220623/CMU_MH_PRM_20220623.csv' with CSV HEADER;

-- outpatient health program

create table raw.cmu_outpatient_health_prm (
	mci_uniq_id VARCHAR, 
	svc_start_dt VARCHAR, 
	svc_end_dt VARCHAR, 
	service_category VARCHAR
);

\COPY raw.cmu_outpatient_health_prm from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220623/CMU_OUTPATIENT_HEALTH_PRM_20220623.csv' with CSV HEADER;

-- physical health program: 16 total tables

create table raw.cmu_physical_health_prm_1 (
	source VARCHAR, 
	claim_nbr VARCHAR, 
	mci_uniq_id VARCHAR, 
	svc_start_dt VARCHAR, 
	svc_end_dt VARCHAR, 
	svc_cat_grp_nbr VARCHAR, 
	scu_cd VARCHAR, 
	source_load_date VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_physical_health_prm_1 from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220703/CMU_PHYSICAL_HEALTH_PRM_20220701_1.csv' with CSV HEADER;

create table raw.cmu_physical_health_prm_2 (
	source VARCHAR, 
	claim_nbr VARCHAR, 
	mci_uniq_id VARCHAR, 
	svc_start_dt VARCHAR, 
	svc_end_dt VARCHAR, 
	svc_cat_grp_nbr VARCHAR, 
	scu_cd VARCHAR, 
	source_load_date VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_physical_health_prm_2 from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220703/CMU_PHYSICAL_HEALTH_PRM_20220701_2.csv' with CSV HEADER;

create table raw.cmu_physical_health_prm_3 (
	source VARCHAR, 
	claim_nbr VARCHAR, 
	mci_uniq_id VARCHAR, 
	svc_start_dt VARCHAR, 
	svc_end_dt VARCHAR, 
	svc_cat_grp_nbr VARCHAR, 
	scu_cd VARCHAR, 
	source_load_date VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_physical_health_prm_3 from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220703/CMU_PHYSICAL_HEALTH_PRM_20220701_3.csv' with CSV HEADER;

create table raw.cmu_physical_health_prm_4 (
	source VARCHAR, 
	claim_nbr VARCHAR, 
	mci_uniq_id VARCHAR, 
	svc_start_dt VARCHAR, 
	svc_end_dt VARCHAR, 
	svc_cat_grp_nbr VARCHAR, 
	scu_cd VARCHAR, 
	source_load_date VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_physical_health_prm_4 from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220703/CMU_PHYSICAL_HEALTH_PRM_20220701_4.csv' with CSV HEADER;

create table raw.cmu_physical_health_prm_5 (
	source VARCHAR, 
	claim_nbr VARCHAR, 
	mci_uniq_id VARCHAR, 
	svc_start_dt VARCHAR, 
	svc_end_dt VARCHAR, 
	svc_cat_grp_nbr VARCHAR, 
	scu_cd VARCHAR, 
	source_load_date VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_physical_health_prm_5 from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220703/CMU_PHYSICAL_HEALTH_PRM_20220701_5.csv' with CSV HEADER;

create table raw.cmu_physical_health_prm_6 (
	source VARCHAR, 
	claim_nbr VARCHAR, 
	mci_uniq_id VARCHAR, 
	svc_start_dt VARCHAR, 
	svc_end_dt VARCHAR, 
	svc_cat_grp_nbr VARCHAR, 
	scu_cd VARCHAR, 
	source_load_date VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_physical_health_prm_6 from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220703/CMU_PHYSICAL_HEALTH_PRM_20220701_6.csv' with CSV HEADER;

create table raw.cmu_physical_health_prm_7 (
	source VARCHAR, 
	claim_nbr VARCHAR, 
	mci_uniq_id VARCHAR, 
	svc_start_dt VARCHAR, 
	svc_end_dt VARCHAR, 
	svc_cat_grp_nbr VARCHAR, 
	scu_cd VARCHAR, 
	source_load_date VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_physical_health_prm_7 from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220703/CMU_PHYSICAL_HEALTH_PRM_20220701_7.csv' with CSV HEADER;

create table raw.cmu_physical_health_prm_8 (
	source VARCHAR, 
	claim_nbr VARCHAR, 
	mci_uniq_id VARCHAR, 
	svc_start_dt VARCHAR, 
	svc_end_dt VARCHAR, 
	svc_cat_grp_nbr VARCHAR, 
	scu_cd VARCHAR, 
	source_load_date VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_physical_health_prm_8 from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220703/CMU_PHYSICAL_HEALTH_PRM_20220701_8.csv' with CSV HEADER;

create table raw.cmu_physical_health_prm_9 (
	source VARCHAR, 
	claim_nbr VARCHAR, 
	mci_uniq_id VARCHAR, 
	svc_start_dt VARCHAR, 
	svc_end_dt VARCHAR, 
	svc_cat_grp_nbr VARCHAR, 
	scu_cd VARCHAR, 
	source_load_date VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_physical_health_prm_9 from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220703/CMU_PHYSICAL_HEALTH_PRM_20220701_9.csv' with CSV HEADER;

create table raw.cmu_physical_health_prm_10 (
	source VARCHAR, 
	claim_nbr VARCHAR, 
	mci_uniq_id VARCHAR, 
	svc_start_dt VARCHAR, 
	svc_end_dt VARCHAR, 
	svc_cat_grp_nbr VARCHAR, 
	scu_cd VARCHAR, 
	source_load_date VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_physical_health_prm_10 from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220703/CMU_PHYSICAL_HEALTH_PRM_20220701_10.csv' with CSV HEADER;

create table raw.cmu_physical_health_prm_11 (
	source VARCHAR, 
	claim_nbr VARCHAR, 
	mci_uniq_id VARCHAR, 
	svc_start_dt VARCHAR, 
	svc_end_dt VARCHAR, 
	svc_cat_grp_nbr VARCHAR, 
	scu_cd VARCHAR, 
	source_load_date VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_physical_health_prm_11 from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220703/CMU_PHYSICAL_HEALTH_PRM_20220701_11.csv' with CSV HEADER;

create table raw.cmu_physical_health_prm_12 (
	source VARCHAR, 
	claim_nbr VARCHAR, 
	mci_uniq_id VARCHAR, 
	svc_start_dt VARCHAR, 
	svc_end_dt VARCHAR, 
	svc_cat_grp_nbr VARCHAR, 
	scu_cd VARCHAR, 
	source_load_date VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_physical_health_prm_12 from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220703/CMU_PHYSICAL_HEALTH_PRM_20220701_12.csv' with CSV HEADER;

create table raw.cmu_physical_health_prm_13 (
	source VARCHAR, 
	claim_nbr VARCHAR, 
	mci_uniq_id VARCHAR, 
	svc_start_dt VARCHAR, 
	svc_end_dt VARCHAR, 
	svc_cat_grp_nbr VARCHAR, 
	scu_cd VARCHAR, 
	source_load_date VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_physical_health_prm_13 from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220703/CMU_PHYSICAL_HEALTH_PRM_20220701_13.csv' with CSV HEADER;

create table raw.cmu_physical_health_prm_14 (
	source VARCHAR, 
	claim_nbr VARCHAR, 
	mci_uniq_id VARCHAR, 
	svc_start_dt VARCHAR, 
	svc_end_dt VARCHAR, 
	svc_cat_grp_nbr VARCHAR, 
	scu_cd VARCHAR, 
	source_load_date VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_physical_health_prm_14 from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220703/CMU_PHYSICAL_HEALTH_PRM_20220701_14.csv' with CSV HEADER;

create table raw.cmu_physical_health_prm_15 (
	source VARCHAR, 
	claim_nbr VARCHAR, 
	mci_uniq_id VARCHAR, 
	svc_start_dt VARCHAR, 
	svc_end_dt VARCHAR, 
	svc_cat_grp_nbr VARCHAR, 
	scu_cd VARCHAR, 
	source_load_date VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_physical_health_prm_15 from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220703/CMU_PHYSICAL_HEALTH_PRM_20220701_15.csv' with CSV HEADER;

create table raw.cmu_physical_health_prm_16 (
	source VARCHAR, 
	claim_nbr VARCHAR, 
	mci_uniq_id VARCHAR, 
	svc_start_dt VARCHAR, 
	svc_end_dt VARCHAR, 
	svc_cat_grp_nbr VARCHAR, 
	scu_cd VARCHAR, 
	source_load_date VARCHAR, 
	creation_dt VARCHAR
);

\COPY raw.cmu_physical_health_prm_16 from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220703/CMU_PHYSICAL_HEALTH_PRM_20220701_16.csv' with CSV HEADER;

-- placement program

create table raw.cmu_placement_prm (
	plcmnt_seq_nbr VARCHAR, 
	plcmnt_entry_date VARCHAR, 
	plcmnt_end_date VARCHAR, 
	mci_uniq_id VARCHAR, 
	srvc_key VARCHAR, 
	srvc_group_1 VARCHAR, 
	srvc_group_2 VARCHAR, 
	plcmnt_type_desc VARCHAR, 
	source_load_date VARCHAR, 
	create_ts VARCHAR
);

\COPY raw.cmu_placement_prm from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220623/CMU_PLACEMENT_PRM_20220623.csv' with CSV HEADER;

-- removal program

create table raw.cmu_removal_prm (
	mci_uniq_id VARCHAR, 
	rmvl_type_amend VARCHAR, 
	rmvl_seq_nbr VARCHAR, 
	rmvl_entry_dt VARCHAR, 
	rmvl_entry_age VARCHAR, 
	rmvl_entry_mo VARCHAR, 
	rmvl_entry_yr VARCHAR, 
	rmvl_exit_dt VARCHAR, 
	rmvl_rtrn_rsn VARCHAR, 
	rmvl_rtrn_cde VARCHAR, 
	perm_status VARCHAR, 
	rmvl_exit_age VARCHAR, 
	rmvl_exit_mo VARCHAR, 
	rmvl_exit_yr VARCHAR, 
	rmvl_in_care VARCHAR, 
	rmvl_los_days VARCHAR, 
	cyf_rmvl_reentry_time VARCHAR, 
	rem_prnt_bh VARCHAR, 
	rem_prnt_chld_conf VARCHAR, 
	rem_prnt_cope VARCHAR, 
	rem_prnt_skls VARCHAR, 
	rem_relinq VARCHAR, 
	rem_resump VARCHAR, 
	rem_sex_abus VARCHAR, 
	rem_truancy VARCHAR, 
	rem_unk VARCHAR, 
	view_build_date VARCHAR, 
	create_date VARCHAR
);

\COPY raw.cmu_removal_prm from program ' sed ''/^\s*$/d'' /mnt/data/projects/acdhs-housing/data/20220623/CMU_REMOVAL_PRM_20220623.csv' with CSV HEADER;