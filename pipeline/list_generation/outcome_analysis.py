import pandas as pd
from sqlalchemy import create_engine
import seaborn as sns
import matplotlib.pyplot as plt
import datetime
from pandas.tseries.offsets import DateOffset


today = datetime.date.today()
outreach_start_date = '2024-10-31' # the first list for which we did randomization has as_of_date 2024-10-31 
intervals_in_months = [-1, 12] # NOTE: -1 == all time
outcome_threshold_months = 12

def get_treatment_cases(engine, remove_cases_before_nov=False):
    q = f"""
        with most_recent_eviction as (
            select distinct on (mci_uniq_id)
            mci_uniq_id,
            filingdt
            from clean.eviction_client_matches
            order by 1, 2 desc)
        select 
            mci_uniq_id, 
            client_hash,
            as_of_date,
            score,
            ct_group as trial_group, 
            group_assignment_date, 
            selected_for_outreach,
            outreach_selection_date, 
            tier as original_tier, 
            rank_within_group, 
            rank_abs_no_ties
        from acdhs_production.control_treatment_assignment cta left join most_recent_eviction mre using(mci_uniq_id)
        where outreach_selection_date = date(db_recorded_timestamp)
        """
    if remove_cases_before_nov:
        q += " and filingdt >= '10-24-2024'"
    return pd.read_sql(q, engine, parse_dates=['as_of_date', 'group_assignment_date', 'outreach_selection_date'])

def read_assignment_table(engine, remove_cases_before_nov=False):
    q = f"""
        with most_recent_eviction as (
            select distinct on (mci_uniq_id)
            mci_uniq_id,
            filingdt
            from clean.eviction_client_matches
            order by 1, 2 desc)
        select 
            mci_uniq_id, 
            client_hash,
            as_of_date,
            score,
            ct_group as trial_group, 
            group_assignment_date, 
            selected_for_outreach,
            outreach_selection_date, 
            tier as original_tier, 
            rank_within_group, 
            rank_abs_no_ties,
            db_recorded_timestamp
        from acdhs_production.control_treatment_assignment cta left join most_recent_eviction mre using(mci_uniq_id)
        where group_assignment_date >= '{outreach_start_date}' and group_assignment_date is not null
    """
    if remove_cases_before_nov:
        q += " and filingdt >= '10-24-2024'"
    return pd.read_sql(q, engine, parse_dates=['as_of_date', 'group_assignment_date', 'outreach_selection_date'])

def get_unique_assignments(engine, remove_cases_before_nov=False):
    # get all assignment rows
    assignment_table = read_assignment_table(engine, remove_cases_before_nov)
    # get treatment cases 
    treatment_df = get_treatment_cases(engine, remove_cases_before_nov)
    treatment_df['assignment_frozen_date'] = treatment_df['outreach_selection_date']
    # remove treatment cases to get remaining assignment rows
    control_potential_df = assignment_table[~assignment_table['mci_uniq_id'].isin(treatment_df['mci_uniq_id'])].copy()
    # update control and potential treatment cases 
    control_potential_df['assignment_frozen_date'] = control_potential_df['group_assignment_date']
    control_potential_df.loc[control_potential_df['trial_group']=='treatment', 'trial_group'] = 'potential_treatment'
    # drop duplicates: keep the first assignment 
    control_potential_df.sort_values(by='db_recorded_timestamp', ascending=True, inplace=True) 
    control_potential_df.drop_duplicates(subset='mci_uniq_id', inplace=True) # one person may have been selected for outreach multiple times if there are multiple evictions
    out_table = pd.concat([treatment_df, control_potential_df])
    return out_table

def recompute_control_tiers(assignments):
    frozen_dates = assignments['assignment_frozen_date'].unique()
    frozen_dates.sort()
    assignments['tier'] = assignments['original_tier']
    # print(assignments['assignment_frozen_date'].unique())
    for frozen_date in frozen_dates:
        # get tier score divisions from sampled treatment cases
        ## top
        min_top_treatment_score = assignments.loc[(assignments['assignment_frozen_date']==frozen_date) & (assignments['trial_group']=='treatment') & (assignments['original_tier']=='top')]['score'].min()
        assignments.loc[(assignments['assignment_frozen_date']==frozen_date) & (assignments['trial_group']=='control') & (assignments['score']>=min_top_treatment_score), 'tier'] = 'top'
        ## middle
        min_mid_treatment_score = assignments.loc[(assignments['assignment_frozen_date']==frozen_date) & (assignments['trial_group']=='treatment') & (assignments['original_tier']=='middle')]['score'].min()
        assignments.loc[(assignments['assignment_frozen_date']==frozen_date) & (assignments['trial_group']=='control') & (assignments['score']>=min_mid_treatment_score) & (assignments['score']<min_top_treatment_score), 'tier'] = 'middle'
    return assignments

def add_demographics(engine, cases):
    q = """
        select mci_uniq_id, dob, dod, gender, race from clean.client_feed
        """
    demo_table = pd.read_sql(q, engine, parse_dates=['dob', 'dod'])
    demo_table = pd.merge(cases, demo_table, how='left', on=['mci_uniq_id'])
    # age
    demo_table['age'] = ((demo_table['as_of_date'] - demo_table['dob']) / 365.25).dt.days
    # dummy variables for race and gender
    race_cols = pd.get_dummies(demo_table['race'])
    demo_table = demo_table.join(race_cols)
    gender_cols = pd.get_dummies(demo_table['gender'])
    demo_table = demo_table.join(gender_cols)
    demo_table = demo_table.rename({"1~Male": "Male", "Black/African American": "Black"}, axis=1)
    return demo_table

def add_prev_homeless(engine, cases):
    q = "select mci_uniq_id, client_hash, program_start_dt, program_end_dt, data_type from pretriage.hl_table hl"
    hl_table_mci = pd.read_sql(q, engine, parse_dates=['program_start_dt', 'program_end_dt'])
    table_hl = pd.merge(cases, hl_table_mci, how='left', on=['mci_uniq_id'])
    full_table = cases.copy()
    for month_cnt in intervals_in_months:
        table_hl_tmp = table_hl.copy()
        col_str = f'prev_hl_{month_cnt}_mos'
        if month_cnt == -1:
            col_str = 'prev_hl'
            table_hl_tmp = table_hl_tmp.loc[table_hl_tmp['program_start_dt']<table_hl_tmp['as_of_date']]
        else:
            table_hl_tmp = table_hl_tmp.loc[(table_hl_tmp['program_start_dt']<table_hl_tmp['as_of_date'])&(table_hl_tmp['program_start_dt']>table_hl_tmp['as_of_date']-DateOffset(months=month_cnt))]
        table_hl_tmp[col_str] = 1
        table_hl_tmp = table_hl_tmp[['mci_uniq_id', col_str]]
        table_hl_tmp.drop_duplicates(inplace=True)
        full_table = pd.merge(full_table, table_hl_tmp, how='left', on=['mci_uniq_id'])
        full_table[col_str] = full_table[col_str].fillna(0)
    return full_table

def add_prev_rental_assistance(engine, cases):
    q = """
    select
        mci_uniq_id,
        last_payment_dt
    from clean.rental_assistance_payment_status 
    """
    ra_table = pd.read_sql(q, engine, parse_dates=['last_payment_dt'])
    table_ra = pd.merge(cases, ra_table, how='left', on=['mci_uniq_id'])
    # full_table_prev = full_table.copy()
    for month_cnt in intervals_in_months:
        table_ra_tmp = table_ra.copy()
        col_str = f'prev_ra_{month_cnt}_mos'
        if month_cnt == -1:
            col_str = 'prev_ra'
            table_ra_tmp = table_ra_tmp.loc[table_ra_tmp['last_payment_dt']<table_ra_tmp['as_of_date']]
        else:
            table_ra_tmp = table_ra_tmp.loc[(table_ra_tmp['last_payment_dt']<table_ra_tmp['as_of_date'])&(table_ra_tmp['last_payment_dt']>table_ra_tmp['as_of_date']-DateOffset(months=month_cnt))]
        table_ra_tmp[col_str] = 1
        table_ra_tmp = table_ra_tmp[['mci_uniq_id', col_str]]
        table_ra_tmp.drop_duplicates(inplace=True)
        cases = pd.merge(cases, table_ra_tmp, how='left', on=['mci_uniq_id'])
        cases[col_str] = cases[col_str].fillna(0)
    return cases

def add_prev_bh(engine, cases):
    q = """
    select 
        unhashed_mci_uniq_id as mci_uniq_id,
        event_end_date
    from clean.cmu_behavior_health_prm"""
    bh_table = pd.read_sql(q, engine, parse_dates=['event_end_date'])
    table_bh = pd.merge(cases, bh_table, how='left', on=['mci_uniq_id'])
    for month_cnt in intervals_in_months:
        table_bh_tmp = table_bh.copy()
        col_str = f'prev_bh_{month_cnt}_mos'
        if month_cnt == -1:
            col_str = 'prev_bh'
            table_bh_tmp = table_bh_tmp.loc[table_bh_tmp['event_end_date']<table_bh_tmp['as_of_date']]
        else:
            table_bh_tmp = table_bh_tmp.loc[(table_bh_tmp['event_end_date']<table_bh_tmp['as_of_date'])&(table_bh_tmp['event_end_date']>table_bh_tmp['as_of_date']-DateOffset(months=month_cnt))]
        table_bh_tmp[col_str] = 1
        table_bh_tmp = table_bh_tmp[['mci_uniq_id', col_str]]
        table_bh_tmp.drop_duplicates(inplace=True)
        cases = pd.merge(cases, table_bh_tmp, how='left', on=['mci_uniq_id'])
        cases[col_str] = cases[col_str].fillna(0)
    return cases

def add_prev_mh(engine, cases):
    q = """
    select 
      unhashed_mci_uniq_id as mci_uniq_id,
      event_end_date
    from clean.cmu_mh_prm;
    """
    mh_table = pd.read_sql(q, engine, parse_dates=['event_end_date'])
    table_mh = pd.merge(cases, mh_table, how='left', on=['mci_uniq_id'])
    for month_cnt in intervals_in_months:
        table_mh_tmp = table_mh.copy()
        col_str = f'prev_mh_{month_cnt}_mos'
        if month_cnt == -1:
            col_str = 'prev_mh'
            table_mh_tmp = table_mh_tmp.loc[table_mh_tmp['event_end_date']<table_mh_tmp['as_of_date']]
        else:
            table_mh_tmp = table_mh_tmp.loc[(table_mh_tmp['event_end_date']<table_mh_tmp['as_of_date'])&(table_mh_tmp['event_end_date']>table_mh_tmp['as_of_date']-DateOffset(months=month_cnt))]
        table_mh_tmp[col_str] = 1
        table_mh_tmp = table_mh_tmp[['mci_uniq_id', col_str]]
        table_mh_tmp.drop_duplicates(inplace=True)
        cases = pd.merge(cases, table_mh_tmp, how='left', on=['mci_uniq_id'])
        cases[col_str] = cases[col_str].fillna(0)
    return cases

def add_prev_ph(engine, cases):
    q = """
        select 
        unhashed_mci_uniq_id as mci_uniq_id,
        svc_end_dt
    from clean.cmu_physical_health_prm ph --join acdhs_production.predictions p on (p.client_hash=ph.mci_uniq_id)
    """
    ph_table = pd.read_sql(q, engine, parse_dates=['svc_end_dt'])
    table_ph = pd.merge(cases, ph_table, how='left', on=['mci_uniq_id'])
    for month_cnt in intervals_in_months:
        table_ph_tmp = table_ph.copy()
        col_str = f'prev_ph_{month_cnt}_mos'
        if month_cnt == -1:
            col_str = 'prev_ph'
            table_ph_tmp = table_ph_tmp.loc[table_ph_tmp['svc_end_dt']<table_ph_tmp['prediction_date']]
        else:
            table_ph_tmp = table_ph_tmp.loc[(table_ph_tmp['svc_end_dt']<table_ph_tmp['prediction_date'])&(table_ph_tmp['svc_end_dt']>table_ph_tmp['prediction_date']-DateOffset(months=month_cnt))]
        table_ph_tmp[col_str] = 1
        table_ph_tmp = table_ph_tmp[['mci_uniq_id', col_str]]
        table_ph_tmp.drop_duplicates(inplace=True)
        cases = pd.merge(cases, table_ph_tmp, how='left', on=['mci_uniq_id'])
        cases[col_str] = cases[col_str].fillna(0)
    return cases

def get_hl_outcomes(engine, cases):
    q = f"""
    select 
        mci_uniq_id,
        program_start_dt as hl_start_dt,
        program_end_dt as hl_end_dt,
        coalesce(program_end_dt,'{today}'::date) as hl_end_dt_coalesce,
        1 as hl_count,
        coalesce(program_end_dt,'{today}'::date)-program_start_dt as hl_days
    from pretriage.hl_table
    where program_start_dt > '{outreach_start_date}'::date
    """
    hl_df = pd.read_sql(q, engine, parse_dates=['hl_start_dt', 'hl_end_dt', 'hl_end_dt_coalesce']) # outreach_start_date filter is just to reduce the number of rows returned (since we're not looking for any outcomes before that date)
    hl_df_matches = pd.merge(cases, hl_df, how='inner', on=['mci_uniq_id'])
    hl_df_matches = hl_df_matches.loc[(hl_df_matches['hl_start_dt']>hl_df_matches['assignment_frozen_date']) & (hl_df_matches['hl_start_dt']<=hl_df_matches['assignment_frozen_date']+DateOffset(months=outcome_threshold_months))]
    # get binary
    hl_df_binary = hl_df_matches.drop_duplicates(subset=['mci_uniq_id']) # binary outcome so only need one valid outcome date per person
    hl_df_binary = hl_df_binary[['mci_uniq_id', 'assignment_frozen_date', 'hl_start_dt', 'hl_end_dt', 'hl_end_dt_coalesce']]
    df = cases.copy()
    df = pd.merge(df, hl_df_binary, how='left', on=['mci_uniq_id', 'assignment_frozen_date'])
    df['hl_binary'] = (~df['hl_start_dt'].isna()).astype('int')
    # get count
    hl_df_count = hl_df_matches.groupby(["mci_uniq_id",'assignment_frozen_date'])["hl_count"].sum()
    df = pd.merge(df, hl_df_count, how='left', on=['mci_uniq_id', 'assignment_frozen_date'])
    df['hl_count'].fillna(0, inplace=True)
    # get length of stay
    hl_df_days = hl_df_matches.groupby(["mci_uniq_id",'assignment_frozen_date'])["hl_days"].sum()
    df = pd.merge(df, hl_df_days, how='left', on=['mci_uniq_id', 'assignment_frozen_date'])
    df['hl_days'].fillna(0, inplace=True)
    return df

def get_bh_outcomes(engine, cases):
    q = f"""
    select 
        unhashed_mci_uniq_id as mci_uniq_id,
        event_beg_date as bh_start_dt,
        event_end_date as bh_end_dt,
        coalesce(event_end_date,'{today}'::date) as bh_end_dt_coalesce,
        1 as bh_count,
        coalesce(event_end_date,'{today}'::date)-event_beg_date as bh_days
    from clean.cmu_behavior_health_prm
    where event_beg_date > '{outreach_start_date}'::date
    """
    bh_df = pd.read_sql(q, engine, parse_dates=['bh_start_dt', 'bh_end_dt', 'bh_end_dt_coalesce']) # outreach_start_date filter is just to reduce the number of rows returned (since we're not looking for any outcomes before that date)
    bh_df_matches = pd.merge(cases, bh_df, how='inner', on=['mci_uniq_id'])
    bh_df_matches = bh_df_matches.loc[(bh_df_matches['bh_start_dt']>bh_df_matches['assignment_frozen_date']) & (bh_df_matches['bh_start_dt']<=bh_df_matches['assignment_frozen_date']+DateOffset(months=outcome_threshold_months))]
    # get binary    
    bh_df_binary = bh_df_matches.drop_duplicates(subset=['mci_uniq_id']) # binary outcome so only need one valid outcome date per person
    bh_df_binary = bh_df_binary[['mci_uniq_id', 'assignment_frozen_date', 'bh_start_dt', 'bh_end_dt', 'bh_end_dt_coalesce']]
    df = cases.copy()
    df = pd.merge(df, bh_df_binary, how='left', on=['mci_uniq_id', 'assignment_frozen_date'])
    df['any_bh'] = (~df['bh_start_dt'].isna()).astype('int')
    # get count
    bh_df_count = bh_df_matches.groupby(["mci_uniq_id",'assignment_frozen_date'])["bh_count"].sum()
    df = pd.merge(df, bh_df_count, how='left', on=['mci_uniq_id', 'assignment_frozen_date'])
    df['bh_count'].fillna(0, inplace=True)
    # get length of stay
    bh_df_days = bh_df_matches.groupby(["mci_uniq_id",'assignment_frozen_date'])["bh_days"].sum()
    df = pd.merge(df, bh_df_days, how='left', on=['mci_uniq_id', 'assignment_frozen_date'])
    df['bh_days'].fillna(0, inplace=True)
    return df

def get_hl_outcomes_new(engine, cases):
    # get all relevant (after assignment_frozen_date) homelessness spells
    q = f"""
    select 
        mci_uniq_id,
        program_start_dt as hl_start_dt,
        program_end_dt as hl_end_dt,
        coalesce(program_end_dt,'{today}'::date) as hl_end_dt_coalesce,
        1 as hl_count,
        coalesce(program_end_dt,'{today}'::date)-program_start_dt as hl_days
    from pretriage.hl_table
    where program_start_dt > '{outreach_start_date}'::date
    """
    hl_df = pd.read_sql(q, engine, parse_dates=['hl_start_dt', 'hl_end_dt', 'hl_end_dt_coalesce']) # outreach_start_date filter is just to reduce the number of rows returned (since we're not looking for any outcomes before that date)
    hl_df_matches = pd.merge(cases, hl_df, how='inner', on=['mci_uniq_id'])
    hl_df_matches = hl_df_matches.loc[(hl_df_matches['hl_start_dt']>hl_df_matches['assignment_frozen_date']) & (hl_df_matches['hl_start_dt']<=hl_df_matches['assignment_frozen_date']+DateOffset(months=outcome_threshold_months))]
    # now compute outcomes
    ## binary
    hl_binary = hl_df_matches.copy()
    hl_binary.sort_values(by='hl_start_dt', ascending=True, inplace=True) 
    hl_binary['hl_binary'] = hl_binary.groupby('mci_uniq_id').cumcount() == 0
    hl_binary['hl_binary'] = hl_binary['hl_binary'].astype(int)
    hl_binary = hl_binary.loc[hl_binary['hl_binary']==1]
    hl_binary = pd.merge(cases['mci_uniq_id'], hl_binary, how='left', on=['mci_uniq_id'])
    hl_binary['hl_binary'].fillna(value=0, inplace=True)
    hl_binary['hl_binary'] = hl_binary['hl_binary'].astype(int)
    hl_binary = hl_binary[['mci_uniq_id', 'hl_binary', 'hl_start_dt']]
    ## count
    hl_count = hl_df_matches.copy()
    hl_count = hl_count.groupby(["mci_uniq_id"])["hl_count"].sum().reset_index()
    hl_count = hl_count[['mci_uniq_id', 'hl_count']]
    ## days
    hl_days = hl_df_matches.copy()
    hl_days = hl_days.groupby(["mci_uniq_id"])["hl_days"].sum().reset_index()
    hl_days = hl_days[['mci_uniq_id', 'hl_days']]
    return hl_df_matches, hl_binary, hl_count, hl_days

def get_bh_outcomes_new(engine, cases):
    q = f"""
    select 
        unhashed_mci_uniq_id as mci_uniq_id,
        event_beg_date as bh_start_dt,
        event_end_date as bh_end_dt,
        coalesce(event_end_date,'{today}'::date) as bh_end_dt_coalesce,
        1 as bh_count,
        coalesce(event_end_date,'{today}'::date)-event_beg_date as bh_days
    from clean.cmu_behavior_health_prm
    where event_beg_date > '{outreach_start_date}'::date
    """
    bh_df = pd.read_sql(q, engine, parse_dates=['bh_start_dt', 'bh_end_dt', 'bh_end_dt_coalesce']) # outreach_start_date filter is just to reduce the number of rows returned (since we're not looking for any outcomes before that date)
    bh_df_matches = pd.merge(cases, bh_df, how='inner', on=['mci_uniq_id'])
    bh_df_matches = bh_df_matches.loc[(bh_df_matches['bh_start_dt']>bh_df_matches['assignment_frozen_date']) & (bh_df_matches['bh_start_dt']<=bh_df_matches['assignment_frozen_date']+DateOffset(months=outcome_threshold_months))]
    # now compute outcomes
    ## binary
    bh_binary = bh_df_matches.copy()
    bh_binary['bh_binary'] = bh_binary.groupby('mci_uniq_id').cumcount() == 0
    bh_binary['bh_binary'] = bh_binary['bh_binary'].astype(int)
    bh_binary = bh_binary.loc[bh_binary['bh_binary']==1]
    bh_binary = pd.merge(cases['mci_uniq_id'], bh_binary, how='left', on=['mci_uniq_id'])
    bh_binary['bh_binary'].fillna(value=0, inplace=True)
    bh_binary['bh_binary'] = bh_binary['bh_binary'].astype(int)
    bh_binary = bh_binary[['mci_uniq_id', 'bh_binary', 'bh_start_dt']]
    ## count
    bh_count = bh_df_matches.copy()
    bh_count = bh_count.groupby(["mci_uniq_id"])["bh_count"].sum().reset_index()
    bh_count = bh_count[['mci_uniq_id', 'bh_count']]
    ## days
    bh_days = bh_df_matches.copy()
    bh_days = bh_days.groupby(["mci_uniq_id"])["bh_days"].sum().reset_index()
    bh_days = bh_days[['mci_uniq_id', 'bh_days']]
    return bh_df_matches, bh_binary, bh_count, bh_days

def get_mh_outcomes(engine, cases):
    q = f"""
    select 
        unhashed_mci_uniq_id as mci_uniq_id,
        event_start_date as mh_start_dt,
        event_end_date as mh_end_dt,
        coalesce(event_end_date,'{today}'::date) as mh_end_dt_coalesce,
        1 as mh_count,
        coalesce(event_end_date,'{today}'::date)-event_start_date as mh_days
    from clean.cmu_mh_prm
    where event_start_date > '{outreach_start_date}'::date
    """
    mh_df = pd.read_sql(q, engine, parse_dates=['mh_start_dt', 'mh_end_dt', 'mh_end_dt_coalesce']) # outreach_start_date filter is just to reduce the number of rows returned (since we're not looking for any outcomes before that date)
    mh_df_matches = pd.merge(cases, mh_df, how='inner', on=['mci_uniq_id'])
    mh_df_matches = mh_df_matches.loc[(mh_df_matches['mh_start_dt']>mh_df_matches['assignment_frozen_date']) & (mh_df_matches['mh_start_dt']<=mh_df_matches['assignment_frozen_date']+DateOffset(months=outcome_threshold_months))]
    # get binary
    mh_df_binary = mh_df_matches.drop_duplicates(subset=['mci_uniq_id']) # binary outcome so only need one valid outcome date per person
    mh_df_binary = mh_df_binary[['mci_uniq_id', 'assignment_frozen_date', 'mh_start_dt', 'mh_end_dt', 'mh_end_dt_coalesce']]
    df = cases.copy()
    df = pd.merge(df, mh_df_binary, how='left', on=['mci_uniq_id', 'assignment_frozen_date'])
    df['any_mh'] = (~df['mh_start_dt'].isna()).astype('int')
    # get count
    mh_df_count = mh_df_matches.groupby(["mci_uniq_id",'assignment_frozen_date'])["mh_count"].sum()
    df = pd.merge(df, mh_df_count, how='left', on=['mci_uniq_id', 'assignment_frozen_date'])
    df['mh_count'].fillna(0, inplace=True)
    # get length of stay
    mh_df_days = mh_df_matches.groupby(["mci_uniq_id",'assignment_frozen_date'])["mh_days"].sum()
    df = pd.merge(df, mh_df_days, how='left', on=['mci_uniq_id', 'assignment_frozen_date'])
    df['mh_days'].fillna(0, inplace=True)
    return df

def get_mh_outcomes_new(engine, cases):
    q = f"""
    select 
        unhashed_mci_uniq_id as mci_uniq_id,
        event_start_date as mh_start_dt,
        event_end_date as mh_end_dt,
        coalesce(event_end_date,'{today}'::date) as mh_end_dt_coalesce,
        1 as mh_count,
        coalesce(event_end_date,'{today}'::date)-event_start_date as mh_days
    from clean.cmu_mh_prm
    where event_start_date > '{outreach_start_date}'::date
    """
    mh_df = pd.read_sql(q, engine, parse_dates=['mh_start_dt', 'mh_end_dt', 'mh_end_dt_coalesce']) # outreach_start_date filter is just to reduce the number of rows returned (since we're not looking for any outcomes before that date)
    mh_df_matches = pd.merge(cases, mh_df, how='inner', on=['mci_uniq_id'])
    mh_df_matches = mh_df_matches.loc[(mh_df_matches['mh_start_dt']>mh_df_matches['assignment_frozen_date']) & (mh_df_matches['mh_start_dt']<=mh_df_matches['assignment_frozen_date']+DateOffset(months=outcome_threshold_months))]
    # now compute outcomes
    ## binary
    mh_binary = mh_df_matches.copy()
    mh_binary['mh_binary'] = mh_binary.groupby('mci_uniq_id').cumcount() == 0
    mh_binary['mh_binary'] = mh_binary['mh_binary'].astype(int)
    mh_binary = mh_binary.loc[mh_binary['mh_binary']==1]
    mh_binary = pd.merge(cases['mci_uniq_id'], mh_binary, how='left', on=['mci_uniq_id'])
    mh_binary['mh_binary'].fillna(value=0, inplace=True)
    mh_binary['mh_binary'] = mh_binary['mh_binary'].astype(int)
    mh_binary = mh_binary[['mci_uniq_id', 'mh_binary', 'mh_start_dt']]
    ## count
    mh_count = mh_df_matches.copy()
    mh_count = mh_count.groupby(["mci_uniq_id"])["mh_count"].sum().reset_index()
    mh_count = mh_count[['mci_uniq_id', 'mh_count']]
    ## days
    mh_days = mh_df_matches.copy()
    mh_days = mh_days.groupby(["mci_uniq_id"])["mh_days"].sum().reset_index()
    mh_days = mh_days[['mci_uniq_id', 'mh_days']]
    return mh_df_matches, mh_binary, mh_count, mh_days

def get_er_outcomes(engine, cases):
    q = f"""
    select 
        unhashed_mci_uniq_id as mci_uniq_id,
        svc_start_dt as er_start_dt,
        1 as er_count
    from clean.cmu_physical_health_prm 
    where svc_start_dt > '{outreach_start_date}'::date and scu_cd='ER'
    """
    er_df = pd.read_sql(q, engine, parse_dates=['er_start_dt']) # outreach_start_date filter is just to reduce the number of rows returned (since we're not looking for any outcomes before that date)
    er_df_matches = pd.merge(cases, er_df, how='inner', on=['mci_uniq_id'])
    er_df_matches = er_df_matches.loc[(er_df_matches['er_start_dt']>er_df_matches['assignment_frozen_date']) & (er_df_matches['er_start_dt']<=er_df_matches['assignment_frozen_date']+DateOffset(months=outcome_threshold_months))]
    # get binary
    er_df_binary = er_df_matches.drop_duplicates(subset=['mci_uniq_id']) # binary outcome so only need one valid outcome date per person
    er_df_binary = er_df_binary[['mci_uniq_id', 'assignment_frozen_date', 'er_start_dt']]
    df = cases.copy()
    df = pd.merge(df, er_df_binary, how='left', on=['mci_uniq_id', 'assignment_frozen_date'])
    df['any_er'] = (~df['er_start_dt'].isna()).astype('int')
    # get count
    er_df_count = er_df_matches.groupby(["mci_uniq_id",'assignment_frozen_date'])["er_count"].sum()
    df = pd.merge(df, er_df_count, how='left', on=['mci_uniq_id', 'assignment_frozen_date'])
    df['er_count'].fillna(0, inplace=True)
    # get length of stay
    # er_df_days = er_df_matches.groupby(["mci_uniq_id",'assignment_frozen_date'])["er_days"].sum()
    # df = pd.merge(df, er_df_days, how='left', on=['mci_uniq_id', 'assignment_frozen_date'])
    # df['er_days'].fillna(0, inplace=True)
    return df

def get_er_outcomes_new(engine, cases):
    q = f"""
    select 
        unhashed_mci_uniq_id as mci_uniq_id,
        svc_start_dt as er_start_dt,
        1 as er_count
    from clean.cmu_physical_health_prm 
    where svc_start_dt > '{outreach_start_date}'::date and scu_cd='ER'
    """
    er_df = pd.read_sql(q, engine, parse_dates=['er_start_dt']) # outreach_start_date filter is just to reduce the number of rows returned (since we're not looking for any outcomes before that date)
    er_df_matches = pd.merge(cases, er_df, how='inner', on=['mci_uniq_id'])
    er_df_matches = er_df_matches.loc[(er_df_matches['er_start_dt']>er_df_matches['assignment_frozen_date']) & (er_df_matches['er_start_dt']<=er_df_matches['assignment_frozen_date']+DateOffset(months=outcome_threshold_months))]
    # now compute outcomes
    ## binary
    er_binary = er_df_matches.copy()
    er_binary['er_binary'] = er_binary.groupby('mci_uniq_id').cumcount() == 0
    er_binary['er_binary'] = er_binary['er_binary'].astype(int)
    er_binary = er_binary.loc[er_binary['er_binary']==1]
    er_binary = pd.merge(cases['mci_uniq_id'], er_binary, how='left', on=['mci_uniq_id'])
    er_binary['er_binary'].fillna(value=0, inplace=True)
    er_binary['er_binary'] = er_binary['er_binary'].astype(int)
    er_binary = er_binary[['mci_uniq_id', 'er_binary', 'er_start_dt']]
    ## count
    er_count = er_df_matches.copy()
    er_count = er_count.groupby(["mci_uniq_id"])["er_count"].sum().reset_index()
    er_count = er_count[['mci_uniq_id', 'er_count']]
    return er_df_matches, er_binary, er_count

def get_assignments(engine, remove_cases_before_nov=False):
    assignments_table = get_unique_assignments(engine, remove_cases_before_nov)
    assignments_table_new_tiers = recompute_control_tiers(assignments_table)
    assignments_table_new_tiers['selected_for_outreach'] = assignments_table_new_tiers['selected_for_outreach'].astype(bool)
    return assignments_table_new_tiers

def get_factors(engine, assignments_table):
    assignments_table = add_demographics(engine, assignments_table)
    assignments_table = add_prev_homeless(engine, assignments_table)
    assignments_table = add_prev_rental_assistance(engine, assignments_table)
    assignments_table = add_prev_bh(engine, assignments_table)
    assignments_table = add_prev_mh(engine, assignments_table)
    return assignments_table

def get_outcomes(engine, cases_df):
    cases_df = get_hl_outcomes(engine, cases_df)
    cases_df = get_bh_outcomes(engine, cases_df)
    cases_df = get_mh_outcomes(engine, cases_df)
    cases_df = get_er_outcomes(engine, cases_df)
    cases_df['selected_for_outreach'] = cases_df['selected_for_outreach'].astype(bool)
    return cases_df

def get_case_outcomes(engine, remove_cases_before_nov=False):
    # get assignments
    assignments_table = get_unique_assignments(engine, remove_cases_before_nov)
    # recompute tiers
    assignments_table_new_tiers = recompute_control_tiers(assignments_table)
    assignments_table_new_tiers['selected_for_outreach'] = assignments_table_new_tiers['selected_for_outreach'].astype(bool)
    cases_df = assignments_table_new_tiers
    # return assignments_table_new_tiers
    # get outcomes
    cases_id_min_date = cases_df.copy()[['mci_uniq_id', 'assignment_frozen_date', 'tier', 'trial_group']]
    cases_id_min_date.sort_values(by='assignment_frozen_date', ascending=True, inplace=True) 
    cases_id_min_date.drop_duplicates(subset=['mci_uniq_id'], inplace=True, keep='first') # we don't care which outreach selection date each outcome applies to, just the person id (to classify as treatment or control)
    cases_id_min_date = cases_id_min_date.loc[(cases_id_min_date['trial_group']=='treatment') | (cases_id_min_date['trial_group']=='control')]
    hl_outcomes, hl_binary, hl_count, hl_days = get_hl_outcomes_new(engine, cases_id_min_date)
    bh_outcomes, bh_binary, bh_count, bh_days = get_bh_outcomes_new(engine, cases_id_min_date)
    mh_outcomes, mh_binary, mh_count, mh_days = get_mh_outcomes_new(engine, cases_id_min_date)
    er_outcomes, er_binary, er_count = get_er_outcomes_new(engine, cases_id_min_date)
    cases_df['selected_for_outreach'] = cases_df['selected_for_outreach'].astype(bool)
    return {'cases': cases_df, 'trial_group': cases_id_min_date, 'outcomes': {'hl': {'spells': hl_outcomes, 'binary': hl_binary, 'count': hl_count, 'days': hl_days}, 'bh': {'spells': bh_outcomes, 'binary': bh_binary, 'count': bh_count, 'days': bh_days}, 'mh': {'spells': mh_outcomes, 'binary': mh_binary, 'count': mh_count, 'days': mh_days}, 'er': {'spells': er_outcomes, 'binary': er_binary, 'count': er_count}}}