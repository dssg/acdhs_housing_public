import pandas as pd

from src.pipeline.utils.utils import get_db_conn


def _format_time_clause(knowledge_date_column, as_of_date, time_window, is_future):
    """Given the column name, and date constraints, return the time clause for filtering rows
    
        args:
            knowledge_date_column (str): The column name indicating the start of an event. Used to filter the incidents happened before/after the as_of_date
            as_of_date (str date YYYY-mm-dd): The date at which we conduct the anaylsis (e.g., when the predictions were made)
            time_window (str, PostgresSQL interval): The time interval we intereset in
            future (bool): Whether the time interval is for the future, or past.  
    
    """

    if is_future:
        time_clause = f"{knowledge_date_column} > '{as_of_date}' and {knowledge_date_column} < '{as_of_date}'::date + interval '{time_window}'"
    else:
        time_clause = f"{knowledge_date_column} < '{as_of_date}' and {knowledge_date_column} > '{as_of_date}'::date - interval '{time_window}'"

    return time_clause


def _get_program_feed_involvement(db_conn, client_hashes, as_of_date, time_window, is_future):
    """ Given a list of clients, return their program feed service involvement summary 

        args:
            db_conn: Connection to the database (SQLAlchemy)
            client_hashes (List[str]): The list of clients we are interested in
            as_of_date: The date at which the predictions were generated
            time_window: The time interval we are interested in. Has to be a PostgreSQL "interval" type
            future (Bool): Where the time interval is for the future or past. Defaults to future (True) 

    """
    client_hashes = ["'"+ x + "'" for x in client_hashes]   

    # Involvement Feed
    time_clause  = _format_time_clause('prog_dt', as_of_date, time_window, is_future)   
    q = f"""
        select 
            client_hash,
            program_key,
            program_name,
            min(prog_dt) as first_involvement,
            max(prog_dt) as latest_involvement,
            count(*) as num_programs
        from clean.involvement_feed left join lookup.program_feed using(program_key)
        where client_hash in ({",".join(client_hashes)})
        and {time_clause}
        group by 1, 2, 3
        order by 4 desc;
    """

    feed = pd.read_sql(q, db_conn)

    feed['table'] = 'feed'


    return feed
  

def _get_mental_health_service_utilization(db_conn, client_hashes, as_of_date, time_window, is_future):
    """ Given a list of client hashes give the mental health events that happen to them in the given time horizon """

    client_hashes = ["'"+ x + "'" for x in client_hashes]

    time_clause  = _format_time_clause('event_start_date', as_of_date, time_window, is_future)
    q = f"""
        select 
            mci_uniq_id as client_hash,
            lower(event_type) as event_type,
            min(event_start_date) as first_involvement,
            max(event_start_date) as latest_involvement,
            count(*) as num_mh_events
        from clean.cmu_mh_prm
        where mci_uniq_id in ({",".join(client_hashes)})
        and {time_clause}
        group by 1, 2
        order by 3 desc;
    """

    mh = pd.read_sql(q, db_conn)
    mh['table'] = 'mh'

    return mh


def _get_behavioral_health_diagnoses(db_conn, client_hashes, as_of_date, time_window, is_future, diagnoses_codes):
    """Given a list of client hashes get specific diagnoses in a given time horizon"""

    client_hashes = ["'"+ x + "'" for x in client_hashes]
    diagnoses_codes = ["'"+ x + "'" for x in diagnoses_codes]

    # probably this should be the event-end_date?? given that its a diagnoses?
    time_clause  = _format_time_clause('event_beg_date', as_of_date, time_window, is_future)

    q = f"""
        select 
            mci_uniq_id as client_hash,
            split_part(diagnosis_code, '.', 1),
            lower(diagnosis_sub_category) as diagnosis_sub_category,
            min(event_beg_date) as first_involvement,
            max(event_beg_date) as latest_involvement,
            count(*) as num_bh_events
        from clean.cmu_behavior_health_prm
        where mci_uniq_id in ({",".join(client_hashes)})
        and {time_clause}
        and split_part(diagnosis_code, '.', 1) in ({",".join(diagnoses_codes)})
        group by 1, 2, 3
        order by 4 desc;
    """

    bh = pd.read_sql(q, db_conn)
    bh['table'] = 'bh'

    return bh



def _get_behavioral_health_service_utilization(db_conn, client_hashes, as_of_date, time_window, is_future):
    """Given a list of client hashes get the behavioral health events that happen to them in the given time horizon """

    client_hashes = ["'"+ x + "'" for x in client_hashes]

    time_clause  = _format_time_clause('event_beg_date', as_of_date, time_window, is_future)

    q = f"""
        select 
            mci_uniq_id as client_hash,
            event_type,
            diagnosis_sub_category,
            min(event_beg_date) as first_involvement,
            max(event_beg_date) as latest_involvement,
            count(*) as num_bh_events
        from clean.cmu_behavior_health_prm
        where mci_uniq_id in ({",".join(client_hashes)})
        and {time_clause}
        group by 1, 2, 3
        order by 4 desc;
    """

    bh = pd.read_sql(q, db_conn)
    bh['table'] = 'bh'

    return bh


def _get_eviction_case_counts(db_conn, client_hashes, as_of_date, time_window, is_future):
    """Given a list of client hashes get the eviction cases that were filed against them in the given time horizon """

    client_hashes = ["'"+ x + "'" for x in client_hashes]

    time_clause = _format_time_clause('filingdt', as_of_date, time_window, is_future)

    q = f"""
        select 
            hashed_mci_uniq_id as client_hash, 
            min(filingdt) as first_filing, 
            max(filingdt) as latest_filing,
            count(matter_id) as num_eviction_cases
        from clean.eviction_client_matches join clean.eviction using(matter_id) 
        where hashed_mci_uniq_id in ({",".join(client_hashes)})
        and {time_clause}
        group by 1
    """

    evictions = pd.read_sql(q, db_conn)
    evictions['table'] = 'eviction'

    return evictions


def _get_demographics(db_conn, client_hashes):
    """Get the demographics for a given set of clients"""

    client_hashes = ["'"+ x + "'" for x in client_hashes]

    q = f"""
        select distinct on (client_hash)
            client_hash,
            dob,
            extract(year from age(current_date, dob))::int as age,
            gender,
            legal_sex,
            race,
            split_part(std_zip, '.', 1) as zipcode
        from clean.client_feed 
        where client_hash in ({",".join(client_hashes)})
        order by client_hash, dob;
    """

    demo = pd.read_sql(q, db_conn)

    return demo


def _get_model_scores(db_conn, client_hashes, model_id, as_of_date, use_production_schema=True):
    """Given a set of clients and a model_id fetch the mdoel predictions and """

    client_hashes = ["'"+ x + "'" for x in client_hashes]

    if use_production_schema:
        q = f"""
            select 
                client_hash,
                as_of_date,
                prediction_date,
                round(score, 3) as score,
                rank_abs_no_ties as rank_abs,
                label_value as true_label
            from acdhs_production.predictions
            where model_id={model_id}
            and as_of_date = '{as_of_date}'::date
            and client_hash in ({",".join(client_hashes)})
            order by score desc;
        """
    else:    
        q = f"""
        select 
            client_hash,
            score,
            rank_abs_no_ties as rank_abs,
            label_value as true_label
        from test_results.predictions p 
        join pretriage.client_id_mapping cim on p.entity_id = cim.client_id
        where model_id={model_id}
        and as_of_date='{as_of_date}'::date
        and client_hash in ({",".join(client_hashes)})
        order by score desc;
    """ 

    scores = pd.read_sql(q, db_conn)
    
    return scores


def _get_homeless_service_utilization(db_conn, client_hashes, as_of_date, time_window, is_future):
    """Given a set of clients, fetch details of homeless service"""


    client_hashes = ["'"+ x + "'" for x in client_hashes]

    time_clause = _format_time_clause('program_start_dt', as_of_date, time_window, is_future)

    q = f"""
         select                                  
            client_hash,
            (coalesce(program_end_dt, '{as_of_date}'::date) - program_start_dt) + 1 as days_hl,
            1::int as homeless_spell
        from pretriage.hl_table
        where client_hash in ({",".join(client_hashes)})
        and {time_clause}    
    """

    hl = pd.read_sql(q, db_conn)

    if hl.empty:
        hl['client_hash'] = client_hashes

    return hl


def get_client_hashes(db_conn, as_of_date, model_group_id):
    q = f"""
        select distinct client_hash from triage_metadata.models
        join public.cohort_default_2106c0bf26588dd38df11478f1dbd20a cohort on cohort.as_of_date = models.train_end_time
        join pretriage.client_id_mapping cim on cim.client_id = cohort.entity_id
        where model_group_id = {model_group_id} and train_end_time = '{as_of_date}' and client_hash != ''
    """
    clients = pd.read_sql(q, db_conn)
    return clients


def get_service_involvement_clients_time_window(db_conn, client_hashes, as_of_date, time_window, specific_programs=None, is_future=True):
    """ Given a list of clients, return a summery of their past and future (if possible) service involvement

        args:
            db_conn: Connection to the database (SQLAlchemy)
            client_hashes (List[str]): The list of clients we are interested in
            as_of_date: The date at which the predictions were generated
            time_window: The time interval we are interested in. Has to be a PostgreSQL "interval" type
            future (Bool): Where the time interval is for the future or past. Defaults to future (True) 

    """
    

    feed = _get_program_feed_involvement(db_conn, client_hashes, as_of_date, time_window, is_future)

    bh = _get_behavioral_health_service_utilization(db_conn, client_hashes, as_of_date, time_window, is_future)

    mh = _get_mental_health_service_utilization(db_conn, client_hashes, as_of_date, time_window, is_future)

    evictions = _get_eviction_case_counts(db_conn, client_hashes, as_of_date, time_window, is_future)

    hl = _get_homeless_service_utilization(db_conn, client_hashes, as_of_date, time_window, is_future)

    
    # Generating the client summaries
    # TODO: Move these groupby clauses into the respective functions
    feed_grp = feed.groupby('client_hash').count()['num_programs']
    bh_grp = bh.groupby('client_hash').count()['num_bh_events']
    mh_grp = mh.groupby('client_hash').count()['num_mh_events']
    eviction_grp = evictions.groupby('client_hash').count()['num_eviction_cases']
    hl_grp = hl.groupby('client_hash').sum()['days_hl']

    feed_count_dfs = []
    if specific_programs:
        for prog_key in specific_programs:
            df = feed.groupby('client_hash')['program_key'].apply(lambda x: (x==prog_key).sum()) #.reset_index(name=f'count_{prog_key}'))
            df = df.rename(f"count_{prog_key}")
            feed_count_dfs.append(df)

    # When using concat, it's important that all the Series have the client hash as their index to ensure validity
    # creating a df with all the indexes and no columns to ensure that the indexes are preserved through the concat
    df = pd.DataFrame(index=client_hashes)

    merged_df = pd.concat([df, feed_grp, bh_grp, mh_grp, eviction_grp, hl_grp] + feed_count_dfs, axis=1).fillna(0)

    if is_future:
        time_suffix = f'_next_{time_window}'
    else:
        time_suffix = f'_past_{time_window}'

    merged_df.columns = [x + time_suffix for x in merged_df.columns]

    return merged_df


def generate_csv(conn, clients, as_of_date, time_windows, model_id, specific_programs=None, get_future_stats=False, save_target=None):
    """
        For the given time windows get past and future service involvement
    """

    csv_df  = pd.DataFrame()
    for i, time_window in enumerate(time_windows):
        
        df = get_service_involvement_clients_time_window(
            conn,
            clients,
            as_of_date=as_of_date,
            specific_programs=specific_programs,
            time_window=time_window,
            is_future=False
        )

        if get_future_stats:
            df_next = get_service_involvement_clients_time_window(
                conn,
                clients,
                as_of_date=as_of_date,
                time_window=time_window,
                is_future=True
            )

            
            df = df.join(df_next, how='left')

        if i > 0:
            csv_df = csv_df.join(df)
        else:
            csv_df = df

    demographics = _get_demographics(conn, clients).set_index('client_hash')
    scores = _get_model_scores(conn, clients, model_id, as_of_date, use_production_schema=False).set_index('client_hash')

    csv_df = demographics.join(scores).join(csv_df).sort_values('score', ascending=False)

    if save_target:
        csv_df.to_csv(save_target)

    
    return csv_df



if __name__ == '__main__':
    conn = get_db_conn()

    clients = ['22ECD5638C', '000161871A']
    time_windows = ['1year', '2year']
    as_of_date = '2021-09-01'
    model_id=81

    df = generate_csv(
        conn,
        clients,
        as_of_date,
        time_windows,
        model_id,
        'test_csv.csv'
    )

    print(df)



    # print(df.head())
