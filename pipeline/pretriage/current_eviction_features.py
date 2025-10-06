
import pandas as pd
from pretriage.precompute_features import generate_most_recent_features
from pretriage.precompute_features import generate_aggregate_most_recent_features


def _generate_date_series(engine, start_date, end_date, interval):

    q = f"""
        select generate_series(
            '{start_date}'::date,
            '{end_date}'::date,
            '{interval}'::interval
        )::date::varchar as as_of_dates
    """

    return pd.read_sql(q, engine).as_of_dates.tolist()

def generate_current_eviction_features(engine, start_date, end_date, interval='1 month'):

    ## filingdt features, specific to eviction
    source_table = 'pretriage.eviction_client_matches_id ecmi left join clean.eviction_landlords el on ecmi.matter_id = el.matter_id'
    source_columns = [
        '*'
    ]
    date_column = 'filingdt'
    distinct_on_column = 'client_id'
    quantities =[
        'client_id as entity_id', 
        'filingdt',
        "'{as_of_date}'::date - filingdt::date as days_since_filing",
        'city',
        'zip_cd',
        'districtcourtno',
        'claimamount',
        'monthlyrentamount',
        'tenanthasrepresentation',
        'landlordhasrepresentation',
        'city_of_pgh_flag',
        'lower(unique_displaynm) as landlord',
        'participantcategory as landlord_category', 
        "case when unique_displaynm='HACP' or unique_displaynm='ACHA' then 1 else 0 end as is_HA",
    ]

    # as_of_dates=_generate_date_series(engine, '2015-01-28', '2023-11-28', '1 month')
    as_of_dates=_generate_date_series(engine, start_date, end_date, interval)

    generate_most_recent_features(
        engine=engine,
        from_obj=source_table,
        source_columns=source_columns,
        date_column=date_column,
        distinct_on_column=distinct_on_column,
        quantities=quantities,
        as_of_dates=as_of_dates,
        target_table='pretriage.most_recent_eviction'
    )
    
    ## dispositiondt features, specific to eviction
    source_table = 'pretriage.eviction_client_matches_id ecmi'
    source_columns = [
        '*'
    ]
    date_column = 'dispositiondt'
    distinct_on_column = 'client_id'
    quantities =[
        'client_id as entity_id', 
        'dispositiondt',
        "'{as_of_date}'::date - dispositiondt::date as days_since_judgment",
        'judgement_for_tenant',
        'judgement_for_landlord',
        'settled',
        'withdrawn',
        'dismissed'
    ]

    generate_most_recent_features(
        engine=engine,
        from_obj=source_table,
        source_columns=source_columns,
        date_column=date_column,
        distinct_on_column=distinct_on_column,
        quantities=quantities,
        as_of_dates=as_of_dates,
        target_table='pretriage.most_recent_eviction_dspndt'
    )

    ## ofp_issue_dt features, specific to eviction
    source_table = 'pretriage.eviction_client_matches_id ecmi'
    source_columns = [
        '*'
    ]
    date_column = 'ofp_issue_dt'
    distinct_on_column = 'client_id'
    quantities =[
        'client_id as entity_id', 
        'ofp_issue_dt',
        "'{as_of_date}'::date - ofp_issue_dt::date as days_since_ofp",
        'order_for_possession'
    ]

    generate_most_recent_features(
        engine=engine,
        from_obj=source_table,
        source_columns=source_columns,
        date_column=date_column,
        distinct_on_column=distinct_on_column,
        quantities=quantities,
        as_of_dates=as_of_dates,
        target_table='pretriage.most_recent_eviction_ofpdt'
    )

    # TODO - I think this is more efficient if we precreate the lanlord stats at eviction time by doing a self join
    ## filingdt features, aggregated over landlord table
    source_table = 'clean.eviction_landlords el join clean.eviction e on el.matter_id = e.matter_id'
    source_columns = [
        'e.matter_id as matter_id',
        'max(el.current_best_uniqnm) as landlord_name',
        "case when max(ofp_issue_dt) < '{as_of_date}'::date then max(ofp_issue_dt) else null end as ofp_issue_dt",
        'bool_or(judgement_for_landlord) as judgement_for_landlord',
        'bool_or(judgement_for_tenant) as judgement_for_tenant',
        'bool_or(settled) as settled'
    ]
    source_groupby_column = 'e.matter_id'
    date_column = 'filingdt'
    # aggregate fields

    agg_quantities = [
        'landlord_name',
        # 'matter_id as case_id',
        'count(distinct matter_id) as case_count',
        'count(distinct ofp_issue_dt) as ofp_count',
        'sum(judgement_for_landlord::int) as win_count',
        'sum(judgement_for_tenant::int) as loss_count',
        'sum(settled::int) as settled_count',
        'count(distinct ofp_issue_dt)::float / count(distinct matter_id) as ofp_rate',
        'sum(judgement_for_landlord::int)::float / count(distinct matter_id) as win_rate',
        'sum(judgement_for_tenant::int)::float / count(distinct matter_id) as loss_rate',
        'sum(settled::int)::float / count(distinct matter_id) as settle_rate'
    ]
    agg_groupby_column = 'landlord_name'
    
    # ToDO - Improve this set up where we do not expose the CTE name we use in the internal query to the source table name (i.e., `agg_table`).
    # The current implementation requires the user to know that the CTE's name is agg_table, which is not ideal. 
    agg_source_table = 'pretriage.eviction_client_matches_id ecmi left join (agg_table at left join clean.eviction_landlords el on el.current_best_uniqnm = at.landlord_name) la2 on ecmi.matter_id = la2.matter_id'
    distinct_on_column = 'client_id'
    quantities =[
        'client_id as entity_id', 
        'ecmi.matter_id',
        'filingdt',
        'case_count',
        'ofp_count',
        'win_count', 
        'loss_count', 
        'settled_count', 
        'ofp_rate',
        'win_rate',
        'loss_rate',
        'settle_rate'
    ]

    generate_aggregate_most_recent_features(
        engine=engine,
        base_table_name='eviction_landlord',
        from_obj=source_table,
        source_columns=source_columns,
        source_groupby_column=source_groupby_column,
        date_column=date_column,
        agg_quantities=agg_quantities,
        agg_groupby_column=agg_groupby_column,
        agg_source_table=agg_source_table,
        distinct_on_column=distinct_on_column,
        quantities=quantities,
        as_of_dates=as_of_dates,
        target_table='pretriage.most_recent_eviction_landlord'
    )