import logging

from pipeline.utils.utils import get_db_engine


def generate_location_level_eviction_aggregates(engine, aggregate_level, table_schema='pretriage'):
    ''' This is not generalized yet    
    '''
    
    table_name = f'eviction_aggregates_at_{aggregate_level}_level'
    
    q = f'''
    
    set role rg_staff;
    
    drop table if exists {table_schema}.{table_name};
    
    create table {table_schema}.{table_name} as (
        with base as (
            select 
                matter_id,
                max(ofp_issue_dt) as ofp_issue_dt,
                max(e.judgement_for_landlord::int) as judgement_for_landlord,
                max(e.judgement_for_tenant::int) as judgement_for_tenant,
                max(e.settled::int) as settled,
                lower(max({aggregate_level}::text)) as {aggregate_level}, 
                min(filingdt) as filingdt,
                bool_or(program_start_dt is not null)::int as case_led_to_hl 
            from pretriage.eviction_client_matches_id e left join pretriage.homelessness_id hi
            on e.client_id = hi.client_id 
            and hi.program_start_dt > e.filingdt 
            and hi.program_start_dt <= e.filingdt + '1 year'::interval
            group by 1
        )
        select 
            {aggregate_level}, 
            -- last day of the month ( we are doing a monthly aggregate)
            (date_trunc('month', filingdt)::date + '1 month'::interval - '1 day'::interval)::date as agg_knowledge_date, 
            max(date_trunc('month', filingdt)::date + '1 year'::interval - '1 day'::interval)::date as label_knowledge_date,
            count(distinct matter_id) as case_count,
            count(ofp_issue_dt) as ofp_count, 
            sum(judgement_for_landlord::int) as ll_win_count, 
            sum(judgement_for_tenant::int) as ll_loss_count, 
            sum(settled::int) as settled_count, 
            sum(case_led_to_hl) as led_to_hl_count,
            case when count(distinct matter_id) > 0 then count(ofp_issue_dt)::float / count(distinct matter_id) else null end as ofp_rate, 
            case when count(distinct matter_id) > 0 then sum(judgement_for_landlord::int)::float / count(distinct matter_id) else null end as ll_win_rate, 
            case when count(distinct matter_id) > 0 then sum(judgement_for_tenant::int)::float / count(distinct matter_id) else null end as ll_loss_rate, 
            case when count(distinct matter_id) > 0 then sum(settled::int)::float / count(distinct matter_id) else null end as settle_rate,
            case when count(distinct matter_id) > 0 then round(sum(case_led_to_hl)::numeric / count(distinct matter_id), 4) else null end as label_rate
        from base
        where {aggregate_level} != '' and {aggregate_level} is not null
        group by 1, 2
    );
    
    create index on {table_schema}.{table_name}({aggregate_level});
    create index on {table_schema}.{table_name}(agg_knowledge_date);
    create index on {table_schema}.{table_name}(label_knowledge_date);
    '''
    logging.info(q)
    
    with engine.begin() as conn:
        conn.execute(q)
        
def generate_landlord_level_eviction_aggregates(engine, table_schema='pretriage', table_name='landlord_level_eviction_aggregates'):
    
    # table_name = f'landlord_level_eviction_aggregates'
    
    q = f'''
    
    set role rg_staff;
    
    drop table if exists {table_schema}.{table_name};
    
    create table {table_schema}.{table_name} as (
        with base as (
            SELECT 
                matter_id, 
                min(filingdt) as filing_date, 
                max(lower(unique_displaynm)) as landlord,
                max(ofp_issue_dt) as ofp_date,
                bool_or(program_start_dt is not null)::int as led_to_hl
            FROM pretriage.eviction_client_matches_id ecmi left join clean.eviction_landlords el using(matter_id)
                left join pretriage.homelessness_id hi
                        on ecmi.client_id = hi.client_id 
                        and hi.program_start_dt > ecmi.filingdt 
                        and hi.program_start_dt <= ecmi.filingdt + '1year'::interval
            group by 1
            order by 1, 2
        )
        select 
            landlord, 
            (date_trunc('month', filing_date)::date + '1 month'::interval - '1 day'::interval)::date as agg_knowledge_date,
            max(date_trunc('month', filing_date)::date + '1 year'::interval - '1 day'::interval)::date as label_knowledge_date,
            count(distinct matter_id) as case_count, 
            sum(led_to_hl) as cases_led_to_hl,
            sum(led_to_hl)::float / count(distinct matter_id) as label_rate
        from base
        where landlord != '' and landlord is not null
        group by 1, 2
    );
    
    create index on {table_schema}.{table_name}(landlord);
    create index on {table_schema}.{table_name}(agg_knowledge_date);
    create index on {table_schema}.{table_name}(label_knowledge_date);
    
    '''

    logging.info(q)
    
    with engine.begin() as conn:
        conn.execute(q)

 
engine = get_db_engine()
aggregate_levels = ['city', 'districtcourtno', 'zip_cd']

for agg in aggregate_levels:
    generate_location_level_eviction_aggregates(engine, agg)