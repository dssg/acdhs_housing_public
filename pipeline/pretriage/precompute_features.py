import os
import logging
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger.addHandler(logging.StreamHandler())

from sqlalchemy.event import listens_for
from sqlalchemy.pool import Pool


# assume group role to ensure shared permissions
@listens_for(Pool, "connect")
def assume_role(dbapi_con, connection_record):
    cur = dbapi_con.cursor()
    cur.execute(f"set role {os.getenv('PGROLE')};")
    cur.execute(f"select current_user;")
    logging.debug(f'Listened for connection and changed role to {cur.fetchone()[0]}')


def generate_most_recent_features(engine, from_obj, source_columns, date_column, distinct_on_column, quantities, as_of_dates, target_table, db_role=None):
    """ Generating a feature table that contains information about a "most recent" event. 
        Meant as a function to run prior to running a triage eperiment and outputs a table that can be used as a `from_obj` in the feature config. 
        Currently triage deosn't allow a natural way of creating these types of features in the feature config directly.  

        Args:
            engine: SQLAlchemy engine
            from_obj (str): The original table from which the features are created. Could be a table name, a join statement, or a subquery
            source_columns (List[str]): The columns from the from_obj that is used in creating the most recent type features
            date_column (str): The column we can use to filter records prior to an as_of_date (aka knowledge_date in triage land)
            distinct_on_column (str): the column which we want to select unique rows (e.g., could be the entity_id)
            quantities (List[str]): A list of fields that we want to include/calculate. These could be column names, or inline calculations
            as_of_dates (List[str]): A list of as_of_dates for which we want to caluclate the features. 
                These will be the knowledge dates in the triage feature calculation
            target_table (str): The name of the final table we want to create (has to be in the format <schema_name>.<table_name>)
            db_role (str, optional): The database role name to use for table creation 
    """
    
    query_template = ""
    
    if db_role is not None:
        query_template += f"set role '{db_role}';"
    
    query_template += """  
    drop table if exists {temp_table_name};
        
    create table {temp_table_name} as ( 
        with date_filtered as (
            select 
            {source_columns} 
            from {from_obj}
            where {date_column} < '{as_of_date}'::date
        )
        select distinct on ({distinct_on_column})
            {quantities},
            '{as_of_date}'::date as knowledge_date
        from date_filtered
        order by {distinct_on_column}, {date_column} desc
    )
    """

    source_columns = ', '.join(source_columns)
    quantities = ', '.join(quantities)

    temp_tables = list()
    
    for as_of_date in as_of_dates:

        date_str = ''.join(as_of_date.split('-'))
        source_t_no_schema = from_obj.split()[0].split('.')[1]

        table_name = f'most_recent_{source_t_no_schema}_{date_str}'
        quantites_formatted = quantities.format(as_of_date=as_of_date)

        q = query_template.format(
            temp_table_name=table_name,
            from_obj=from_obj,
            source_columns=source_columns,
            date_column=date_column,
            distinct_on_column=distinct_on_column,
            quantities=quantites_formatted,
            as_of_date=as_of_date
        )

        logger.info(f'Creating the temp table {table_name}:')
        logger.info(q)

        with engine.begin() as conn:
            conn.execute(q)
            temp_tables.append(table_name)

        logger.info('Success!')

    logger.info('All temp tables created. Creating the final table...')
    
    set_role_statement = ''
    if db_role is not None:
        set_role_statement = f"set role '{db_role}';"

    q = f'''
    DROP TABLE IF EXISTS {target_table};
    
    {set_role_statement}
    
    CREATE TABLE {target_table} as (
    '''

    for i, tt in enumerate(temp_tables):
        if i > 0 :
            q+= 'UNION ALL'
        
        q += f'''
        select * from {tt}
        '''
    
    q += ');'

    logger.info(q)

    with engine.begin() as conn:
        conn.execute(q)

    q = f"alter table {target_table} owner to rg_staff" 

    with engine.begin() as conn:
        logger.info(f"alter table {target_table} owner to rg_staff" )
        conn.execute(q)

    logger.info('Dropping the temp tables')
    for tt in temp_tables:
        q = f'drop table {tt}'

        with engine.begin() as conn:
            conn.execute(q)

        logger.info(f'Dropped {tt}')
    


def generate_aggregate_most_recent_features(engine, base_table_name, from_obj, source_columns, source_groupby_column, date_column, agg_quantities, agg_groupby_column, agg_source_table, distinct_on_column, quantities, as_of_dates, target_table):
    """ Generating a feature table that contains aggregated information about a "most recent" event. 
        Meant as a function to run prior to running a triage eperiment and outputs a table that can be used as a `from_obj` in the feature config. 
        Currently triage deosn't allow a natural way of creating these types of features in the feature config directly.  

        Args:
            engine: SQLAlchemy engine
            from_obj (str): The original table from which the features are created. Could be a table name, a join statement, or a subquery
            source_columns (List[str]): The columns from the from_obj that is used in creating the most recent type features
            date_column (str): The column we can use to filter records prior to an as_of_date (aka knowledge_date in triage land)
            distinct_on_column (str): the column which we want to select unique rows (e.g., could be the entity_id)
            quantities (List[str]): A list of fields that we want to include/calculate. These could be column names, or inline calculations
            as_of_dates (List[str]): A list of as_of_dates for which we want to caluclate the features. 
                These will be the knowledge dates in the triage feature calculation
            target_table (str): The name of the final table we want to create (has to be in the format <schema_name>.<table_name>)
    """
    
    query_template= """
    drop table if exists {temp_table_name};
    
    create table {temp_table_name} as ( 
        with date_filtered as (
            select 
            {source_columns} 
            from {from_obj}
            where {date_column} < '{as_of_date}'::date
            group by {source_groupby_column}
        ),
        agg_table as (
            select 
            {agg_quantities}
            from date_filtered
            group by {agg_groupby_column}
        )
        select distinct on ({distinct_on_column})
            {quantities},
            '{as_of_date}'::date as knowledge_date
        from {agg_source_table}
        where {date_column} < '{as_of_date}'::date
        order by {distinct_on_column}, {date_column} desc
    )
    """

    source_columns = ', '.join(source_columns)
    
    agg_quantities = ', '.join(agg_quantities)
    quantities = ', '.join(quantities)

    temp_tables = list()
    
    for as_of_date in as_of_dates:

        date_str = ''.join(as_of_date.split('-'))
        # source_t_no_schema = from_obj.split()[0].split('.')[1]
        
        table_name = f'most_recent_{base_table_name}_{date_str}'
        quantites_formatted = quantities.format(as_of_date=as_of_date)

        q = query_template.format(
            temp_table_name=table_name,
            from_obj=from_obj,
            source_columns=source_columns.format(as_of_date=as_of_date),
            source_groupby_column=source_groupby_column,
            date_column=date_column,
            distinct_on_column=distinct_on_column,
            quantities=quantites_formatted,
            as_of_date=as_of_date,
            agg_quantities=agg_quantities,
            agg_groupby_column=agg_groupby_column,
            agg_source_table=agg_source_table
        )

        logger.info(f'Creating the temp table {table_name}:')
        logger.info(q)

        with engine.begin() as conn:
            conn.execute(q)
            temp_tables.append(table_name)

        logger.info('Success!')

    logger.info('All temp tables created. Creating the final table...')

    q = f'''
    DROP TABLE IF EXISTS {target_table};

    CREATE TABLE {target_table} as (
    '''

    for i, tt in enumerate(temp_tables):
        if i > 0 :
            q+= 'UNION ALL'
        
        q += f'''
        select * from {tt}
        '''
    
    q += ');'

    logger.info(q)

    with engine.begin() as conn:
        conn.execute(q)

    q = f"alter table {target_table} owner to rg_staff" 
    with engine.begin() as conn:
        logger.info(f"alter table {target_table} owner to rg_staff" )
        conn.execute(q)

    logger.info('Dropping the temp tables')
    for tt in temp_tables:
        q = f'drop table {tt}'

        with engine.begin() as conn:
            conn.execute(q)

        logger.info(f'Dropped {tt}')
    