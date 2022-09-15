import logging

from src.pipeline.cohort.hl_table_creator import create_hl_table
from utils import (check_if_table_exists, create_index, create_table,
                   get_db_conn, get_db_engine, insert_into_table, set_role,
                   timer)


def create_cohort_query(as_of_date, config):
    """Function to generate cohort sql query

    Args:
        as_of_date (str): Date used for analysis
        config (object): Config file which includes all necessary parameters

    Returns:
        str: Returns sql query text to generate cohort table
    """

    # select config paras
    filepath = config["cohort_config"]["filepath"]
    schema_name = config["db_config"]["schema_name"]

    with open(filepath, "r") as f:
        cohort_sql = f.read()

    cohort_query = cohort_sql.format(
        as_of_date=as_of_date, schema_name=schema_name, **config["cohort_config"]
    )

    return cohort_query


@timer
# TODO: instead of having this be separate function parameters, do it in args and kwargs instead
def create_cohort_table(config, as_of_dates):
    """Generates Cohort table in DB

    Args:
        config (object): Config file which includes all necessary info
        as_of_dates (list): list of as_of_dates contained in the cohort
    """

    # don't rerun if specified in config
    if not config.cohort_config.rerun and not config.rerun_all:
        logging.debug('not rerunning cohort')
        return

    # setup db connection
    db_engine = get_db_engine()
    db_conn = get_db_conn()

    # select config paras
    role_name = config["db_config"]["role_name"]
    schema_name = config["db_config"]["schema_name"]
    table_name = config["cohort_config"]["table_name"]

    set_role(db_conn, role_name)

    # create preprocessed homelessness table
    logging.info('generating homelessness table')
    create_hl_table(config)

    # inserts content into empty table
    for as_of_date in as_of_dates:

        # print(f"Adding cohort for date {as_of_date}.")

        # loads query text
        table_query = create_cohort_query(as_of_date, config)

        # check if table already exists
        exists = check_if_table_exists(db_engine, schema_name, table_name)

        # creates table if does not exist yet
        if not exists:
            create_table(
                db_conn,
                schema_name,
                table_name,
                table_query,
            )
            db_conn.execute("commit")

        # else inserts into existing table
        else:
            insert_into_table(
                db_conn,
                schema_name,
                table_name,
                table_query,
            )
            db_conn.execute("commit")

    create_index(db_conn, schema_name, table_name,
                 ["client_hash", "as_of_date"])
