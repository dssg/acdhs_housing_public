from utils import *


@timer
def create_label_table(config):

    # don't rerun if specified in config
    if not config.label_config.rerun and not config.rerun_all:
        logging.debug('not rerunning labels')
        return

    # setup db connection
    db_engine = get_db_engine()
    db_conn = get_db_conn()

    # select config paras
    file_path = config["label_config"]["filepath"]
    role_name = config["db_config"]["role_name"]
    schema_name = config["db_config"]["schema_name"]
    table_name = config["label_config"]["table_name"]
    cohort_table = config["cohort_config"]["table_name"]
    label_timespan = config["temporal_config"]["label_timespan"]

    set_role(db_conn, role_name)
    create_schema(db_conn, schema_name)
    drop_table(db_conn, schema_name, table_name)

    with open(file_path, "r") as f:
        table_query = f.read()

    table_query = table_query.format(
        schema_name=schema_name,
        cohort_table=cohort_table,
        label_timespan=label_timespan,
    )

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
