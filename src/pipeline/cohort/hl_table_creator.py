from utils import *


@timer
def create_hl_table(config):
    """Generates preprocessed homelessness table in DB

    Args:
        config (object): Config file which includes all necessary info
    """

    # setup db connection
    db_conn = get_db_conn()

    # select config paras
    role_name = config["db_config"]["role_name"]
    schema_name = config["db_config"]["schema_name"]
    file_path = config["hl_definition_config"]["filepath"]
    table_name = config["hl_definition_config"]["table_name"]

    # set role, create schema, and drop table
    set_role(db_conn, role_name)

    # load sql query
    with open(file_path, "r") as f:
        table_query = f.read()

    table_query = table_query.format(**config["hl_definition_config"])

    # create table
    create_table(
        db_conn,
        schema_name,
        table_name,
        table_query,
    )
    db_conn.execute("commit")

    create_index(db_conn, schema_name, table_name, ["client_hash"])
    create_index(db_conn, schema_name, table_name, ["program_start_dt"])
    create_index(db_conn, schema_name, table_name, [
                 "client_hash", "program_start_dt"])
    create_index(db_conn, schema_name, table_name, [
                 "client_hash", "program_start_dt", "data_type"])
