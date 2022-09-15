import yaml
from munch import munchify
from utils import *


with open("src/pipeline/config.yaml", "r") as config_file:
    config = munchify(yaml.safe_load(config_file))

@timer
def create_state_programs_table(config, schema_name, table_name):
    """Generates consolidated state programs data in DB

    Args:
        config (object): Config file which includes all necessary info
    """

    # setup db connection
    db_conn = get_db_conn()

    # select config paras
    role_name = config["db_config"]["role_name"]
    file_path = 'src/etl/create_helper_tables/state_programs_consolidated.sql'

    # set role, create schema, and drop table
    set_role(db_conn, role_name)
    create_schema(db_conn, schema_name)
    drop_table(db_conn, schema_name, table_name)

    # load sql query
    with open(file_path, "r") as f:
        table_query = f.read()

    # create table
    create_table(
        db_conn,
        schema_name,
        table_name,
        table_query,
    )
    db_conn.execute("commit")

    create_index(db_conn, schema_name, table_name,
                 ["client_hash", "category"])
    create_index(db_conn, schema_name, table_name, [
                 "client_hash", "category", "elig_begin_date"])
    create_index(db_conn, schema_name, table_name, [
                 "client_hash", "category", "elig_begin_date", "elig_end_date"])


schema_name = 'clean'
table_name = 'state_programs_consolidated'

print("generating table", table_name, "in schema", schema_name)
create_state_programs_table(config, schema_name = schema_name, table_name = table_name)
print(f"done generating table {table_name} in schema {schema_name}")
