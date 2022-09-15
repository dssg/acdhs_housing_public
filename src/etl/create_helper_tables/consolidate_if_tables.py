import yaml
from munch import munchify
from utils import *

with open("src/pipeline/config.yaml", "r") as config_file:
    config = munchify(yaml.safe_load(config_file))


@timer
def create_if_table(config, if_imputed_duration, table_name, data_type, schema_name):
    """Generates preprocessed involvement feed table in DB

    Args:
        config (object): Config file which includes all necessary info
    """

    # setup db connection
    db_conn = get_db_conn()

    # select config paras
    role_name = config["db_config"]["role_name"]
    file_path = 'src/etl/create_helper_tables/program_involvement_consolidated.sql'

    # set role, create schema, and drop table
    set_role(db_conn, role_name)
    create_schema(db_conn, schema_name)
    drop_table(db_conn, schema_name, table_name)

    # load sql query
    with open(file_path, "r") as f:
        table_query = f.read()

    table_query = table_query.format(
        if_imputed_duration=if_imputed_duration, data_type=data_type)

    # create table
    create_table(
        db_conn,
        schema_name,
        table_name,
        table_query,
    )
    db_conn.execute("commit")

    create_index(db_conn, schema_name, table_name,
                 ["client_hash", "project_type"])
    create_index(db_conn, schema_name, table_name, [
                 "client_hash", "project_type", "program_start_dt"])
    create_index(db_conn, schema_name, table_name, [
                 "client_hash", "project_type", "program_start_dt", "program_end_dt"])


# specifies hl spell duration in absence of daily information (involvement feed)
if_imputed_duration = "1 month"
# specifies which data type to use for the definition of homelessness. Could be hmis, feed, or both
schema_name = 'clean'
table_name = 'program_involvement_consolidated'
data_type = "('feed', 'hmis', 'hmis_ph_waiting_list')"


print("generating table", table_name, "in schema", schema_name)
create_if_table(config, if_imputed_duration,
                table_name, data_type, schema_name)
print("done generating table:", table_name)
