"""
Use this script to create feature-label-pairs for each individual who has faced an eviction before a specific date.
The function create_new_feature_table() creates a new table in the db schema 'modelling'.
The function extend_and_save_table() takes an existing feature-label-table and extends it with new columns.
Make sure to specify the table names and comment out the function that should not be used.
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import time

# get credentials from environment variables
user = os.getenv('PGUSER')
password = os.getenv('PGPASSWORD')
host = os.getenv('PGHOST')
port = os.getenv('PGPORT')
database = os.getenv('PGDATABASE')

# configure connection to postgres
engine = create_engine("postgresql://{}:{}@{}:{}/{}".format(user, password, host, port, database))

# open a connect
db_conn = engine.connect()

# run query to generate features and save them as a table

def extend_sql_query(program_keys, columns_to_avoid=[]):
	
	add_features_for_program_keys1 = """"""
	add_features_for_program_keys2 = """"""
	add_features_for_program_keys3 = """"""

	#for key in program_keys[:2]:
	for key in program_keys:
		if "nr_of_months_in_program_" + str(key) in columns_to_avoid:
			print(key, "is omitted because a column with the same name already exists")
		else:
			add_features_for_program_keys1 += """,
		part_of_program_""" + str(key) + """ as (
			select client_hash, count(*) as nr_of_months_in_program_""" + str(key) + """
			from clean.involvement_feed if2
			where program_key = """ + str(key) + """
			and prog_dt < '2019-01-01'::date
			group by client_hash
		)"""
			add_features_for_program_keys2 += """,
			case
				when nr_of_months_in_program_""" + str(key) + """ > 0 then 1 else 0
			end p_""" + str(key)
			add_features_for_program_keys3 += """
				left join part_of_program_""" + str(key) + """ using(client_hash)"""

	return add_features_for_program_keys1, add_features_for_program_keys2, add_features_for_program_keys3


def create_new_feature_table(program_keys):
    # read the file
    with open("feature_generator.sql", "r") as f:
        sql_template = f.read()

    table_name = "acdhs_program_participation_and_evictions"
    current_date="""'2019-01-01'::date"""

    eviction_in_last_x_years="""'1 years'"""
    hl_in_next_y_months="""'6 months'"""
    homelessness_programs = """(29, 32, 263)"""
    add_features_for_program_keys1, add_features_for_program_keys2, add_features_for_program_keys3 = extend_sql_query(program_keys)

    sql_template = sql_template.format(
        table_name=table_name, current_date=current_date, eviction_in_last_x_years=eviction_in_last_x_years, homelessness_programs=homelessness_programs,
        hl_in_next_y_months=hl_in_next_y_months, add_features_for_program_keys1=add_features_for_program_keys1,
        add_features_for_program_keys2=add_features_for_program_keys2, add_features_for_program_keys3=add_features_for_program_keys3)

    # look at the content
    print(sql_template)

    # drop the table if it exists already
    db_conn.execute("""drop table if exists modelling. """ + table_name + """;""")

    #df = pd.read_sql(sql_template, db_conn)

    db_conn.execute(sql_template)

    db_conn.execute("commit")


# add additional columns to the table
def extend_and_save_table(schema_name, old_table_name, new_table_name, program_keys):

    current_columns = list(pd.read_sql("select column_name from information_schema.columns where table_name = 'acdhs_program_participation_and_evictions';", db_conn)["column_name"])

    print("Currently, the following columns exist", current_columns)
    add_features_for_program_keys1, add_features_for_program_keys2, add_features_for_program_keys3 = extend_sql_query(program_keys, current_columns)
    sql = """set role "acdhs-housing-role";
    create table if not exists """ + schema_name + ".""" + new_table_name + """ as
    with """ + add_features_for_program_keys1.replace(",", "", 1) + """
    select *""" + add_features_for_program_keys2 + """
    from """ + schema_name + "." + old_table_name + add_features_for_program_keys3 +"""
    ;"""
    print("now execute the sql query")
    db_conn.execute(sql)
    db_conn.execute("commit")
    print("sql query executed :)")



if __name__ == "__main__":
    program_keys = list(pd.read_sql("""select distinct program_key from lookup.program_feed pf;""", db_conn)["program_key"])[:15]
    #program_keys = [2, 3, 50, 249, 19]

    # create a new feature table
    #create_new_feature_table(program_keys)

    # extend existing table and save as a new feature table
    schema_name="modelling"
    old_table_name="acdhs_program_participation_and_evictions"
    new_table_name="acdhs_program_participation_and_evictions4"

    # first drop the table
    db_conn.execute("drop table if exists " + schema_name + "." + new_table_name + ";")


    # and now create it again
    start_time = time.time()
    extend_and_save_table(schema_name=schema_name, old_table_name=old_table_name, new_table_name=new_table_name, program_keys=program_keys)
    print("Duration: {} seconds".format(time.time() - start_time))
