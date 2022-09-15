import logging

import yaml
from munch import munchify
from src.pipeline.config.feature_config import *


@timer
def generate_feature_table(config, feature_group_name, feature_table_names, print_sql_without_executing, sql_script=None, knowledge_date=None, feature_cols=None, defaults=None):

    # setup db connection
    db_engine = get_db_engine()
    db_conn = get_db_conn()

    # select config paras
    role_name = config["db_config"]["role_name"]
    schema_name = config["db_config"]["schema_name"]
    cohort_table = config["db_config"]["schema_name"] + \
        "." + config["cohort_config"]["table_name"]

    if sql_script:
        # use the specific sql template
        with open(sql_script) as f:
            sql_template = f.read()

        sql = sql_template.format(
            schema_name=schema_name,
            cohort=cohort_table,
        )

        table_name = f'feature_{feature_group_name}'

    elif len(feature_cols) == 0:
        # zero features to create
        logging.info(
            'there are zero features for this feature group (batch). Not creating table.')
        return

    else:
        # use the generic sql_template
        with open('src/pipeline/features/generate_features.sql') as f:
            sql_template = f.read()

        sql = sql_template.format(
            feature_cols=',\n'.join(feature_cols),
            cohort=cohort_table,
            knowledge_date=knowledge_date,
            **defaults
        )

        table_name = f'feature_{feature_group_name}_{knowledge_date}'

    # if a table with the same name already exists, adjust the table name in order for it to be unique
    # this is the case if a table was already created to not exceed the db table column limit
    batch_count = 2
    while table_name in feature_table_names:
        table_name = f'{table_name.split("_batch", 1)[0]}_batch{batch_count}'
        batch_count += 1

    if print_sql_without_executing:
        print("")
        print("-----------")
        print("")
        print("schema name:", schema_name)
        print("table name:", table_name)
        print("")
        print(f'{sql};')

    else:

        set_role(db_conn, role_name)
        create_schema(db_conn, schema_name)
        drop_table(db_conn, schema_name, table_name)

        logging.info(
            f"Generating table for feature group {feature_group_name} / table name: {table_name} / schema name: {schema_name} / nr of features: {len(feature_cols) if feature_cols else '?'}")

        # check if table already exists
        exists = check_if_table_exists(
            db_engine, schema_name, table_name)

        # creates table if does not exist yet
        if not exists:
            create_table(
                db_conn,
                schema_name,
                table_name,
                sql,
            )
            db_conn.execute("commit")

        # else inserts into existing table
        else:
            insert_into_table(
                db_conn,
                schema_name,
                table_name,
                sql,
            )
            db_conn.execute("commit")

    return table_name


@timer
def get_feature_table_names(config):
    logging.debug('not rerunning features, get the feature table names')
    # do not rerun features
    db_conn = get_db_conn()
    # get all feature table names
    all_feature_tables = list(pd.read_sql(
        f"select table_name from information_schema.tables where table_schema = '{config['db_config']['schema_name']}' and table_name like 'feature_%%';", db_conn)['table_name'])
    # of all existing table feature name, get those that are associated with any of the feature groups specified in the config file
    feature_table_names = [s for s in all_feature_tables if any(
        f'feature_{f}' in s for f in config.feature_config.feature_groups)]
    logging.debug(f'found feature_table_names: {feature_table_names}')

    return feature_table_names


@timer
def create_feature_tables(config, print_sql_without_executing=False):
    """Create a table in the database for each feature group specified in the config file.

    Args:
        config (dict): The entire config.
        """

    if not config.feature_config.rerun and not config.rerun_all:
        logging.info('not generating features again')
        # TODO: check if feature should be regerenated, i.e., maybe the previous experiment ran on different timesplits.
        return get_feature_table_names(config)

    feature_table_names = []

    # load the feature config dictionary
    feature_config_dict, feature_default_params = get_feature_config(config)

    for k in config.feature_config.feature_groups:
        if k not in feature_config_dict.keys():
            logging.warning(
                f'Warning: {k} is not specified in the feature config file!')

    # drop feature groups that are not specified in the config file
    feature_config_dict_subset = {k: feature_config_dict[k] for k in feature_config_dict.keys(
    ) & config.feature_config.feature_groups}

    for feature_group_name, feature_group in feature_config_dict_subset.items():

        # check if the feature group has a specific sql file
        if 'sql_script' in feature_group.keys():
            # this feature_group has a specific sql file. Run this file and continue with the next feature group
            # generate the feature table
            feature_table_names.append(generate_feature_table(
                config, feature_group_name, feature_table_names, print_sql_without_executing, sql_script=feature_group['sql_script']))
            continue

        default_knowledge_date = feature_group['defaults'].pop(
            'knowledge_date')
        all_feature_types = {k: v for k,
                             v in feature_group.items() if '_features' in k}
        # roll down default knowledge_date for features where knowledge_date is not specified
        for k, v in all_feature_types.items():
            for i, f in enumerate(v):
                if 'knowledge_date' not in f:
                    feature_group[k][i]['knowledge_date'] = default_knowledge_date
        # now get all knowledge_dates
        all_knowledge_dates = list(set(
            [item['knowledge_date'] for sublist in all_feature_types.values() for item in sublist]))

        for knowledge_date in all_knowledge_dates:

            feature_cols = []

            if 'numeric_features' in feature_group:

                for numeric_feature in [v for v in feature_group['numeric_features'] if v['knowledge_date'] == knowledge_date]:
                    defaults = feature_group['defaults']
                    # add defaults if not specified
                    defaults.update(
                        {k: v for k, v in feature_default_params.items() if k not in defaults})
                    numeric_feature.update(
                        {k: v for k, v in defaults.items() if k not in numeric_feature})

                    if numeric_feature["generate_days_since_last"] == True:
                        # calculate number of days since last event
                        last_known_date = """(max({knowledge_date}) filter (where true{filter_addition}))""".format(
                            **numeric_feature)
                        days_since_last_col = """case when {last_known_date} is null then {days_since_last_impute_value} else DATE_PART('day', ch.as_of_date::timestamp - {last_known_date}::timestamp) end "days_since_{name}" """.format(
                            last_known_date=last_known_date, **numeric_feature)
                        feature_cols.append(days_since_last_col)
                        days_since_last_col_log = """case when {last_known_date} is null then log({days_since_last_impute_value}) else log(DATE_PART('day', ch.as_of_date::timestamp - {last_known_date}::timestamp)) end "days_since_{name}_log" """.format(
                            last_known_date=last_known_date, **numeric_feature)
                        feature_cols.append(days_since_last_col_log)

                        # flag if number of days since last value was imputed
                        days_since_last_imputation_flag = """case when {last_known_date} is null then 1 else 0 end "days_since_{name}_imp" """.format(
                            last_known_date=last_known_date, **numeric_feature)
                        feature_cols.append(days_since_last_imputation_flag)

                    else:
                        # generate a flag for when individuals are currently enrolled in program
                        flag_column = """case when (COUNT(*) filter (where tsrange(ch.as_of_date::timestamp - interval '100year', ch.as_of_date::timestamp) @> {knowledge_date}::timestamp{filter_addition})) > 0 then 1 else 0 end "flag_{name}" """.format(
                            **numeric_feature)
                        feature_cols.append(flag_column)

                    for interval in numeric_feature['intervals']:

                        for agg_fcn in numeric_feature['agg_fcns']:

                            aggregation = """({agg_fcn}({column}) filter (where tsrange(ch.as_of_date::timestamp - interval '{interval}', ch.as_of_date::timestamp) @> {knowledge_date}::timestamp{filter_addition}))""".format(
                                interval=interval, agg_fcn=agg_fcn, **numeric_feature)

                            last_x_agg_col = """case when {aggregation} is null then {impute_value} else {aggregation} end "{agg_fcn}({name},@{interval})" """.format(
                                aggregation=aggregation, interval=interval, agg_fcn=agg_fcn, **numeric_feature)
                            feature_cols.append(last_x_agg_col)

                            if 'count' not in agg_fcn.lower():
                                # if the aggregation function is not count, add an imputation flag column since values might be imputed for aggregations resulting in null values
                                last_x_agg_col_imputation_flag = """case when {aggregation} is null then 1 else 0 end "{agg_fcn}({name},@{interval})_imp" """.format(
                                    aggregation=aggregation, interval=interval, agg_fcn=agg_fcn, **numeric_feature)
                                feature_cols.append(
                                    last_x_agg_col_imputation_flag)

                    if len(feature_cols) > 500:
                        # save a first bach of features in a table to make sure that the postgres table column limit is not exceeded
                        # generate the feature table
                        feature_table_names.append(generate_feature_table(config, feature_group_name, feature_table_names,
                                                   print_sql_without_executing, knowledge_date=knowledge_date, feature_cols=feature_cols, defaults=defaults))
                        feature_cols = []

            # generate the feature table
            feature_table_names.append(generate_feature_table(config, feature_group_name, feature_table_names,
                                       print_sql_without_executing, knowledge_date=knowledge_date, feature_cols=feature_cols, defaults=defaults))

    logging.debug(f'Feature table names: {feature_table_names}')
    return [f for f in feature_table_names if f is not None]


def main():

    with open("src/pipeline/config.yaml", "r") as config_file:
        config = munchify(yaml.safe_load(config_file))

    create_feature_tables(config, print_sql_without_executing=True)


if __name__ == "__main__":
    main()
