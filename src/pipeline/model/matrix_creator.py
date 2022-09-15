# takes in 3 tables: cohort, features, labels, and joins them
import logging
import os
import sys
import time

import governance
import pandas as pd
import utils
from ohio.recipe.dbjoin import pg_join_queries


@utils.timer
def read_matrices_from_disk(config, splits, experiment_id):
    """Read the matrices from disk (if they exist) and returns them one split at a time.

    Args:
        config (dict): _description_
        splits (list): A list of all splits. Each split is a tuple: (list of training as of dates, validation as of date)
        experiment_id (int): experiment id

    Yields:
        generator: yields train- and validate-matrix one split at a time
    """

    # check if all csv files exist, if so, return them one by one
    all_files_exist = True
    matrix_pairs = []
    for split in splits:
        training_as_of_dates, train_end_date, _, validate_date, as_of_dates = utils.get_split_info(
            split)
        _, _, _, _, train_filename = governance.create_hash_for_split(
            config, experiment_id, train_end_date, training_as_of_dates, 'train')
        _, _, _, _, validate_filename = governance.create_hash_for_split(
            config, experiment_id, validate_date, as_of_dates, 'validate')

        # check that for this split, train and ba validate sets exist
        if not os.path.exists(train_filename) or not os.path.exists(validate_filename):
            all_files_exist = False
            logging.debug(f'not found {train_filename} or {validate_filename}')
            break

        logging.debug(
            f'reading matrix csv from disk. train_filename: {train_filename} / validate_filename: {validate_filename}')
        matrix_pairs.append((train_filename, validate_filename))

    if all_files_exist:
        logging.info('loaded matrices from disk')
        for train_filename, validate_filename in matrix_pairs:
            yield pd.read_csv(train_filename), pd.read_csv(validate_filename)
        return
    else:
        logging.info('not all matrices were found on disk.')


@utils.timer
def check_for_non_unique_columns(feature_table):

    logging.debug('now drop all columns with only one distinct value')
    # drop all columns with only one distinct value
    start_time = time.perf_counter()
    dropped_columns = []
    for col in feature_table.columns:
        if len(col) > 57:
            logging.warning(
                f'Column name: {col} [{len(col)} characters]. Column names in PostgreSQL must be less than the maximum length of 59 characters.')
        if len(feature_table[col].unique()) == 1:
            feature_table.drop(col, inplace=True, axis=1)
            dropped_columns.append(col)

    logging.info(
        f' {len(dropped_columns)} columns were dropped because they contained only one distinct value [time elapsed: {time.perf_counter() - start_time:.4f} seconds]')
    logging.debug(f' dropped columns: {dropped_columns}')

    try:
        df_memory_usage = feature_table.memory_usage(deep=True).sum()
        logging.info(
            f'the entire merged df takes {df_memory_usage} bites of memory!')
    except:
        logging.info('could not calculate dataframe memory usage :(')

    start_time = time.perf_counter()

    # check for NaNs in data
    nan_cols = feature_table.isna().any()[lambda x: x]
    if len(nan_cols):
        logging.warning(
            f'these columns have NaN values: {[col for col, _ in nan_cols.items()]}. You may want to rerun cohort, label, & features')
    logging.info(
        f"Finished looking for nan_cols in {time.perf_counter() - start_time:.4f} seconds")

    return feature_table


@utils.timer
def create_matrix(config, splits, feature_table_names, experiment_id):
    """Join all feature tables and label table together.

    Args:
        config (dict or Munch obj): entire config file
        splits (list): A list of all splits. Each split is a tuple: (list of training as of dates, validation as of date)

    Returns:
        list: matrix_pairs
    """

    # don't rerun if specified in config
    if not config.matrix_config.rerun and not config.rerun_all:
        logging.debug('not generating matrices.')
        return

    logging.info('Generate matrices.')

    # check that directory exists for saving csv data
    data_base_filepath = utils.get_module_filepath(
        config, experiment_id, 'matrix_config')
    utils.create_directory_if_not_exists(data_base_filepath)

    db_conn = utils.get_db_conn()

    logging.debug('loading label table')

    logging.debug('loading feature tables')
    start_time_features = time.perf_counter()
    # create list of all tables that we want to join (i.e. label table + all feature tables)

    # specify table names to keep them for the first table
    keys = ['client_hash', 'as_of_date']

    queries = [
        f'select * from {config.db_config.schema_name}.{config.label_config.table_name} order by client_hash, as_of_date']
    table_length = db_conn.execute(
        f'''select count(*) from {config.db_config.schema_name}.{config.label_config.table_name};''').fetchone()[0]

    for count, feature_table_name in enumerate(feature_table_names):
        feature_table_length = db_conn.execute(
            f'''select count(*) from {config.db_config.schema_name}.{feature_table_name};''').fetchone()[0]
        if feature_table_length != table_length:
            logging.error(
                f'attempting to merge feature table {feature_table_name}, which contains {feature_table_length} columns, with the label table, which contains {table_length} columns. Aborting.')
            sys.exit(
                "Make sure all feature and label tables have equyl lengths. Aborting!")

        table_column_names = db_conn.execute(f'''
            select column_name, data_type
            from information_schema.columns
            where table_schema = '{config.db_config.schema_name}'
            and table_name = '{feature_table_name}';
            ''').fetchall()

        table_column_names_transformed = []
        for col_name, col_type in table_column_names:
            if col_name not in keys:
                if any(s in col_name for s in ('days_since', 'flag', 'imp')):
                    table_column_names_transformed.append(
                        f'''"{col_name}"::smallint''')
                elif col_type in ('decimal', 'numeric', 'real', 'double precision'):
                    table_column_names_transformed.append(
                        f'''round("{col_name}"::numeric,5) as "{col_name}"''')
                elif col_type in ('bigint'):
                    table_column_names_transformed.append(
                        f'''"{col_name}"::integer''')
                else:
                    table_column_names_transformed.append(f'''"{col_name}"''')

        queries.append(f'''
            select
            {", ".join(table_column_names_transformed)}
            from {config.db_config.schema_name}.{feature_table_name} order by client_hash, as_of_date''')

    logging.debug(
        f"feature tables loaded after {time.perf_counter() - start_time_features:.4f} seconds")

    # join all the tables together into one big one
    start_time = time.perf_counter()

    engine = utils.get_db_engine(pool_size=30, max_overflow=300)

    filename = f"{utils.get_base_filepath(config, experiment_id)}/data/ohio_all_joined_feature_tables.csv"

    logging.info(f'joining tables with ohio')

    count = 0
    with open(filename, 'w', newline='') as fdesc:
        for line in pg_join_queries(queries, engine):
            fdesc.write(line)
            count += 1
            print(f'join feature tables: row {count} of {table_length}       ', end='\r')

    print("\ntables joined and saved to disk as csv")

    logging.info(
        f'joined feature tables with label table [time elapsed: {time.perf_counter() - start_time:.4f} seconds]')

    start_time = time.perf_counter()

    joined_data = pd.read_csv(filename)

    column_dtypes = {'client_hash': 'string',
                     'as_of_date': 'datetime64[ns]', f'{config.label_config.table_name}': 'int32'}

    logging.debug('convert data types of joined table')
    start_time_features = time.perf_counter()
    logging.info(
        f' BEFORE TYPE COVERSION: the joined table df takes {joined_data.memory_usage(deep=True).sum()} bites of memory!')

    # change data type to reduce memory
    for col_name, col_type in joined_data.dtypes.items():
        if 'int' in col_type.name:
            column_dtypes[col_name] = "int16"
        if 'float' in col_type.name:
            column_dtypes[col_name] = "float32"

    joined_data = joined_data.astype(column_dtypes)

    df_memory_usage = joined_data.memory_usage(deep=True).sum()
    logging.info(
        f' ! AFTER TYPE COVERSION : the joined table df takes {df_memory_usage} bites of memory!')

    joined_data = check_for_non_unique_columns(joined_data)

    logging.info(
        f'loaded joined tables [time elapsed: {time.perf_counter() - start_time:.4f} seconds]')

    # generating the matrices
    for split_count, split in enumerate(splits):
        split_start_time = time.perf_counter()
        training_as_of_dates, train_end_date, train_start_date, validate_date, as_of_dates = utils.get_split_info(
            split)

        # subset big table into train and validate data
        train_data = joined_data[(joined_data.as_of_date >= train_start_date) & (
            joined_data.as_of_date <= train_end_date)]
        validate_data = joined_data[joined_data.as_of_date == validate_date]

        try:
            df_memory_usage_train = train_data.memory_usage(deep=True).sum()
            df_memory_usage_val = validate_data.memory_usage(deep=True).sum()
            logging.info(
                f'the train data df of split nr {split_count + 1} takes {df_memory_usage_train} bites of memory!')
            logging.info(
                f'the validate data df of split nr {split_count + 1} takes {df_memory_usage_val} bites of memory!')
        except:
            logging.info(
                'could not calculate train and/or val data df memory usage :(')

        # save training data
        start_time = time.perf_counter()
        cohort_hash, feature_hash, label_hash, matrix_hash_train, train_filename = governance.create_hash_for_split(
            config, experiment_id, train_end_date, training_as_of_dates, 'train')
        train_data.to_csv(train_filename, index=False)
        governance.save_matrix_metadata_to_db(
            config, 'train', train_end_date, cohort_hash, feature_hash, label_hash, matrix_hash_train, experiment_id)
        logging.info(
            f'stored train data to disk: {train_filename} & saved matrix metadata to db [time elapsed: {time.perf_counter() - start_time:.4f} seconds]')

        # save validation data
        start_time = time.perf_counter()
        cohort_hash, feature_hash, label_hash, matrix_hash_validate, validate_filename = governance.create_hash_for_split(
            config, experiment_id, validate_date, as_of_dates, 'validate')
        validate_data.to_csv(validate_filename, index=False)
        governance.save_matrix_metadata_to_db(
            config, 'validate', validate_date, cohort_hash, feature_hash, label_hash, matrix_hash_validate, experiment_id)
        logging.info(
            f'stored validate data to disk: {validate_filename} & saved matrix metadata to db [time elapsed: {time.perf_counter() - start_time:.4f} seconds]')

        logging.info(
            f"Finished generating matrices for split {split_count + 1} of {len(splits)} in {time.perf_counter() - split_start_time:.4f} seconds")
