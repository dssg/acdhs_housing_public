import logging
import os
import re
import sys
import time
from datetime import datetime

import joblib
import pandas as pd
import plotly.io as pio
import yaml
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from munch import munchify
from pyexpat import features
from scipy.stats import spearmanr
from sqlalchemy import create_engine, inspect


def timer(func):
    """Decorator that prints the runtime of the decorated function"""

    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        logging.info(f"Finished {func.__name__!r} in {run_time:.4f} seconds")
        return value

    return wrapper_timer


def get_db_engine(pool_size=5, max_overflow=10):
    """
    Sets up SQL engine
    """

    # load variables from .env file
    load_dotenv('.env')

    # get credentials from environment variables
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT")
    database = os.getenv("PGDATABASE")

    # configure connection to postgres
    engine = create_engine(
        "postgresql://{}:{}@{}:{}/{}".format(
            user,
            password,
            host,
            port,
            database,
        ),
        pool_size=pool_size,
        max_overflow=max_overflow
    )

    return engine


def get_db_conn():
    """
    Connects to the sql database
    Returns db connection
    """

    engine = get_db_engine()

    connection = engine.connect()

    return connection


def create_schema(db_conn, schema_name):
    """Create schema if not exists

    Args:
        db_conn (object): Database connection
        schema_name (_type_): Name of schema that should be created
    """

    query = f"create schema if not exists {schema_name}"

    try:
        db_conn.execute(query)
    except:
        logging.warning(f"{schema_name} creation failed")


def set_role(db_conn, role_name):
    """Sets the role for the postgres database

    Args:
        db_conn (object): database connection
        role (str, optional): Role name used for database connection. Defaults to 'acdhs-housing'.
    """

    query = f"set role '{role_name}';"

    try:
        db_conn.execute(query)
    except:
        logging.warning("role setting failed.")


def drop_table(db_conn, schema_name, table_name):
    """Drops sql table if exists

    Args:
        db_conn (object): Database connection
        schema_name (str): Schema name
        table_name (str): Table name to be dropped
    """
    query = f"drop table if exists {schema_name}.{table_name}"

    try:
        db_conn.execute(query)
        logging.debug(f"Table {schema_name}.{table_name} dropped (if exists)")
    except:
        logging.warning(f"Failed to drop table {schema_name}.{table_name}")


def check_if_table_exists(db_engine, schema_name, table_name):
    """Checks whether a table with {table_name} exists in schema {schema_name}

    Args:
        db_engine (object): Database engine
        schema_name (str): Schema name
        table_name (str): Table name
    """

    ins = inspect(db_engine)
    exists = ins.dialect.has_table(
        db_engine.connect(), table_name, schema_name)
    if exists:
        pass
    else:
        logging.debug(f"Table {schema_name}.{table_name} does not exist yet.")
    return exists


def create_table(db_conn, schema_name, table_name, table_query=None):
    """Creates table if not exists
    Args:
        db_conn (object): Database connection
        schema_name (str): Schema name
        table_name (str): Table name to be created
        table_query (str, optional): If table_query is specified, it populates the table with query.
        Defaults to generating an empty table
    """

    if table_query is not None:
        query = f"create table {schema_name}.{table_name} as ({table_query});"
    else:
        query = f"create table {schema_name}.{table_name} ();"

    try:
        db_conn.execute(query)
        logging.debug(f"Table {schema_name}.{table_name} has been created")
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logging.warning(message)
        logging.warning(f"Failed to create table {schema_name}.{table_name}")


def insert_into_table(db_conn, schema_name, table_name, table_query):
    """Inserts into existing table
    Args:
        db_conn (object): Database connection
        schema_name (str): Schema name
        table_name (str): Table name to be inserted into
        table_query (str): SQL query that will be inserted into table.
    """

    query = f"insert into {schema_name}.{table_name} ({table_query});"

    try:
        db_conn.execute(query)
    except:
        logging.warning(
            f"Failed to insert sql query into table {schema_name}.{table_name}")


def get_table(db_conn, schema_name, table_name):
    return pd.read_sql(f'''
        select *
        from {schema_name}.{table_name}
    ''', db_conn)


def get_table_where(db_conn, schema_name, table_name, where):
    return pd.read_sql(f'''
        select *
        from {schema_name}.{table_name}
        where {where}
    ''', db_conn)


@timer
def create_index(db_conn, schema_name, table_name, index_columns):
    """Function to set the index columns in a SQL DB

    Args:
        db_conn (object): Database connection
        schema_name (str): Schema name
        table_name (str): Table name
        index_columns (list): List of column names
    """

    if not isinstance(index_columns, list):
        raise TypeError("Expected list for index_columns")

    if len(index_columns) > 1:
        query = (
            f"create index on {schema_name}.{table_name}({', '.join(index_columns)});"
        )
    else:
        query = f"create index on {schema_name}.{table_name}({''.join(index_columns)});"

    try:
        db_conn.execute(query)
        logging.debug(
            f"Created index for column(s) {index_columns} in table {schema_name}.{table_name}")
    except:
        logging.warning(
            f"Failed to create index for column(s) {index_columns} in table {schema_name}.{table_name}")


def df_to_feature_matrix(df):
    drop_cols = ['client_hash', 'as_of_date', 'homelessness_label']
    feature_matrix = df.drop(columns=drop_cols).values
    labels = df.homelessness_label.tolist()
    logging.debug(
        f'feature matrix shape: {feature_matrix.shape}. In total there are {sum(labels)} [ {sum(labels)/len(labels)} % ] positive labels (out of {len(labels)} individuals.)')

    return feature_matrix, labels


def read_sql_file(filename):
    """ Reads a file from disk, strips newlines and spaces at end of lines
        Used to make the feature, label, and cohort hashes
    Args:
        filename (str): path to sql file
    """
    with open(filename, 'r') as f:
        return '\n'.join([line.rstrip() for line in f.readlines() if line != '\n'])


def get_split_info(split):
    """Retrieve information from a specific train evaluation timesplit.

    Args:
        split (tuple): (list of training as of dates, validation as of date)

    Returns:
        list: training_as_of_dates
        date: train_end_date
        date: train_start_date
        date: validate_date
        list: as_of_dates
    """
    # get start and end dates for training and validation for current split
    training_as_of_dates = split[0]
    train_end_date = split[0][0]
    train_start_date = split[0][-1]
    validate_date = split[1]
    # get a list of all as_of_dates fot this split, consisting of all training as of dates and the validation date
    as_of_dates = training_as_of_dates + [validate_date]
    return training_as_of_dates, train_end_date, train_start_date, validate_date, as_of_dates


def get_matrix_filename(config, experiment_id, matrix_date, matrix_type, matrix_hash):
    """Get the filename to save the matrix as a csv.

    Args:
        config (dict or Munch obj): entire config file
        matrix_date (date): the matrix's last as_of_date
        matrix_type (str): 'train' or 'evaluate'
        matrix_hash (str): matrix hash

    Returns:
        str: matrix filename
    """

    base = get_module_filepath(config, experiment_id, 'matrix_config')
    sanitized_date = str_from_dt(matrix_date).replace(' ', '_')
    return f'{base}/{matrix_type}-{sanitized_date}-matrix_hash-{matrix_hash}.csv'


def read_model_from_pkl_file(config, experiment_id, model_id):
    """ read model from pickle file saved to disk

    Args:
        config (dict or Munch object): the entire config file
        model_id (int): model_id for model we want to return
    """
    logging.debug('  Loading model #{} from pkl file...'.format(model_id))
    basepath = get_module_filepath(config, experiment_id, 'modeling_config')
    pkl_filename = f'{basepath}/model-{model_id}.pkl'

    return joblib.load(pkl_filename)


def dt_from_str(dt_str):
    if isinstance(dt_str, datetime):
        return dt_str
    return datetime.strptime(dt_str, "%Y-%m-%d")


def parse_delta_string(delta_string):
    """Given a string in a postgres interval format (e.g., '1 month'),
    parse the units and value from it.

    Assumptions:
    - The string is in the format 'value unit', where
      value is an int and unit is one of year(s), month(s), day(s),
      week(s), hour(s), minute(s), second(s), microsecond(s), or an
      abbreviation matching y, d, w, h, m, s, or ms (case-insensitive).
      For example: 1 year, 1year, 2 years, 1 y, 2y, 1Y.

    :param delta_string: the time interval to convert
    :type delta_string: str

    :return: time units, number of units (value)
    :rtype: tuple

    :raises: ValueError if the delta_string is not in the expected format

    """
    match = parse_delta_string.pattern.search(delta_string)
    if match:
        (pre_value, units) = match.groups()
        return (units, int(pre_value))

    raise ValueError(
        "Could not parse value from time delta string: {!r}".format(
            delta_string)
    )


parse_delta_string.pattern = re.compile(r"^(\d+) *([^ ]+)$")


def convert_str_to_relativedelta(delta_string):
    """Given a string in a postgres interval format (e.g., '1 month'),
    convert it to a dateutil.relativedelta.relativedelta.

    Assumptions:
    - The string is in the format 'value unit', where
      value is an int and unit is one of year(s), month(s), day(s),
      week(s), hour(s), minute(s), second(s), microsecond(s), or an
      abbreviation matching y, d, w, h, m, s, or ms (case-insensitive).
      For example: 1 year, 1year, 2 years, 1 y, 2y, 1Y.

    :param delta_string: the time interval to convert
    :type delta_string: str

    :return: the time interval as a relativedelta
    :rtype: dateutil.relativedelta.relativedelta

    :raises: ValueError if the delta_string is not in the expected format

    """
    (units, value) = parse_delta_string(delta_string)

    verbose_match = convert_str_to_relativedelta.pattern_verbose.search(units)
    if verbose_match:
        unit_type = verbose_match.group(1) + "s"
        return relativedelta(**{unit_type: value})

    try:
        unit_type = convert_str_to_relativedelta.brief_units[units.lower()]
    except KeyError:
        pass
    else:
        if unit_type == "minutes":
            logging.info(f'Time delta units "{units}" converted to minutes.')
        return relativedelta(**{unit_type: value})

    raise ValueError(
        "Could not handle units. Units: {} Value: {}".format(units, value))


convert_str_to_relativedelta.pattern_verbose = re.compile(
    r"^(year|month|day|week|hour|minute|second|microsecond)s?$"
)

convert_str_to_relativedelta.brief_units = {
    "y": "years",
    "d": "days",
    "w": "weeks",
    "h": "hours",
    "m": "minutes",
    "s": "seconds",
    "ms": "microseconds",
}


def str_from_dt(dt):
    if isinstance(dt, str):
        return dt
    return dt.strftime('%Y-%m-%d')


def drop_rows_where(schema_name, table_name, where=''):
    ''' drops rows from the specified table with the corresponding model_id '''
    db_conn = get_db_conn()

    # check if table exists first
    if not check_if_table_exists(db_conn, schema_name, table_name):
        return

    delete_sql = f'''
        delete from {schema_name}.{table_name}
        where {where};

        commit;
    '''

    # delete rows from model metadata table
    db_conn.execute(delete_sql)


def drop_rows_with_model_id(schema_name, table_name, model_id, where=''):
    logging.debug('schema_name is ', schema_name)
    ''' drops rows from the specified table with the corresponding model_id '''
    db_conn = get_db_conn()

    # check if table exists first
    if not check_if_table_exists(db_conn, schema_name, table_name):
        return

    delete_sql = f'''
        delete from {schema_name}.{table_name}
        where model_id = {model_id} {'and' if where else ''} {where};

        commit;
    '''

    # delete rows from model metadata table
    db_conn.execute(delete_sql)


def create_directory_if_not_exists(filepath):
    if not os.path.exists(filepath):
        os.makedirs(filepath)


def check_experiment_run(config):
    if 'prod' in config.db_config.schema_name:
        answer = input(
            f"You are about to run an experiment in production! This will change the db schema {config.db_config.schema_name}. Wanna continue?\n")
        if answer.lower() in ["y", "yes"]:
            answer2 = input(f"Are you solving homelessness?\n")
            if answer2.lower() == "literally":
                print("Starting the experiment...")
                return
        sys.exit("Sorry, wrong passcode! See you next time...")


def generate_model_group_str(model_name, hyperparams):
    hyperparams_str = ', '.join(
        [f'{name}={val}' for name, val in hyperparams.items()])
    return f'{model_name}({hyperparams_str})'


def generate_plot_title_for_model(plot_type, model, date_type, date_str):
    """ Generate a pretty plot name string for use in title

    Args:
        plot_type (str): type of plot 
        model (obj): model object
        date_type (str): should be either "trained" or "validated"
        date_str (str): relevant date
    """
    model_group_str = generate_model_group_str(
        model.model_name, model.hyperparams)
    return f'{plot_type}: model id {model.model_id}: {model_group_str} {date_type} on {date_str}'


def read_config(filename):
    with open(f"src/pipeline/{filename}", "r") as config_file:
        return munchify(yaml.safe_load(config_file))


def get_experiment_id_from_model_id(config, model_id):
    """ Get experiment id for an already run experiment

    Args:
        config (dict): the config file
        model_id (int): model_id

    """
    return get_table_where(
        get_db_conn(),
        config.db_config.schema_name,
        config.modeling_config.model_table_name,
        where=f'model_id = {model_id}'
    ).experiment_id.values[0]


def get_base_filepath(config, experiment_id, add_experiment_id=True):
    """ gets base filepath with relevant experiment id, 

    Args:
        config (dict): the whole config file
        experiment_id (int): relevant experiment id
    """

    base = f"{config.base_filepath}/{config.db_config.schema_name}"
    if add_experiment_id:
        base = f"{base}/experiment-{experiment_id}"

    return base


def get_module_filepath(config, experiment_id, module_name):
    """ gets module filepath with relevant experiment id, 
        if module is part of evaluation, add evaluation subdir to it

    Args:
        config (dict): the whole config file
        experiment_id (int): relevant experiment id
    """
    if module_name in ('evaluate_over_time', 'feature_importance', 'prk', 'score_distribution', 'feature_correlation'):
        module_path = f"{config.evaluation.save_filepath}/{module_name}"
    else:
        module_path = config[module_name].save_filepath

    return f"{get_base_filepath(config, experiment_id, module_name!='logging')}/{module_path}"


def start_logger(config, experiment_id, pipeline_start_time):
    """ once experiment id exists, have config

    Args:
        config (dict): the whole config file
        experiment_id (int): relevant experiment id
        pipeline_start_time (str): start time for pipeline
        backlog (list[str]): strings we wanted to put in the config before it existed

        """
    logging_filepath = get_module_filepath(config, experiment_id, 'logging')
    create_directory_if_not_exists(logging_filepath)

    sanitized_date = str_from_dt(pipeline_start_time).replace(
        '/', '-').replace(' ', '_')
    logging.basicConfig(filename=f'{logging_filepath}/{config.logging.filename}-{sanitized_date}.log',
                        encoding='utf-8', format='%(levelname)s:%(message)s', level=logging.DEBUG)
    logging.captureWarnings(True)

    # ignore matplotlib output because it's overwhelming
    for name, logger in logging.root.manager.loggerDict.items():
        if name.startswith('matplotlib'):
            logger.disabled = True

    if experiment_id is None:
        logging.info(f'new experiment started at: {pipeline_start_time}')
    else:
        logging.info(
            f'experiment #{experiment_id} started at: {pipeline_start_time}')

    logging.debug(f'\n\nCONFIG: {str(dict(config))}\n\n')


def save_plotly(fig, base_filepath, filename, plot_type):
    # save static, interactive, and json version of plots
    # calculate and create necessary dirs
    static_path = f'{base_filepath}/static/'
    interactive_path = f'{base_filepath}/interactive/'
    json_path = f'{base_filepath}/json/'
    create_directory_if_not_exists(static_path)
    create_directory_if_not_exists(interactive_path)
    create_directory_if_not_exists(json_path)

    fig.write_image(f'{static_path}/{filename}.png')
    fig.write_html(f'{interactive_path}/{filename}.html')
    with open(f'{json_path}/{filename}.json', 'w') as f:
        f.write(fig.to_json())

    logging.info(
        f'{plot_type} plot has been saved as: {static_path}/{filename}.png')


def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def sanitize_param_val(p):
    """ make param value consistent (so no 10 vs 10.0 happens when writing to db)

    Args:
        p (str, int, float): a parameter value

    Returns:
        (str) sanitized parameter value
    """

    # handle floats
    if '.' in str(p) and str(p).replace('.', '').isnumeric():
        # remove superfluous .0
        if str(p).split('.')[1] == '0':
            return str(int(p))
        return str(p)
    # either an int or str, so we can return it directly
    return str(p)


def find_curves_for_model_id(config, experiment_id, model_id):
    # find prk curves
    base_filepath = get_module_filepath(config, experiment_id, 'prk')

    prk_curves = []
    filename = f'model-{model_id}.json'
    json_path = f'{base_filepath}/json/{filename}'
    prk_curves.append(pio.read_json(json_path))

    for attribute in config.evaluation.groups:
        filename = f'model-{model_id}-eval_across_group-{attribute}.json'
        json_path = f'{base_filepath}/json/{filename}'

        prk_curves.append(pio.read_json(json_path))

    # find feature importance curve
    base_filepath = get_module_filepath(
        config, experiment_id, 'feature_importance')
    filename = f'model-{model_id}.json'
    json_path = f'{base_filepath}/json/{filename}'

    # find score distribution curves
    base_filepath = get_module_filepath(
        config, experiment_id, 'score_distribution')
    filename = f'model-{model_id}-score_distribution.json'
    json_path = f'{base_filepath}/json/{filename}'
    try:
        sd_curves = pio.read_json(f'{base_filepath}/json/{filename}')
    except:
        sd_curves = []

    return prk_curves, [sd_curves]


def get_metrics_and_top_k(config):
    results_table = get_table_where(
        get_db_conn(),
        config.db_config.schema_name,
        config.evaluation.metrics_table_name,
        f'true limit 50'
    )

    top_k = sorted(results_table.top_n.unique())

    ignore_cols = ('precision', 'recall', 'top_n', 'acceptance_rate',
                   'threshold', 'model_id', 'validation_date')
    other_metrics = [c for c in results_table.columns if c not in ignore_cols]
    metrics = ['precision', 'recall'] + other_metrics

    return metrics, top_k


def jaccard_similarity(a, b):
    return len(set(a).intersection(set(b))) / len(set(a).union(set(b)))


def rank_correlation(a, b):
    return spearmanr(a, b).correlation


def avg_similarity(features_df, metric):
    val_dates = sorted(features_df.validation_date.unique())

    if len(val_dates) <= 1:
        return -1

    similarities = []
    for date1, date2 in zip(val_dates, val_dates[1:]):
        date1_set = features_df[features_df.validation_date == date1].sort_values(
            by='rank').feature_name
        date2_set = features_df[features_df.validation_date == date2].sort_values(
            by='rank').feature_name
        similarities.append(metric(date1_set, date2_set))

    return sum(similarities) / len(similarities)


def get_race_and_gender():
    query = f'''
        select
            distinct on (client_hash)
            client_hash,
            race,
            gender
        from clean.client_feed
    '''

    group_info = pd.read_sql(query, get_db_conn())
    return group_info


def get_future_hl(config):
    query = f'''
        select
            ch.client_hash,
            ch.as_of_date,
            case when (min(program_start_dt)) is null then 9999 else DATE_PART('day', (min(program_start_dt))::timestamp - ch.as_of_date::timestamp) end "days_until_next_hl",
            case when (min(program_start_dt)) is null then 0 else 1 end "hl_in_the_future"
        from
            {config.db_config.schema_name}.{config.cohort_config.table_name} as ch
            left join (
                        select *
                        from {config.db_config.schema_name}.{config.hl_definition_config.table_name}
                        ) as f
                on ch.client_hash = f.client_hash and f.program_start_dt > ch.as_of_date
        group by ch.client_hash, ch.as_of_date;
    '''

    future_hl = pd.read_sql(query, get_db_conn())
    return future_hl


def get_past_and_future_rental_assistance(config):

    rental_assistance_programs = "'30', '296', '297', '298'"

    query = f'''
        select
            ch.client_hash,
            ch.as_of_date,
            case when (min(f1.program_start_dt)) is null then 9999 else DATE_PART('day', (min(f1.program_start_dt))::timestamp - ch.as_of_date::timestamp) end "days_until_next_rental_assistance",
            case when (min(f1.program_start_dt)) is null then 0 else 1 end "rental_assistance_in_the_future",
            case when (max(f2.program_end_dt)) is null then 9999 else DATE_PART('day', ch.as_of_date::timestamp - (max(f2.program_end_dt))::timestamp) end "days_since_last_rental_assistance",
            case when (max(f2.program_end_dt)) is null then 0 else 1 end "rental_assistance_in_the_past"
        from
            {config.db_config.schema_name}.{config.cohort_config.table_name} as ch
            left join (
                        select *
                        from clean.program_involvement_consolidated pic
                        where data_type = 'feed'
                        and project_type in ({rental_assistance_programs})
                        ) as f1
                on ch.client_hash = f1.client_hash and f1.program_start_dt > ch.as_of_date
            left join (
                        select *
                        from clean.program_involvement_consolidated pic
                        where data_type = 'feed'
                        and project_type in ({rental_assistance_programs})
                        ) as f2
                on ch.client_hash = f2.client_hash and f2.program_end_dt < ch.as_of_date
        group by ch.client_hash, ch.as_of_date;
    '''

    past_and_future_rental_assistance = pd.read_sql(query, get_db_conn())
    return past_and_future_rental_assistance


def get_existing_model_group_id(config, model_id):
    db_conn = get_db_conn()

    model_group_id = db_conn.execute(f'''select model_group_id
        from {config.db_config.schema_name}.{config.modeling_config.model_table_name}
        where model_id = {model_id}
        ;''').fetchone()[0]

    return model_group_id


def aggregate_hl_programs_by_label(config, clients):
    """ Used in postmodeling: figure out which specific homelessness programs selected clients are
        in (rather than just start or end date). So we subset the program_involvement_consolidated
        table to only homelessness programs, then join them with the source table to find the
        specific program type.

        FIXME: some Nans show up sometimes. debug if we still have time (if not, sorry @Kasun)

    Args:
        config (dict): the entire config file
        clients (list): clients in our list that we want to know specific program info for

    Returns:
        (pd.DataFrame): out of selected clients, how many were involved in which program
    """
    clients_str = ", ".join([f"'{c}'" for c in clients])
    query = '''
    select
        f.data_type as "source table",
        f.project_type,
        count(distinct ch.client_hash) as "nr of individuals"
    from
        {schema_name}.{cohort} as ch
        left join (
            select *
            from clean.program_involvement_consolidated
            where (
                (data_type = 'feed')
                and
                (project_type in {program_keys_detailed})
            )
            or (
                (data_type = 'hmis')
                and
                (project_type in (
                    select distinct project_type_id::varchar
                    from clean.cmu_hmis_current_prm
                    where project_type in {hmis_programs}
                    )
                )
            )
            or (
                (data_type = 'hmis_ph_waiting_list')
                and
                (project_type in (
                    select distinct hud_project_type_id::varchar
                    from clean.hmis_details
                    where hud_project_type_desc in {hmis_ph_programs}
                    )
                )
            )
            ) as f
            on
                ch.client_hash = f.client_hash
                and
                f.program_start_dt between ch.as_of_date and (ch.as_of_date + '{label_timespan}'::interval) 
    where ch.client_hash in ({clients_str})
    group by f.data_type, f.project_type
    order by "nr of individuals" desc
    ;
    '''.format(
        schema_name=config.db_config.schema_name,
        cohort=config.cohort_config.table_name,
        clients=clients,
        label_timespan=config.temporal_config.label_timespan,
        clients_str=clients_str,
        **config['hl_definition_config']
    )

    return pd.read_sql(query, get_db_conn())
