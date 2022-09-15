import collections
import json
import logging
import os
from hashlib import md5

import joblib
import pandas as pd
import sqlalchemy
import utils
from dateutil.relativedelta import relativedelta


def find_model_id(model_name, hyperparams, config, split, experiment_id):
    ''' find id of model trained on hyperparameters, if it exists
        if it does not exist, return -1

    Args:
        model_name (str): name of model (i.e. decision_tree)
        hyperparams (dict): hyperparameters to feed to model
        config (dict or Munch obj): entire config file
        train_matrix_hash (str): hash value for train matrix hash 
        train_end_date (str): last as_of_date in training data
    '''

    db_conn = utils.get_db_conn()
    # if model metadata table does not exist, then obviously model did not run
    if not utils.check_if_table_exists(db_conn, config.db_config.schema_name, config.modeling_config.model_table_name):
        return -1

    # create sql query for finding the correct model_id. This has to check that for each model_id, a row exists
    # for each hyperparameter, the row has the correct value, and there are no other specified hyperparameters.

    # base str that checks that the parameter name and parameter value are what we expect, if it's not, put a null value
    param_check_base = "max(case when model_param_name='{name}' and model_param_value='{value}' then '{value}' else null end) as {name}"

    # fills out case_base for each individual hyperparameter
    hyperparams_subquery = ',\n'.join(
        [param_check_base.format(name=k, value=v) for k, v in hyperparams.items()])

    # makes sure all the above hyperparameters are not null (i.e. were specified in at least one row in metadata table)
    hyperparams_not_null = ' and '.join(
        [param + ' is not null' for param in hyperparams])

    # get relevant split info
    training_as_of_dates, train_end_date, _, _, _ = utils.get_split_info(split)
    _, _, _, train_matrix_hash, _ = create_hash_for_split(
        config, experiment_id, train_end_date, training_as_of_dates, 'train')

    # construct full query
    sql_query = f'''
    with model_info as (
        select
            mm.experiment_id,
            mm.model_id,
            mm.model_group_id,
            max(mm.model_name) as model_name,
            max(train_end_date) as train_end_date,
            max(train_matrix_hash) as train_matrix_hash,
            {hyperparams_subquery}
        from {config.db_config.schema_name}.{config.modeling_config.model_table_name} mm
        left join {config.db_config.schema_name}.{config.modeling_config.model_params_table_name} mp using(model_id)
        group by mm.experiment_id, mm.model_id, mm.model_group_id
    ),
    model_hyperparam_count as (
        select model_id, count(*)
        from {config.db_config.schema_name}.{config.modeling_config.model_table_name}
        group by model_id
    )
    select *
    from model_info
    left join model_hyperparam_count using(model_id)
    where
        experiment_id = '{experiment_id}' and
        model_name = '{model_name}' and
        train_end_date = '{train_end_date}' and
        count = {len(hyperparams)} and
        train_matrix_hash = '{train_matrix_hash}' and
        {hyperparams_not_null}
    '''
    # TODO: this does not find already run models
    # TODO: in the future: check if the exact same model has been run in a different experiment and if yes, copy everything

    # read data: in case there is a future bug and multiple model_ids are returned, sort by model_id
    model_df = pd.read_sql(sql_query, db_conn).sort_values('model_id')

    # if no model was found
    if not len(model_df):
        return -1

    model_id = model_df.model_id.values[0]
    logging.info(f'Model {model_id} already exists.')

    logging.info(
        f'found model(s) with the same hyperparams in experiment(s) with id(s): {model_df.experiment_id.values}')

    # if it ever happens that multiple models are returned, we should know about it
    if len(model_df) > 1:
        logging.warning(
            f'found multiple models with the same hyperparams. model_id: {model_df.model_id.values}')

    return model_id


def save_matrix_metadata_to_db(config, matrix_type, matrix_date, cohort_hash, feature_hash, label_hash, matrix_hash, experiment_id):
    """Save matrix metadata to the db.

    Args:
        config (dict or Munch obj): entire config file
        matrix_type (str): 'train' or 'validate'
        matrix_date (date): the matrix's last as_of_date (i.e. for either training or validation data)
        cohort_hash (str): cohort hash
        feature_hash (str): feature hash
        label_hash (str): label hash
        matrix_hash (str): matrix hash
    """

    table_name = config.matrix_config.table_name
    schema_name = config.db_config.schema_name

    # save to db
    db_conn = utils.get_db_conn()

    utils.set_role(db_conn, config.db_config.role_name)

    # drop previous row if it exists
    where = f"matrix_hash='{matrix_hash}' and experiment_id = {experiment_id}"
    utils.drop_rows_where(schema_name, table_name, where=where)

    # create new row
    col_names = ('experiment_id', 'matrix_type', 'matrix_date',
                 'cohort_hash', 'feature_hash', 'label_hash', 'matrix_hash')
    col_data = (experiment_id, matrix_type, matrix_date,
                cohort_hash, feature_hash, label_hash, matrix_hash)

    matrix_row = pd.DataFrame({k: [v] for k, v in zip(col_names, col_data)})
    matrix_row.to_sql(
        con=db_conn,
        name=table_name,
        schema=schema_name,
        if_exists="append",
        index=False,
    )


def drop_existing_metadata(config, model_id):
    """ drop existing metadata associated with the same model id on a different run

    Args:
        config (dict or Munch object): the entire config file
        model_id (int): model_id for which we want to drop data
    """

    where = f"model_id = {model_id}"
    utils.drop_rows_where(config.db_config.schema_name,
                          config.modeling_config.model_table_name, where)


def drop_existing_results_and_predictions(config, model_id):
    """ drop existing results, and predictions associated with the same model id on a different run

    Args:
        config (dict or Munch object): the entire config file
        model_id (int): model_id for which we want to drop data
    """

    where = f"model_id = {model_id}"
    utils.drop_rows_where(config.db_config.schema_name,
                          config.evaluation.metrics_table_name, where)
    utils.drop_rows_where(config.db_config.schema_name,
                          config.modeling_config.predictions_table_name, where)


def create_hash_for_split(config, experiment_id, matrix_date, as_of_dates, matrix_type):
    """Create various hashes for a specific timesplit: cohort_hash, feature_hash, label_hash, matrix_hash

    Args:
        config (dict or Munch obj): entire config file
        matrix_date (date): the matrix's last as_of_date
        as_of_dates (list): list of as_of_dates contained in the matrix
        matrix_type (str): 'train' or 'evaluate'

    Returns:
        str: cohort_hash
        str: feature_hash
        str: label_hash
        str: matrix_hash
        str: filename
    """
    cohort_hash = create_cohort_hash(config.cohort_config, as_of_dates)
    feature_hash = create_feature_hash(config.feature_config, cohort_hash)
    label_hash = create_label_hash(config.label_config, cohort_hash)
    matrix_hash = create_matrix_hash(
        matrix_date, matrix_type, cohort_hash, label_hash, feature_hash)
    filename = utils.get_matrix_filename(
        config, experiment_id, matrix_date, matrix_type, matrix_hash)
    return cohort_hash, feature_hash, label_hash, matrix_hash, filename


def create_feature_hash(feature_config, cohort_hash):
    """Create feature hash, including the feature config file (and the other sql queries if they exist)

    Args:
        feature_config (dict or Munch obj): feature config
        cohort_hash (str): cohort hash

    Returns:
        str: feature hash
    """
    to_hash = [json.dumps(
        {k: v for k, v in feature_config.items() if k != 'rerun'})]
    for group in sorted(feature_config.feature_groups):
        filename = f'src/pipeline/config/{group}.sql'
        if os.path.exists(filename):
            to_hash.append(utils.read_sql_file(filename))
    # TODO: this hashed the entire feature config file. It would be bettern to generate the feature sql files and hash only those.
    to_hash.append(utils.read_sql_file(feature_config.feature_config_filename))

    to_hash.append(cohort_hash)

    to_hash_str = '\n\n'.join(to_hash)
    return create_hash(to_hash_str)


def create_label_hash(label_config, cohort_hash):
    """Create label hash, including the relevant sql queries

    Args:
        label_config (dict or Munch obj): label config
        cohort_hash (str): cohort hash

    Returns:
        str: label hash
    """
    to_hash = [json.dumps(
        {k: v for k, v in label_config.items() if k != 'rerun'})]
    to_hash.append(utils.read_sql_file(label_config.filepath))
    to_hash.append(cohort_hash)

    to_hash_str = '\n\n'.join(to_hash)
    return create_hash(to_hash_str)


def create_cohort_hash(cohort_config, as_of_dates):
    """Create cohort hash, including the relevant sql queries

    Args:
        cohort_config (dict or Munch obj): cohort config
        as_of_dates (list): list of as_of_dates contained in the matrix

    Returns:
        str: cohort hash
    """
    to_hash = [json.dumps(
        {k: v for k, v in cohort_config.items() if k != 'rerun'})]
    to_hash.append(utils.read_sql_file(cohort_config.filepath))
    as_of_dates_str = ', '.join(
        [utils.str_from_dt(as_of_date) for as_of_date in as_of_dates])
    to_hash.append(as_of_dates_str)
    to_hash_str = '\n\n'.join(to_hash)
    return create_hash(to_hash_str)


def create_matrix_hash(matrix_date, matrix_type, cohort_hash, label_hash, feature_hash):
    """ Create matrix hash, which includes:
        - label, feature, and cohort hash
        - matrix info: whether 'train' or 'validate' and relevant date
    Args:
        matrix_date: last relevant date for matrix (e.g. if training matrix, last as_of date)
        matrix_type (str): either train / validate
        cohort_hash (str): cohort hash
        label_hash (str): label hash
        feature_hash (str): feature hash
    """

    return create_hash('\n\n'.join([cohort_hash, feature_hash, label_hash, utils.str_from_dt(matrix_date), matrix_type]))


def create_hash(obj):
    """ Calculate md5sum hash string of object
    Args:
        object to be hashed
    """
    return md5(str(obj).encode('utf-8')).hexdigest()


def save_model(model, config):
    """ save model data: both pkl file of model to disk and the associated metadata to table in pipeline

    Args:
        model (obj): model that was trained
        train_matrix_hash (str): hash of training matrix data
        train_end_date (str): last as of date

    """
    logging.debug(f"saving pkl file for model {model.model_id}")

    joblib.dump(model, model.pkl_filename)

    # save model params to db
    # create dictionary of values to save
    values = {
        'experiment_id': model.experiment_id,
        'model_id': model.model_id,
        'model_group_id': model.group_id,
        'model_name': model.model_name,
        'model_param_name': [],
        'model_param_value': [],
    }

    # since multiple params may exist for each model, it stores param names and vals in list
    for name, val in model.hyperparams.items():
        values['model_param_name'].append(name)
        values['model_param_value'].append(utils.sanitize_param_val(val))

    # convert to dataframe to use nice pandas .to_sql method
    model_rows = pd.DataFrame.from_dict(values)

    # save to db
    db_conn = utils.get_db_conn()
    utils.set_role(db_conn, config.db_config.role_name)

    model_rows.to_sql(
        con=db_conn,
        name=config.modeling_config.model_params_table_name,
        schema=config.db_config.schema_name,
        if_exists="append",
        index=False,
        dtype={'model_param_value': sqlalchemy.types.String()}
    )
    logging.debug(
        f'model {model.model_id}: params saved to {config.db_config.schema_name}.{config.modeling_config.model_params_table_name}')


def save_prediction(model, feature_df, config):
    """ save prediction scores to db

    Args:
        model (obj): modeling object already trained
        feature_df (DataFrame): dataframe we are predicting on (including features)

    Return:
        dataframe of predictions
    """
    logging.debug(f"writing predictions for model_id {model.model_id} to db")

    # keep only necessary columns for saving the prediction
    feature_df = feature_df[['client_hash', 'as_of_date',
                             'homelessness_label']].reset_index(drop=True)

    # add necessary vars to df that will be written
    feature_df.loc[:, "score"] = model.scores[:, 1]
    feature_df["model_id"] = model.model_id

    # save to db
    db_conn = utils.get_db_conn()

    utils.set_role(db_conn, config.db_config.role_name)

    feature_df.to_sql(
        con=db_conn,
        name=config.modeling_config.predictions_table_name,
        schema=config.db_config.schema_name,
        if_exists="append",
        index=False,
    )

    return feature_df


def get_group_id(config, experiment_id, model_name):
    """ look at model metadata and find the largest model group id, add 1 to it

    Args:
        config (dict): the whole config file

    """

    db_conn = utils.get_db_conn()
    utils.set_role(db_conn, config.db_config.role_name)
    logging.info(
        f'add row to table {config.modeling_config.model_group_table_name} to get new model group id')
    new_model_group_id = db_conn.execute(f'''insert into {config.db_config.schema_name}.{config.modeling_config.model_group_table_name}
        (experiment_id, model_name)
        values ('{experiment_id}', '{model_name}')
        returning model_group_id;''').fetchone()[0]

    return new_model_group_id


def duplicate_group_id_for_model(config, split, model_name, model_id, group_id, experiment_id):
    """ if we load model from pkl file, duplicate model id and assign it to the new group_id

    Args:
        config (dict): the whole config file
        model (obj): model object
        new_group_id (int): group id to change to
    """

    # get relevant split info
    training_as_of_dates, train_end_date, _, _, _ = utils.get_split_info(split)
    _, _, _, train_matrix_hash, _ = create_hash_for_split(
        config, experiment_id, train_end_date, training_as_of_dates, 'train')

    db_conn = utils.get_db_conn()

    # get all the info from the existing model, duplicate the db entry but with the new model_group_id
    table_query = f'''
        select
            distinct on (model_id, model_param_name, model_param_value)
            {experiment_id} as experiment_id,
            model_id,
            '{group_id}' as model_group_id,
            model_name,
            train_end_date,
            train_matrix_hash,
            model_param_name,
            model_param_value
        from {config.db_config.schema_name}.{config.modeling_config.model_table_name}
        where
            model_name = '{model_name}' and
            model_id = '{model_id}' and
            train_end_date = '{train_end_date}' and
            train_matrix_hash = '{train_matrix_hash}'
        '''

    utils.insert_into_table(db_conn, config.db_config.schema_name,
                            config.modeling_config.model_table_name, table_query)


def set_up_db_schema_for_new_experiment(config, exp_desc):
    """ look at experiment metadata and find the largest experiment id, add 1 to it
    Args:
        config (dict): the whole config file

    """

    db_conn = utils.get_db_conn()

    utils.set_role(db_conn, config.db_config.role_name)

    logging.info(
        'setting up the db for the new experiment. create schema and all tables needed if they do not exist yet')
    logging.debug(
        f'create schema {config.db_config.schema_name} if not exists')
    utils.create_schema(db_conn, config.db_config.schema_name)

    # load sql query and then create the table if it does not exist already
    with open(config.experiment_config.sql_filepath) as f:
        db_set_up_query = f.read()
    db_set_up_query = db_set_up_query.format(config=config)
    db_conn.execute(db_set_up_query)

    # now that the db is set up, generate a new experiment and get the experiment id
    logging.info(
        f'add row to table {config.experiment_config.experiment_table_name} to get new experiment id')

    # TODO: save exp_config as json in experiment_metadata table
    exp_config = json.dumps(config)

    new_experiment_id = db_conn.execute(f'''insert into {config.db_config.schema_name}.{config.experiment_config.experiment_table_name} (exp_desc)
        values ('{exp_desc}' )
        returning experiment_id;''').fetchone()[0]

    return new_experiment_id


def matrices_hashes_correspond_to_experiment(config, experiment_id, matrix_hash_train, matrix_hash_validate):

    db_conn = utils.get_db_conn()

    # construct full query
    sql_query = f'''
    select matrix_hash
    from {config.db_config.schema_name}.{config.matrix_config.table_name}
    where experiment_id = {experiment_id}
    '''
    # read data: in case there is a future bug and multiple model_ids are returned, sort by model_id
    experiment_matrices = pd.read_sql(sql_query, db_conn).matrix_hash

    if collections.Counter(experiment_matrices) == collections.Counter(matrix_hash_train + matrix_hash_validate):
        logging.info(
            'The hashes of the train and validation matrices correspond to those stored in the db for the ongoing experiment')
        return True
    else:
        logging.warning(
            'The hashes of the train and validation matrices do not correspond to those stored in the db for the ongoing experiment!')
        return False


def get_new_model_id(config, experiment_id, model_group_id, model_name, train_end_date, split):
    # TODO: add description
    # model_trainer will pass model_id in. If a model with same parameters has been run before,
    # it will find the model_id from the model params table and pass it in here. If it finds
    # no such model, then model_id = -1 and we create a new model_id here.

    # get necessary information for train matrix hash
    training_as_of_dates, train_end_date, _, _, _ = utils.get_split_info(split)
    _, _, _, train_matrix_hash, _ = create_hash_for_split(
        config, experiment_id, train_end_date, training_as_of_dates, 'train')

    db_conn = utils.get_db_conn()
    utils.set_role(db_conn, config.db_config.role_name)
    logging.info(
        f'add row to table {config.modeling_config.model_table_name} to get new model id')
    new_model_id = db_conn.execute(f'''insert into {config.db_config.schema_name}.{config.modeling_config.model_table_name}
        (experiment_id, model_group_id, model_name, train_end_date, train_matrix_hash)
        values ('{experiment_id}', '{model_group_id}', '{model_name}', '{train_end_date}', '{train_matrix_hash}')
        returning model_id;''').fetchone()[0]

    return new_model_id


def get_train_matrix_info_from_model_id(config, experiment_id, model_id):
    """ given a model id, get the filepath and date for the corresponding training matrix 

    Args:
        config (dict): the whole config file
        experiment_id (int): relevant experiment id
        model_id (int): model id

    Returns:
        (str) filepath of corresponding training matrix
    """

    db_conn = utils.get_db_conn()

    matrix_date, matrix_hash = utils.get_table_where(
        db_conn,
        config.db_config.schema_name,
        config.modeling_config.model_table_name,
        f'model_id = {model_id}'
    ).iloc[0][['train_end_date', 'train_matrix_hash']]

    matrix_date = utils.str_from_dt(matrix_date)

    return utils.get_matrix_filename(config, experiment_id, matrix_date, 'train', matrix_hash), matrix_date


def get_validate_matrix_info_from_model_id(config, experiment_id, model_id):
    """ given a model id, get the filepath and date for the corresponding validation matrix 

    Args:
        config (dict): the whole config file
        experiment_id (int): relevant experiment id
        model_id (int): model id

    Returns:
        (str) filepath of corresponding training matrix
    """

    db_conn = utils.get_db_conn()

    train_end_date = utils.get_table_where(
        db_conn,
        config.db_config.schema_name,
        config.modeling_config.model_table_name,
        f'model_id = {model_id}'
    ).iloc[0].train_end_date

    # get number of months from label_timespan in config: this is a bit hacky
    label_timespan = relativedelta(
        months=+int(config.temporal_config.label_timespan.split()[0]))
    validate_date = utils.str_from_dt(train_end_date + label_timespan)

    # find matrix with this validation date
    matrix_hash = utils.get_table_where(
        db_conn,
        config.db_config.schema_name,
        config.matrix_config.table_name,
        f"experiment_id = {experiment_id} and matrix_date = '{validate_date}' and matrix_type = 'validate'"
    ).iloc[0].matrix_hash

    return utils.get_matrix_filename(config, experiment_id, validate_date, 'validate', matrix_hash), validate_date


@utils.timer
def save_feature_importance(config, top_features):
    """

    Args:
        config (dict): the whole config file
        model_id (int): model id
        top_features (pd.DataFrame): top features to save to db

    """
    utils.set_role(
        utils.get_db_conn(),
        config.db_config.role_name
    )

    top_features.to_sql(
        con=utils.get_db_conn(),
        name=config.evaluation.feature_importance.table_name,
        schema=config.db_config.schema_name,
        if_exists='append',
        index=False
    )


def find_feature_importance(config, model_id):
    return utils.get_table_where(
        utils.get_db_conn(),
        config.db_config.schema_name,
        config.evaluation.feature_importance.table_name,
        f"model_id = {model_id}"
    )


def save_feature_corr(config, feature_corr_matrix):
    """
    Args:
        config (dict): the whole config file
        feature_corr_matrix: correlation matrix

    """
    utils.set_role(
        utils.get_db_conn(),
        config.db_config.role_name
    )

    feature_corr_matrix.to_sql(
        con=utils.get_db_conn(),
        name=config.evaluation.feature_correlation.table_name,
        schema=config.db_config.schema_name,
        if_exists='replace',
        index=False
    )


def get_feature_matrix_with_predictions(config, experiment_id, model):
    validate_filename, _ = get_validate_matrix_info_from_model_id(
        config, experiment_id, model.model_id)
    feature_df = pd.read_csv(validate_filename)

    # add predictions to feature_df
    keys_dtypes = {
        'client_hash': 'string',
        'as_of_date': 'datetime64[ns]',
        'homelessness_label': 'int8'
    }

    predictions_df = utils.get_table_where(
        utils.get_db_conn(),
        config.db_config.schema_name,
        config.modeling_config.predictions_table_name,
        f'model_id = {model.model_id}'
    ).astype(keys_dtypes).sort_values(by='score', ascending=False)

    return feature_df.astype(keys_dtypes).merge(predictions_df, on=list(keys_dtypes), how='left')
