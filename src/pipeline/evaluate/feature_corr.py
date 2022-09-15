import argparse
import logging

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import utils
from src.pipeline import governance


def calc_feature_corr(config, model, train_data):

    schema_name = config.db_config.schema_name
    model_id = model.model_id
    feature_importance_table_name = config.evaluation.feature_importance.table_name
    feature_names = [c for c in train_data.columns if c not in (
        'client_hash', 'as_of_date', 'homelessness_label')]  # at the very least, filter these out

    top_n = config.evaluation.feature_correlation.top_n \
        if config.evaluation.feature_correlation.top_n > 0 else len(feature_names)

    check_model_name_query = f"""
    select model_name from {schema_name}.model_metadata  where model_id= {model_id};
    """

    check_model_name = pd.read_sql(
        check_model_name_query, utils.get_db_conn())['model_name']

    if top_n > 0 and ("baseline" not in check_model_name):
        query = f"""
        select feature_name from(
            select feature_name ,model_id, row_number() over (partition by model_id order by value desc) as feature_rank
            from {schema_name}.{feature_importance_table_name} 
            ) top_fe
        where feature_rank <=  {top_n} and model_id = {model_id} ;
    """
        try:
            feature_names = pd.read_sql(query, utils.get_db_conn())[
                'feature_name'].tolist()
        except:
            logging.warning(
                f"couldn't find feature importance table for feature correlation")

    # only take the feature_names columns of the matrix, for top_n if specifed
    train_data = train_data.loc[:, train_data.columns.isin(feature_names)]

    # convert pandas data frame to numpy array  for faster computation correlation
    train_data = train_data.to_numpy()
    # rowvar makes sure its comparing columns of the nparray
    corr = np.corrcoef(train_data, rowvar=False)
    corr = pd.DataFrame(corr)  # need to convert back to df

    corr.columns = feature_names
    corr.index = feature_names

    corr['model_id'] = model.model_id  # add model ID (maybe delete)

    return corr


def plot_top_feature_corr(config, model, feature_corr_matrix=None):
    """ plot feature correlation and save to file. If top_features is not passed in, it
        assumes that calc_feature_corr has already ran and pulls results from db.

    Args:
        config: the entire config
        model_id: relevant model id
        feature_corr_matrix: correlation matrix of the top_n features
    """

    # get results if not explicitly passed in
    if feature_corr_matrix is None:
        feature_corr_matrix = utils.get_table_where(
            utils.get_db_conn(),
            config.db_config.schema_name,
            config.evaluation.feature_correlation.table_name,
            f'where model_id = {model.model_id}'
        )

    # will only happen if results were not saved to db
    if feature_corr_matrix is None:
        logging.warning(
            f'Cannot find feature_correlation results in {config.db_config.schema_name}.{config.evaluation.feature_correlation.table_name}.')
        return

    # want to drop model_id
    feature_corr_matrix = feature_corr_matrix.drop(columns=['model_id'])

    fig = go.Figure(go.Heatmap(
        z=feature_corr_matrix,
        x=feature_corr_matrix.columns,
        y=feature_corr_matrix.columns,
        colorscale=px.colors.diverging.RdBu,
        zmin=-1,
        zmax=1
    ))

    filepath = utils.get_module_filepath(
        config, model.experiment_id, 'feature_correlation')

    utils.save_plotly(
        fig, filepath, f'model-{model.model_id}', 'feature-correlation')


def main():
    parser = argparse.ArgumentParser(
        description='calculate feature correlation')
    parser.add_argument('-e', '--experiment_id', type=int, required=True,
                        help='experiment_id for feature corr calculation')
    parser.add_argument('-m', '--model_id', type=int, required=True,
                        help='model_id for feature corr calculation')
    parser.add_argument('-c', '--config_filename', type=str,
                        required=True, help='relevant config file')
    args = parser.parse_args()

    config = utils.read_config('config.yaml')
    model = utils.read_model_from_pkl_file(
        config, args.experiment_id, args.model_id)
    train_data = pd.read_csv(governance.get_train_matrix_info_from_model_id(
        config, args.experiment_id, model.model_id)[0])
    # only want to plot for top_n features
    top_n = config.evaluation.feature_correlation.top_n

    # check if  already written, so we don't have to recalculate
    feature_corr_matrix = utils.get_table_where(
        utils.get_db_conn(),
        config.db_config.schema_name,
        config.evaluation.feature_correlation.table_name,
        f"model_id = {model.model_id}"
    )

    if not len(feature_corr_matrix):
        feature_corr_matrix = calc_feature_corr(config, model, train_data)
        governance.save_feature_corr(config, feature_corr_matrix)

    if top_n > 0 and top_n < 100:  # setting 100 for max plot
        plot_top_feature_corr(config, model, feature_corr_matrix)


if __name__ == '__main__':
    main()
