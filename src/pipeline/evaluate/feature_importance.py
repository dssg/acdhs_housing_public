import argparse
import logging

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.io as pio
import utils
from src.pipeline import governance

pio.templates.default = "plotly_white"


@utils.timer
def calc_feature_importance(config, model, train_data):
    """ calculate feature importance plot and save to file

    Args:
        config: the entire config
        model: model object
        train_data: training data used to train model

    Notes:
        store_top_n from config: specify if you want to look at only the top n most influential features. Defaults to all features
    """

    feature_names = [c for c in train_data.columns if c not in (
        'client_hash', 'as_of_date', 'homelessness_label')]

    top_n = config.evaluation.feature_importance.store_top_n \
        if config.evaluation.feature_importance.store_top_n > 0 else len(feature_names)

    if hasattr(model.model, 'feature_importances_'):
        importance_df = pd.DataFrame({
            'feature_name': feature_names,
            'value': model.model.feature_importances_
        }).sort_values(by='value', ascending=False).iloc[:top_n]
    elif hasattr(model.model, 'coef_'):
        importance_df = pd.DataFrame({
            'feature_name': feature_names,
            'value': model.model.coef_[0, :],
            'abs_value': [abs(x) for x in model.model.coef_[0, :]]
        }).sort_values(by='abs_value', ascending=False).iloc[:top_n].drop(columns='abs_value')
    else:
        logging.error(
            f'Feature importance not implemented for model type {model.model_name}!')
        return None

    importance_df['model_id'] = model.model_id
    return importance_df


@utils.timer
def plot_top_feature_importance(config, model, train_end_date, top_features_df):
    """ plot feature importance and save to file. If top_features is not passed in, it
        assumes that calc_feature_importance has already ran and pulls results from db.

    Args:
        config: the entire config
        model_id: relevant model id
        train_end_date: last as_of_date in the training data
        top_features_df: the top n features to plot
    """

    top_features_df['abs_value'] = [abs(x) for x in top_features_df['value']]
    top_features_df = top_features_df.sort_values(by='abs_value', ascending=False)[
        :config.evaluation.feature_importance.plot_top_n]
    top_features_df = top_features_df[:
                                      config.evaluation.feature_importance.plot_top_n]

    # will only happen if results were not saved to db
    if top_features_df is None:
        logging.warning(
            f'Cannot find feature_importance results in {config.db_config.schema_name}.{config.evaluation.feature_importance.table_name}.')
        return

    fig = px.bar(
        top_features_df,
        x='value',
        y='feature_name',
        orientation='h',
        title=utils.generate_plot_title_for_model(
            'Feature importance', model, 'trained', train_end_date),
        labels={
            'feature_name': 'Feature',
            'value': 'Feature importance'
        }
    )
    fig.update_layout(yaxis={'categoryorder': 'total ascending'})

    filepath = utils.get_module_filepath(
        config, model.experiment_id, 'feature_importance')
    utils.save_plotly(
        fig, filepath, f'model-{model.model_id}', 'feature importance')

    return fig


def plot_top_feature_distributions(config, model, experiment_id, train_end_date, top_features_df, train_matrix):
    """ Plots feature distributions for the top most important features
        Assumes that feature_importance has already been run, since those are the
        features we will plot here (going through all features would take forever)

    Args:
        feature_df (pd.DataFrame): features to consider
    """

    # get topmost important features from db
    top_features_df['abs_value'] = [abs(x) for x in top_features_df.value]
    top_features_df = top_features_df.sort_values(by='abs_value', ascending=False)[
        :config.evaluation.feature_importance.plot_top_n]

    # if features weren't found in db, can't make plot
    if not len(top_features_df):
        logging.warning(
            f'Cannot find feature_importance results in {config.db_config.schema_name}.{config.evaluation.feature_importance.table_name}.')
        return

    # create separate dfs for positive and negative labels
    neg_label = train_matrix[train_matrix.homelessness_label ==
                             0][top_features_df.feature_name.values]
    pos_label = train_matrix[train_matrix.homelessness_label ==
                             1][top_features_df.feature_name.values]

    #fig, axes = plt.subplots(nrows=len(top_features_df), ncols=3, sharex=False, sharey=False, figsize=(14, 30))
    fig = plt.figure(constrained_layout=True,
                     figsize=(14, 3*len(top_features_df)))
    fig.suptitle('Feature distributions')

    subfigs = fig.subfigures(nrows=len(top_features_df), ncols=1)

    for feature_name, subfig in zip(top_features_df.feature_name, subfigs):
        logging.debug(
            f'feature_distributions for model_id {model.model_id}: plotting feature {feature_name}')
        subfig.suptitle(feature_name)
        axes = subfig.subplots(nrows=1, ncols=3)

        series_neg_label = neg_label[feature_name]
        series_pos_label = pos_label[feature_name]

        def density_args(series): return {
            'ind': np.linspace(min(series), max(series), num=50),
            'bw_method': 'silverman'
        }

        try:
            series_neg_label.plot.density(
                ax=axes[0], **density_args(series_neg_label))
            series_neg_label.plot.density(
                ax=axes[2], **density_args(series_neg_label))

            series_pos_label.plot.density(
                ax=axes[1], **density_args(series_pos_label))
            series_pos_label.plot.density(
                ax=axes[2], **density_args(series_pos_label))
        except Exception as e:
            print(e)
            logging.debug(
                f'feature_distributions got following exception: {e}')

    plt.title(utils.generate_plot_title_for_model(
        'Feature distributions', model, 'trained', train_end_date))
    filepath = utils.get_module_filepath(
        config, experiment_id, 'feature_importance')
    plt.savefig(f'{filepath}/model_{model.model_id}-feature_distributions.png')


def main():
    # parse command line args
    parser = argparse.ArgumentParser(description='plot feature importance')
    parser.add_argument('-m', '--model_id', type=int,
                        required=True, help='model_id for what we want to plot')
    parser.add_argument('-c', '--config_filename', type=str,
                        required=True, help='relevant config file')
    args = parser.parse_args()

    config = utils.read_config(f"{args.config_filename}.yaml")
    experiment_id = utils.get_experiment_id_from_model_id(
        config, args.model_id)
    model = utils.read_model_from_pkl_file(
        config, experiment_id, args.model_id)
    train_filename, train_end_date = governance.get_train_matrix_info_from_model_id(
        config, experiment_id, model.model_id)
    train_df = pd.read_csv(train_filename)

    # check if feature importance already written, so we don't have to recalculate
    top_features = utils.get_table_where(
        utils.get_db_conn(),
        config.db_config.schema_name,
        config.evaluation.feature_importance.table_name,
        f"model_id = {model.model_id}"
    )

    if not len(top_features):
        top_features = calc_feature_importance(config, model, train_df)
        print(top_features)
        governance.save_feature_importance(config, top_features)
    plot_top_feature_importance(config, model, train_end_date, top_features)

    plot_top_feature_distributions(
        config, model, experiment_id, train_end_date, top_features, train_df)


if __name__ == '__main__':
    main()
