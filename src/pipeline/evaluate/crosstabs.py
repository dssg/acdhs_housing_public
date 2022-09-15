import argparse

import numpy as np
import pandas as pd
import utils
from src.pipeline import governance


def feature_crosstabs(config, model, feature_df, top_k):
    ''' calculate avg feature value for different groups for a given model

    Args:
        config (dict): the entire config file
        feature_df (pd.DataFrame): features from validation matrix


    Notes: assume feature importance has already been calculated
    '''

    top_features_df = utils.get_table_where(
        utils.get_db_conn(),
        config.db_config.schema_name,
        config.evaluation.feature_importance.table_name,
        f'model_id = {model.model_id}'
    )

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

    # make predictions: first n values are 1, rest are 0
    predictions = np.zeros(len(predictions_df), dtype='bool')
    predictions[:top_k] = 1
    predictions_df['prediction'] = predictions

    # merge predictions with feature df
    feature_df = feature_df.astype(keys_dtypes).merge(
        predictions_df, on=list(keys_dtypes), how='left')

    # calculate which rows in feature_df correspond to tp, fp, etc
    selections = {
        'pos_label': feature_df.homelessness_label == 1,
        'neg_label': feature_df.homelessness_label == 0,
        'pos_pred': feature_df.prediction == 1,
        'neg_pred': feature_df.prediction == 0,
        'tp': (feature_df.homelessness_label == 1) & (feature_df.prediction == 1),
        'fn': (feature_df.homelessness_label == 1) & (feature_df.prediction == 0),
        'fp': (feature_df.homelessness_label == 0) & (feature_df.prediction == 1),
        'tn': (feature_df.homelessness_label == 0) & (feature_df.prediction == 0)
    }

    # create crosstab dataframe
    crosstab = top_features_df[['feature_name', 'value']].rename(
        columns={'value': 'importance'})

    def get_range(series): return str((min(series), max(series)))
    crosstab['feature_range'] = [
        get_range(feature_df[f]) for f in crosstab.feature_name]
    crosstab['pos_pred_range'] = [get_range(
        feature_df[f][selections['pos_pred']]) for f in crosstab.feature_name]
    crosstab['neg_pred_range'] = [get_range(
        feature_df[f][selections['neg_pred']]) for f in crosstab.feature_name]
    crosstab['pos_label_range'] = [get_range(
        feature_df[f][selections['pos_pred']]) for f in crosstab.feature_name]
    crosstab['neg_label_range'] = [get_range(
        feature_df[f][selections['neg_pred']]) for f in crosstab.feature_name]

    # add avg feature value for each feature in crosstab
    for name, selection in selections.items():
        crosstab[name] = [np.mean(feature_df[selection][f])
                          for f in crosstab.feature_name]

    return crosstab


if __name__ == '__main__':
    # parse command line args
    parser = argparse.ArgumentParser(description='plot feature importance')
    parser.add_argument('-m', '--model_id', type=int,
                        required=True, help='model_id for what we want to plot')
    parser.add_argument('-c', '--config_filename', type=str,
                        required=True, help='relevant config file')
    parser.add_argument('-k', '--top_k', type=int, required=True,
                        help='top k to select for positive class label')
    args = parser.parse_args()

    config = utils.read_config(f"{args.config_filename}.yaml")
    experiment_id = utils.get_experiment_id_from_model_id(
        config, args.model_id)
    model = utils.read_model_from_pkl_file(
        config, experiment_id, args.model_id)
    validate_filename, validate_date = governance.get_validate_matrix_info_from_model_id(
        config, experiment_id, model.model_id)
    feature_df = pd.read_csv(validate_filename)

    df = feature_crosstabs(config, model, feature_df, args.top_k)
    print(df)
