import argparse
import logging
import os
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.io as pio
import seaborn as sns
import utils
import yaml
from munch import munchify
from src.pipeline.evaluate.evaluation_metrics import *

# set default template
pio.templates.default = "plotly_white"


def iterative_plots(config, model_ids):
    """given a list of models, iteratively generate prk curve plots in order of the ids"""
    query = f"""
        select *
        from {config.db_config.schema_name}.{config.modeling_config.predictions_table_name}
        where model_id in ({','.join([str(model_id) for model_id in model_ids])})
        order by model_id, score desc
    """

    results_df = pd.read_sql(query, utils.get_db_conn())

    plt.figure()

    for model_i, model_id in enumerate(model_ids):
        # get data associated with model_id
        model_df = results_df[results_df.model_id == model_id]

        predictions = np.zeros(len(model_df))
        labels = model_df.homelessness_label.to_numpy()

        metric_data = []
        # generate precision and recall every 5 people
        # will already be sorted based on order from sql query
        for i in range(1, len(model_df), 5):
            predictions[:i] = 1
            metric_data.append(
                {'top_n': i, 'metric': 'precision', 'value': ppv(labels, predictions)})
            metric_data.append(
                {'top_n': i, 'metric': 'recall', 'value': tpr(labels, predictions)})

        sns.lineplot(data=pd.DataFrame(metric_data),
                     x="top_n", y="value", hue="metric")
        path = './model_ids-{}.png'.format(
            '_'.join([str(model_id) for model_id in model_ids[:model_i+1]]))
        plt.legend([], [], frameon=False)
        plt.savefig(path)


@utils.timer
def evaluate_over_time_plot(config, experiment_id, filename='', top_n=None, selected_model_groups=None):
    """This function generates a plot that displays a metric over time across all models

    Args:
        config (dict): config file. It has to include a metric, top_n, and max_regret parameter. 
        metric (str): ['precision', 'recall'], Metric used to evaluate the performance
        top_n (int): Top n cutoff used to evaluate the performance
        max_regret (int, 0 - 1): Tolerance for which models to include in evaluation plot. If max_regret is 1, all models are included. If max_regret is 0, only those with at least 1 best performance across all time-periods are included
    """

    schema_name = config.db_config.schema_name
    model_table_name = config.modeling_config.model_table_name
    model_params_table_name = config.modeling_config.model_params_table_name
    metrics_table_name = config.evaluation.metrics_table_name

    metric = config["evaluation"]["evaluate_over_time"]["metric"]
    top_n = top_n if top_n else config["evaluation"]["evaluate_over_time"]["top_n"]
    max_regret = config["evaluation"]["evaluate_over_time"]["max_regret"]

    if selected_model_groups is not None:
        subset_selected_model_groups = f'and model_group_id in ({selected_model_groups})'
    else:
        subset_selected_model_groups = ''

    query = f"""
    select 
        model_id, 
        model_group_id, 
        model_name, 
        validation_date, 
        {metric},
        max({metric}) over(partition by validation_date) as best_{metric},
        max({metric}) over(partition by validation_date) - {metric} as regret,
        string_agg(model_param_name || ': ' || model_param_value, '<br>' order by model_param_name, model_param_value) as model_params
        from {schema_name}.{metrics_table_name}
        left join {schema_name}.{model_table_name} using(model_id)
        left join {schema_name}.{model_params_table_name} using (model_id, model_name, model_group_id)
        where
            top_n = {top_n} and
            model_id in (
                select distinct model_id
                from {schema_name}.{model_table_name}
                where experiment_id = {experiment_id}
                {subset_selected_model_groups}
                )
        group by (model_id, model_group_id, model_name, {metric}, validation_date) 
        order by (model_id, validation_date)
    """

    try:
        results_df = pd.read_sql(query, utils.get_db_conn())
    except:
        logging.error(f"The following sql query failed: {query}")
        sys.exit()

    # remove models that are consistently below max regret
    threshold_model_groups = results_df[results_df['regret']
                                        <= max_regret].model_group_id.unique()
    results_df = results_df[(
        results_df["model_group_id"].isin(threshold_model_groups))]

    # add labels
    metric_label = metric.capitalize() + "@" + str(top_n)
    max_metric = max(results_df[metric])

    # sort values
    results_df = results_df.sort_values(["model_group_id", "validation_date"])
    best_results = results_df.groupby('validation_date')[
        f'best_{metric}'].agg(max).reset_index()

    # plot
    fig = px.line(
        results_df,
        x='validation_date',
        y=metric,
        color='model_name',
        line_group='model_group_id',
        hover_data={
            'model_id': True,
            'model_name': False,
            metric: False,
            'model_group_id': True,
            'validation_date': True,
            'model_params': True,
        },
        labels={
            "model_name": "Model types",
            metric: metric_label,
            "validation_date": "Validation date",
            "model_params": ""
        }
    )

    fig.update_traces(opacity=0.5)

    fig.add_scatter(
        x=best_results['validation_date'],
        y=best_results[f'best_{metric}'],
        name='Best case',
        mode='lines',
        line={
            'color': 'grey',
            'dash': 'dash',
        },
    )

    # update layout
    fig.update_layout(font_size=20)

    if max_metric < 0.5:
        fig.update_yaxes(range=[0, 0.5])
    else:
        fig.update_yaxes(range=[0, 1])

    # save plot
    base_filepath = utils.get_module_filepath(
        config, experiment_id, 'evaluate_over_time')
    if not os.path.exists(base_filepath):
        os.makedirs(base_filepath)

    path = f"{base_filepath}/eval_over_time_{metric}@{top_n}_maxregret_{max_regret}_experiment_{experiment_id}{filename}.png"
    fig.write_image(path, width=1000, height=800, scale=1)

    logging.info(f'Stored evaluation over time plot in {path}')

    return fig


def plot_metric_distribution(config, experiment_id, top_n=None, by_model_type=True):
    """This function plots the distribution of precision at top_n across all models for a given experiment

    Args:
        config (dict): config file.
        experiment_id (int): Identifies experiment of a given run
        metric (str): Selects which metric to plot. If metric is None, then metric is taken from config file
        top_n (int): Top n cutoff used to obtain precision estimates. If top_n is None, then top_n is taken from config file
        by_model_type (bool): If True, plot facets by model type. Otherwise, plot visualises overall distribution
    """

    schema_name = config.db_config.schema_name
    metrics_table_name = config.evaluation.metrics_table_name
    model_table_name = config.modeling_config.model_table_name
    metric = config.evaluation.evaluate_over_time.metric
    top_n = top_n if top_n else config.evaluation.evaluate_over_time.top_n

    query = f"""
    select {metric}, model_name
    from {schema_name}.{metrics_table_name}
    left join {schema_name}.{model_table_name} using(model_id)
    where top_n = {top_n}
    and experiment_id = {experiment_id}
    and model_name not like '%%baseline%%'
    """

    df = pd.read_sql(query, utils.get_db_conn())

    model_name = "model_name" if by_model_type else None

    metric_label = metric.capitalize() + "@" + str(top_n)

    fig = px.histogram(
        df,
        x=metric,
        facet_row=model_name,
        color=model_name,
        title=f'{metric.capitalize()} across all models, at k = {top_n}',
        labels={
            "model_name": "Model types",
            metric: metric_label,
        },
    )
    fig.update_yaxes(matches=None)
    fig.update_layout(font_size=20)

    fig.for_each_annotation(lambda a: a.update(text=""))
    fig.for_each_yaxis(lambda y: y.update(title=''))
    fig.add_annotation(x=-0.025, y=0.5, text="Count",
                       textangle=-90, xref="paper", yref="paper")

    return fig


def main():

    # run: python src/pipeline/evaluate/visualize.py --experiment_id 1
    # get help with: python src/pipeline/evaluate/visualize.py -h

    parser = argparse.ArgumentParser(
        description='Regenerate the evaluation over time.')
    parser.add_argument('-e_id', '--experiment_id', type=int, required=True,
                        help='the exeriment id for which the evaluation over time plot should be regenerated')
    parser.add_argument('-c', '--config_filename', type=str,
                        required=True, help='config file name.')
    parser.add_argument('--filename', type=str,
                        required=False, help='plot file name addition.')

    args = parser.parse_args()

    with open(f"src/pipeline/{args.config_filename}.yaml", "r") as config_file:
        config = munchify(yaml.safe_load(config_file))

    logging.info(
        f'regenerating the evaluation plot over time for experiment with id {args.experiment_id}')
    evaluate_over_time_plot(config, args.experiment_id, args.filename)


if __name__ == "__main__":
    main()
