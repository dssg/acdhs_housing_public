import logging

import aequitas.plot as ap
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.io as pio
import utils
from aequitas.bias import Bias
from aequitas.group import Group
from src.pipeline.evaluate.evaluation_metrics import *

pio.templates.default = "plotly_white"


def calculate_metrics(y, selection):
    """Calculate various metrics: acceptance_rate, recall, fpr, fnr, tnr, precision, fdr, forate, npv.

    Args:
        y (pd series): labels
        selection (pd series): selected individuals

    Returns:
        dict: {metric: value} 
    """
    metric_values = {}
    for metric_name, metric in all_metrics.items():
        metric_value = metric(y, selection)
        metric_values[metric_name] = metric_value

    return metric_values


def store_results_in_db(config, model, validation_date, eval_top_n_all):
    """save evaluation results scores to db

    Args:
        config (dict): configuration
        model_id (int): the id of the trained model
        eval_top_n_all (dict): evaluation results
    """

    logging.debug('writing evaluation metrics to db')

    # add necessary vars to df that will be written
    results_df = pd.DataFrame.from_dict(eval_top_n_all, orient="index")
    results_df.reset_index(inplace=True)
    results_df = results_df.rename(columns={"index": "top_n"})
    results_df["model_id"] = model.model_id
    results_df["validation_date"] = validation_date

    # save to db
    db_conn = utils.get_db_conn()

    utils.set_role(db_conn, config.db_config.db_name)

    results_df.to_sql(
        con=db_conn,
        name=config.evaluation.metrics_table_name,
        schema=config.db_config.schema_name,
        if_exists='append',
        index=False
    )


@utils.timer
def plot_score_distributions(config, model, df_validation):

    eval_filepath = utils.get_module_filepath(
        config, model.experiment_id, 'score_distribution')
    utils.create_directory_if_not_exists(eval_filepath)

    # plot score distribution
    # sns.histplot(data=df_validation, x="score", hue="homelessness_label", bins=50,
    #             multiple="stack", kde=False).set(title='Score distribution by label')

    fig1 = px.histogram(
        df_validation,
        x='score',
        color='homelessness_label',
        nbins=150,
        title='Score distribution by label',
        facet_col='homelessness_label'
    )
    fig1.update_yaxes(matches=None)

    path = '{}/model-{}-score_distribution.pdf'.format(
        eval_filepath, model.model_id)

    utils.save_plotly(fig1, eval_filepath,
                      f'model-{model.model_id}-score_distribution', 'score_distribution')

    logging.info(f'score distribution plots have been saved as: {path}')

    return fig1


def get_eval_at_top_values(df_validation_len):
    """ figure out which top_k values to evaluate for (PRK curves)

    Args:
        df_validation_len (int): length of df validation

    Returns:
        (list): top_k values to eval for

    """
    eval_at_top = [int(i) for i in np.linspace(
        0, min(10000, df_validation_len), num=200)]
    eval_at_top.extend(list(range(0, 1001, 20)))
    eval_at_top = list(set(eval_at_top))
    eval_at_top.sort()

    return eval_at_top


@utils.timer
def plot_metrics(config, model, validation_date, df_validation):
    """Plot the metrics (e.g., precision and recall) for top_n individuals.

    Args:
        config (dict): configuration
        model_id (int): the id of the trained model
        df_validation (df): data frame containing features, predictions and labels
    """

    df_validation = df_validation.sort_values(
        by=['score'], ascending=False).reset_index()

    y_validation = df_validation["homelessness_label"]

    metric_data = []
    for metric in config.evaluation.metrics:
        df_validation["selected"] = 0
        eval_at_top = get_eval_at_top_values(df_validation.shape[0])
        for i in eval_at_top:
            df_validation.loc[df_validation.index[:i], 'selected'] = 1
            # TODO: how to handle ties?
            metric_data.append({'top_n': i, 'metric': metric, 'value': all_metrics[metric](
                y_validation, df_validation["selected"])})

    fig = px.line(
        pd.DataFrame(metric_data),
        x="top_n",
        y="value",
        color="metric",
        title=utils.generate_plot_title_for_model(
            'PRK curve', model, 'validated', validation_date)
    )

    # save both a static and interactive version of plots
    eval_filepath = utils.get_module_filepath(
        config, model.experiment_id, 'prk')

    utils.save_plotly(fig, eval_filepath,
                      f'model-{model.model_id}', 'generic prk')


@utils.timer
def plot_metrics_across_groups(config, model, validation_date, df_validation, validate_data):
    """Plot the metrics (e.g., precision and recall) for top_n individuals across groups.

    Args:
        config (dict): configuration
        model_id (int): the id of the trained model
        df_validation (df): data frame containing features, predictions and labels
    """
    group_info = utils.get_race_and_gender()

    df_validation = df_validation.merge(
        group_info, on=['client_hash'], how='left')

    if 'days_since_hl_imp' in validate_data.columns:
        validation_subset = validate_data[[
            'client_hash', 'as_of_date', 'days_since_hl_imp']]

        df_validation = df_validation.merge(
            validation_subset, on=['client_hash', 'as_of_date'], how='left')

    df_validation = df_validation.sort_values(
        by=['score'], ascending=False).reset_index()

    for attribute, groups in config.evaluation.groups.items():
        logging.debug(f"Evaluation across different values for: {attribute}")

        metric_data = []
        for metric in config.evaluation.metrics:
            df_validation["selected"] = 0
            eval_at_top = get_eval_at_top_values(df_validation.shape[0])
            for i in eval_at_top:
                df_validation.loc[df_validation.index[:i], 'selected'] = 1
                # TODO: how to handle ties?

                for group in groups:
                    df_group = df_validation[df_validation[attribute] == group]
                    if sum(df_group["selected"]) == 0:
                        #print("   0 individuals are selected from the group", group, "(", i, "individuals selected in total )")
                        continue
                    elif sum(df_group["homelessness_label"]) == 0:
                        #print("   0 individuals have a positive label from the group", group, "(", sum(df_validation["homelessness_label"]), "individuals with positive label in total )")
                        continue
                    #print("   ", attribute, "=", group, "consisting of", df_group.shape[0], "individuals [", df_group.shape[0]/df_validation.shape[0], "% of the entire validation set population ]")
                    metric_data.append({'top_n': i, 'metric': metric, 'group': group, 'value': all_metrics[metric](
                        df_group["homelessness_label"], df_group["selected"])})

        fig = px.line(
            pd.DataFrame(metric_data),
            x="top_n",
            y="value",
            color="metric",
            line_dash="group",
            title=utils.generate_plot_title_for_model(
                f'PRK curve {attribute}', model, 'validated', validation_date)
        )

        # save both a static and interactive version of plots
        # calculate and create necessary dirs
        eval_filepath = utils.get_module_filepath(
            config, model.experiment_id, 'prk')
        utils.save_plotly(
            fig, eval_filepath, f'model-{model.model_id}-eval_across_group-{attribute}', 'prk across groups')


@utils.timer
def evaluate(config, model, validation_date, df_validation, validate_data):
    """Evaluate the model: plot specified metrics and store all metrics to db.

    Args:
        config (dict): configuration
        model_id (int): the id of the trained model
        df_validation (df): data frame containing features, predictions and labels

    Returns:
        dataframe: model groups and names of best performing models
    """

    eval_top_n = {}
    eval_top_n_all = {}

    predictions = df_validation["score"]
    y_validation = df_validation["homelessness_label"]

    # loop through the specified top_n in the config file. Calculate precision and recall for the entire validation set, as a sanity check.
    all_top_n = [t for t in config.evaluation.top_n if t < len(y_validation)]
    for top_n in all_top_n + [len(y_validation)]:
        # get indices of n highest scores
        idx_top_n = np.argpartition(predictions, -top_n)[-top_n:]
        scores_top_n = predictions[idx_top_n]
        threshold = min(scores_top_n)
        df_validation["selected"] = 0
        df_validation.loc[idx_top_n, "selected"] = 1
        # TODO: how to handle ties?

        eval_top_n_all[top_n] = {"threshold": threshold}
        eval_top_n_all[top_n].update(calculate_metrics(
            y_validation, df_validation["selected"]))

    eval_top_n = {top_n: {k: v for k, v in results.items() if k in (
        config.evaluation.metrics + ["threshold"])} for top_n, results in eval_top_n_all.items()}

    store_results_in_db(config, model, validation_date, eval_top_n_all)

    plot_score_distributions(config, model, df_validation)
    plot_metrics(config, model, validation_date, df_validation)
    plot_metrics_across_groups(
        config, model, validation_date, df_validation, validate_data)


def select_best_models(config, experiment_id, top_n=None, metric=None, nr_best_models=None, by_model_type=None, min_time_splits_per_model=0, exclude_baselines='Yes'):
    """Returns the best models from the database, according to a specified metrics

    Args:
        config (dict): configuration
        nr_best_models (int): number of top models to return. If by_model_type is specified, then returns number of models for each model_type
        by_model_type (bool): indicates whether or not models should be ranked by model_type. If True, then function returns top models for each model type
    """

    schema_name = config.db_config.schema_name
    metrics_table_name = config.evaluation.metrics_table_name
    model_table_name = config.modeling_config.model_table_name
    metric = config.post_modeling.select_best_models.metric
    top_n = config.post_modeling.select_best_models.top_n
    nr_best_models = config.post_modeling.select_best_models.nr_best_models
    by_model_type = by_model_type if by_model_type else config.post_modeling.select_best_models.by_model_type

    query_generic = f"""
    with eval_table as (
        select 
            model_group_id, 
            model_name,
            array_agg(model_id::int) as model_ids,
            avg({metric}) as avg_{metric},
            row_number() over(partition by model_name order by avg({metric})  desc) rank_{metric}
        from {schema_name}.{metrics_table_name}
        left join (
            select
                *,
                count(model_id) over (partition by model_group_id) as nr_of_model_ids
            from {schema_name}.{model_table_name}
        ) as model_table using(model_id)
        where
    	    top_n = {top_n} and
            model_id in (
            	select distinct model_id
                from {schema_name}.model_metadata
                where experiment_id = {experiment_id}
                )
            {"and model_name not like 'baseline%%'" if exclude_baselines == 'Yes' else ""}
            and model_table.nr_of_model_ids >= {min_time_splits_per_model}
        group by model_group_id, model_name
        )"""

    # return models with best avg precision for each model_type
    if by_model_type:
        query = query_generic + f"""
        select model_group_id, model_name, model_ids, avg_{metric}
        from eval_table
        where rank_precision <= {nr_best_models}
        """

    # return models with best avg precision
    else:
        query = query_generic + f"""
        select model_group_id, model_name, model_ids, avg_{metric}
        from eval_table
        order by avg_{metric} desc
        limit {nr_best_models}
        """

    db_conn = utils.get_db_conn()
    return pd.read_sql(query, db_conn)


def get_data_for_audit(config, experiment_id, model_id):
    """Generates dataframe of scores, labels, and client demographics for audit analysis

    Args: 
        config (dict): Config file
        experiment_id (int): Experiment identifier
        model_id (int): Model_id of model for which bias audit should be runs

    Returns: Dataframe with client_hash, as_of_date, and client characteristics
    """

    query = f"""
    select 
        client_hash, 
        as_of_date,
        homelessness_label as label_value,
        score,
        c.gender,
        c.race, 
        hl.days_since_hl_imp::varchar
    from {config.db_config.schema_name}.{config.modeling_config.predictions_table_name}
    left join {config.db_config.schema_name}.{config.modeling_config.model_table_name}
        using(model_id)
    left join clean.client_feed c 
        using(client_hash)
    left join {config.db_config.schema_name}.feature_hl_table_program_end_dt hl
	    using(client_hash, as_of_date)
    where model_id = {model_id}
    and experiment_id = {experiment_id}
    """

    df = pd.read_sql(query, utils.get_db_conn())

    return df


def generate_audit_crosstab(audit_df, config, top_n=None):
    """Generates the audit crosstab by attribute group, where attribute information is pulled from config

    Args:
        audit_df (dataframe): Dataset to be used for audit crosstabs. Must contain score, label_value, and groups
        config (dict): Config file

    Returns:
        dataframe: Returns dataframe of metrics by attribute-group pair
    """

    audit_groups_and_attributes = {
        k: str(v[0]) for k, v in config.post_modeling.bias_audit.groups.items()}
    top_n = top_n if top_n else config.post_modeling.bias_audit.top_n

    # Initialize Aequitas
    g = Group()

    # returns a dataframe of the group counts and group value bias metrics.
    xtab, _ = g.get_crosstabs(
        audit_df,
        score_thresholds={'rank_abs': [top_n]},
        attr_cols=audit_groups_and_attributes)

    return xtab


def generate_audit_plots(audit_df, config, bias_metric=None, top_n=None, disparity_tolerance=None):

    audit_groups = config.post_modeling.bias_audit.groups.keys()
    audit_groups_and_attributes = {
        k: str(v[0]) for k, v in config.post_modeling.bias_audit.groups.items()}
    top_n = top_n if top_n else config.post_modeling.bias_audit.top_n
    bias_metric = bias_metric if bias_metric else config.post_modeling.bias_audit.metric
    disparity_tolerance = disparity_tolerance if disparity_tolerance else config.post_modeling.bias_audit.disparity_tolerance

    b = Bias()

    xtab = generate_audit_crosstab(audit_df, config, top_n)

    bdf = b.get_disparity_predefined_groups(
        xtab,
        original_df=audit_df,
        ref_groups_dict=audit_groups_and_attributes)

    plots = []
    for group in audit_groups:
        try:
            plots.append(ap.absolute(bdf, bias_metric, group,
                         fairness_threshold=disparity_tolerance))
        except:
            print("Error, moving on to next plot")
            continue

    return plots
