import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import seaborn as sns
import streamlit as st
import utils
from src.pipeline import governance
from src.pipeline.evaluate.crosstabs import feature_crosstabs
from src.pipeline.evaluate.feature_importance import plot_top_feature_importance
from src.pipeline.evaluate.model_evaluator import (calculate_metrics,
                                                   generate_audit_crosstab,
                                                   generate_audit_plots,
                                                   get_data_for_audit,
                                                   plot_score_distributions,
                                                   select_best_models)
from src.pipeline.evaluate.visualize import (evaluate_over_time_plot,
                                             plot_metric_distribution)


@st.cache
def get_feature_matrix(config, experiment_id, model):
    return governance.get_feature_matrix_with_predictions(config, experiment_id, model)


def display_chart_group(charts, name, height):
    if charts is None or None in charts:
        return
    st.subheader(name)
    for chart in charts:
        chart.update_layout(height=height)
        st.plotly_chart(chart, use_container_width=True)


@st.cache
def get_crosstabs(config, model, feature_df, top_k):
    return feature_crosstabs(config, model, feature_df, int(top_k))


@st.cache(allow_output_mutation=True)
def get_eval_over_time(config, experiment_id, top_n, selected_model_groups=None):
    return evaluate_over_time_plot(config, experiment_id, '', top_n=top_n, selected_model_groups=selected_model_groups)


@st.cache(allow_output_mutation=True)
def get_metric_distribution(config, experiment_id, top_n):
    return plot_metric_distribution(config, experiment_id, top_n=top_n)


@st.cache(allow_output_mutation=True)
def get_audit_plots(audit_df, config, bias_metric, top_n):
    return generate_audit_plots(audit_df, config, bias_metric, top_n=top_n)


def investigate_model_performance(config, experiment_id, top_k):
    plot = get_metric_distribution(config, experiment_id, top_n=top_k)
    plot.update_layout(height=800)
    st.plotly_chart(plot, use_container_width=True)


def get_validation_dates(config, experiment_id):

    query_validations_dates = f'''
    select matrix_date
    from {config.db_config.schema_name}.{config.matrix_config.table_name}
    where matrix_type = 'validate'
    and experiment_id = {experiment_id}
    order by 1 desc;
    '''
    db_conn = utils.get_db_conn()

    validation_dates = pd.read_sql(query_validations_dates, db_conn)[
        'matrix_date']

    return validation_dates


def investigate_best_models(config, experiment_id, top_k):
    st.write('Select best model groups according to which metric and top k?')

    metrics, _ = utils.get_metrics_and_top_k(config)

    validation_dates = get_validation_dates(config, experiment_id)

    col, col2, _ = st.columns([1, 5, 0.1])

    with col:
        metric = st.selectbox('Metric', metrics)
        num_models = st.text_input('# model groups to return')
        min_time_splits_per_model = st.selectbox('min time splits per model', range(1, len(
            validation_dates) + 1), index=range(1, len(validation_dates) + 1).index(len(validation_dates)))
        by_model_type = st.radio('Best overall or best per model type?', [
                                 'Overall', 'By model type'])
        exclude_baselines = st.radio('Exclude baselines?', ['Yes', 'No'])

    if not metric or not num_models:
        return

    with col2:
        st.subheader(
            f'Evaluating top {num_models} models with best {metric}@{top_k}')

    with st.spinner('Finding best models'):
        by_model_type_bool = by_model_type == 'By model type'
        best_models_df = select_best_models(
            config, experiment_id, top_k, metric, num_models, by_model_type_bool, min_time_splits_per_model=min_time_splits_per_model, exclude_baselines=exclude_baselines)

    with st.spinner('Generating evaluation over time plot...'):
        eval_over_time_for_selected_model_group_ids(config, experiment_id, top_k, ', '.join(
            [str(m) for m in list(best_models_df.model_group_id)]), col=col2)

    _, col, _ = st.columns([0.5, 1, 0.5])
    col.write("Best models:")
    col.write(best_models_df)

    for model_group_id, model_ids, avg_metric in zip(best_models_df.model_group_id, best_models_df.model_ids, best_models_df[f'avg_{metric}']):
        with st.spinner('Reading models from disk...'):
            models = [utils.read_model_from_pkl_file(
                config, experiment_id, model_id) for model_id in model_ids]
        model_str = utils.generate_model_group_str(
            models[0].model_name, models[0].hyperparams)
        if model_str.startswith('baseline'):
            continue

        col.write(
            f'Model group id #{model_group_id}: {model_str} over {len(model_ids)} time splits')
        col.write(f'   average {metric}: {avg_metric}')

        feature_top_n = config.evaluation.feature_importance.plot_top_n

        top_features_models = [utils.get_table_where(
            utils.get_db_conn(),
            config.db_config.schema_name,
            config.evaluation.feature_importance.table_name,
            f"model_id = {model_id}"
        ).sort_values(by='value', ascending=False)[:feature_top_n] for model_id in model_ids]

        for df in top_features_models:
            df['rank'] = range(1, len(df)+1)

        validation_dates = utils.get_table_where(
            utils.get_db_conn(),
            config.db_config.schema_name,
            config.evaluation.metrics_table_name,
            f"model_id in ({','.join([str(m) for m in model_ids])})"
        )

        # merge with validation dates
        over_all_models = pd.concat(top_features_models).merge(
            validation_dates, on='model_id', how='left').sort_values(by=['validation_date', 'rank'])

        col.write(
            f'Which features consistently show up in the top {feature_top_n}?')
        jaccard_sim = utils.avg_similarity(
            over_all_models, utils.jaccard_similarity)
        rank_corr = utils.avg_similarity(
            over_all_models, utils.rank_correlation)
        col.write(
            f'Avg Jaccard similarity: {jaccard_sim:.2f}. Avg Rank correlation: {rank_corr:.2f}')

        fig = px.line(
            over_all_models,
            x='validation_date',
            y='rank',
            line_shape='spline',
            render_mode='svg',
            line_dash='feature_name',
            color='feature_name',
            markers=True
        )
        fig.update_layout(height=900, yaxis={'autorange': 'reversed'})
        fig.update_traces(opacity=0.75, line={'width': 3})

        col.plotly_chart(fig, use_container_width=True)

        # TODO: average Jaccard similarity & rank correlation over time (bt subsequent models)


def eval_over_time_for_selected_model_group_ids(config, experiment_id, top_k, selected_model_groups, col=None):
    model_group_ids = get_ids_from_str(selected_model_groups)
    model_str = []
    for model_group_id in model_group_ids:

        model_ids = utils.get_table_where(
            utils.get_db_conn(),
            config.db_config.schema_name,
            config.modeling_config.model_table_name,
            where=f"model_group_id = '{model_group_id}'"
        ).model_id

        with st.spinner('Reading models from disk...'):
            models = [utils.read_model_from_pkl_file(
                config, experiment_id, model_id) for model_id in model_ids]

        model_str.append((model_group_id, utils.generate_model_group_str(
            models[0].model_name, models[0].hyperparams)))

    if col is None:
        _, col, _ = st.columns([1, 5, 0.1])

    with col:
        st.subheader(f'Evaluating selected model groups over time')
        for model_group, m_str in model_str:
            st.write(f'Model group {model_group}: {m_str}')

    with st.spinner('Creating evaluation over time plot'):
        plot = get_eval_over_time(
            config, experiment_id, top_n=top_k, selected_model_groups=selected_model_groups)
        plot.update_layout(
            height=800,
        )

    with col:
        st.plotly_chart(plot, use_container_width=True)


def investigate_model_group(config, experiment_id, top_k):
    col, _, _ = st.columns([0.5, 1, 1.5])
    with col:
        selected_model_groups = st.text_input(
            'Look at particular model groups')
        if not selected_model_groups:
            return

    eval_over_time_for_selected_model_group_ids(
        config, experiment_id, top_k, selected_model_groups)

    # TODO: analysis: average perf over time
    # TODO: compare prk curves
    # TODO: Jaccard similarity and rank correlation


def investigate_specific_model(config, experiment_id, top_k):
    col, _, _ = st.columns([0.5, 1, 1.5])
    with col:
        model_id = st.text_input('Look at a particular model id')
        if not model_id:
            return

    model_id = int(model_id)

    model = utils.read_model_from_pkl_file(config, experiment_id, model_id)

    prk_curves, sd_curves = utils.find_curves_for_model_id(
        config, experiment_id, model_id)

    top_features = utils.get_table_where(
        utils.get_db_conn(),
        config.db_config.schema_name,
        config.evaluation.feature_importance.table_name,
        f"model_id = {model_id}"
    )

    _, train_end_date = governance.get_train_matrix_info_from_model_id(
        config, experiment_id, model.model_id)
    fi_curve = plot_top_feature_importance(
        config, model, train_end_date, top_features)

    st.header(
        f'Picked model {model.model_id}: {utils.generate_model_group_str(model.model_name, model.hyperparams)}')

    _, col, _ = st.columns([0.5, 1, 0.5])

    with st.spinner('Loading validation matrix'):
        feature_df = get_feature_matrix(config, experiment_id, model)

    if not sd_curves:
        sd_curves = plot_score_distributions(config, model, feature_df)

    with col:
        display_chart_group(prk_curves, 'PRK curves', 600)
        display_chart_group(sd_curves, 'Score distributions', 600)
        display_chart_group([fi_curve], 'Feature importance', 800)

    if st.button('Generate crosstabs (this might take a while...)'):
        # only generate crosstabs after button is pressed
        with st.spinner('Creating crosstabs'):
            crosstab = get_crosstabs(config, model, feature_df, int(top_k))

        _, col, _ = st.columns([0.1, 1, 0.1])
        with col:
            st.write('Crosstabs')
            st.write(crosstab)


def investigate_model_output(config, experiment_id, top_k):
    col, _, _ = st.columns([0.5, 1, 1.5])
    with col:
        model_id = st.text_input('Look at the output of a particular model id')
        if not model_id:
            return

    model_id = int(model_id)

    model = utils.read_model_from_pkl_file(config, experiment_id, model_id)

    st.header(
        f'Picked model {model.model_id}: {utils.generate_model_group_str(model.model_name, model.hyperparams)}')

    _, col, _ = st.columns([0.5, 1, 0.5])

    with st.spinner('Loading model output'):
        feature_df = get_feature_matrix(config, experiment_id, model)
        #top_k_list = feature_df.nlargest(n=top_k, columns=['score']).index

        df_sorted = feature_df.sort_values(
            by='score', ascending=False).reset_index()
        df_sorted.loc[:, 'selected'] = 0
        df_sorted.loc[:top_k-1, 'selected'] = 1

        group_info = utils.get_race_and_gender()

        column_types = {'client_hash': 'string',
                        'as_of_date': 'datetime64[ns]'}

        df_sorted = df_sorted.merge(
            group_info, on=['client_hash'], how='left').astype(column_types)

        df_future_hl = utils.get_future_hl(config).astype(column_types)

        df_sorted = df_sorted.merge(
            df_future_hl, on=['client_hash', 'as_of_date'], how='left')

        df_sorted["hl_in_the_future"] = df_sorted["hl_in_the_future"].replace([0, 1], [
                                                                              "no", "yes"])

        past_and_future_rental_assistance = utils.get_past_and_future_rental_assistance(
            config).astype(column_types)

        df_sorted = df_sorted.merge(past_and_future_rental_assistance, on=[
                                    'client_hash', 'as_of_date'], how='left')

        df_sorted["rental_assistance_in_the_future"] = df_sorted["rental_assistance_in_the_future"].replace([
                                                                                                            0, 1], ["no", "yes"])
        df_sorted["rental_assistance_in_the_past"] = df_sorted["rental_assistance_in_the_past"].replace([
                                                                                                        0, 1], ["no", "yes"])

        df_sorted["race"] = df_sorted["race"].replace(
            ["Black/African American"], "African American")
        df_sorted["gender"] = df_sorted["gender"].replace(
            ["1~Male", "2~Female"], ["Male", "Female"])
        df_sorted["prev. homeless"] = df_sorted["days_since_hl_imp"].replace([0, 1], [
                                                                             "Yes", "No"])

        default_columns = ['client_hash', 'as_of_date', 'score', 'homelessness_label', 'prev. homeless', 'hl_in_the_future', 'days_until_next_hl', 'days_since_days_hl_imp', 'race', 'gender', 'days_since_days_hl', 'days_since_hl', 'days_until_next_rental_assistance', 'rental_assistance_in_the_future', 'days_since_last_rental_assistance', 'rental_assistance_in_the_past',
                           'SUM(days_hl,@100year)', 'age', 'age_imputation', 'days_since_*all_feed', 'days_since_*all_feed_imp', 'COUNT(*all_feed,@3month)', 'COUNT(*all_feed,@6month)', 'COUNT(*all_feed,@1year)', 'COUNT(*all_feed,@5year)', 'COUNT(*all_feed,@100year)', 'COUNT(*distinct_feed,@3month)', 'COUNT(*distinct_feed,@6month)', 'COUNT(*distinct_feed,@1year)', 'COUNT(*distinct_feed,@5year)', 'COUNT(*distinct_feed,@100year)']

        # choose only this subset of the df to make more efficient
        df_sorted = df_sorted.loc[:, default_columns + ['selected']]
        model_output = df_sorted.loc[(
            df_sorted.selected == 1), default_columns].reset_index()

    _, col, _ = st.columns([0.1, 1, 0.1])
    with col:
        st.write('Model output')
        st.write(f'cohort size: {df_sorted.shape[0]} individuals\n')
        st.write(model_output)
        st.write(
            f'nr of individuals with label=0 who become homeless later on in the future: {df_sorted[(df_sorted["selected"] == 1) & (df_sorted["hl_in_the_future"] == "yes") & (df_sorted["homelessness_label"] == 0)].shape[0]}')
        st.write(f'nr of individuals who receive RA in the future: {df_sorted[(df_sorted["selected"] == 1) & (df_sorted["rental_assistance_in_the_future"] == "yes")].shape[0]} [label=0 : {df_sorted[(df_sorted["selected"] == 1) & (df_sorted["rental_assistance_in_the_future"] == "yes") & (df_sorted["homelessness_label"] == 0)].shape[0]} // label=1 : {df_sorted[(df_sorted["selected"] == 1) & (df_sorted["rental_assistance_in_the_future"] == "yes") & (df_sorted["homelessness_label"] == 1)].shape[0]}]')
        st.write(f'nr of individuals who receive RA in the past: {df_sorted[(df_sorted["selected"] == 1) & (df_sorted["rental_assistance_in_the_past"] == "yes")].shape[0]} [label=0 : {df_sorted[(df_sorted["selected"] == 1) & (df_sorted["rental_assistance_in_the_past"] == "yes") & (df_sorted["homelessness_label"] == 0)].shape[0]} // label=1 : {df_sorted[(df_sorted["selected"] == 1) & (df_sorted["rental_assistance_in_the_past"] == "yes") & (df_sorted["homelessness_label"] == 1)].shape[0]}]')

        fig = px.histogram(df_sorted[(df_sorted["selected"] == 1) & (df_sorted["hl_in_the_future"] == "yes")], x="days_until_next_hl",
                           title='Days until interaction with homelessness services', nbins=40, color='homelessness_label')
        st.plotly_chart(fig, use_container_width=True)

    _, col, _ = st.columns([0.1, 1, 0.1])
    with col:
        aggregate_hl_programs = utils.aggregate_hl_programs_by_label(
            config, model_output['client_hash'])
        st.write(aggregate_hl_programs)

    col1, col2, col3 = st.columns([0.3, 1, 1])
    with col1:
        df_sorted.loc[(df_sorted.selected == 1) & (
            df_sorted.homelessness_label == 1), 'outcome'] = 'TP'
        df_sorted.loc[(df_sorted.selected == 1) & (
            df_sorted.homelessness_label == 0), 'outcome'] = 'FP'
        df_sorted.loc[(df_sorted.selected == 0) & (
            df_sorted.homelessness_label == 0), 'outcome'] = 'TN'
        df_sorted.loc[(df_sorted.selected == 0) & (
            df_sorted.homelessness_label == 1), 'outcome'] = 'FN'

        st.write('Homelessness label:')
        st.write(pd.concat([df_sorted["homelessness_label"].value_counts(sort=False, dropna=True),
                 df_sorted["homelessness_label"].value_counts(sort=False, normalize=True)], axis=1, keys=['count', '%']))
        st.write('Confusion matrix:')
        st.write(df_sorted['outcome'].value_counts(sort=False, dropna=False))
    with col2:
        st.write('Race:')
        st.write(pd.concat([df_sorted["race"].value_counts(sort=False, dropna=False), df_sorted["race"].value_counts(sort=False, normalize=True), df_sorted[(df_sorted['outcome'] == 'TP') | (df_sorted['outcome'] == 'FP')]["race"].value_counts(sort=False, dropna=False), df_sorted[df_sorted['outcome'] == 'TP']["race"].value_counts(
            sort=False, dropna=False), df_sorted[df_sorted['outcome'] == 'FP']["race"].value_counts(sort=False, dropna=False), df_sorted[df_sorted['outcome'] == 'TN']["race"].value_counts(sort=False, dropna=False), df_sorted[df_sorted['outcome'] == 'FN']["race"].value_counts(sort=False, dropna=False)], axis=1, keys=['count', '%', 'selected', 'TP', 'FP', 'TN', 'FN']))
    with col3:
        st.write('Gender:')
        st.write(pd.concat([df_sorted["gender"].value_counts(sort=False, dropna=False), df_sorted["gender"].value_counts(sort=False, normalize=True), df_sorted[(df_sorted['outcome'] == 'TP') | (df_sorted['outcome'] == 'FP')]["gender"].value_counts(sort=False, dropna=False), df_sorted[df_sorted['outcome'] == 'TP']["gender"].value_counts(
            sort=False, dropna=False), df_sorted[df_sorted['outcome'] == 'FP']["gender"].value_counts(sort=False, dropna=False), df_sorted[df_sorted['outcome'] == 'TN']["gender"].value_counts(sort=False, dropna=False), df_sorted[df_sorted['outcome'] == 'FN']["gender"].value_counts(sort=False, dropna=False)], axis=1, keys=['count', '%', 'selected', 'TP', 'FP', 'TN', 'FN']))

    _, col, _ = st.columns([0.1, 1, 0.1])

    with col:

        model_output_aggregation_plots = []

        for group in ["race", "gender", "prev. homeless"]:
            temp = df_sorted.groupby(
                ["outcome", group]).size().reset_index(name="n")

            temp["per"] = (
                df_sorted.groupby(["outcome", group])
                .size()
                .groupby(level=0)
                .apply(lambda x: 100 * x / float(x.sum()))
                .values
            )

            temp = temp.melt(id_vars=["outcome", "n", "per"])

            fig = px.bar(
                temp,
                x="outcome",
                y="per",
                color="value",
                facet_col_wrap=2,
                labels=dict(value=group, per="Percentage (%)", variable=""),
                text_auto=True
            )
            fig.update_layout(height=900, yaxis={'autorange': 'reversed'})
            fig.update_xaxes(categoryorder='array', categoryarray=[
                             'TP', 'FP', 'FN', 'TN'])
            model_output_aggregation_plots.append(fig)

        display_chart_group(model_output_aggregation_plots,
                            'Outcomes by race, gender, and previously homeless in %', 600)


def investigate_model_bias(config, experiment_id, top_k):

    metrics, _ = utils.get_metrics_and_top_k(config)

    col, _, _ = st.columns([0.5, 1, 1.5])
    with col:
        model_id = st.text_input(
            'Generate bias audit for a particular model id')
        bias_metric = st.selectbox(
            'Select metric to evaluate bias on', metrics, index=metrics.index('recall'))
    if not model_id:
        return

    model_id = int(model_id)
    bias_metric = 'tpr' if bias_metric == 'recall' else bias_metric

    audit_df = get_data_for_audit(config, experiment_id, model_id)

    # get crosstabs
    xtab = generate_audit_crosstab(audit_df, config, top_n=top_k)
    st.write(xtab)

    # plot bias plots
    plots = get_audit_plots(
        audit_df, config, bias_metric=bias_metric, top_n=top_k)
    for p in plots:
        st.altair_chart(p)


def investigate_model_bias_across_models(config, experiment_id, top_k):

    metrics, _ = utils.get_metrics_and_top_k(config)

    col1, col2, _ = st.columns([0.5, 3, 0.01])
    with col1:
        model_ids = st.text_input(
            'Generate fairness pareto plot for several model ids')
        bias_metric = st.selectbox(
            'Select bias metric', metrics, index=metrics.index('recall'))
        performance_metric = st.selectbox(
            'Select performance metric', metrics, index=metrics.index('precision'))
        bias_attribute = st.text_input('Attribute', value='race')
        group_1 = st.text_input('Group 1', value='Black/African American')
        group_2 = st.text_input('Group 2', value='White')
    if not model_ids:
        return

    model_ids = get_ids_from_str(model_ids)

    bias_metric = 'tpr' if bias_metric == 'recall' else bias_metric

    performance_and_fairness = pd.DataFrame()

    for model_id in model_ids:

        model = utils.read_model_from_pkl_file(config, experiment_id, model_id)

        audit_df = get_data_for_audit(config, experiment_id, model_id)

        # get crosstabs
        xtab = generate_audit_crosstab(audit_df, config, top_n=top_k)

        predictions = audit_df["score"]

        # get indices of n highest scores
        idx_top_k = np.argpartition(predictions, -top_k)[-top_k:]
        audit_df["selected"] = 0
        audit_df.loc[idx_top_k, "selected"] = 1

        performance_top_k = calculate_metrics(
            audit_df["label_value"], audit_df["selected"])[performance_metric]
        recall_diff = list(xtab[(xtab['attribute_name'] == bias_attribute) & (xtab['attribute_value'] == group_1)][bias_metric])[
            0] - list(xtab[(xtab['attribute_name'] == bias_attribute) & (xtab['attribute_value'] == group_2)][bias_metric])[0]

        y_axis_title = f'performance: {performance_metric} @{top_k}'
        x_axis_title = f'fairness: {bias_metric}[{group_1}] - {bias_metric}[{group_2}]'

        performance_and_fairness = pd.concat([performance_and_fairness, pd.DataFrame.from_records(
            [{y_axis_title: performance_top_k, x_axis_title: recall_diff, 'model_id': model_id, 'model_name': model.model_name, 'hyperparams': str(model.hyperparams), 'group_id': model.group_id}])])

    fig = px.scatter(performance_and_fairness, x=x_axis_title, y=y_axis_title,
                     color='model_name', hover_data=['model_id', 'model_name', 'hyperparams', 'group_id'])
    fig.update_traces(marker={'size': 15, 'opacity': 0.6})
    fig.update_layout(yaxis_range=[0, 0.5], xaxis_range=[-1, 1])

    with col2:
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)


def error_analysis(config, experiment_id, top_k):
    return


def get_ids_from_str(ids):
    return [int(i.strip()) for i in ids.split(",")]


def compare_model_outputs(config, experiment_id, top_k):
    col, _, _ = st.columns([0.5, 1, 1.5])
    with col:
        model_groups_ids = st.text_input(
            'Compare outputs of different model groups')
        if not model_groups_ids:
            return

    with st.spinner('Calculating correlation between model outputs for different time splits'):
        model_groups_ids = get_ids_from_str(model_groups_ids)
        models_at_dates = []
        db_conn = utils.get_db_conn()
        for group_id in model_groups_ids:
            query_model_ids = f'''
                select distinct on (model_id, validation_date)
                model_id, validation_date
                from {config.db_config.schema_name}.{config.modeling_config.model_table_name} m
                left join {config.db_config.schema_name}.{config.evaluation.metrics_table_name} using(model_id)
                where model_group_id = {group_id}
            '''
            models_at_dates.extend(db_conn.execute(query_model_ids).fetchall())

        selected_individuals = {}
        validation_dates = list(
            set([utils.str_from_dt(vd) for (_, vd) in models_at_dates]))
        validation_dates.sort(reverse=True)

        ncol = 4
        cols = st.columns(ncol)
        selected_models = []

        for split_count, validation_date in enumerate(validation_dates):
            model_ids = [int(m) for (m, vd) in models_at_dates if utils.str_from_dt(
                vd) == validation_date]

            for model_id in model_ids:

                model = utils.read_model_from_pkl_file(
                    config, experiment_id, model_id)
                if split_count == 0:
                    selected_models.append(
                        f'Model {model.model_id}: {utils.generate_model_group_str(model.model_name, model.hyperparams)}')

                feature_df = get_feature_matrix(config, experiment_id, model)

                df_sorted = feature_df.sort_values(
                    by='score', ascending=False).reset_index()
                df_sorted.loc[:, 'selected'] = 0
                df_sorted.loc[:top_k-1, 'selected'] = 1

                model_output = df_sorted.loc[(
                    df_sorted.selected == 1), 'client_hash']
                selected_individuals[model_id] = model_output

            corr_matrix = pd.DataFrame(
                index=model_ids, columns=model_ids, dtype=float)

            rank_corr_matrix = pd.DataFrame(
                index=model_ids, columns=model_ids, dtype=float)
            for r in model_ids:
                for c in model_ids:
                    corr_matrix.at[r, c] = utils.jaccard_similarity(
                        selected_individuals[c], selected_individuals[r])

            for r in model_ids:
                for c in model_ids:
                    rank_corr_matrix.at[r, c] = utils.rank_correlation(
                        selected_individuals[c], selected_individuals[r])

            mask = np.zeros_like(corr_matrix)
            mask[np.triu_indices_from(mask)] = True
            with sns.axes_style("white"):
                fig, ax = plt.subplots()
                ax = sns.heatmap(corr_matrix, mask=mask, square=True,
                                 vmin=0, vmax=1, linewidths=.5, annot=True)

            col = cols[split_count % ncol]
            col.write(
                f'Jaccard Similarity for time split {split_count+1}: {validation_date}')
            col.write(fig)

            mask_rank = np.zeros_like(rank_corr_matrix)
            mask_rank[np.triu_indices_from(mask_rank)] = True
            with sns.axes_style("white"):
                fig2, ax2 = plt.subplots()
                ax2 = sns.heatmap(rank_corr_matrix, mask=mask_rank, square=True,
                                  vmin=-1, vmax=1, linewidths=.5, annot=True)

            col = cols[split_count % ncol]
            col.write(
                f'Rank Correlation for time split {split_count+1}: {validation_date}')
            col.write(fig2)

        col, _ = st.columns([10, 0.1])

        with col:
            st.write(selected_models)


def cohort_over_time(config, experiment_id, top_k):

    query_cohort_over_time = f'''
    select
        as_of_date,
        count(distinct client_hash) as nr_of_individuals,
        sum(homelessness_label) as nr_of_pos_labels
    from {config.db_config.schema_name}.{config.cohort_config.table_name} as ch
    left join {config.db_config.schema_name}.{config.label_config.table_name} using(client_hash, as_of_date)
    group by 1
    order by 1 desc;
    '''

    db_conn = utils.get_db_conn()

    cohort_size_over_time = pd.read_sql(query_cohort_over_time, db_conn)
    validation_dates = get_validation_dates(config, experiment_id)

    fig = px.line(
        cohort_size_over_time,
        x="as_of_date",
        y=["nr_of_individuals", "nr_of_pos_labels"],
        title=f'cohort and label size over time for experiment with id {experiment_id} from schema {config.db_config.schema_name}'
    )
    fig.add_hline(y=top_k, line_width=1, line_dash="dash",
                  line_color="purple", name='top_k')
    for date in validation_dates:
        fig.add_vline(x=date, line_width=1, line_dash="dash",
                      line_color="green", name='validation dates')

    _, col, _ = st.columns([0.1, 1, 0.1])
    with col:
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)


def main_app():
    st.set_page_config(layout='wide', page_title='Postmodeling')

    col1, _, col2 = st.columns([1, 0.1, 5])

    with col1:
        col1.write(
            'Put in a config file, experiment id, and top k to see eval over time')
        config_filename = st.text_input('Config filename', 'config')

    if not config_filename:
        return

    config = utils.read_config(f"{config_filename}.yaml")

    experiment_dir = f'{config.base_filepath}/{config.db_config.schema_name}'
    experiment_opts = [s.split('-')[1]
                       for s in os.listdir(experiment_dir) if '-' in s]

    _, top_k_opts = utils.get_metrics_and_top_k(config)

    with col1:
        experiment_id = st.selectbox(
            'Experiment id', experiment_opts, index=len(experiment_opts)-1)
        top_k = st.selectbox('Top k', top_k_opts, index=top_k_opts.index(100))

    experiment_id = int(experiment_id)
    top_k = int(top_k)

    with st.spinner('Creating evaluation over time plot'):
        plot = get_eval_over_time(config, experiment_id, top_n=top_k)
        plot.update_layout(
            height=800,
        )

    with col2:
        st.plotly_chart(plot, use_container_width=True)

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10 = st.tabs(
        ['Cohort Over Time', 'Model performance', 'Best Models', 'Model Group', 'Particular Model', 'Model Output', 'Compare Model Outputs', 'Bias Audit', 'Bias Audit Across Models', 'Error Analysis'])

    with tab1:
        cohort_over_time(config, experiment_id, top_k)

    with tab2:
        investigate_model_performance(config, experiment_id, top_k)

    with tab3:
        investigate_best_models(config, experiment_id, top_k)

    with tab4:
        investigate_model_group(config, experiment_id, top_k)

    with tab5:
        investigate_specific_model(config, experiment_id, top_k)

    with tab6:
        investigate_model_output(config, experiment_id, top_k)

    with tab7:
        compare_model_outputs(config, experiment_id, top_k)

    with tab8:
        investigate_model_bias(config, experiment_id, top_k)

    with tab9:
        investigate_model_bias_across_models(config, experiment_id, top_k)

    with tab10:
        error_analysis(config, experiment_id, top_k)


if __name__ == '__main__':
    main_app()
