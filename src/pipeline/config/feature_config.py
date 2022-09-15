import pandas as pd
from utils import *


def get_distinct_categorical_values(feature_group, col_name, from_obj):
    # manually memoize some values to save time (physical health has 70M rows)
    lookup_table = {
        'physical_health': {
            'source': ['Outpatient', 'Inpatient'],
            'scu_cd': ['ER', 'Other']
        }
    }

    if feature_group in lookup_table and col_name in lookup_table[feature_group]:
        return lookup_table[feature_group][col_name]

    # actually calculate categorical values if not in lookup table
    query = f'''
        with tab as ({from_obj})
        select {col_name}, count({col_name})
        from tab
        group by {col_name}
    '''

    categorical_df = pd.read_sql(query, get_db_conn())

    return [col for col in categorical_df[categorical_df['count'] >= 10][col_name] if col is not None]


def get_feature_config(config):
    feature_default_params = {
        'days_since_last_impute_value': 9999,
        'impute_value': -1,
        'generate_days_since_last': True,
        # TODO: specify more reasonable imputation values
        'filter_addition': '',
        'agg_fcns': ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'],
        'intervals': ['3month', '6month', '1year', '2year', '3year', '4year', '5year', '100year'],
    }

    feature_config_dict = {
        'evictions': {
            'defaults': {
                'knowledge_date': 'filingdt',
                'from_obj': """
                    select
                        distinct on (hashed_mci_uniq_id, matter_id)
                        hashed_mci_uniq_id as client_hash,
                        e.*
                    from clean.eviction_client_matches ecm
                    left join clean.eviction e using(matter_id)
                    """,
            },
            'numeric_features': [
                {
                    'agg_fcns': ['COUNT'],
                    'column':'*',
                    'name': '*evictions',  # this generates a column called "days_since_*evictions" which is used for the baseline called "baseline_days_since_current_filing"
                },
                {
                    'agg_fcns': ['COUNT'],
                    'column':'*',
                    'name': '*judgement_for_tenant',
                    'filter_addition': ' and (judgement_for_tenant is true)',
                    'knowledge_date': 'dispositiondt',
                },
                {
                    'agg_fcns': ['COUNT'],
                    'column':'*',
                    'name': '*judgement_for_landlord',
                    'filter_addition': ' and (judgement_for_landlord is true)',
                    'knowledge_date': 'dispositiondt',
                },
                {
                    'agg_fcns': ['COUNT'],
                    'column':'*',
                    'name': '*all_ofp',
                    'intervals': ['1year', '100year'],
                    'knowledge_date': 'ofp_issue_dt',
                },
                {
                    'agg_fcns': ['SUM', 'AVG', 'MIN', 'MAX'],
                    'column':'claimamount',
                    'name': 'claimamount',
                },
                {
                    'agg_fcns': ['SUM', 'AVG', 'MIN', 'MAX'],
                    'column':'monthlyrentamount',
                    'name': 'monthlyrentamount',
                },
                {
                    'agg_fcns': ['SUM', 'AVG', 'MIN', 'MAX'],
                    'column':'totaljudgmentamount',
                    'name': 'totaljudgmentamount',
                    'knowledge_date': 'dispositiondt',
                },
            ],
            'categorical_features': []
        },
        'hl_table': {
            'defaults': {
                'knowledge_date': 'program_end_dt',
                'from_obj': f"""
                    select
                        *,
                        (program_end_dt - program_start_dt) as days_hl
                    from {config.db_config.schema_name}.hl_table
                    """,
            },
            'numeric_features': [
                {
                    'column': '*',
                    'name': 'hl',  # this generates a column called "days_since_hl" which is used for the baseline called "baseline_days_since_last_hl"
                    'agg_fcns': ['COUNT'],
                },
                {
                    'column': 'days_hl',
                    'name': 'days_hl',
                    'generate_days_since_last': False,
                },
            ]
        },
        'public_housing': {
            'defaults': {
                'knowledge_date': 'moveoutdate_new',
                'from_obj': f"""
                    select
                        *,
                        hashed_mci_uniq_id as client_hash,
                        case when moveoutdate is null then moveindate else moveoutdate end moveoutdate_new,
                        (moveoutdate - moveindate) as days_in_ph
                    from clean.public_housing
                    """,
            },
            'numeric_features': [
                {
                    'knowledge_date': 'moveindate',
                    'column': '*',
                    'name': 'ph',
                    'agg_fcns': ['COUNT'],
                },
                {
                    'column': 'days_in_ph',
                    'name': 'days_in_ph',
                },
            ]
        },
        'address_feed': {
            'defaults': {
                'knowledge_date': 'eff_date',
                'from_obj': f"""
                        select 
                            *
                            from clean.address_feed
                    """,
            },
            'numeric_features': [
                {
                    'column': 'distinct address_line_1',
                    'name': 'uniq_addresses',
                    'agg_fcns': ['COUNT'],
                },
                {
                    'column': 'distinct zip_cd',
                    'name': 'uniq_zip_cds',
                    'agg_fcns': ['COUNT'],
                }
            ]
        },
        'baseline_day_diff_ofp': {
            'sql_script': 'src/pipeline/config/baseline_day_diff_ofp.sql'
        },
        'demographics_simple': {
            'sql_script': 'src/pipeline/config/demographics_simple.sql'
        },
        'hl_trends': {
            'sql_script': 'src/pipeline/config/hl_trends.sql'
        },
        'eviction_trends': {
            'sql_script': 'src/pipeline/config/eviction_trends.sql'
        },
        'month_dummies': {
            'sql_script': 'src/pipeline/config/month_dummies.sql'
        },
        'mental_health': {
            'defaults': {
                'knowledge_date': 'event_end_date',
                'shortname': 'mh',
                'from_obj': f"""
                    select
                        *,
                        mci_uniq_id as client_hash,
                        event_start_date - lag(event_end_date) over w as lag_mh
                    from clean.cmu_mh_prm
                    window w as (partition by mci_uniq_id order by event_end_date, event_start_date)
                """,
            },
            'numeric_features': [
                {
                    'column': '*',
                    'name': '*mh',
                    'agg_fcns': ['COUNT']
                },
                {
                    'column': 'lag_mh',
                    'name': 'time_bt_mh',
                    'agg_fcns': ['AVG', 'MIN', 'MAX']
                },
                {
                    'column': 'event_duration',
                    'name': 'mh_duration',
                    'agg_fcns': ['AVG', 'MIN', 'MAX', 'SUM']
                }
            ]
        },
        'behavioral_health': {
            'defaults': {
                'knowledge_date': 'event_end_date',
                'shortname': 'bh',
                'from_obj': f"""
                    select
                        *,
                        mci_uniq_id as client_hash,
                        event_beg_date - lag(event_end_date) over w as lag_bh,
                        split_part(diagnosis_code, '.', 1) as diagnosis_category_code
                    from clean.cmu_behavior_health_prm
                    window w as (partition by mci_uniq_id order by event_end_date, event_beg_date)
                """,
            },
            'numeric_features': [
                {
                    'column': '*',
                    'name': '*bh',
                    'agg_fcns': ['COUNT']
                },
                {
                    'column': 'lag_bh',
                    'name': 'time_bt_bh',
                    'agg_fcns': ['AVG', 'MIN', 'MAX']
                },
                {
                    'column': 'event_duration',
                    'name': 'bh_duration',
                    'agg_fcns': ['AVG', 'MIN', 'MAX', 'SUM']
                }
            ],
        },
        'physical_health': {
            'defaults': {
                'knowledge_date': 'svc_end_dt_new',
                'shortname': 'ph',
                'from_obj': f"""
                    select
                        *,
                        case when svc_end_dt is null then svc_start_dt else svc_end_dt end svc_end_dt_new,
                        mci_uniq_id as client_hash,
                        svc_start_dt - lag(svc_end_dt) over w as lag_ph
                    from clean.cmu_physical_health_prm
                    window w as (partition by mci_uniq_id order by svc_end_dt, svc_start_dt)
                """,
            },
            'numeric_features': [
                {
                    'knowledge_date': 'svc_start_dt',
                    'column': '*',
                    'name': '*ph',
                    'agg_fcns': ['COUNT']
                },
                {
                    'column': 'lag_ph',
                    'name': 'time_bt_ph',
                    'agg_fcns': ['AVG', 'MIN', 'MAX']
                },
                {
                    'column': 'svc_end_dt - svc_start_dt',
                    'name': 'ph_duration',
                    'agg_fcns': ['AVG', 'MIN', 'MAX', 'SUM']
                }
            ]
        },
        'cyf': {
            'defaults': {
                'knowledge_date': 'plcmnt_end_date',
                'shortname': 'cyf',
                'intervals': ['100year'],
                'from_obj': f"""
                    select
                        *,
                        mci_uniq_id as client_hash,
                        plcmnt_end_date - lag(plcmnt_entry_date) over w as lag_cyf
                    from clean.cmu_placement_prm
                    window w as (partition by mci_uniq_id order by plcmnt_end_date, plcmnt_entry_date)
                """,
            },
            'numeric_features': [
                {
                    'column': '*',
                    'name': '*cyf_placements',
                    'agg_fcns': ['COUNT'],
                },
                {
                    'column': 'lag_cyf',
                    'name': 'time_bt_cyf',
                    'agg_fcns': ['AVG', 'MIN', 'MAX']
                },
                {
                    'column': 'plcmnt_end_date - plcmnt_entry_date',
                    'name': '*cyf_placement_duration',
                    'agg_fcns': ['AVG', 'SUM', 'MIN', 'MAX'],
                }
            ]
        },
        'baseline_age_first_involvement': {
            'defaults': {
                'knowledge_date': 'program_start_dt',
                'shortname': 'age_if',
                'intervals': ['100year'],
                'impute_value': 150,
                'generate_days_since_last': False,
                'from_obj': f"""
                    select
                        client_hash,
                        program_start_dt,
                        date_part('year', age(program_start_dt, dob)) as curr_age
                from clean.program_involvement_consolidated
                    left join clean.client_feed cf
                    using(client_hash)
                """,
            },
            'numeric_features': [
                {
                    'column': 'curr_age',
                    'name': 'age_first_*feed',
                    'agg_fcns': ['MIN']
                }, {
                    'column': 'curr_age',
                    'name': 'age_first_adult_*feed',
                    'agg_fcns': ['MIN'],
                    'filter_addition': ' and curr_age >= 18'
                }
            ]
        }
        # TODO: currently, apart of the hmis details table information is included in the consolidated if. But there we only consider the waiting list dates and not the actual move in date. So I guess we should also create features that consider the move in dates for any client into any housing program. This is just an idea and the dict below is a first pass. It is not tested. And it might make sens to generate it dinamically as is done with program_involvement below.
        # 'hmis_details_ph_move_in': {
        #     'defaults': {
        #         'knowledge_date': 'move_in_dt',
        #         'from_obj': """
        #             select
        #                 hashed_mci_uniq_id as client_hash,
        #                 hud_project_type_desc as project_type,
        #                 *
        #             from clean.hmis_details hd
        #             where move_in_dt is not null
        #             group by hashed_mci_uniq_id, hud_project_type_desc, move_in_dt
        #         """,
        #     },
        #     'count_features': [
        #         {
        #             'column':'*',
        #             'name': '*move_in_dt',
        #         },
        #     ],
        # },
    }

    # add count_features for each program type in hmis_details_ph_move_in

    db_conn = get_db_conn()
    set_role(db_conn, config.db_config.role_name)

    # from the table containing the consolidated program involvment, select all programs in which at least 1 person from our cohort has participated in before any of the validation dates, i.e., before the max as_of_date
    if_consolidated_sql = f'''select project_type, data_type, count(*) as nr_of_individuals
        from clean.program_involvement_consolidated pic
        where
            program_start_dt < (
                select max(as_of_date) from {config.db_config.schema_name}.{config.cohort_config.table_name}
            )
            and client_hash in (
                select distinct client_hash 
                from {config.db_config.schema_name}.{config.cohort_config.table_name}
            )
        group by project_type, data_type;'''

    # get all projects in which at least 10 individuals ever participated in from the consolidated involvement feed (if) table
    if_project_types = pd.read_sql(if_consolidated_sql, db_conn)
    if_project_types = if_project_types[if_project_types['nr_of_individuals'] > 10]

    logging.debug(
        f'Generating feature dicts in feature config for {if_project_types.shape[0]} involvement feed project types.')

    # first, generate variables for total program involvement
    if_count_features = [{
        'agg_fcns': ['COUNT'],
        'column': '*',
        'name': '*all_feed',
    }, {
        'agg_fcns': ['COUNT'],
        'column': 'DISTINCT(project_type)',
        'name': '*distinct_feed'
    }, {
        'agg_fcns': ['SUM'],
        'column': 'case when (program_end_dt <= ch.as_of_date) then (program_end_dt - program_start_dt) else (ch.as_of_date - program_start_dt) end',
        'name': 'days_in_*all_feed'
    }]

    count = 1
    # loop through all projects and add a feature dictionary
    for index, row in if_project_types.iterrows():
        feature_name = f'{row["data_type"]}_{row["project_type"]}'.replace(
            " ", "").replace("'", "").replace("/", "")
        feature_dict = {
            'agg_fcns': ['COUNT'],
            'column': '*',
            'name': feature_name,
            # 'filter_addition': f" and (project_type = '{row['project_type']}' and data_type = '{row['data_type']}')"
        }
        # this adds a flag for current participation
        feature_current_dict = {
            'agg_fcns': [],
            'intervals': [],
            'generate_days_since_last': False,
            'column': '*',
            'name': f'{feature_name}_current',
            'filter_addition': f" and (project_type = '{row['project_type']}' and data_type = '{row['data_type']}' and not (program_end_dt <= ch.as_of_date))"
        }
        if_count_features.append(feature_dict)
        if_count_features.append(feature_current_dict)
        count += 1
        if config.config_version == 'dev' and count > 5:
            break

    involvement_feed_dict = {
        'defaults': {
            'knowledge_date': 'program_start_dt',
            'from_obj': f"""
                select *
                from clean.program_involvement_consolidated
                {'limit 1000' if config.config_version == 'dev' else ''}
                """,
        },
        'numeric_features': if_count_features,
    }

    feature_config_dict['program_involvement'] = involvement_feed_dict

    # health feature groups: add categorical features for specified column name on the fly
    categorical_groups = {
        'mental_health': ['event_type'],
        'behavioral_health': ['event_type', 'diagnosis_category_code'],
        'physical_health': ['source', 'scu_cd'],
        'cyf': ['srvc_group_2']
    }
    for feature_group, col_names in categorical_groups.items():
        from_obj = feature_config_dict[feature_group]['defaults']['from_obj']
        shortname = feature_config_dict[feature_group]['defaults']['shortname']
        for col_name in col_names:
            feature_config_dict[feature_group]['numeric_features'] += [{
                'agg_fcns': ['COUNT'],
                'column': '*',
                'name': categorical_value.replace(' ', '_'),
                'filter_addition': f" and {col_name} = '{categorical_value}'"
            } for categorical_value in get_distinct_categorical_values(feature_group, col_name, from_obj)]

            feature_config_dict[feature_group]['numeric_features'] += [{
                'agg_fcns': ['MIN', 'MAX', 'AVG'],
                'column': f'lag_{shortname}',
                'name': f"time_bt_{shortname}_{categorical_value.replace(' ', '_')}",
                'filter_addition': f" and {col_name} = '{categorical_value}'"
            } for categorical_value in get_distinct_categorical_values(feature_group, col_name, from_obj)]

    # add count_features for each state program category (clean.state_programs_consolidated)

    # select all program categories in which at least 1 person from our cohort has participated in before any of the validation dates, i.e., before the max as_of_date
    state_program_sql = f'''select category, count(*) as nr_of_individuals
        from clean.state_programs_consolidated
        where
            elig_begin_date < (
                select max(as_of_date) from {config.db_config.schema_name}.{config.cohort_config.table_name}
            )
            and client_hash in (
                select distinct client_hash 
                from {config.db_config.schema_name}.{config.cohort_config.table_name}
            )
        group by category;'''

    # get all state programs in which at least 10 individuals ever participated in from the consolidated state program table
    state_program_categories = pd.read_sql(state_program_sql, db_conn)
    state_program_categories = state_program_categories[
        state_program_categories['nr_of_individuals'] > 10]

    logging.debug(
        f'Generating features for {state_program_categories.shape[0]} state program types.')

    # first, generate variables for total state program involvement
    state_count_features = [{
        'agg_fcns': ['COUNT'],
        'column': '*',
        'name': '*all_state',
    }, {
        'agg_fcns': ['COUNT'],
        'column': 'distinct(category)',
        'name': '*distinct_state'
    }, {
        'agg_fcns': ['SUM'],
        'column': 'case when (elig_end_date <= ch.as_of_date) then (elig_end_date - elig_begin_date) else (ch.as_of_date - elig_begin_date) end',
        'name': 'days_in_*all_state'
    }]

    # loop through all projects and add a feature dictionary
    for index, row in state_program_categories.iterrows():
        feature_name = f'{row["category"]}'.replace(
            " ", "").replace("'", "").replace("/", "")
        feature_dict = {
            'agg_fcns': ['COUNT'],
            'column': '*',
            'name': feature_name,
            'filter_addition': f" and (category = '{row['category']}')"
        }
        # this adds a flag for current participation
        feature_current_dict = {
            'agg_fcns': [],
            'intervals': [],
            'generate_days_since_last': False,
            'column': '*',
            'name': f'{feature_name}_current',
            'filter_addition': f" and (category = '{row['category']}' and not (elig_end_date <= ch.as_of_date))"
        }
        state_count_features.append(feature_dict)
        state_count_features.append(feature_current_dict)

    state_program_dict = {
        'defaults': {
            'knowledge_date': 'elig_begin_date',
            'from_obj': """
                select *
                from clean.state_programs_consolidated
                """,
        },
        'numeric_features': state_count_features,
    }

    feature_config_dict['state_programs'] = state_program_dict

    return feature_config_dict, feature_default_params
