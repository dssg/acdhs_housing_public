import argparse
import logging
from datetime import datetime
from itertools import product

import utils
import yaml
from munch import munchify
from src.pipeline.model.baselines import get_all_baselines
from src.pipeline.model.model import Model

import governance
from cohort.cohort_creator import create_cohort_table
from evaluate import feature_importance, model_evaluator, visualize
from features.features_creator import (create_feature_tables,
                                       get_feature_table_names)
from labels.label_creator import create_label_table
from model.matrix_creator import create_matrix, read_matrices_from_disk
from time_splitter.time_splitter import split_time, str_from_dt


def run_pipeline(config_filename, experiment_id=None, exp_desc=None):

    with open(f"src/pipeline/{config_filename}.yaml", "r") as config_file:
        config = munchify(yaml.safe_load(config_file))

    pipeline_start_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    utils.start_logger(config, experiment_id, pipeline_start_time)

    utils.check_experiment_run(config)

    # create temporal splits and generate a list of all dates of analysis based on the given splits
    logging.info('Creating splits')
    splits, as_of_dates = split_time(config["temporal_config"])

    if not experiment_id:
        # set up new experiment and generate experiment id
        experiment_id = governance.set_up_db_schema_for_new_experiment(
            config, exp_desc)

        logging.info(
            f'running a new experiment with id: {experiment_id} // experiment description: {exp_desc}')

        # create cohort table and save to db
        logging.info('creating cohort table')
        create_cohort_table(config, as_of_dates)

        # create feature table and save to db
        logging.info("Generate the feature table(s)")
        feature_table_names = create_feature_tables(config)

        # create label table and save to db
        create_label_table(config)

        # generate matrix and save to disk
        create_matrix(config, splits, feature_table_names, experiment_id)

    else:
        # experiment id has been provided
        # check if config matches existing experiment
        all_train_matrix_hashes = []
        all_validate_matrix_hashes = []
        for split in splits:
            # get relevant split info
            training_as_of_dates, train_end_date, train_start_date, validate_date, as_of_dates = utils.get_split_info(
                split)
            cohort_hash, feature_hash, label_hash, matrix_hash_train, train_filename = governance.create_hash_for_split(
                config, experiment_id, train_end_date, training_as_of_dates, 'train')
            cohort_hash, feature_hash, label_hash, matrix_hash_validate, validate_filename = governance.create_hash_for_split(
                config, experiment_id, validate_date, as_of_dates, 'validate')
            all_train_matrix_hashes.append(matrix_hash_train)
            all_validate_matrix_hashes.append(matrix_hash_validate)

        if not governance.matrices_hashes_correspond_to_experiment(config, experiment_id, all_train_matrix_hashes, all_validate_matrix_hashes):
            logging.error(
                f'trying to continue experiment with id {experiment_id} but the matrices either do not exist yet or are not the same. Experiment cannot be continued.')
            return
        else:
            logging.info(f'continuing the experiment with id: {experiment_id}')
            # get feature table names
            feature_table_names = get_feature_table_names(config)

    model_group_str_to_id = {}

    # read matrices from disk one by one
    all_matrices = read_matrices_from_disk(config, splits, experiment_id)

    # train and predict
    # go through each split
    for split_count, (split, (train_data, validate_data)) in enumerate(zip(splits, all_matrices)):

        if split_count < config.temporal_config.start_with_split:
            logging.info(
                f' time split number {split_count + 1} of {len(list(splits))} is being skipped !')
            continue

        logging.info(
            f'[training matrix shape: {train_data.shape} // validation matrix shape: {validate_data.shape}]')
        # go through each model name
        model_names = config.modeling_config.model_names
        # add baselines if specified in config
        if config.modeling_config.run_all_baselines:
            model_names += [b for b in get_all_baselines(
                names_only=True) if b not in model_names]

        for model_count, model_name in enumerate(model_names):
            all_params = config.modeling_config.model_params.get(
                model_name, {'none': ['none']})
            param_names = [name for name in all_params]

            # go through all possible hyperparameter combinations
            nr_of_param_combinations = len(
                list(product(*[p for p in all_params.values()])))
            for param_count, param_vals in enumerate(product(*[p for p in all_params.values()])):
                # running a new model
                hyperparams = {name: val for name,
                               val in zip(param_names, param_vals)}

                pipeline_status = f'Time split: {split_count + 1} of {len(list(splits))} / model: {model_count + 1} of {len(model_names)} / parameter combination: {param_count + 1} of {nr_of_param_combinations}   '
                logging.info(pipeline_status)
                print(pipeline_status, end='\r')

                # make nice date strings for logging
                train_end_date = str_from_dt(split[0][0])
                validation_date = str_from_dt(split[1])

                # Start training: lots of model governance needs to happen first
                logging.info(
                    f'Training {model_name} with hyperparams {hyperparams} up to date {train_end_date}')

                # check if a model with these hyperparameters has run before
                model_id = governance.find_model_id(
                    model_name, hyperparams, config, split, experiment_id)
                model_group_str = utils.generate_model_group_str(
                    model_name, hyperparams)

                if model_id != -1:
                    logging.info(
                        f'this model has already run, see model id: {model_id}. Continue with next model.')
                    model_group_id = utils.get_existing_model_group_id(
                        config, model_id)
                    logging.info(
                        f'Existing model group id has been found: {model_group_id}')
                    model_group_str_to_id[model_group_str] = model_group_id
                    continue
                    # add comment: we could also break but this would not run models for splits that have not been created yet

                # check if the same model with the same hyperparameters has already run
                if model_group_str not in model_group_str_to_id:
                    # this is the first split, generate a new group id for this model & hyperparameters
                    group_id = governance.get_group_id(
                        config, experiment_id, model_name)
                    model_group_str_to_id[model_group_str] = group_id
                    logging.debug(f'generate new model group id: {group_id}')
                else:
                    # this is not the first split, hence, group id already exists
                    group_id = model_group_str_to_id[model_group_str]
                    logging.debug(
                        f'model part of existing model group id: {group_id}')

                model_id = governance.get_new_model_id(
                    config, experiment_id, group_id, model_name, train_end_date, split)
                # create model object and train
                model = Model(model_name, hyperparams, model_id,
                              group_id, experiment_id, config)

                # train model
                model.train(train_data)
                governance.save_model(model, config)  # save model params

                # Start prediction
                logging.info(
                    f'Predicting on validation date {validation_date}')
                model.predict(validate_data)
                predictions_and_labels = governance.save_prediction(
                    model, validate_data, config)

                # save prk curves (by group) to disk and saves specified evaluations metrics and top_k in db
                model_evaluator.evaluate(
                    config, model, validation_date, predictions_and_labels, validate_data)

                if model_name.startswith('baseline'):
                    continue

                # calculate feature importance and save top features to db
                top_features = feature_importance.calc_feature_importance(
                    config, model, train_data)
                if top_features is not None:
                    governance.save_feature_importance(config, top_features)

                    # generate feature importance plot and save to disk
                    feature_importance.plot_top_feature_importance(
                        config, model, train_end_date, top_features)

    visualize.evaluate_over_time_plot(config, experiment_id)


@utils.timer
def main():

    # run: python src/pipeline/main.py --config_filename config_dev --experiment_id 1
    # get help with: python src/pipeline/main.py -h

    parser = argparse.ArgumentParser(
        description='Running our beautiful pipeline.')
    parser.add_argument('-c', '--config_filename', type=str,
                        required=True, help='config file name.')
    parser.add_argument('-e_id', '--experiment_id', type=int, required=False,
                        help='Already existing exeriment id if an experiment is to be continued. If not provided, a new experiment with a new id will be generated.')
    parser.add_argument('-e_desc', '--experiment_desc', type=str, required=False,
                        help='Description of the experiment. Only considered if experiment_id is not provided!')

    args = parser.parse_args()

    run_pipeline(args.config_filename,
                 args.experiment_id, args.experiment_desc)

    logging.info(f'finished running the pipeline :)')
    print('\nfinished running the pipeline :)\n')


if __name__ == "__main__":
    main()
