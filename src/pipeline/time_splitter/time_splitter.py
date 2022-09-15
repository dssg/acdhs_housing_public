from utils import *


def split_time(config_temporal):
    """_summary_

    Args:
        config_temporal (dict): Takes a config_temporal file that holds the relevant parameters

    Returns:
        splits (list): A list of all splits. Each split is a tuple: (list of training as of dates, validation as of date)
        all_as_of_dates (list): A sorted list of all as_of_dates. Useful for building the cohort.
    """

    # Grab temporal information from the config file
    feature_start_date = dt_from_str(config_temporal["feature_start_date"])
    feature_end_date = dt_from_str(config_temporal["feature_end_date"])
    last_validation_date = dt_from_str(config_temporal["last_validation_date"])
    training_as_of_date_frequency = convert_str_to_relativedelta(
        config_temporal["training_as_of_date_frequency"]
    )
    model_update_frequency = convert_str_to_relativedelta(
        config_temporal["model_update_frequency"]
    )
    label_timespan = convert_str_to_relativedelta(
        config_temporal["label_timespan"])
    nr_of_validation_folds = config_temporal["nr_of_validation_folds"]

    if feature_start_date > feature_end_date:
        raise ValueError("Label start time after label end time.")

    if feature_end_date > last_validation_date - label_timespan:
        raise ValueError(
            "Feature end date plus label timespan is after validation date."
        )

    # Generate first fold
    as_of_dates = []
    while feature_end_date >= feature_start_date:
        as_of_dates.append(feature_end_date)
        feature_end_date = feature_end_date - training_as_of_date_frequency

    splits = [(as_of_dates, last_validation_date)]

    # Generate and append the rest of the folds
    for i in range(1, nr_of_validation_folds):
        new_as_of_train_dates = [
            d - i * model_update_frequency
            for d in as_of_dates
            if (d - i * model_update_frequency) >= feature_start_date
        ]
        new_validate_as_of_date = last_validation_date - i * model_update_frequency
        splits.append((new_as_of_train_dates, new_validate_as_of_date))

    # Takes the training validation splits and returns a single sorted list of all as_of_dates. Useful for building the cohort.
    all_as_of_dates = []
    for train_dates, last_validation_date in splits:
        all_as_of_dates += train_dates + [last_validation_date]
    all_as_of_dates = sorted(list(set(all_as_of_dates)))

    return splits, all_as_of_dates
