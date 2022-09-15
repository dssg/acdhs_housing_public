import inspect
import logging
import random as rd
import sys

import numpy as np
import utils


def get_all_baselines(names_only=False):
    curr_members = inspect.getmembers(sys.modules[__name__])
    def is_baseline(name): return name.startswith(
        'Baseline') and not name == 'Baseline'
    #NAME_TO_MODEL[utils.camel_to_snake(baseline)] = baseline_class

    return [utils.camel_to_snake(name) if names_only else (utils.camel_to_snake(name), obj)
            for name, obj in curr_members if is_baseline(name)]


class Baseline():
    def __init__(self, feature_groups=None, required_feature_group=None, **kwargs):
        # check that the features needed for the baseline are generated
        if required_feature_group is not None and required_feature_group not in feature_groups:
            logging.warning('Feature group', required_feature_group,
                            'needed for baseline but not implemented!')
            exit(1)


class BaselineDaysSinceLastHL(Baseline):

    def fit(self, feature_matrix, labels):
        pass

    def predict_proba(self, feature_matrix):
        feature = feature_matrix['days_since_hl']
        not_hl_score = (feature / feature.abs().max()).to_numpy()
        hl_score = 1 - not_hl_score
        return np.column_stack((not_hl_score, hl_score))


class BaselineBaserate(Baseline):
    def __init__(self, **kwargs):
        rd.seed(kwargs['random_state'])

    def fit(self, feature_matrix, labels):
        self.baserate = sum(labels) / len(labels)

    def predict_proba(self, feature_matrix):
        hl_score = np.array(rd.choices(
            [0, 1], weights=[1-self.baserate, self.baserate], k=len(feature_matrix)))
        not_hl_score = 1 - hl_score
        return np.column_stack((not_hl_score, hl_score))


class BaselineDaysSinceCurrentFiling(Baseline):

    def fit(self, feature_matrix, labels):
        pass

    def predict_proba(self, feature_matrix):
        feature = feature_matrix['days_since_*evictions']
        not_hl_score = (feature / feature.abs().max()).to_numpy()
        hl_score = 1 - not_hl_score
        return np.column_stack((not_hl_score, hl_score))


class BaselineDaysSinceOFP(Baseline):

    def fit(self, feature_matrix, labels):
        pass

    def predict_proba(self, feature_matrix):
        feature = feature_matrix['day_diff_ofp']
        not_hl_score = (feature / feature.abs().max()).to_numpy()
        hl_score = 1 - not_hl_score
        return np.column_stack((not_hl_score, hl_score))


class BaselineNumDistinctPrograms(Baseline):

    def fit(self, feature_matrix, labels):
        pass

    def predict_proba(self, feature_matrix):
        feature = feature_matrix['COUNT(*distinct_feed,@100year)']
        hl_score = (feature / feature.abs().max()).to_numpy()
        not_hl_score = 1 - hl_score
        return np.column_stack((not_hl_score, hl_score))


class BaselineNumProgramInvolvementSpells(Baseline):

    def fit(self, feature_matrix, labels):
        pass

    def predict_proba(self, feature_matrix):
        feature = feature_matrix['COUNT(*all_feed,@100year)']
        hl_score = (feature / feature.abs().max()).to_numpy()
        not_hl_score = 1 - hl_score
        return np.column_stack((not_hl_score, hl_score))


class BaselineDaysSinceLastProgramInvolvement(Baseline):

    def fit(self, feature_matrix, labels):
        pass

    def predict_proba(self, feature_matrix):
        feature = feature_matrix['days_since_*all_feed']
        not_hl_score = (feature / feature.abs().max()).to_numpy()
        hl_score = 1 - not_hl_score
        return np.column_stack((not_hl_score, hl_score))


class BaselineAgeAtFirstProgramInvolvement(Baseline):

    def fit(self, feature_matrix, labels):
        pass

    def predict_proba(self, feature_matrix):
        feature = feature_matrix['MIN(age_first_*feed,@100year)']
        not_hl_score = (feature / feature.abs().max()).to_numpy()
        hl_score = 1 - not_hl_score
        return np.column_stack((not_hl_score, hl_score))


class BaselineAgeAtFirstAdultProgramInvolvement(Baseline):

    def fit(self, feature_matrix, labels):
        pass

    def predict_proba(self, feature_matrix):
        feature = feature_matrix['MIN(age_first_adult_*feed,@100year)']
        not_hl_score = (feature / feature.abs().max()).to_numpy()
        hl_score = 1 - not_hl_score
        return np.column_stack((not_hl_score, hl_score))


class BaselineTotalDaysInProgramInvolvement(Baseline):

    def fit(self, feature_matrix, labels):
        pass

    def predict_proba(self, feature_matrix):
        feature = feature_matrix['SUM(days_in_*all_feed,@100year)']
        hl_score = (feature / feature.abs().max()).to_numpy()
        not_hl_score = 1 - hl_score
        return np.column_stack((not_hl_score, hl_score))
