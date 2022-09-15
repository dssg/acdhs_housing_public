# superclass for modeling
import logging
import re

import src.pipeline.model.baselines as baselines
import utils
from lightgbm import LGBMClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import AdaBoostClassifier, RandomForestClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.svm import LinearSVC
from sklearn.tree import DecisionTreeClassifier
from src.pipeline.model.scaled_lr import ScaledLogisticRegression
from utils import *
from xgboost import XGBClassifier

NAME_TO_MODEL = {
    'scaled_logistic_regression': ScaledLogisticRegression,
    'decision_tree': DecisionTreeClassifier,
    'random_forest': RandomForestClassifier,
    'adaboost': AdaBoostClassifier,
    'lgbm': LGBMClassifier,
    'calibrated_classifier': CalibratedClassifierCV,
    'knn': KNeighborsClassifier,
    'mlpc': MLPClassifier,
    'xg_boost': XGBClassifier,
}

# add all the baselines
for baseline, baseline_class in baselines.get_all_baselines():
    NAME_TO_MODEL[baseline] = baseline_class


class Model:
    def __init__(self, model_name, hyperparams, model_id, group_id, experiment_id, config):
        # check that the type of model we want is implemented
        if model_name not in NAME_TO_MODEL:
            logging.error(f'model {model_name} not implemented!')
            exit(1)

        # basic model params
        self.model_name = model_name
        self.hyperparams = hyperparams
        self.model_id = model_id
        self.group_id = group_id
        self.experiment_id = experiment_id

        if self.model_name.startswith("baseline"):
            self.model = NAME_TO_MODEL[model_name](
                random_state=config.random_seed, feature_groups=config.feature_config.feature_groups, **hyperparams
            )
        elif self.model_name == 'knn':
            self.model = NAME_TO_MODEL[model_name](**hyperparams)
        elif self.model_name == 'calibrated_classifier':
            logging.info(
                f'instantiating {self.model_name} with parameters: {hyperparams}')
            base_estimator = hyperparams.pop('base_estimator')
            if base_estimator == 'linear_svc':
                logging.info(f'instantiating linear_svc')
                self.model = NAME_TO_MODEL[model_name](
                    base_estimator=LinearSVC(
                        random_state=config.random_seed, **hyperparams)
                )
            elif base_estimator == 'gaussian_nb':
                logging.info(f'instantiating gaussian_nb')
                self.model = NAME_TO_MODEL[model_name](
                    base_estimator=GaussianNB(**hyperparams)
                )
        elif self.model_name == 'mlpc':
            if 'hidden_layer_sizes' in hyperparams:
                logging.debug(
                    f"changing hidden_layer_sizes type: {hyperparams['hidden_layer_sizes']}")
                logging.debug(
                    f"old type: {type(hyperparams['hidden_layer_sizes'])}")
                hyperparams['hidden_layer_sizes'] = tuple(
                    hyperparams['hidden_layer_sizes'])
                logging.debug(
                    f"new type: {type(hyperparams['hidden_layer_sizes'])}")
            self.model = NAME_TO_MODEL[model_name](
                random_state=config.random_seed, **hyperparams
            )
        else:
            self.model = NAME_TO_MODEL[model_name](
                random_state=config.random_seed, **hyperparams
            )

        # for saving pkl file
        self.pkl_dir_path = utils.get_module_filepath(
            config, experiment_id, 'modeling_config')
        utils.create_directory_if_not_exists(self.pkl_dir_path)

        logging.info(
            f'generating new model with model_id: {self.model_id} and hyperparams {self.hyperparams}')

        self.pkl_filename = "{}/model-{}.pkl".format(
            self.pkl_dir_path, self.model_id)

    @timer
    def train(self, train_df):
        """ train generic model

        Args:
            train_df: dataframe including labels
        """
        # split training dataframe into features and labels
        feature_matrix, labels = utils.df_to_feature_matrix(train_df)
        self.model.fit(feature_matrix, labels)

    def predict(self, feature_df):
        """ predict using generic model

        Args:
            feature_df: dataframe including labels
        """
        if self.model_name.startswith("baseline"):
            self.scores = self.model.predict_proba(feature_df)
        else:
            feature_matrix, _ = df_to_feature_matrix(feature_df)
            self.scores = self.model.predict_proba(feature_matrix)
