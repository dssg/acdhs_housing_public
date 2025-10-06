import os
import logging
import argparse
import pandas as pd
import yaml

import nbformat as nbf
import shutil

from datetime import date
from datetime import datetime
from triage.experiments import SingleThreadedExperiment, MultiCoreExperiment
from triage.component.timechop.timechop import Timechop
from triage import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.event import listens_for
from sqlalchemy.pool import Pool
from sqlalchemy.engine import Engine

import sys
from dotenv import load_dotenv
load_dotenv()
python_paths = os.getenv("PYTHONPATH")
# Split the PYTHONPATH variable into a list of paths
paths = python_paths.split(':')
# Add the paths to the system path
for path in paths:
    sys.path.append(path)

from utils.utils import read_yaml
from utils.project_constants import PROJECT_PATH, LOGS_PATH, EXPERIMENT_CONFIG_PATH, CODE_BASEPATH
from pretriage.current_eviction_features import generate_current_eviction_features
# from pipeline.pretriage.deprecated.create_eviction_aggregate_tables import create_aggregate_tables

from pipeline.pretriage.non_entity_id_aggregate_features import generate_location_level_eviction_aggregates, generate_landlord_level_eviction_aggregates

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
now = datetime.now()
date_time = now.strftime("%Y-%m-%d_%H:%M:%S")

fh = logging.FileHandler(f'{LOGS_PATH}/triage_experiment_{date_time}.log', mode='w')
fh.setFormatter(formatter)
logger.addHandler(fh)
# logger.addHandler(logging.StreamHandler())

def generate_experiment_report():
    template_path = 'notebooks/triage_reports/triage_summary_report_template.ipynb'

    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    output_path = f'notebooks/triage_reports/triage_summary_report_{timestamp}.ipynb'

    shutil.copyfile(template_path, output_path)

    os.system(f'jupyter nbconvert --execute --inplace --to notebook {output_path}')
    os.system(f'jupyter nbconvert  {output_path} --to html')

def run_experiment(configfile_path, labelconfig_path=None, label_name=None, model_comment=None, feature_config_path=None, replace=False, save_predictions=False, n_jobs=1, only_validate=False, n_timesplits=None, run_precomputes=False):

    logger.info(f'Reading the config file at {configfile_path}')
    config = read_yaml(configfile_path)

    # set experiment metadata
    if labelconfig_path is not None:
        config['label_config'] = {'filepath': labelconfig_path, 'name': label_name}
    
    if model_comment is not None:    
        config['model_comment'] = model_comment

    # read feature configurations
    compute_recent_eviction_features = False
    if feature_config_path:
        feature_group_files = [ x for x in os.listdir(feature_config_path) if x.endswith('.yaml')]
        feature_aggs = list()
        
        logger.info('Reading feature definitions from separate configs')
        for fg in feature_group_files:      
            logger.info(f'Feature group: {fg}')
            with open(f'{feature_config_path}/{fg}') as f:
                d = yaml.full_load(f)
                if d['prefix'] == 'curr_case' or d['prefix'] == 'curr_case_dspn' or d['prefix'] == 'curr_case_landlord' or d['prefix'] == 'curr_case_ofp':
                    compute_recent_eviction_features = True
                feature_aggs.append(d)

        config['feature_aggregations'] = feature_aggs
        
    else: 
        feature_aggs = config['feature_aggregations']
        for d in feature_aggs:
            if d['prefix'] == 'curr_case' or d['prefix'] == 'curr_case_dspn' or d['prefix'] == 'curr_case_landlord' or d['prefix'] == 'curr_case_ofp':
                compute_recent_eviction_features = True
                break
    
    # assume group role to ensure shared permissions
    @listens_for(Engine, "connect")
    def assume_role(dbapi_con, connection_record):
        print(dir(dbapi_con))
        cur = dbapi_con.cursor()
        cur.execute(f"set role rg_staff;")
        # with dbapi_con.cursor() as cur:
        #     # cur.execute(f"set role {os.getenv('PGROLE')};")
        #     cur.execute(f"set role kasun;")
            # cur.execute(f"select current_user;")
            # logging.info(f'Listened for connection and changed role to {cur.fetchone()[0]}')    
        # cur = dbapi_con.cursor()
        
    db_url = URL(
        'postgres',
        host=os.getenv('PGHOST'),
        username=os.getenv('PGUSER'),
        database=os.getenv('PGDATABASE'),
        password=os.getenv('PGPASSWORD'),
        port=os.getenv('PGPORT'),
    )

    db_engine = create_engine(db_url)

    # if we need to compute recent eviction features
    if compute_recent_eviction_features:
        timechop = Timechop(**config['temporal_config'])
        result = timechop.chop_time()
        start_date = min([x['train_matrix']['first_as_of_time'] for x in result] + [y['first_as_of_time'] for x in result for y in x['test_matrices']])
        end_date = max([x['train_matrix']['last_as_of_time'] for x in result] + [y['last_as_of_time'] for x in result for y in x['test_matrices']])
        generate_current_eviction_features(db_engine, start_date, end_date) # default interval is 1 month
        
        levels = ['city', 'districtcourtno', 'zip_cd']
        for level in levels:
            # Aggregating stats for location attributes
            generate_location_level_eviction_aggregates(db_engine, level)
            
        # Aggregating stats for landlords
        generate_landlord_level_eviction_aggregates(db_engine)
        
        # create_aggregate_tables(db_engine)

    if n_jobs > 1:
        experiment = MultiCoreExperiment(
            config=config,
            db_engine=db_engine,
            n_processes=n_jobs,
            n_db_processes=2,
            project_path=PROJECT_PATH,
            replace=replace,
            save_predictions=save_predictions
        )
    else:
        experiment = SingleThreadedExperiment(
            config=config,
            db_engine=db_engine,
            project_path=PROJECT_PATH,
            replace=replace,
            save_predictions=save_predictions
        )
    
    if n_timesplits is not None:
        # Providing the option to run only the last(most recent) n timesplits in the experiment
        experiment.split_definitions = experiment.split_definitions[-n_timesplits:]

    experiment.validate()

    if only_validate:
        return 
    
    experiment.run()
    
    generate_experiment_report()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Run the Triage Experiment for the ACDHS Housing Project")

    parser.add_argument(
        "-c",
        "--configfile",
        type=str,
        help='Name of the config file (Not the full path)',
        required=True
    )

    parser.add_argument(
        "-n",
        "--njobs",
        type=int,
        help='Number of concurrent jobs to run',
        required=True
    )

    parser.add_argument(
        "--save-predictions",
        dest='save_predictions_flag',
        action='store_true',
        help='Whether to recalculate the precomputed joins'
        # default=True
    )

    parser.add_argument(
        '--replace',
        dest='replace_flag',
        action='store_true',
        help='Whether to set replace flag to True (defaults to false)'
    )

    parser.add_argument(
        '-lc',
        "--labelconfigfile",
        type=str,
        help='Name of the label config file (not the full path)',
        required=False
    )

    parser.add_argument(
        '-ln',
        "--labelname",
        type=str,
        help='Label name',
        required=False
    )

    # if not specified, use the features in the main configfile
    # TODO eventually define features by listing all files instead of always using all the files in the feature_groups directory
    parser.add_argument(
        "--usefeatureconfigdir",
        dest='featureconfig_flag',
        action='store_true',
        help="Whether to use all features in the feature_groups dir instead of feature definitions from yaml config file"
    )

    parser.add_argument(
        '-mc',
        "--modelcomment",
        type=str,
        help="Model comment",
        required=False
    )
    
    parser.add_argument(
        '-ns',
        "--splits",
        type=int,
        help="number of validation splits",
        required=False
    )
    
    
    args = parser.parse_args()

    print(args)
    
    if args.featureconfig_flag:
        feature_config_path=f'{EXPERIMENT_CONFIG_PATH}/feature_groups/'
    else:
        feature_config_path=None
        
    if args.labelconfigfile is not None:
        labelconfig_path = f'{EXPERIMENT_CONFIG_PATH}/labels/{args.labelconfigfile}'
    else:
        labelconfig_path = None

    run_experiment(
        configfile_path=f'{EXPERIMENT_CONFIG_PATH}/{args.configfile}',
        labelconfig_path=labelconfig_path,
        label_name=args.labelname,
        feature_config_path=feature_config_path,
        model_comment=args.modelcomment,
        replace=args.replace_flag, 
        save_predictions=args.save_predictions_flag, 
        n_jobs=args.njobs, 
        only_validate=False,
        n_timesplits=args.splits,
        run_precomputes=False
    )
    
    
