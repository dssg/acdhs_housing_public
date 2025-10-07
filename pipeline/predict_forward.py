"""
This script will run the preselected ML model to generate the list of clients for outreach for the trial. 
We have already selected models through audition and postmodeling, narrowed it down to one model group
Use the Retrainer module in triage to retrain the model using data until the prediction date
"""
import logging
import pandas as pd
import argparse

from datetime import datetime

from triage.predictlist import Retrainer, predict_forward_with_existed_model
from triage.predictlist.utils import experiment_config_from_model_group_id

from pretriage.current_eviction_features import generate_current_eviction_features
from pipeline.pretriage.non_entity_id_aggregate_features import generate_location_level_eviction_aggregates, generate_landlord_level_eviction_aggregates

from pipeline.utils.utils import get_db_engine
from pipeline.utils.project_constants import PROJECT_PATH, LOGS_PATH


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
now = datetime.now()
date_time = now.strftime("%Y-%m-%d_%H:%M:%S")

fh = logging.FileHandler(f'{LOGS_PATH}/predict_forward_run_{date_time}.log', mode='w')
fh.setFormatter(formatter)
logger.addHandler(fh)


def write_to_acdhs_production(db_engine, model_id, as_of_date):
    '''
        write predictios to the acdhs_production schema
    '''

    q = '''
    
        set role 'rg_staff';

        create schema if not exists acdhs_production;

        create table if not exists acdhs_production.predictions (
            model_id int,
            client_hash varchar,
            entity_id bigint,
            as_of_date date,
            prediction_date date,
            score float,
            rank_abs_no_ties int,
            rank_pct_no_ties float,
            rank_abs_with_ties int,
            rank_pct_with_ties float,
            matrix_uuid text,
            label_value int,
            test_label_timespan interval
        );

        create index on acdhs_production.predictions(entity_id);
        create index on acdhs_production.predictions(as_of_date);
        create index on acdhs_production.predictions(prediction_date);
    '''

    logging.info('Making sure the acdhs_production schema and predictions table exist!')
    with db_engine.begin() as conn:
        conn.execute(q)


    insert_q = f"""
        insert into acdhs_production.predictions
            select 
                model_id,
                client_hash,
                entity_id,
                as_of_date::date,
                current_date as prediction_date,
                score,
                rank_abs_no_ties,
                rank_pct_no_ties,
                rank_abs_with_ties,
                rank_pct_with_ties,
                matrix_uuid,
                label_value,
                test_label_timespan
            from triage_production.predictions p join pretriage.client_id_mapping cim on p.entity_id = cim.client_id 
            where model_id = {model_id} and as_of_date = '{as_of_date}'::date 
            order by score desc
        ;
    """
    with db_engine.begin() as conn:
        conn.execute(insert_q)
    

def model_group_id_from_model_id(db_engine, model_id):
    
    q = f'''
        select model_group_id
        from triage_metadata.models
        where model_id = {model_id}
    '''
    
    return pd.read_sql(q, db_engine).iloc[0, 0]


def model_id_from_model_group_id_train_end_time(db_engine, model_group_id, train_end_time):

    if train_end_time is None: 
        te_clause = "order by train_end_time desc limit 1"
    else:
        te_clause = "and train_end_time = '{train_end_time}'::date"

    q = f"""
        select 
            model_id
        from traige_metadata.models
        where model_group_id = {model_group_id}
        {te_clause}
    """

    return pd.read_sql(q, db_engine).at[0, 'model_id']



def predict_forward_no_retrain(db_engine, project_path, prediction_date, model_id=None, model_group_id=None, train_end_time=None, test_run=False):
    """
    Generate predictions using an existing model object.
    args:
        db_engine: sqlachemy engine
        prediction_date (str): The date as of which the predictions are generated 'YYYY-MM-DD'
        model_id (int): id of the model if known
        model_group_id (int): if the model_id is not known, we need the model_group_id
        train_end_time (int): if the model_id is not known, this can be supplied in addition to the model_group_id
            if not provided, the model corresponding to the latest train_end_time for the model group will be used. 
    """

    if (model_id is None) and (model_group_id is None):
        raise ValueError('Either the model_id or model_group_id need to be provided!')

    if model_id is None:
        model_id = model_id_from_model_group_id_train_end_time(db_engine, model_group_id, train_end_time)

    logging.info(f' Using model_id {model_id} for predicting as of {prediction_date}')

    predict_forward_with_existed_model(
        db_engine=db_engine,
        model_id=model_id,
        project_path=project_path,
        as_of_date=prediction_date
    )

    logging.info('Successfully generated predictions and written to triage_production.predictions')

    if not test_run:
        logging.info('Copying predictions to the acdhs_production schema...')
        write_to_acdhs_production(db_engine, model_id, prediction_date)

    logging.info(f'Predict forward as of {prediction_date} using model {model_id} succesfully completed!')


def predict_forward_with_retrain(db_engine, prediction_date, model_group_id, project_path):
    """ Given a model_group_id and a prediction date, retrain the an instance of the model group using data until the prediction date
        and generate predictions as of the prediction_date

        Args:
            db_engine()
            prediction_date (str): 
            model_group_id (int):
            project_path
    """
    
    # Todo -- Add the functionality to submit a model id here (we can fetch the model group id from that). This way, we can have a consistent interface to both
    # predict forward cases
    
    retrain_obj = Retrainer(
        db_engine=db_engine,
        project_path=project_path,
        model_group_id=model_group_id
    )
    
    logging.info(f'Retraining the model upto {prediction_date}')
    retrain_obj.retrain(prediction_date)
    
    logging.info(f'Generating predictions as of {prediction_date}')
    retrain_obj.predict(prediction_date)
    
    
def run_predict_forward(prediction_date, project_path, model_id, retrain=False, model_group_id=None, train_end_time=None, is_test_run=False):
    """ Run the predict forward pipeline
        Args:
            prediction_date (str): Date as of which the predictions are generated
            project_path (str): Where the model object is or new model should be saved
            retrain (bool): Whether to retrain the model (need to provide a model_group_id)
            model_id (int): triage Model ID to use for generating prediction
            model_group_id (int, optional)     
    """
    
    # Retrain True case is not handled yet
    if retrain:
        logging.warning('This case is not handled yet, exiting!')
        return

    db_engine = get_db_engine()
    
    generate_current_eviction_features(db_engine, start_date=prediction_date, end_date=prediction_date) # default interval is 1 month
    levels = ['city', 'districtcourtno', 'zip_cd']
    for level in levels:
        # Aggregating stats for location attributes
        generate_location_level_eviction_aggregates(db_engine, level)
        
    # Aggregating stats for landlords
    generate_landlord_level_eviction_aggregates(db_engine)
    
    logging.info(f'Generating predictions using Model {model_id}, as of {prediction_date} ')
    predict_forward_no_retrain(
        db_engine=db_engine,
        project_path=project_path,
        prediction_date=prediction_date,
        model_id=model_id,
        test_run=is_test_run
    )
        

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Generate predictions with an existing model")

    parser.add_argument(
        "-m",
        "--model_id",
        type=int,
        help='triage model id for generating predictions (int)',
        required=True
    )
    

    parser.add_argument(
        "-d",
        "--as_of_date",
        type=str,
        help='Prediction date',
        required=True
    )

    parser.add_argument(
        "-t",
        "--test",
        dest='testrun_flag',
        action='store_true',
        help='whether this is a test run. If set, predictions are only written to the triage_production schema not to acdhs_production'  
    )
    
    args = parser.parse_args()
    
    run_predict_forward(
        prediction_date=args.as_of_date,
        project_path=PROJECT_PATH,
        model_id=args.model_id,
        is_test_run=args.testrun_flag
    )
    
    






