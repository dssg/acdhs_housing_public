from triage.component.postmodeling.add_predictions import add_predictions

from triage.util.db import create_engine
from sqlalchemy.engine.url import URL
import os
db_url = URL(
            'postgres',
            host=os.getenv('PGHOST'),
            username=os.getenv('PGUSER'),
            database=os.getenv('PGDATABASE'),
            password=os.getenv('PGPASSWORD'),
            port=5432,
        )
db_engine = create_engine(db_url)

add_predictions(
    db_engine=db_engine, # The database connection
    model_groups=[161, 162, 163], # List of model groups  
    project_path='s3://dsapp-social-services-migrated/acdhs_housing/triage_experiments/', # where the models and matrices are stored
    experiment_hashes=[
        '4bbb2008aa72bc7ea48d5a4b72d73404'
    ], # Restricting models (in the above model groups) based on exeriment (optional)
    # train_end_times_range={
        # 'range_start_date': '2015-01-01',
        # 'range_end_date': '2017-01-01'
    # }, # Restricing models based on train end times (optional). Intervals are inclusive and can be open ended. 
    rank_order='worst', # How to break ties
    replace=True # Whether to replace existing predictions
)