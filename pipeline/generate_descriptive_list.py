import os
import joblib
import pandas as pd
import seaborn as sns
import json
import matplotlib.pyplot as plt
import matplotlib as mpl
import psycopg2
import aequitas.plot as ap
import shap
import numpy as np

from src.pipeline.utils.utils import get_db_engine

engine = get_db_engine()



def get_model_predictions(engine, model_id, as_of_date):
    q = f"""
        select 
        *
        from acdhs_production.predictions
        where model_id={model_id}
        order by score desc;
    """ 
    
    df = pd.read_sql(q, engine)
    
    return df

