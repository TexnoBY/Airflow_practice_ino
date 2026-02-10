import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG, Asset
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.sdk import task

# Constants
OWNER = "airflow"
DAG_ID = 'load_to_mongodb_consumer'
LONG_DESCRIPTION = '''

#Description

'''
SHORT_DESCRIPTION = 'short description'
FILE_PATH = '/opt/airflow/data/tiktok_google_play_reviews.csv'
TMP_FILE_PATH = '/opt/airflow/data/processed_tmp.csv'

tmp_file_asset = Asset(uri=TMP_FILE_PATH)

default_args = {
    'owner': OWNER,
    'start_date': datetime(2026, 2, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
        dag_id='load_to_mongodb_consumer',
        default_args=default_args,
        description=SHORT_DESCRIPTION,
        tags=['example_my'],
        schedule=[tmp_file_asset],
        catchup=False,
) as dag:
    @task
    def mongo_load_task():
        if not os.path.exists(TMP_FILE_PATH):
            raise FileNotFoundError(f"Processed file not found at {TMP_FILE_PATH}")

        df = pd.read_csv(TMP_FILE_PATH)

        hook = MongoHook(mongo_conn_id='mongodb_default')
        client = hook.get_conn()
        db = client.my_database

        db.processed_data.insert_many(df.to_dict('records'))
        print(f"Successfully loaded {len(df)} records to MongoDB")

        os.remove(TMP_FILE_PATH)


    mongo_load_task()
