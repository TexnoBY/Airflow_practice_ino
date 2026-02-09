import os.path
import re

import pandas as pd
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import datetime, timedelta

from airflow.sdk import TaskGroup, task

OWNER = "airflow"
DAG_ID = 'process_data_to_mongo_classic'

LONG_DESCRIPTION = '''

#Description

'''

SHORT_DESCRIPTION = 'short description'

FILE_PATH = '/opt/airflow/data/tiktok_google_play_reviews.csv'
TMP_FILE_PATH = '/opt/airflow/data/processed_tmp.csv'

default_args = {
    'owner': OWNER,
    'start_date': datetime(2026, 2, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def check_file_content():
    path ='/opt/airflow/data/tiktok_google_play_reviews.csv'
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return 'data_processing_group.replace_nulls'
    return 'log_empty_file'


def mongo_load():
    df = pd.read_csv('/opt/airflow/data/processed_tmp.csv')
    hook = MongoHook(mongo_conn_id='mongodb_default')
    db = hook.get_conn().my_database
    db.processed_data.insert_many(df.to_dict('records'))
    os.remove('/opt/airflow/data/processed_tmp.csv')


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    tags=['example_my'],
    description=SHORT_DESCRIPTION,
    schedule=None,
    catchup=False,

) as dag:
    dag.doc_md = LONG_DESCRIPTION


    start = EmptyOperator(
        task_id='Start'
    )

    wait_for_file = FileSensor(
        task_id='wait_for_data_file',
        filepath='data/tiktok_google_play_reviews.csv',
        fs_conn_id='fs_default',
        poke_interval=30,
        timeout=600
    )

    branch_task = BranchPythonOperator(
        task_id='check_file_status',
        python_callable=check_file_content
    )

    mongo_upload = PythonOperator(
        task_id='data_process',
        python_callable=mongo_load
    )

    log_empty = BashOperator(
        task_id='log_empty_file',
        bash_command='echo "WARN: The file at /opt/airflow/data/tiktok_google_play_reviews.csv is empty!"'
    )

    with TaskGroup(group_id='data_processing_group') as data_processing_group:
        @task
        def replace_nulls(file_path):
            df = pd.read_csv(file_path)
            df = df.fillna("-")
            df.to_csv('/opt/airflow/data/processed_tmp.csv', index=False)
            return TMP_FILE_PATH


        @task
        def sort_data(file_path):
            df = pd.read_csv(file_path)

            if 'at' in df.columns:
                df['at'] = pd.to_datetime(df['at'])
                df = df.sort_values(by='at')
            df.to_csv(file_path, index=False)
            return file_path


        @task
        def clean_content(file_path):
            df = pd.read_csv(file_path)

            def clean_text(text):
                if not isinstance(text, str): return text
                return re.sub(r'[^a-zA-Z0-9\s.,!?;:-]', '', text).strip()

            if 'content' in df.columns:
                df['content'] = df['content'].apply(clean_text)

            df.to_csv(file_path, index=False)
            return file_path


        clean_content(sort_data(replace_nulls(FILE_PATH)))



    end = EmptyOperator(
        task_id='end',
        trigger_rule='none_failed_min_one_success'
    )

    start >> wait_for_file >> branch_task >> [data_processing_group, log_empty] >> end

    data_processing_group >> mongo_upload >> end
    log_empty >> end