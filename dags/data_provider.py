import os.path
import shutil
import time
from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

OWNER = "airflow"
DAG_ID = 'copy_data_for_processing'

LONG_DESCRIPTION = '''

#Description

'''


SHORT_DESCRIPTION = 'short description'

default_args = {
    'owner': OWNER,
    'start_date': datetime(2026, 2, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def copy_test_file():
    source = '/opt/airflow/example_data/tiktok_google_play_reviews.csv'
    dest = '/opt/airflow/data/tiktok_google_play_reviews.csv'

    time.sleep(5)

    if os.path.exists(source):
        shutil.copy(source, dest)
    else:
        raise FileNotFoundError("Check your example_data folder!")


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

    copy_file = PythonOperator(
        task_id='Copy_datafile',
        python_callable=copy_test_file
    )

    end = EmptyOperator(
        task_id='End'
    )

    start >> copy_file >> end