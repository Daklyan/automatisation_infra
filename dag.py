import airflow
from datetime import datetime, timedelta
from subprocess import Popen, PIPE
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
from airflow import DAG, macros
from airflow.decorators import task
import os

YEAR = '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}'
MONTH = '{{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}'
DAY = '{{ macros.ds_format(ds, "%Y-%m-%d", "%d") }}'
YESTERDAY = '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%d") }}'

dag = DAG(
    dag_id='grp5',
    schedule_interval='0 0 * * *',
    max_active_runs=1,
    start_date=datetime(2022, 2, 14),
)

@task(task_id='download_raw_data', dag=dag)
def download_raw_data(year, month, day):
    raw_data_path = "/data/grp5/raw"

    url = f'https://opendata.paris.fr/api/v2/catalog/datasets/comptages-routiers-permanents/exports/csv?refine=t_1h%3A{year}%2F{month}%2F{day}&timezone=UTC'
    r = requests.get(url, allow_redirects=True)
    open(f'/tmp/data-{year}-{month}-{day}.csv', 'wb').write(r.content)
    put = Popen(["hadoop", "fs", "-put", f"/tmp/data-{year}-{month}-{day}.csv", raw_data_path], stdin=PIPE, bufsize=-1)
    put.communicate()
    os.remove(f'/tmp/data-{year}-{month}-{day}.csv')
    print("ok")


download_raw_data = download_raw_data(YEAR, MONTH, YESTERDAY)

clean_data = BashOperator(
    task_id="spark_job_clean",
    bash_command=f'/usr/bin/spark-submit --master yarn --deploy-mode cluster --class esgi.circulation.Clean hdfs:///jars/grp5/circulation.jar  hdfs:///data/grp5/raw/data-{YEAR}-{MONTH}-{DAY}.csv hdfs:///data/grp5/cleaned',
    dag=dag
)

transform_data = BashOperator(
    task_id="spark_job_transform",
    bash_command=f"/usr/bin/spark-submit --master yarn --deploy-mode cluster --class esgi.circulation.Jointure  hdfs:///jars/grp5/circulation.jar  hdfs:///data/grp5/cleaned/data-{YEAR}-{MONTH}-{DAY}.csv hdfs:///data/grp5/joined",
    dag=dag
)

download_raw_data >> clean_data >> transform_data
