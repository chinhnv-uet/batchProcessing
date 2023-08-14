from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator
from operators.downloadFileOperator import downloadFileOperator
from operators.stagingOperator import stagingOperator
from operators.EtlOperator import EtlOperator

    
with DAG(
    dag_id="etlTradingData",
    start_date=datetime(2023, 8, 15, 11, 0, 0),
    catchup=False,
    tags=['etl', 'v1'],
    schedule_interval=timedelta(days=1)
):
    # downloadFileOperator(task_id="ok")
    stage1 = downloadFileOperator(task_id="downloadFile")
    stage2 = stagingOperator(task_id="staging")
    stage3 = EtlOperator(task_id="Etl")
    
    stage1 >> stage2 >> stage3
