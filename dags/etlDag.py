import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator
from operators.downloadFileOperator import downloadFileOperator
from operators.stagingOperator import stagingOperator

# class file:
#     def __init__(self, fileName):
#         self.fileName = fileName
        
#     def setName(self, fileName):
#         self.fileName = fileName
        
#     def getName(self):
#         return self.fileName
    
with DAG(
    dag_id="my_dags",
    start_date=datetime.datetime(2023, 8, 8),
    schedule="@weekly",
):
    # downloadFileOperator(task_id="ok")
    helo1 = downloadFileOperator(task_id="downloadFile")
    helo2 = stagingOperator(task_id="staging")
    helo1 >> helo2
