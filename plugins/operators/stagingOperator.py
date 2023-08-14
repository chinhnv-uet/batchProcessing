from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import subprocess
import os

class stagingOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        
    def execute(self, context):
        fileName = Variable.get("fileName")
        if fileName == 'error':
            self.log.info("No new file for staging")
            return
        
        self.log.info("Start staging file")
        
        #mkdir hdfs
        hdfs_path = '/sgx_data/'
        # command = f'hdfs dfs -mkdir {hdfs_path}'
        # subprocess.run(command, shell=True)
        
        source_path = fileName
        putCommand = f'hdfs dfs -put {source_path} {hdfs_path}'
        subprocess.run(putCommand, shell=True)
        
        os.remove(fileName)