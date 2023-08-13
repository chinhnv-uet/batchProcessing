from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import requests
import json
import pandas as pd
import urllib.request
import urllib.error
import os
import logging
import sys
import datetime
import zipfile
from airflow.models import Variable

class downloadFileOperator(BaseOperator):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
    
    def download_file(self, df, name):
        link = "https://links.sgx.com/1.0.0/derivatives-historical/{}/WEBPXTICK_DT.zip"
        field = "Data File"
        
        # check if dont have new data
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        date_string = yesterday.strftime("%d %b %Y")
        if df['Date'][0] != date_string: # 0 is latest index of uploaded data
            self.log.info(f"No new data today, at {datetime.datetime.now()}")
            return "WEBPXTICK_DT-20230803.csv"

        # if have new data
        filename = df[field][0]
        try:
            with urllib.request.urlopen(link.format(df['key'][0]), timeout=10) as response, open(filename, 'wb') as out_file:
                data = response.read()
                out_file.write(data)
                logging.info(f'Download file today: {filename} successfully')
                return filename
        except urllib.error.URLError as e:
            logging.error(f'ConnectionError. Error occurred while downloading {filename}, type = {name}, id = {df["key"][0]}')
            return "error"
        except Exception as e:
            logging.error(f'TimeoutError. Error occurred while downloading {filename}, type = {name}, id = {df["key"][0]}')
            return "error"
            
    '''
    this function send api get list files recent and related information as id, uploaded date, fileName
    Using json_normalize to convert from json type to dataframe
    '''
    def getDataFiles(self):
        url = "https://api3.sgx.com/infofeed/Apps?A=COW_Tickdownload_Content&B=TimeSalesData&C_T=20&noCache=1689701183686"
        try:
            response = requests.request("GET", url)
        except requests.ConnectionError as e:
            self.log.error('ConnectionError. Error occurred while read api getDataFile')
            return None
        data = json.loads(response.text)
        df = pd.json_normalize(data['items'])
        df = df.dropna(how='any')
        return df
    
    def extractFile(self, filename):
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            zip_ref.extractall()
        os.remove(filename)
        return filename[:-3] + 'csv'
    
    def execute(self, context):
        self.log.info("Start download file")
        df = self.getDataFiles()
        if df is None: # connection error
            file_name = 'error'
        else:
            file_name = self.download_file(df, "Tick")
            if file_name == 'error': #cannt find file today
                Variable.set("fileName", "error")
            else:
                pass
                # file_name = self.extractFile(file_name)
        Variable.set("fileName", file_name)
        self.log.info("File name:", file_name)
            