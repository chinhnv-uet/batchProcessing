import requests
import json
import pandas as pd
import urllib.request
import urllib.error
import os
import logging
import sys
import datetime

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('download.log'),
    ]
)
    
'''
Download different type of file by option name:
"TICK" for WEBPXTICK_DT-*.zip
"Tick Data Structure" for TickData_structure.dat
"Trade Cancellation" for TC_*.txt
"Trade Cancellation Data Structure" for TC_structure.dat

links is list of links to download corresponding file
downloadHistorical = True to download files not on today, False for files on today
'''        
def download_file(df, links, name, downloadHistorical):
    link = links[name]['link']
    field = links[name]['column']
    
    if not downloadHistorical: # download uploaded file today. It have data of yesterday
        # check if dont have new data
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        date_string = yesterday.strftime("%d %b %Y")
        if df['Date'][0] != date_string: # 0 is latest index of uploaded data
            logging.info(f"No new data today, at {datetime.datetime.now()}")
            sys.exit(1)

        # download if have new data
        filename = df[field][0]
        try:
            with urllib.request.urlopen(link.format(df['key'][0]), timeout=10) as response, open(filename, 'wb') as out_file:
                data = response.read()
                out_file.write(data)
                logging.info(f'Download file today: {filename} successfully')
        except urllib.error.URLError as e:
            logging.error(f'ConnectionError. Error occurred while downloading {filename}, type = {name}, id = {df["key"][0]}')
        except Exception as e:
            logging.error(f'TimeoutError. Error occurred while downloading {filename}, type = {name}, id = {df["key"][0]}')
            
    else: # download historical files, (files not on today)
        beginId = 1
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        date_string = yesterday.strftime("%d %b %Y")
        if df['Date'][0] != date_string:
            beginId = 0 #if latest file is not on today, download from id = 0
            
        cwd = os.getcwd()
        for id in range(beginId, len(df)):
            filename = df[field][id]
            filepath = os.path.join(cwd, filename)
            if os.path.exists(filepath): # skip exists file
                logging.info(f'File exists. Skipping download of {filename}')
                continue
            else:
                try:
                    with urllib.request.urlopen(link.format(df['key'][id]), timeout=10) as response, open(filename, 'wb') as out_file:
                        data = response.read()
                        out_file.write(data)
                        logging.info(f'Download file: {filename} successfully')
                except urllib.error.URLError as e:
                    logging.error(f'ConnectionError. Error occurred while downloading {filename}, type = {name}, id = {df["key"][id]}')
                    continue
                except Exception as e:
                    logging.error(f'TimeoutError. Error occurred while downloading {filename}, type = {name}, id = {df["key"][id]}')
                    continue
        
'''
this function send api get list files recent and related information as id, uploaded date, fileName
Using json_normalize to convert from json type to dataframe
'''
def getDataFiles():
    url = "https://api3.sgx.com/infofeed/Apps?A=COW_Tickdownload_Content&B=TimeSalesData&C_T=20&noCache=1689701183686"
    try:
        response = requests.request("GET", url)
    except requests.ConnectionError as e:
        logging.error('ConnectionError. Error occurred while read api getDataFile')
        sys.exit(1)
    data = json.loads(response.text)
    df = pd.json_normalize(data['items'])
    df = df.dropna(how='any')
    return df


if __name__ == '__main__':
    df = getDataFiles()
    
    downloadHistorical = False
    
    # Read file config
    with open('link.json', 'r') as f:
        links = json.load(f)
    
    #start download 4 type of file
    logging.info(f'Start download webpxtick_dt file, historical = {downloadHistorical}')
    download_file(df, links, "Tick", downloadHistorical)
    
    logging.info(f'Start download TickData_struct file, historical = {downloadHistorical}')
    download_file(df, links, "Tick Data Structure", downloadHistorical)
    
    logging.info(f'Start download tc_* file, historical = {downloadHistorical}')
    download_file(df, links, "Trade Cancellation", downloadHistorical)
    
    logging.info(f'Start download tc_structure file, historical = {downloadHistorical}')
    download_file(df, links, "Trade Cancellation Data Structure", downloadHistorical)