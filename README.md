# Batch Processing : ETL pipeline, data modelling of trading data

## Table of Contents
- [Batch Processing : ETL pipeline, data modelling of trading data](#batch-processing--etl-pipeline-data-modelling-of-trading-data)
  - [Table of Contents](#table-of-contents)
  - [1. Introduction](#1-introduction)
    - [Technologies used](#technologies-used)
  - [2. Implementation overview](#2-implementation-overview)
  - [3. Design](#3-design)
  - [4. Settings](#4-settings)
    - [Prerequisites](#prerequisites)
    - [Important note](#important-note)
  - [5. Implementation](#5-implementation)
    - [5.1 Create database in Clickhouse](#51-create-database-in-clickhouse)
    - [5.2 Create directory in HDFS](#52-create-directory-in-hdfs)
    - [5.3 Run airflow](#53-run-airflow)
  - [6. Visualize result](#6-visualize-result)


## 1. Introduction 
Data is collected from Singapore Exchange website about their transaction daily, and the company's analytic teams are interested in understanding their business transactions.  
We will build ETL pipelines that will transform raw data into processed data, and store them in Clickhouse dbms for ad hoc analytics.

Data downloaded from website included bid, ask, and traded transactions recorded in a day. A file has about 4 million records and is uploaded to the website daily. 
<br>
<img src='imgs\dataOverview.png' alt="dataset overview">

### Technologies used
- Python
- HDFS
- Airflow
- Spark

## 2. Implementation overview 
First, define requirements and data modeling.
Build an data pipeline:
- Download latest data file from website. Website often upload file of yesterday transaction data at noon. File stored in csv format.
- Store raw data into HDFS: downloaded file is uploaded to HDFS for later processing.
- Process data with spark and store processed data into ClickHouse.  
  
Using Airflow to orchestrate pipeline workflow.

<img src = imgs/architecture.png alt = "data pipeline">

## 3. Design 
  <img src=imgs/diagram.png alt="Star schema">
  <p style="text-align: center;"> <b> <i> Designed data warehouse</i> </b> </p>
</div>

<br><br>
  <img src=imgs\workflow.png alt="workflow">
  <p style="text-align: center;"> <b> <i> Airflow workflow </i> </b> </p>
</div>

## 4. Settings

### Prerequisites
- HDFS
- Clickhouse
- Airflow

### Important note
- HDFS run in localhost, port 8020
- Clickhouse: run in localhost, port 8123
- Airflow
- Remember to connect airfow to dags folder and plugins folder
- Remember to download [clickhouse-jdbc](https://mvnrepository.com/artifact/com.clickhouse/clickhouse-jdbc) file and add to jars folder of python (or spark)

## 5. Implementation
### 5.1 Create database in Clickhouse

<b> Command </b>

 ```shell
python3 createTable.py
 ``` 
 
<br>
<b>Result </b>

<br>
<img src=imgs/clickhouse.png alt="clickhouse">

<br>

### 5.2 Create directory in HDFS
```shell
hdfs dfs -mkdir /sgx_data/
```
### 5.3 Run airflow
<b>Run airflow in webserver</b>
<br>
<img src=imgs/airflow.png alt="airflow WebUI">
<br> 
<b> See result<b>  <br>
<img src=imgs/resultAirflow.png alt="result">

## 6. Visualize result
Data in hdfs  
<br>
<img src=imgs/hdfs.png alt="fileInHdfs">

Dim date  
<br>
<img src=imgs/DimDate.png alt="DimDate">

Fact table  
<br>
<img src=imgs/Fact.png alt="Fact table">