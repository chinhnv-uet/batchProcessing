run:
start: dfs, clickhouse, airflow
remember add clickhouse-jdbc in jars of python

run createTable.py
create hdfs
start DAG in airflow
[2023-08-15, 00:37:11 +07] {EtlOperator.py:142} INFO - Done read file WEBPXTICK_DT-20230809.csv, with 3072769 record