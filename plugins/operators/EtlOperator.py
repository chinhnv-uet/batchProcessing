from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable
from pyspark.sql import SparkSession
import clickhouse_connect
from pyspark.sql import functions as F
import pandas as pd

class EtlOperator(BaseOperator):

    def __init__(
            self,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def insertDefaultDim(self, df, uri, spark):
        client = clickhouse_connect.get_client(host='localhost', port=8123, username='', password='')
        #dim action
        row1 = ['A', "Ask"]
        row2 = ['B', "Bid"]
        row3 = ['T', "Traded"]
        data = [row1, row2, row3]
        client.insert('SgxTrading.Dim_Action', data, column_names=['Action_Key', 'Action'])
        self.log.info("Insert dim action successfully")
        
        #dim contract type
        row1 = ['F', "Futures"]
        row2 = ['P', "Put"]
        row3 = ['C', "Call"]
        data = [row1, row2, row3]
        client.insert('SgxTrading.Dim_ContractType', data, column_names=['ContractType_Key', 'ContractType'])
        self.log.info("Insert dim contractType successfully")
        
        #dim commodity
        df2 = df.select('Comm').drop_duplicates()
        df2 = df2.withColumn("Commodity_Key", F.monotonically_increasing_id()).withColumnRenamed("Comm", "CommodityCode")
        df2.write.mode("append").format("jdbc").options(**{'url': uri,'dbtable' : 'Dim_Commodity','isolationLevel' : 'NONE'}).save()
        self.log.info("Insert dim commodity successfully")
        
        #dim date
        dates = pd.date_range(start='2020-01-01', end='2030-12-31', freq='D')

        dateDf = pd.DataFrame({'Date_key': dates.strftime('%Y%m%d'),
                   'FullDate': dates.strftime('%Y-%m-%d'),
                   'DayOfWeek': dates.strftime('%a'),
                   'CalendarQuarter': dates.quarter.map({1: 'Q1', 2: 'Q2', 3: 'Q3', 4: 'Q4'})})
        spark_DateDf = spark.createDataFrame(dateDf)
        spark_DateDf.write.mode("append").format("jdbc").options(**{'url': uri, 'dbtable' : 'Dim_Date', 'isolationLevel' : 'NONE'}).save()
        self.log.info("Insert dim date successfully")
        
        #dim month
        MonthCode = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]
        MonthName = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        id = [i for i in range(12)]
        Month = [i for i in range(1, 13)]

        data = list(zip(MonthCode, MonthName, id, Month))
        MonthDf = pd.DataFrame(data, columns=['MonthCode', 'MonthName', 'Month_Key', "Month"])
        spark_MonthDf = spark.createDataFrame(MonthDf)
        spark_MonthDf.write.mode("append").format("jdbc").options(**{'url': uri, 'dbtable' : 'Dim_Month', 'isolationLevel' : 'NONE'}).save()
        self.log.info("Insert dim month successfully")

        #dim time
        times = pd.date_range(start='00:00:00', end='23:59:59', freq='s').time

        timeDf = pd.DataFrame({'Time_Key': [int(time.strftime('%H%M%S')) for time in times],  # Chuyển đổi thành dạng int (hhmmss)
                        'FullTime': [time.strftime('%H:%M:%S') for time in times],  # Chuyển đổi thành dạng hh:mm:ss
                        'Hour': [time.hour for time in times],  # Lấy giờ
                        'Minute': [time.minute for time in times],  # Lấy phút
                        'Second': [time.second for time in times]})  # Lấy giây
        spark_TimeDf = spark.createDataFrame(timeDf)
        spark_TimeDf.write.mode("append").format("jdbc").options(**{'url': uri, 'dbtable' : 'Dim_Time', 'isolationLevel' : 'NONE'}).save()
        self.log.info("Insert dim time successfully")
        
        #dim year
        years = range(2000, 2051)
        yearDf = pd.DataFrame({'Year_Key': range(len(years)), 'Year': years})
        spark_YearDf = spark.createDataFrame(yearDf)
        spark_YearDf.write.mode("append").format("jdbc").options(**{ 'url': uri, 'dbtable' : 'Dim_Year', 'isolationLevel' : 'NONE'}).save()
        self.log.info("Insert dim year successfully")
        
    def updateCommodity(self, df, uri, spark):
        newComm = df.select('Comm').drop_duplicates().withColumnRenamed("Comm", "CommodityCode")
        oldComm = spark.read.format("jdbc").options(**{'url': uri,'dbtable' : 'Dim_Commodity','isolationLevel' : 'NONE'}).load()
        oldComm = comm.select("CommodityCode")
        
        comm_combined = oldComm.union(newComm).dropDuplicates()
        newRecord = comm_combined.subtract(oldComm)
        newData = [(comm.count() + i, newRecord.collect()[i]['CommodityCode']) for i in range(newRecord.count())]
        newDataDf = spark.createDataFrame(data=newData, schema=["Commodity_Key", 'CommodityCode'])
        newDataDf.write.mode("append").format("jdbc").options(**{'url': uri, 'dbtable' : 'Dim_Commodity', 'isolationLevel' : 'NONE'}).save()
        self.log.info("Update dim commodity successfully")
        
    def insertFact(self, df, uri, spark):
        transaction = df.select(['Comm', 'Contract_Type', 'Mth_Code', 'Year', 'Strike', 'Trade_Date', 'Log_Time', 'Price', 'Msg_Code', 'Volume'])
        
        #join commodity
        Comm = spark.read.format("jdbc").options(**{'url': uri, 'dbtable' : 'Dim_Commodity'}).load()
        transaction = transaction.join(Comm, transaction.Comm == Comm.CommodityCode, 'left').select(df["*"], Comm['Commodity_Key'].alias("CommodityId"))
        
        #join month
        Month = spark.read.format("jdbc").options(**{'url': uri, 'dbtable' : 'Dim_Month'}).load()
        transaction = transaction.join(Month, transaction.Mth_Code == Month.MonthCode, 'left').select(transaction["*"], Month['Month_Key'].alias("MonthId"))
        
        # join year
        Year = spark.read.format("jdbc").options(**{'url': uri, 'dbtable' : 'Dim_Year'}).load()
        transaction = transaction.join(Year, transaction.Year == Year.Year, 'left').select(transaction["*"], Year['Year_Key'].alias("YearId"))

        # join date
        Date = spark.read.format("jdbc").options(**{'url': uri, 'dbtable' : 'Dim_Date'}).load()
        transaction = transaction.join(Date, transaction.Trade_Date == Date.Date_Key, 'left').select(transaction["*"], Date['Date_Key'].alias("DateId"))
        
        #join time
        Time = spark.read.format("jdbc").options(**{'url': uri, 'dbtable' : 'Dim_Time'}).load()
        transaction = transaction.join(Time, transaction.Log_Time == Time.Time_Key, 'left').select(transaction["*"], Time['Time_Key'].alias("LogTimeId"))
        
        #join action
        Action = spark.read.format("jdbc").options(**{'url': uri, 'dbtable' : 'Dim_Action'}).load()
        transaction = transaction.join(Action, transaction.Msg_Code == Action.Action_Key, 'left').select(transaction["*"], Action['Action_Key'].alias("ActionId"))
        
        self.log.info("Done join fact table")
        
        transaction = transaction.withColumnRenamed("Contract_Type", 'ContractTypeId').withColumnRenamed("Strike", 'StrikePrice')
        lastDf = transaction.select(['CommodityId', 'MonthId', 'YearId', 'DateId', 'LogTimeId', 'ActionId', 'ContractTypeId', 'StrikePrice', 'Volume'])
        lastDf = lastDf.withColumn('IdTransaction', F.monotonically_increasing_id())
        
        lastDf.write.mode("append").format("jdbc").options(**{ 'url': uri, 'dbtable' : 'Fact_DerivatiesTrading', 'isolationLevel' : 'NONE' }).save()
        self.log.info("Insert fact table successfully")
        
    def execute(self, context):
        spark = SparkSession.builder.appName("loadData") \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()
        fileName = Variable.get("fileName")
        if fileName == 'error':
            self.log.info("No new file for etl")
            return
        df = spark.read.csv(f"hdfs://localhost:8020/sgx_data/{fileName}", header=True, inferSchema=True)
        self.log.info(f"Done read file {fileName}, with {df.count()} record")
        
        # uri = "jdbc:clickhouse://{}:{}/{}".format("localhost", 8123, "SgxTrading")
        # dimAction = spark.read.format("jdbc").options(**{'url': uri,'dbtable' : 'Dim_Action','isolationLevel' : 'NONE'}).load()
        
        # if dimAction.count() == 0:
            # self.log.info("Start load default dimension table")
            # self.insertDefaultDim(df, uri, spark)
            
        # self.log.info("Start update commodity")
        # self.updateCommodity(df, uri, spark)
        
        # self.log.info("Start insert fact table")
        # self.insertFact(df, uri, spark)