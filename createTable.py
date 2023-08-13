import clickhouse_connect

client = clickhouse_connect.get_client(host='localhost', port=8123, username='', password='')

client.command("create database if not exists SgxTrading")

client.command('''create table if not exists SgxTrading.Dim_Month
(
    Month_Key INT,
    MonthCode CHAR,
    Month INT,
    MonthName TEXT
)
    engine = MergeTree ORDER BY Month_Key
        SETTINGS index_granularity = 8192;
''')

client.command('''create table if not exists SgxTrading.Dim_Year
(
    Year_Key INT,
    Year INT,
)
    engine = MergeTree ORDER BY Year_Key
        SETTINGS index_granularity = 8192;''')

client.command('''create table if not exists SgxTrading.Dim_ContractType
(
    ContractType_Key CHAR,
    ContractType TEXT,
)
    engine = MergeTree ORDER BY ContractType_Key
        SETTINGS index_granularity = 8192;''')

client.command('''create table if not exists SgxTrading.Dim_Date
(
    Date_Key INT,
    FullDate DATE,
    DayOfWeek TEXT,
    CalendarQuarter TEXT
)
    engine = MergeTree ORDER BY Date_Key
        SETTINGS index_granularity = 8192;''')

client.command('''create table if not exists SgxTrading.Dim_Action
(
    Action_Key CHAR,
    Action TEXT
)
    engine = MergeTree ORDER BY Action_Key
        SETTINGS index_granularity = 8192;''')

client.command('''create table if not exists SgxTrading.Dim_Time
(
    Time_Key INT,
    FullTime Text,
    Hour Int,
    Minute Int,
    Second Int
)
    engine = MergeTree ORDER BY Time_Key
        SETTINGS index_granularity = 8192;''')

client.command('''create table if not exists SgxTrading.Dim_Commodity
(
    Commodity_Key INT,
    CommodityCode TEXT
)
    engine = MergeTree ORDER BY Commodity_Key
    SETTINGS index_granularity = 8192;''')

client.command('''create table if not exists SgxTrading.Fact_DerivatiesTrading
(
    IdTransaction Int32,
    CommodityId INTEGER,
    MonthId INTEGER,
    YearId INTEGER,
    DateId INTEGER,
    LogTimeId INTEGER,
    ActionId TEXT,
    ContractTypeId TEXT,
    StrikePrice Int32,
    Price         Float32,
    Volume        Int32
)
    engine = MergeTree ORDER BY IdTransaction
        SETTINGS index_granularity = 8192;''')