import yfinance as yf

import findspark
findspark.init("C:\Program Files\spark-3.5.3-bin-hadoop3")

import pyspark as ps
from pyspark.sql import SparkSession

class Dataframe:
    
    def __init__(self, name):
        self._name = name
        
    def get_name(self):
        return self._name
    
aapl_data = yf.Ticker("AAPL")
aapl_obj = Dataframe("apple.csv")
aapl_data.history(period = "1mo").to_csv(aapl_obj.get_name())

pfe_data = yf.Ticker("PFE")
pfe_obj = Dataframe("pfizer.csv")
pfe_data.history(period = "1mo").to_csv(pfe_obj.get_name())

f_data = yf.Ticker("F")
f_obj = Dataframe("ford.csv")
f_data.history(period = "1mo").to_csv(f_obj.get_name())

msft_data = yf.Ticker("MSFT")
msft_obj = Dataframe("microsoft.csv")
msft_data.history(period = "1mo").to_csv(msft_obj.get_name())

ba_data = yf.Ticker("BA")
ba_obj = Dataframe("boeing.csv")
ba_data.history(period = "1mo").to_csv(ba_obj.get_name())

spark = SparkSession.builder.appName("CSV Operations").config("spark.driver.host", "localhost").getOrCreate()

data_df_aapl = spark.read.csv(aapl_obj.get_name(), header=True, inferSchema=True)
data_df_pfe = spark.read.csv(pfe_obj.get_name(), header=True, inferSchema=True)
data_df_f = spark.read.csv(f_obj.get_name(), header=True, inferSchema=True)
data_df_msft = spark.read.csv(msft_obj.get_name(), header=True, inferSchema=True)
data_df_ba = spark.read.csv(ba_obj.get_name(), header=True, inferSchema=True)

data_df_aapl.printSchema()
data_df_aapl.show(10)