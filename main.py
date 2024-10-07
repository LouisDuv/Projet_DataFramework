import yfinance as yf

import findspark
findspark.init("C:\Program Files\spark-3.5.3-bin-hadoop3")

import pyspark as ps
from pyspark.sql import SparkSession

aapl_data = yf.Ticker("AAPL")
historical_data = aapl_data.history(period = "1mo")

historical_data.to_csv("apple.csv")

spark = SparkSession.builder.appName("CSV Operations").config("spark.driver.host", "localhost").getOrCreate()