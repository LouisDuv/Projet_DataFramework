import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

import numpy as np
import glob

from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType, NumericType
from pyspark.sql import DataFrame
from pyspark.sql import Window
from typing import List
from datetime import date

from collections import Counter

from DataFrame import DataframeClass
from FinancialDataAnalyzer import FinancialDataAnalyzer



# aapl = yf.Ticker("AAPL").history(period="5y")
# aapl.to_csv("apple.csv")
# msft = yf.Ticker("MSFT").history(period="5y")
# msft.to_csv("microsoft.csv")
# ba = yf.Ticker("BA").history(period="5y")
# ba.to_csv("boeing.csv")
# f = yf.Ticker("F").history(period="5y")
# f.to_csv("ford.csv")
# pfe = yf.Ticker("PFE").history(period="5y")
# pfe.to_csv("pfizer.csv")

spark = SparkSession.builder.appName("StockAnalysis").config("spark.driver.host", "localhost").getOrCreate()

dataframe_obj = DataframeClass(spark)
analyzer = FinancialDataAnalyzer()

csv_folder_path = 'Stocks_Price'
csv_files = glob.glob(os.path.join(csv_folder_path, "*.csv"))

data_dfs = dataframe_obj.read_multiple_csv(csv_files)


df = dataframe_obj.perform_operation(analyzer.head_and_tail_40)
