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

from script.DataFrame import DataframeClass
from script.pages.homepage import *
from script.business_analysis import *
from collections import Counter



#aapl = yf.Ticker("AAPL").history(period="5y")
#aapl.to_csv("apple.csv")
#ba = yf.Ticker("BA").history(period="5y")
#ba.to_csv("boeing.csv")
#f = yf.Ticker("F").history(period="5y")
#f.to_csv("ford.csv")
#pfe = yf.Ticker("PFE").history(period="5y")
#pfe.to_csv("pfizer.csv")
#meta = yf.Ticker("META").history(period="5y")
#meta.to_csv("meta.csv")
#am = yf.Ticker("AMZN").history(period="5y")
#am.to_csv("amazon.csv")
#mcrs = yf.Ticker("MSFT").history(period="5y")
#mcrs.to_csv("microsoft.csv")

spark = SparkSession.builder.appName("StockVariation").getOrCreate()

dataframe_obj = DataframeClass(spark=spark)

csv_folder_path = 'Stocks_Price'
csv_files = glob.glob(os.path.join(csv_folder_path, "*.csv"))

data_dfs = dataframe_obj.read_multiple_csv(csv_files)

df = max_return_rate(data_dfs, 'y', 200)

print(df)
