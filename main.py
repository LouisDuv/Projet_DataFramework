import yfinance as yf

import glob
import os

import findspark
findspark.init("C:\Program Files\spark-3.5.3-bin-hadoop3")

import pyspark as ps
from pyspark.sql import SparkSession

from DataFrame import DataframeClass


# aapl = yf.Ticker("AAPL").history(period="1y")
# aapl.to_csv("apple.csv")
# msft = yf.Ticker("MSFT").history(period="1y")
# msft.to_csv("microsoft.csv")
# ba = yf.Ticker("BA").history(period="1y")
# ba.to_csv("boeing.csv")
# f = yf.Ticker("F").history(period="1y")
# f.to_csv("ford.csv")
# pfe = yf.Ticker("PFE").history(period="1y")
# pfe.to_csv("pfizer.csv")

def head_and_tail_40(df):
    head_40 = df.limit(40)
    tail_40 = df.orderBy("Date", ascending=False).limit(40).orderBy("Date", ascending=True)
    return head_40.union(tail_40)

dataframe_obj = DataframeClass()

csv_folder_path = 'Stocks_Price'
csv_files = glob.glob(os.path.join(csv_folder_path, "*.csv"))

data_df = dataframe_obj.read_multiple_csv(csv_files)

# dataframe_obj.print_schemas()

result = dataframe_obj.perform_operation_on_each(head_and_tail_40)
for idx, res in enumerate(result):
    print(f"Result for dataframe {idx+1}:")
    res.show(80)
