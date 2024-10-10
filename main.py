import yfinance as yf

import numpy as np

import glob
import os

import findspark
findspark.init("C:\Program Files\spark-3.5.3-bin-hadoop3")

from pyspark.sql import functions as sf
from pyspark.sql.types import NumericType

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

def num_observations(df):
    return df.count()

def descript_stats(df):
    agg_expr = []
    for column in df.columns:
        agg_expr.append(sf.min(column).alias(f"min_{column}"))
        agg_expr.append(sf.max(column).alias(f"max_{column}"))
        if isinstance(df.schema[column].dataType, NumericType):
            agg_expr.append(sf.stddev(column).alias(f"stddev_{column}"))
    result = df.agg(*agg_expr)
    return result

def count_missing(df):
    missing_expr = []
    for column in df.columns:
        if isinstance(df.schema[column].dataType, NumericType):
            missing_expr.append(
                sf.count(sf.when(sf.col(column).isNull() | sf.isnan(column), column)).alias(f"missing_{column}")
            )
        else:
            missing_expr.append(
                sf.count(sf.when(sf.col(column).isNull(), column)).alias(f"missing_{column}")
            )
    return df.select(missing_expr)

dataframe_obj = DataframeClass()

csv_folder_path = 'Stocks_Price'
csv_files = glob.glob(os.path.join(csv_folder_path, "*.csv"))

data_df = dataframe_obj.read_multiple_csv(csv_files)

result = dataframe_obj.perform_operation_on_each(count_missing)
for idx, res in enumerate(result):
    print(f"Result for dataframe {idx+1}:")
    res.show()
    