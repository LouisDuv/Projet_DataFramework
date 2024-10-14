import yfinance as yf

import numpy as np

import glob
import os

import findspark
findspark.init("C:\Program Files\spark-3.5.3-bin-hadoop3")

from pyspark.sql import functions as sf
from pyspark.sql.types import NumericType
from pyspark.sql import DataFrame

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

def head_and_tail_40(df: DataFrame, df_idx: int):
    head_40 = df.limit(40)
    tail_40 = df.orderBy("Date", ascending=False).limit(40).orderBy("Date", ascending=True)
    result = head_40.union(tail_40)
    print(f"Dataframe {df_idx+1}, first and last 40 rows:")
    result.show(80)
    return result

def num_observations(df: DataFrame, df_idx: int):
    result = df.count()
    print(f"Dataframe {df_idx+1}, number of observations: {result}")
    return result

def descript_stats(df: DataFrame, df_idx: int):
    agg_expr = []
    for column in df.columns:
        agg_expr.append(sf.min(column).alias(f"min_{column}"))
        agg_expr.append(sf.max(column).alias(f"max_{column}"))
        if isinstance(df.schema[column].dataType, NumericType):
            agg_expr.append(sf.stddev(column).alias(f"stddev_{column}"))
    result = df.agg(*agg_expr)
    print(f"Dataframe {df_idx+1}, descriptive stats:")
    result.show()
    return result

def count_missing(df: DataFrame, df_idx: int):
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
    result = df.select(missing_expr)
    print(f"Dataframe {df_idx+1}, missing values:")
    result.show()
    return result

def values_correlation(df: DataFrame, df_idx: int, col1: str, col2: str):
    correlation = df.stat.corr(col1, col2)
    print(f"Dataframe {df_idx+1}, correlation between {col1} and {col2}: {correlation}")
    #interesting: correlation between (close-open) and volume
    return correlation

dataframe_obj = DataframeClass()

csv_folder_path = 'Stocks_Price'
csv_files = glob.glob(os.path.join(csv_folder_path, "*.csv"))

data_dfs = dataframe_obj.read_multiple_csv(csv_files)

result = dataframe_obj.perform_operation_on_each(values_correlation, "Close", "Volume")
print("\n\n")
result = dataframe_obj.perform_operation_on_each(values_correlation, "High", "Low")
# for idx, res in enumerate(result):
#     print(f"Result for dataframe {idx+1}:")    