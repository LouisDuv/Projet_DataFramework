
import pandas as pd
from pyspark.sql import functions as sf
from pyspark.sql.types import NumericType
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from collections import Counter


spark = SparkSession.builder.appName("StockVariation").getOrCreate()

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

# Donne le format de l'enregistrement des données : daily, weekly, monthly
# Permet de détecter des patterns non-reconnu (enregistrement de 5 jours, 14, et autres)
# Retourn un message en format str

def get_day(dt) :
    return dt.day

def period_btw_data(df):
    
    init_date = df.select(sf.min("Date")).collect()[0]
    last_date = df.select(sf.max("Date")).collect()[0]

    diff = last_date[0] - init_date[0]

    return diff.days

def most_common_element(arr):
    count = Counter(arr)
    most_common = count.most_common(1)  #Renvoie une liste de tuple (élément, occurrences)
    return most_common[0] if most_common else (None, 0) 

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

def values_correlation(df: DataFrame,  col1: str, col2: str):
    correlation = df.stat.corr(col1, col2)
    print(f"Dataframe correlation between {col1} and {col2}: {correlation}")
    #interesting: correlation between (close-open) and volume
    return correlation