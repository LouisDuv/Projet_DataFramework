import yfinance as yf

import numpy as np

import glob
import os

import pandas as pd

from pyspark.sql import functions as sf
from pyspark.sql.types import NumericType
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg

from DataFrame import DataframeClass

from collections import Counter



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

# About DFs :

#High : Le prix le plus élevé atteint par l'action pendant la journée de bourse
#Low : Le prix le plus bas atteint par l'action pendant la journée de bourse
#Close : Le dernier prix auquel l'action a été échangée à la fin de la journée de bourse
#Open : Le premier prix auquel l'action a été mesurée au début de la journée



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


def differenceBtwDays(df):
    diff_array = []
    for pos, date in enumerate(df):
        
        if(pos + 1 < len(df)):
            tmp1 = df[pos+1]

        diff = tmp1 - date
        diff_array.append(diff.days)

    return diff_array

def most_common_element(arr):
    count = Counter(arr)
    most_common = count.most_common(1)  # Renvoie une liste de tuple (élément, occurrences)
    return most_common[0] if most_common else (None, 0) 

def period_btw_data(df):

    counterDaily = 0
    counterMonthly = 0
    counterYearly = 0
    counterWeekly = 0
    weekDay = []

    df_p = df.toPandas()
    df_p = pd.to_datetime(df_p["Date"])

    differences = differenceBtwDays(df_p)

    for period in differences :

        if period == 1:
            counterDaily +=1
        elif period == 7 :
            counterWeekly +=1
        elif period == 30 or period == 31 :
            counterMonthly +=1
        elif period == 365 :
            counterYearly += 1
        else :
            weekDay.append(period)

    # Reconnaissance d'un pattern
    counterPattern = 0
    
    # Si le nombre de pattern inconnu est supérieur à 20% du nb total de donnée 
    if len(weekDay) > 0.2 * df.count():
        day_pattern, counterPattern = most_common_element(weekDay)
    
    avgDaily = counterDaily * 100 / df.count()
    avgWeekly = counterWeekly * 100 / df.count()
    avgMonthly = counterMonthly * 100 / df.count()
    avgYearly = counterYearly * 100 / df.count()
    avgPattern = counterPattern * 100 / df.count()

    main_pattern = max(avgDaily, avgWeekly, avgMonthly, avgYearly, avgPattern)

    if main_pattern == avgDaily  :
        str = "\n[INFO] Daily information - {} days corresponding to this format.\nAnd {} days not following this pattern".format(counterDaily, df.count() - counterDaily)
        return str

    elif main_pattern == avgWeekly:
        str = "\n[INFO] Weekly information - {} days corresponding to this format.\nAnd {} days not following this pattern".format(counterWeekly, df.count() - counterWeekly)
        return str
    
    elif main_pattern == avgMonthly :
        str = "\n[INFO] Monthly information - {} days corresponding to this format.\nAnd {} days not following this pattern".format(counterMonthly, df.count() - counterMonthly)
        return str
    
    elif main_pattern == avgYearly :
        str = "\n[INFO] Yearly information - {} days corresponding to this format.\nAnd {} days not following this pattern".format(counterYearly, df.count() - counterYearly)
        return str
    elif main_pattern == avgPattern:
        str = "\n[INFO] Main pattern : {} days between information  - {} days corresponding to this format.\nAnd {} days not following this pattern".format(day_pattern, counterPattern, df.count() - counterPattern)
        return str
    else : 
        return "\n[INFO] Erreur dans la lecture de pattern"

def daily_avg_open_price(df, indx):
    df_avg = df.agg(avg('Open').alias("Average Open"))
    average_value = df_avg.collect()[0]
    return float(average_value['Average Open'])
        


dataframe_obj = DataframeClass()

csv_folder_path = 'Stocks_Price'
csv_files = glob.glob(os.path.join(csv_folder_path, "*.csv"))

data_dfs = dataframe_obj.read_multiple_csv(csv_files)

result = dataframe_obj.perform_operation_on_each(daily_avg_open_price)

print(result)