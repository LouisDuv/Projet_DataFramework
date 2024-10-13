import yfinance as yf

import numpy as np

import glob
import os

import pandas as pd

from pyspark.sql import functions as sf
from pyspark.sql.types import NumericType

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
    counterNaN = 0
    weekDay = []
    week = ['Mon', 'Tue', 'Wed', 'Thur','Fry', 'Sat', 'Sun']

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


dataframe_obj = DataframeClass()

csv_folder_path = 'Stocks_Price'
csv_files = glob.glob(os.path.join(csv_folder_path, "*.csv"))

data_df = dataframe_obj.read_multiple_csv(csv_files)

my_fre = period_btw_data(data_df[1])

print(my_fre)

#result = dataframe_obj.perform_operation_on_each(count_missing)
#for idx, res in enumerate(result):
 #   print(f"Result for dataframe {idx+1}:")
  #  res.show()


