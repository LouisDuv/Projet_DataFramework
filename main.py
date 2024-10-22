import yfinance as yf

import numpy as np

import glob
import os

import pandas as pd

from pyspark.sql import functions as sf
from pyspark.sql.types import NumericType
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, month, year, row_number
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType, DoubleType
from pyspark.sql import functions as F

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

# str : Open ou Close pour connaitre l'average des prix par jour 
# Output : average (float)
        
def daily_avg_price(df, str):
    if str == "Open" or str == "Close":
        df_avg = df.agg(avg(str).alias("Average"))
        average_value = df_avg.collect()[0]
        return float(average_value['Average'])

def get_month_name(month_number, year):
    months = [
        "", 
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sept",
        "Oct",
        "Nov",
        "Dec"
    ]
    
    if 1 <= month_number <= 12:
        return months[month_number] + "-"+ str(year)
    else : 
        return -1

# str : Open ou Close pour connaitre l'average des prix par mois 
# Output : dictionnaire avec clé : "mois-year" et valeur : average (Close price ou Open price)

def monthly_avg_price(df, str):
    
    if str == "Open" or str == "Close" :

        dictio_avg_month = {}
        open_df = df.select("Date", str)

        date_open_df = open_df.withColumn("month", month("Date"))
        date_open_df = date_open_df.withColumn("year", year("Date"))

        initMonth = date_open_df.select("month").first()[0]

        array = []
  
        for pos, row in enumerate(date_open_df.collect()):
            
            if pos + 1 == len(date_open_df.collect()) : # POS +1 pour gerer l'entete

                initMonth = row.month
                array.append(getattr(row, str))
                average = sum(array) / len(array)
                dictio_avg_month[get_month_name(initMonth, row.year)] = average

            else : 
                if initMonth == row.month:
                    array.append(getattr(row, str)) # getattr Accéder à l'attribut de la ligne par le str donnée (Open/Close) 
            
                else:
                    average = sum(array) / len(array)
                    dictio_avg_month[get_month_name(initMonth, row.year)] = average
                
                    array = []
                    initMonth = row.month
                    array.append(getattr(row, str))

        
        return dictio_avg_month


# str : Open ou Close pour connaitre l'average des prix par mois 
# Output : dictionnaire avec clé : year et valeur : average (Close price ou Open price)

def yearly_avg_price(df, str):

    if str == "Open" or str == "Close" :

        dictio_avg_year = {}
        open_df = df.select("Date", str)

        date_open_df  = open_df.withColumn("year", year("Date"))
        initYear= date_open_df.select("year").first()[0]

        array = []
        for pos, row in enumerate(date_open_df.collect()):
            
            if pos + 1 == len(date_open_df.collect()) : # POS +1 pour gerer l'entete

                initYear = row.year
                array.append(getattr(row, str))
                average = sum(array) / len(array)
                dictio_avg_year[initYear] = average
            
            else :

                if initYear == row.year:
                    array.append(getattr(row, str)) # getattr Accéder à l'attribut de la ligne par le str donnée (Open/Close)
                
                else :
                    average = sum(array) / len(array)
                    dictio_avg_year[initYear] = average
                    array = []
                
                    initYear = row.year
                    array.append(getattr(row, str))

        return dictio_avg_year


def joined_dfs(df, tmp_df):

    df = df.withColumn("index", F.monotonically_increasing_id())
    tmp_df = tmp_df.withColumn("index", F.monotonically_increasing_id()) # Incrémentation de l'index par la fonction mono..
    
    new_schema = StructType([
            StructField("Stock Variation", DoubleType(), True),
            StructField("Date", DateType(), True),
            StructField("Open", FloatType(), True),
            StructField("High", FloatType(), True),
            StructField("Low", FloatType(), True),
            StructField("Close", FloatType(), True),
            StructField("Volume", FloatType(), True),
            StructField("Dividends", FloatType(), True),
            StructField("Stock Splits", FloatType(), True)
    ])

    # Jointure sur index

    new_df = spark.createDataFrame(tmp_df.join(df, "index").drop("index").collect(),schema = new_schema)

    return new_df


# Mesure the stock variation day to day
# Variation : DAY(N+1) - DAY(N)

def stock_dtd_variation(df):
    

    tmp = []
    tmp_row = 0

    print(len(df.collect()))

    for pos, row in enumerate(df.collect()):
        i = pos + 1 # pos prend en compte l'entete

        if i == 1 :
            tmp_row = row.Volume


        elif i <= len(df.collect()) :
            tmp.append(row.Volume - tmp_row )
            tmp_row = row.Volume
    
    tmp_df = spark.createDataFrame([(val, ) for val in tmp], ["Stock Variation"])
    
    # Jointure des deux tables pour ajout de Varation Stock

    new_df = joined_dfs(df, tmp_df)
    
    return new_df
        
        

dataframe_obj = DataframeClass()

csv_folder_path = 'Stocks_Price'
csv_files = glob.glob(os.path.join(csv_folder_path, "*.csv"))

data_dfs = dataframe_obj.read_multiple_csv(csv_files)

result = stock_dtd_variation(data_dfs[0])#dataframe_obj.perform_operation_on_each(monthly_avg_open_price)

print(result)