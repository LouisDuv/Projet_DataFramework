import yfinance as yf

import numpy as np

import glob
import os

import pandas as pd

import pyspark.pandas as ps

from pyspark.sql import functions as sf
from pyspark.sql.types import NumericType
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, month, year, day, lit
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

def differenceBtwDays(df: DataFrame):
    diff_array = []
    for pos, date in enumerate(df):
        
        if(pos + 1 < len(df)):
            tmp1 = df[pos+1]

        diff = tmp1 - date
        diff_array.append(diff.days)

    return diff_array

def most_common_element(arr):
    count = Counter(arr)
    most_common = count.most_common(1)  #Renvoie une liste de tuple (élément, occurrences)
    return most_common[0] if most_common else (None, 0) 

# Donne le format de l'enregistrement des données : daily, weekly, monthly
# Permet de détecter des patterns non-reconnu (enregistrement de 5 jours, 14, et autres)
# Retourn un message en format str

def period_btw_data(df: DataFrame, df_idx: int):
    counterDaily = 0
    counterWeekly = 0
    counterMonthly = 0
    counterYearly = 0
    weekDay = []
    result = f"Dataframe {df_idx+1}, period:"

    df_p = df.toPandas()
    df_p = pd.to_datetime(df_p["Date"])

    differences = differenceBtwDays(df_p)

    for period in differences:
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

    if main_pattern == avgDaily:
        result += "\n[INFO] Daily information - {} days corresponding to this format.\nAnd {} days not following this pattern\n".format(counterDaily, df.count() - counterDaily)
    elif main_pattern == avgWeekly:
        result += "\n[INFO] Weekly information - {} days corresponding to this format.\nAnd {} days not following this pattern\n".format(counterWeekly, df.count() - counterWeekly)
    elif main_pattern == avgMonthly:
        result += "\n[INFO] Monthly information - {} days corresponding to this format.\nAnd {} days not following this pattern\n".format(counterMonthly, df.count() - counterMonthly)
    elif main_pattern == avgYearly:
        result += "\n[INFO] Yearly information - {} days corresponding to this format.\nAnd {} days not following this pattern\n".format(counterYearly, df.count() - counterYearly)
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
        ps_df = ps.DataFrame(df_avg)
        return ps_df

# Fonction utile pour labelisation des mois

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
# Output : Pyspark.pandas dataframe

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
                    dictio_avg_month[get_month_name(initMonth, row.year)] = np.round(average, 3)
                
                    array = []
                    initMonth = row.month
                    array.append(getattr(row, str))

        
        p_df = pd.DataFrame(list(dictio_avg_month.items()), columns=[f"Period", "Average " + str + " Price($)"])
        ps_df = ps.DataFrame(p_df)

        return ps_df

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
                    dictio_avg_year[initYear] = np.round(average, 3)
                    array = []
                
                    initYear = row.year
                    array.append(getattr(row, str))


        p_df = pd.DataFrame(list(dictio_avg_year.items()), columns=[f"Period", "Average_" + str + "_Price($)"])
        ps_df = ps.DataFrame(p_df)

        return ps_df

# Mesure variation day to day selon la colonne donnée : str
# Variation : CLOSE PRICE - OPEN PRICE
# Return a pyspark dataframe

def dtd_stock_variation(df):
    
    df = df.withColumn("Stock_Variation", lit(0))
    p_df = df.toPandas()

    for pos, row in enumerate(p_df):
       
        i = pos + 1 # pos prend en compte l'entete

        if i <= len(p_df) :
            p_df["Stock_Variation"] = np.round((p_df["Close"] - p_df["Open"]), 3)
  
    ps_df = ps.DataFrame(p_df)

    return ps_df
        

# Mesure des variations pour chaque mois d'une colonne donnée
# str : "Volume", "Open", "Close"
# Variation : DERNIER JOUR DU MOIS VOLUME - PREMIER JOUR DU MOIS VOLUME
# Return: pyspark.pandas dataframe

def monthly_stock_variation(df, str):
    
    df = df.sort("Date", ascending = True)

    month_df = df.withColumn("Month", month("Date"))
    date = month_df.withColumn("Year", year("Date"))

    ini_month = month_df.select("Month").first()[0]
    ini_year = date.select("Year").first()[0]

    tmp_array_volume = []
    stock = {}

    for pos, row in enumerate(date.collect()):

        if ini_month == row.Month:
            tmp_array_volume.append(getattr(row, str))

        else :
                    
            val_beg = tmp_array_volume[0]

            if(len(tmp_array_volume) == 1):
                val_fin = 0
            else :
                val_fin = tmp_array_volume[len(tmp_array_volume)-1]

            stock[get_month_name(ini_month, ini_year)] = np.round(val_fin - val_beg, 3)

            ini_month = row.Month
            ini_year = row.Year

            tmp_array_volume = []
            tmp_array_volume.append(getattr(row, str))


        if pos+1 == len(date.collect()):
                tmp_array_volume.append(getattr(row, str))
                val_beg = tmp_array_volume[0]
                if len(tmp_array_volume) == 1:
                    val_fin = 0
                else :
                    val_fin = tmp_array_volume[-1]

                stock[get_month_name(ini_month, ini_year)] = np.round(val_fin - val_beg, 3)
    
    p_df = pd.DataFrame(list(stock.items()), columns=["Period", "Stock Variation ($)"])
    ps_df = ps.DataFrame(p_df)

    return ps_df

# Mesure le benefice max sur d'un DF
#Retourn un dataframe pandasOnSpark

def max_daily_return(df):
    ps_df = dtd_stock_variation(df)
    print(ps_df)
    s_df = ps_df.to_spark()
    s_df = s_df.select(F.max("Stock_Variation").alias("Maximum_profit"))
    ps_df = ps.DataFrame(s_df)
    return ps_df


# Determine la moyenne des rentabilites des actions sur une période donnée (rentabilité : close-open)
# period : w pour week, m pour month, y pour year
# Return un dataframe panda-spark

def avg_daily_return(df,period):

    if period in ["w", "m", "y"]:

        df = df.sort("Date", ascending = True)
        trigger = False
    
        tmp_df = dtd_stock_variation(df)
        tmp_df = tmp_df.to_spark()

        print(tmp_df)

        nb_sample = 0
        init_date = tmp_df.select("Date").first()[0]
        stock_var = {}
        tmp_array = []

        if period == "w":
            nb_sample = 7
        elif period == "m":
            nb_sample = 31
        elif period == "y":
            nb_sample = 365
        else :
            return -1
    
        for row in tmp_df.collect():
            diff = init_date - row.Date
            if abs(diff.days) < nb_sample:
                tmp_array.append(row.Stock_Variation)
            else :
                trigger = True
                key = f"{init_date} to {row.Date}"
                stock_var[key] = np.round(np.average(np.array(tmp_array)), 3)
                init_date = row.Date
                tmp_array = []

        if trigger == False :
            print("[INFO] Given period too large for the dataset")
            return -1
        
        p_df = pd.DataFrame(list(stock_var.items()), columns=["Period", "Average_Stock_Variation_($)"])
        ps_df = ps.DataFrame(p_df)

        return ps_df
    else :
        return -1

dataframe_obj = DataframeClass()

csv_folder_path = 'Stocks_Price'
csv_files = glob.glob(os.path.join(csv_folder_path, "*.csv"))

data_dfs = dataframe_obj.read_multiple_csv(csv_files)

result = monthly_stock_variation(data_dfs[0], "Open") #dataframe_obj.perform_operation_on_each(monthly_avg_open_price)

print(result)

# result = dataframe_obj.perform_operation_on_each(values_correlation, "Close", "Volume")
# print("\n\n")
# result = dataframe_obj.perform_operation_on_each(values_correlation, "High", "Low")  
