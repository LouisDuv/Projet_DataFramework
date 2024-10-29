import numpy as np

import pandas as pd

import pyspark.pandas as ps

from pyspark.sql.functions import avg, month, year,lit
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("StockVariation").getOrCreate()

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

# Input : df, colonne à examiner (Open, Close, etc), nombre de données à moyenner
# Output : Data 
def moving_average(df, given_col, nb_sample):
    
    df = df.sort("Date", ascending = False)
    
    tmp_array = []

    for pos, row in enumerate(df.collect()):
        
        if pos + 1 <= nb_sample:
            tmp_array.append(getattr(row, given_col))
        else :
            break
    
    moving_average = np.round(np.average(tmp_array), 3)

    p_df = df.toPandas()
    p_df["Moving_Average"] = moving_average
    
    ps_df = ps.DataFrame(p_df.head(nb_sample)) # Retourn un df de la taille du nb de sample donnée

    return ps_df