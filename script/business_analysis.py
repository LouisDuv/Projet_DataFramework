import numpy as np

import pandas as pd

import pyspark.pandas as ps
from datetime import timedelta


from pyspark.sql.functions import avg, month, year,lit
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from script.exploration import values_correlation

spark = SparkSession.builder.appName("StockVariation").getOrCreate()

# Input : period -> w for weekly, y for yearly, m for monthly 
# str -> Open ou Close
# Output : DataFrame (Period, Average_STR_Price($))
        
def avg_price(df, period, str):

    trigger = False

    if str in ["Open", "Close"] and period in ["w", "m", "y"]:
        
        if period == "w":
            nb_samples = 7
        elif period == "y":
            nb_samples = 365
        elif period == "m":
            nb_samples = 30
        
        initDate = df.select("Date").first()[0]
        
        tmp = []
        dictio = {}

        for row in df.collect():
            
            diff = row.Date - initDate
            
            if abs(diff.days) < nb_samples :
                tmp.append(getattr(row, str))
            else :
                trigger = True
                key = f"{initDate} to {row.Date}"
                dictio[key] = np.round(np.average(tmp), 3)
                tmp = []
                initDate = row.Date

        if trigger == False :
            print("[INFO] -> Dataset too small to analyze for the given period")
            return -1
        
        ps_df = ps.DataFrame(list(dictio.items()), columns=["Period", f"Average_{str}_Price($)"])

        return ps_df
    else : 
        print("[INFO] -> Error in one of the parameters, please give (period : w, y, m and existing column for str)")
        return -1

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
    
    p_df = pd.DataFrame(list(stock.items()), columns=["Period", f"Stock_{str}_Variation_($)"])
    ps_df = ps.DataFrame(p_df)

    return ps_df

# Mesure le benefice max sur d'un DF
#Retourn un dataframe pandasOnSpark

def max_daily_return(df):
    ps_df = dtd_stock_variation(df)
    s_df = ps_df.to_spark()
    s_df = s_df.select(F.max("Stock_Variation").alias("Maximum_profit"))
    ps_df = ps.DataFrame(s_df)
    return ps_df

# Determine la moyenne des rentabilites des actions sur une période donnée (rentabilité : close-open)
# period : w pour week, m pour month, y pour year
# Return un dataframe pandaOnSpark

def avg_return(df,period):

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

# Input : 2 Datasets, existing columns for both of them
# Ouput : DataFrame with correlation value
# => Methode Pearson utilisée pour la corrélation

def correlation_btw_stocks(df_1, df_2, col):

    pdf_1 = df_1.selectExpr("{} as D1".format(col)).toPandas()
    pdf_2 = df_2.selectExpr("{} as D2".format(col)).toPandas()

    p_df = pd.concat([pdf_1, pdf_2], axis=1)
    
    return p_df.corr(method="pearson")
 
# Input : df, period [w, m, y]
# Output : PandaOnPyspark df [Period, Return Rate]
# Return Rate : [[Close - Open]/Open] * 100

def return_rate(df, period):

    if period in ["w", "m", "y"]:

        if period == "w":
            nb_samples = 7
        elif period == "y":
            nb_samples = 364
        elif period == "m":
            nb_samples = 30

        init_date = df.select("Date").first()[0]
        init_open = df.select("Open").first()[0]

        dictio = {}
        trigger = False

        for row in df.collect():
            diff = row.Date - init_date
            if abs(diff) >= timedelta(days=nb_samples) :
                trigger = True
                rate = ((row.Close - init_open)/init_open) * 100
                key = f"{init_date} to {row.Date}"
                dictio[key] = rate
                init_date = row.Date
                init_open = row.Open

        if trigger == False:
            print("[INFO] -> Period too large to be used on this dataset")
            return -2 
        ps_df = ps.DataFrame(list(dictio.items()), columns=["Period", "Return_Rate_(%)"])

        return ps_df
    else :
        print("[INFO] -> can't take parameter period in charge")
        return -1

def max_return_rate(df, period):
    ps_df = return_rate(df, period)
    s_df = ps_df.to_spark()

    return ps.DataFrame(s_df.select(F.max("Return_Rate_(%)").alias("Max_Return_Rate")))