import numpy as np

import pandas as pd

import pyspark.pandas as ps
from datetime import timedelta
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
from pyspark.sql import Window



from pyspark.sql.functions import avg, month, year,lit, udf, row_number
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from script.exploration import values_correlation
from script.visualisation import bar_plot, linear_plot,streamlit_test

spark = SparkSession.builder.appName("StockVariation").getOrCreate()

def period_to_day(string : str):
    if string == "w" :
        return 7
    elif string == "m":
        return 30
    elif string == "y":
        return 365

    else : 
        print("[INFO] -> Mistake in the given string ")
        return -1

# Input : period -> w for weekly, y for yearly, m for monthly 
# str -> Open ou Close
# Output : DataFrame (Period, Average_STR_Price($))
# Plot associé : Linear Chart
 
def avg_price(df : DataFrame, period : str, str : str):

    trigger = False
  
    if str in ["Open", "Close"] and period in ["w", "m", "y"]:
        
        nb_samples = period_to_day(period)
        
        initDate = df.select("Date").first()[0]
        
        tmp = []
        dictio = {}

        for row in df.collect():
            
            diff = row.Date - initDate
            
            if abs(diff.days) < nb_samples :
                tmp.append(getattr(row, str))
            else :
                trigger = True
                dictio[row.Date] = np.round(np.average(tmp), 3)
                tmp = []
                initDate = row.Date

        if trigger == False :
            print("[INFO] -> Dataset too small to analyze for the given period")
            return -1
        
        schema = StructType([
            StructField("Period", DateType(), True),
            StructField(f"Average_{str}_Price($)", FloatType(), True)
            ])
        
        data = [(k, float(v)) for k, v in dictio.items()]

        s_df = spark.createDataFrame(data, schema=schema)
        s_df.show()

        #linear_plot(s_df["Period"].to_pandas(), s_df[f"Average_{str}_Price($)"].to_pandas(), period, str)

        return s_df
    else : 
        print("[INFO] -> Error in one of the parameters, please give (period : w, y, m and existing column for str)")
        return -1

# Fonction utile pour labelisation des mois

def get_month_name(month_number : int, year):
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

# Mesure variation day to day
# Variation : CLOSE PRICE - OPEN PRICE
# Return a spark dataframe

def dtd_stock_variation(df : DataFrame):

    def variation(a, b):
        return b-a
    
    variation_udf = udf(variation, FloatType())
    
    s_df = df.withColumn("Stock_Variation", variation_udf(F.col("Close"), F.col("Open")))
    
    #s_df.show()

    return s_df
        
# Mesure des variations pour chaque mois d'une colonne donnée
# str : "Volume", "Open", "Close"
# Variation : DERNIER JOUR DU MOIS CLOS - PREMIER JOUR DU MOIS OPEN
# Return: spark dataframe
# Plot associé : Linear Chart

def monthly_stock_variation(df : DataFrame, nb_of_month : int = None):
    

    init_date = df.select("Date").first()[0]
    init_str = df.select("Open").first()[0]

    stock = {}

    counter = 0

    for pos, row in enumerate(df.collect()):
        
        diff = row.Date - init_date

        if abs(diff) >= timedelta(days = 30) :
            
            counter += 1
            val_beg = init_str

            val_fin = getattr(row, "Close")

            stock[row.Date] = np.round(val_fin - val_beg, 3)

            if counter == nb_of_month :
                break

            init_date= row.Date
            init_str = getattr(row, "Open")

        if pos+1 == len(df.collect()):

                val_beg = init_str
                val_fin = getattr(row, "Close")

                stock[row.Date] = np.round(val_fin - val_beg, 3)
    

    schema = StructType([
        StructField("Period", DateType(), True),
        StructField(f"Stock_Variation_($)", FloatType(), True)
    ])
        
    data = [(k, float(v)) for k, v in stock.items()]

    s_df = spark.createDataFrame(data, schema=schema)
    s_df.show()

    return s_df

# Mesure le benefice max sur d'un DF
#Retourn un dataframe spark

def max_daily_return(df : DataFrame):

    s_df = dtd_stock_variation(df)
    max_df = s_df.select(F.max("Stock_Variation").alias("Maximum_profit"))

    max_df.show()
    return max_df

# Determine la moyenne des rentabilites des actions sur une période donnée (rentabilité : close-open)
# period : w pour week, m pour month, y pour year
# Return un dataframe spark
# Plot associé : Linear Chart

def avg_return(df : DataFrame, period : str):

    if period in ["w", "m", "y"]:

        df = df.sort("Date", ascending = True)
        trigger = False
    
        tmp_df = dtd_stock_variation(df)

        nb_sample = period_to_day(period)
        init_date = tmp_df.select("Date").first()[0]
        stock_var = {}
        tmp_array = []
    
        for row in tmp_df.collect():
            diff = init_date - row.Date
            if abs(diff.days) < nb_sample:
                tmp_array.append(row.Stock_Variation)
            else :
                trigger = True
                key = row.Date
                stock_var[key] = np.round(np.average(np.array(tmp_array)), 3)
                init_date = row.Date
                tmp_array = []

        if trigger == False :
            print("[INFO] Given period too large for the dataset")
            return -1
 
        schema = StructType([
        StructField("Period", DateType(), True),
        StructField("Average_Stock_Variation_($)", FloatType(), True)
        ])
        
        data = [(k, float(v)) for k, v in stock_var.items()]

        s_df = spark.createDataFrame(data, schema=schema)
        s_df.show()

        return s_df


# Input : df, colonne à examiner (Open, Close, etc), nombre de données à moyenner
# Output : RIEN

def moving_average(df : DataFrame, given_col : str, nb_sample : int):
    
    pass

    return -1

# Input : 2 Datasets, existing columns for both of them
# Ouput : Float, correlation value
# Plot associé : Scatter Plot

######################## Regler le probleme INDEX en parametre

def correlation_btw_stocks(df_1 : DataFrame, df_2 : DataFrame, col1 : str, col2 : str):

    window_spec = Window.orderBy()

    df1_indexed = df_1.withColumn("index", row_number().over(window_spec))
    df2_indexed = df_2.withColumn("index", row_number().over(window_spec))

    df_joined = df1_indexed.join(df2_indexed, on="index", how="inner")

    corr = values_correlation(df_joined, 0, col1, col2)

    return corr
 
# Input : df, period [w, m, y]
# Output : Spark df [Period, Return Rate]
# Return Rate : [[Close - Open]/Open] * 100
# Plot associé : Linear Chart

def return_rate(df : DataFrame, period : str):

    if period in ["w", "m", "y"]:

        nb_samples = period_to_day(period)

        init_date = df.select("Date").first()[0]
        init_open = df.select("Open").first()[0]

        dictio = {}
        trigger = False

        for row in df.collect():
            diff = row.Date - init_date
            if abs(diff) >= timedelta(days=nb_samples) :
                trigger = True
                rate = ((row.Close - init_open)/init_open) * 100
                key = row.Date
                dictio[key] = rate
                init_date = row.Date
                init_open = row.Open

        if trigger == False:
            print("[INFO] -> Period too large to be used on this dataset")
            return -2 
        
        schema = StructType([
        StructField("Period", DateType(), True),
        StructField("Return_Rate(%)", FloatType(), True)
        ])
        
        data = [(k, float(v)) for k, v in dictio.items()]

        s_df = spark.createDataFrame(data, schema=schema)
        #s_df.show()

        return s_df

    else :
        print("[INFO] -> can't take parameter period in charge")
        return -1

# Bénéfice Maximum sur une période donnée
# Retourne un Spark DataFrame

def max_return_rate(df : DataFrame, period : str):
    s_df = return_rate(df, period)

    s_df_max = s_df.select(F.max("Return_Rate(%)").alias("Max_Return_Rate"))

    s_df_max.show()

    return s_df_max


# Input : df, nombres d'actions détenues
# Output : PS Dataframe avec col Revenus

def dividend_return(df : DataFrame, stocks_own : float):

    s_df_revenues = df.withColumn("Revenues", F.col("Dividends") * stocks_own)

    s_df_revenues.show()

    return s_df_revenues



########### A OPTIMISER###########
def variation_stocks_volume(df : DataFrame, period_sample : str) :

    init_vol = df.select("Volume").first()[0]
    init_Date = df.select("Date").first()[0]

    nb_samples = period_to_day(period_sample)
    dictio = {}
 
    for pos, row in enumerate(df.collect()):
        diff = row.Date - init_Date
        if abs(diff.days) >= nb_samples or pos +1 == len(df.collect()):
            dictio[row.Date] = np.round((row.Volume - init_vol), 3)
            init_vol = row.Volume
            init_Date = row.Date
    
    schema = StructType([
    StructField("Period", DateType(), True),
    StructField("Variation_Stock_Volume", FloatType(), True)
    ])
        
    data = [(k, float(v)) for k, v in dictio.items()]

    s_df = spark.createDataFrame(data, schema=schema)
        
    s_df.show()

    return s_df