import numpy as np

from datetime import timedelta
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
from pyspark.sql import Window

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from script.exploration import values_correlation
from script.pages.homepage import *

from datetime import datetime

spark = SparkSession.builder.appName("StockVariation").getOrCreate()

# Input : Date until the result
# str -> Open ou Close
# Output : DataFrame (Average_STR_Price($))

#PLOT : BOX PLOT (LINEAR PLOT SINON MAIS BOX PLOT EST ORIGINAL !)

def avg_price_until(df : DataFrame, until : DateType, field : str) -> DataFrame:
    return df.filter(F.col('Date') <= until).agg(F.avg(field).alias(f"Average_{field}_Price_($)"))

# Input : period -> w for weekly, y for yearly, m for monthly 
# str -> Open ou Close
# Output : DataFrame (Average_STR_Price($))

#PLOT : BOX PLOT (LINEAR PLOT SINON MAIS BOX PLOT EST ORIGINAL !)
def avg_price_period(df : DataFrame, period : str, field : str) -> DataFrame:
    
    df_week = df.withColumn('week_nb', F.weekofyear('Date'))
    df_month = df_week.withColumn('month_nb', F.month('Date'))
    df_year =  df_month.withColumn('year', F.year('Date'))

    if period == "w" :
        df_grouped = df_year.groupBy(['week_nb', 'year']).agg(F.avg(field).alias("Average_Price")).orderBy(F.col('year').desc(), F.col("week_nb").desc())
    elif period == "m" :
        df_grouped = df_year.groupBy(['month_nb', 'year']).agg(F.avg(field).alias("Average_Price")).orderBy(F.col('year').desc(), F.col("month_nb").desc())
    elif period == "y" :
        df_grouped = df_year.groupBy('year').agg(F.avg(field).alias("Average_Price")).orderBy(F.col("year").desc())
    else : 
        return -1

    return df_grouped.fillna(0)
        
# Mesure des variations pour chaque mois d'une colonne donnée
# str : "Volume", "Open", "Close"
# Variation : DERNIER JOUR DU MOIS CLOS - PREMIER JOUR DU MOIS OPEN
# Return: spark dataframe


# PLOT : LINEAR PLOT
def stock_variation(df : DataFrame, period : str) -> DataFrame :
    
    df = df.orderBy(F.col('Date').desc())
    window = Window.orderBy('Date')

    df_week = df.withColumn('week_nb', F.weekofyear('Date'))
    df_month = df_week.withColumn('month_nb', F.month('Date'))
    df_year =  df_month.withColumn('year', F.year('Date'))

    if period == 'd' :

        df_next = df.withColumn('next', F.lag('Open').over(window))
        df_variation = df_next.withColumn('Variation_Open_Price', F.col('Open') - F.col('next')).select('Date', 'Variation_Open_Price').orderBy(F.col("Date").desc())
    
    elif period == 'm':

        df_drop = df_year.dropDuplicates(["month_nb", "year"]).select(["Open", 'Date', 'month_nb', 'year']).orderBy(F.col('Date').desc())
        df_next = df_drop.withColumn('next', F.lag('Open').over(window))
        df_variation = df_next.withColumn('Variation_Open_Price', F.col('Open') - F.col('next')).select('Date', 'Variation_Open_Price').orderBy(F.col("Date").desc())

    elif period == 'y' :
        
        df_drop = df_year.dropDuplicates(["year"]).select(["Open", 'Date','year']).orderBy(F.col('Date').desc())
        df_next = df_drop.withColumn('next', F.lag('Open').over(window))
        df_variation = df_next.withColumn('Variation_Open_Price', F.col('Open') - F.col('next')).select('Date', 'Variation_Open_Price').orderBy(F.col("Date").desc())

    return df_variation.fillna(0)


# PLOT : LINEAR PLOT
def stock_variation_until(df : DataFrame, on : DateType, until : DateType, period : str) -> DataFrame:

    filtered_df = df.filter((F.col('Date') >= on) & (F.col('Date') <= until))

    return stock_variation(filtered_df, period)

# Computation of the Rate of the return on a daily interval
# No plot
def return_computation(init_val, current_val):
    return ((current_val - init_val)/init_val)*100


# Mesure le benefice max sur d'un DF
#Retourn un dataframe spark

# PAS DE PLOT, METTRE DANS UNE CASE INFORMATIVE => fonction qui ressort un int 
def max_return(df : DataFrame, period : str):

    dreturn_df = period_return(df, period)
    max_dreturn = dreturn_df.select(F.max("Daily_Return")).collect()[0][0]

    return max_dreturn

# Determine la moyenne des rentabilites des actions sur une période donnée (rentabilité : (close-open/open)*100)
# period : w pour week, m pour month, y pour year
# Return un dataframe spark
# Plot associé : Linear Chart

def period_return(df : DataFrame, period : str):
    
    df = df.orderBy(F.col('Date').desc())
    window = Window.orderBy('Date')

    if period == 'd' :
        
        dreturn_df = df.withColumn('Daily_Return', return_computation(F.col('Open'), F.col('Close')))

    elif period == 'w': 
        week_df =  df.withColumn('week_nb', F.weekofyear('Date'))
        year_df = week_df.withColumn('year_nb', F.year('Date'))

        drop_df = year_df.dropDuplicates(['week_nb', 'year_nb']).select(['Date', 'Open', 'Close', 'week_nb', 'year_nb']).orderBy(F.col('Date').desc())
        window_df = drop_df.withColumn('Close(W+1)', F.lead('Close').over(window))
        dreturn_df = window_df.withColumn('Weekly_Return', return_computation(F.col('Open'), F.col('Close(W+1)'))).select(['Date', 'Open', 'Close(W+1)', 'Weekly_Return']).orderBy(F.col('Date').desc())
        
    elif period == 'm' :
        month_df =  df.withColumn('month_nb', F.month('Date'))
        year_df = month_df.withColumn('year_nb', F.year('Date'))

        drop_df = year_df.dropDuplicates(['month_nb', 'year_nb']).select(['Date', 'Open', 'Close', 'month_nb', 'year_nb']).orderBy(F.col('Date').desc())
        window_df = drop_df.withColumn('Close(M+1)', F.lead('Close').over(window))
        dreturn_df = window_df.withColumn('Monthly_Return', return_computation(F.col('Open'), F.col('Close(M+1)'))).select(['Date', 'Open', 'Close(M+1)', 'Monthly_Return']).orderBy(F.col('Date').desc())

    elif period == 'y':

        year_df = df.withColumn('year_nb', F.year('Date'))

        drop_df = year_df.dropDuplicates(['year_nb']).select(['Date', 'Open', 'Close', 'year_nb']).orderBy(F.col('Date').desc())
        window_df = drop_df.withColumn('Close(Y+1)', F.lead('Close').over(window))
        dreturn_df = window_df.withColumn('Yearly_Return', return_computation(F.col('Open'), F.col('Close(Y+1)'))).select(['Date', 'Open', 'Close(Y+1)', 'Yearly_Return']).orderBy(F.col('Date').desc())


    return dreturn_df.fillna(0)     

def period_return_until(df : DataFrame, on : DateType, until : DateType, period : str) -> DataFrame:

    filtered_df = df.filter((F.col('Date')>= on) & (F.col('Date') <= until))

    return period_return(filtered_df, period)

# LINEAR PLOT
def avg_return(df : DataFrame, period : str):

    dreturn_df = period_return(df, 'd').orderBy(F.col('Date').desc())
    dreturn_df.printSchema()
    
    if period == 'w':
        title = 'Weekly'

        period_df =  dreturn_df.withColumn('week_nb', F.weekofyear('Date'))
        year_df =  period_df.withColumn('year_nb', F.year('Date'))
        df_grouped = year_df.groupBy(['week_nb', 'year_nb']).agg(F.avg("Daily_Return").alias(f"{title}_Return")).orderBy(F.col('year_nb').desc(), F.col('week_nb').desc())

    elif period == 'm':
        title = 'Monthly'

        period_df = dreturn_df.withColumn('month_nb', F.month('Date'))
        year_df =  period_df.withColumn('year_nb', F.year('Date'))
        df_grouped = year_df.groupBy(['month_nb', 'year_nb']).agg(F.avg('Daily_Return').alias(f"{title}_Return")).orderBy(F.col('year_nb').desc(), F.col('month_nb').desc())

    elif period == 'y' : 
        title = 'Yearly'
 
        period_df = dreturn_df.withColumn('year_nb', F.year('Date'))
        df_grouped = period_df.groupBy('year_nb').agg(F.avg('Daily_Return').alias(f"{title}_Return")).orderBy(F.col('year_nb').desc())

    return df_grouped.fillna(0)


# Input : df, colonne à examiner (Open, Close, etc), nombre de données à moyenner sur [Date ; Date + nb_sample]
# Output : RIEN

#LINEAR PLOT
def moving_average(df : DataFrame, field : str, nb_sample : int):

    window = Window.orderBy('Date').rowsBetween(0, nb_sample-1)

    mov_df = df.withColumn('Moving_Average', F.avg(field).over(window))

    return mov_df.fillna(0)

 
# Input : df, period [w, m, y], Purchase_Cost
# Output : Spark df [Period, Return Rate]
# Return Rate : [[Close_Benefice - Open_Cost]/Open_Cost] * 100

def rrate_computation(open_price, final_price, nb_stock):
    init_val = open_price*nb_stock
    final_val = final_price*nb_stock
    return ((final_val-init_val)/init_val)*100


# LINEAR PLOT
def return_rate(df : DataFrame, on : DateType , until : DateType, nb_stock : int) :
    
    sub_df = df.filter((F.col('Date') >= on) & (F.col('Date') <= until))
    rrate_df = sub_df.withColumn('Return_Rate(%)', rrate_computation(F.col('Open'), F.col('Close'), nb_stock))

    return rrate_df

# Bénéfice Maximum sur une période donnée
# Retourne un Spark DataFrame

# PEUT ETRE COMPARER LES MAX RETURN RATE DES ENTREPRISES EN BAR CHART ? 
def max_return_rate(df : DataFrame, on : DateType , until : DateType, nb_stock : int):
    
    s_df = return_rate(df, on, until, nb_stock)

    return s_df.select(F.max("Return_Rate(%)")).collect()[0][0]


#PLOT : SCATTER PLOT

def correlation_btw_stocks(df1 : DataFrame, df2 : DataFrame, col1: str, col2 : str):

    window_1 = Window.orderBy('Date_1')
    window_2 = Window.orderBy('Date_2')

    df1 = df1.select([F.col(col).alias(f"{col}_1") for col in df1.columns])
    df2 = df2.select([F.col(col).alias(f"{col}_2") for col in df2.columns])

    index_df1 = df1.withColumn('index', F.row_number().over(window_1))
    index_df2 = df2.withColumn('index', F.row_number().over(window_2))

    joined_df = index_df1.join(index_df2, on="index", how="inner")

    return values_correlation(joined_df, col1+'_1', col2+'_2')


# Input : df, nombres d'actions détenues
# Output : Spark Dataframe avec col Revenus

def dividend_return(df : DataFrame, stocks_own : float):

    s_df_revenues = df.withColumn("Revenues", F.col("Dividends") * stocks_own)

    s_df_revenues.show()

    return s_df_revenues

