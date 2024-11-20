from pyspark.sql import functions as sf
from pyspark.sql.types import DateType, NumericType
from pyspark.sql import DataFrame
from pyspark.sql import Window
import datetime

import streamlit as st


class FinancialDataAnalyzer:
    
    @st.cache_data
    def spark_to_pandas(_self, _spark_df: DataFrame, *args):
        return _spark_df.toPandas()


    def head_and_tail_40(self, df: DataFrame, *args):
        head_40 = df.limit(40)
        tail_40 = df.orderBy("Date", ascending=False).limit(40).orderBy("Date", ascending=True)
        result = head_40.union(tail_40)
        # print("Dataframe, first and last 40 rows:")
        # result.show(80)
        return self.spark_to_pandas(result, args[0], "head_and_tail")


    def num_observations(self, df: DataFrame, *args):
        result = df.count()
        return result
    
    
    def period_btw_data(self, df: DataFrame, *args):
        init_date = df.select(sf.min("Date")).collect()[0]
        last_date = df.select(sf.max("Date")).collect()[0]
        diff = last_date[0] - init_date[0]
        return diff.days


    def descript_stats(self, df: DataFrame, *args):
        agg_expr = []
        for column in df.columns:
            agg_expr.append(sf.min(column).alias(f"min_{column}"))
            agg_expr.append(sf.max(column).alias(f"max_{column}"))
            if isinstance(df.schema[column].dataType, NumericType):
                agg_expr.append(sf.stddev(column).alias(f"stddev_{column}"))
        result = df.agg(*agg_expr)
        return self.spark_to_pandas(result, args[0], "descriptive_stats")


    def count_missing(self, df: DataFrame, *args):
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
        return self.spark_to_pandas(result, args[0], "count_missing_values")


    def values_correlation(self, df: DataFrame, col1: str, col2: str, *args):
        correlation = df.stat.corr(col1, col2)
        #interesting: correlation between (close-open) and volume
        return correlation


    def ask_interval_time():
        date_format = '%Y-%m-%d'
        print("Define the interval of time you want to analyze (format : YYYY-MM-DD)\n")
        
        from_date = input("Start at :")
        from_date = datetime.strptime(from_date, date_format)
        
        to_date = input("End at :")
        to_date = datetime.strptime(to_date, date_format)
        
        return (from_date, to_date)


    def avg_price_until(df : DataFrame, until : DateType, field : str) -> DataFrame:
        return df.filter(sf.col('Date') <= until).agg(sf.avg(field).alias(f"Average_{field}_Price_($)"))

    # Input : period -> w for weekly, y for yearly, m for monthly 
    # str -> Open ou Close
    # Output : DataFrame (Average_STR_Price($))

    def avg_price_period(self, df : DataFrame, field: str, period: str, *args) -> DataFrame:
        
        df_week = df.withColumn('week_nb', sf.weekofyear('Date'))
        df_month = df_week.withColumn('month_nb', sf.month('Date'))
        df_year =  df_month.withColumn('year', sf.year('Date'))

        if period == "week" :
            df_grouped = df_year.groupBy(['week_nb', 'year']).agg(sf.avg(field).alias(f"{field}_Average_Price")).orderBy(sf.col('year').desc(), sf.col("week_nb").desc())
        elif period == "month" :
            df_grouped = df_year.groupBy(['month_nb', 'year']).agg(sf.avg(field).alias(f"{field}_Average_Price")).orderBy(sf.col('year').desc(), sf.col("month_nb").desc())
        elif period == "year" :
            df_grouped = df_year.groupBy('year').agg(sf.avg(field).alias(f"{field}_Average_Price")).orderBy(sf.col("year").desc())
        else : 
            return -1
        
        result = df_grouped.fillna(0)
        return self.spark_to_pandas(result, args[0], "period_price_average", field, period)


    def stock_variation(self, df : DataFrame, period : str, *args) -> DataFrame :
        
        df = df.orderBy(sf.col('Date').desc())
        window = Window.orderBy('Date')

        df_week = df.withColumn('week_nb', sf.weekofyear('Date'))
        df_month = df_week.withColumn('month_nb', sf.month('Date'))
        df_year =  df_month.withColumn('year', sf.year('Date'))

        if period == 'day' :

            df_next = df.withColumn('next', sf.lag('Open').over(window))
            df_variation = df_next.withColumn('Price_Variation', sf.col('Open') - sf.col('next')).select('Date', 'Price_Variation').orderBy(sf.col("Date").desc())
        
        elif period == 'month':

            df_drop = df_year.dropDuplicates(["month_nb", "year"]).select(["Open", 'Date', 'month_nb', 'year']).orderBy(sf.col('Date').desc())
            df_next = df_drop.withColumn('next', sf.lag('Open').over(window))
            df_variation = df_next.withColumn('Price_Variation', sf.col('Open') - sf.col('next')).select('Date', 'Price_Variation').orderBy(sf.col("Date").desc())

        elif period == 'year' :
            
            df_drop = df_year.dropDuplicates(["year"]).select(["Open", 'Date','year']).orderBy(sf.col('Date').desc())
            df_next = df_drop.withColumn('next', sf.lag('Open').over(window))
            df_variation = df_next.withColumn('Price_Variation', sf.col('Open') - sf.col('next')).select('Date', 'Price_Variation').orderBy(sf.col("Date").desc())

        result = df_variation.fillna(0)
        return self.spark_to_pandas(result, args[0], "stock_price_variation", period)


    def stock_variation_until(self, df : DataFrame, period : str) -> DataFrame:
        on, until = self.ask_interval_time()
        filtered_df = df.filter((sf.col('Date') >= on) & (sf.col('Date') <= until))
        return self.stock_variation(filtered_df, period)


    # Computation of the Rate of the return on a daily interval
    def return_computation(self, init_val, current_val):
        return ((current_val - init_val)/init_val)*100


    def period_return(self, df : DataFrame, period : str, *args):
        df = df.orderBy(sf.col('Date').desc())
        window = Window.orderBy('Date')

        if period == 'day' :
            
            dreturn_df = df.withColumn('Daily_Return', self.return_computation(sf.col('Open'), sf.col('Close')))

        elif period == 'week': 
            week_df =  df.withColumn('week_nb', sf.weekofyear('Date'))
            year_df = week_df.withColumn('year_nb', sf.year('Date'))

            drop_df = year_df.dropDuplicates(['week_nb', 'year_nb']).select(['Date', 'Open', 'Close', 'week_nb', 'year_nb']).orderBy(sf.col('Date').desc())
            window_df = drop_df.withColumn('Close(W+1)', sf.lead('Close').over(window))
            dreturn_df = window_df.withColumn('Weekly_Return', self.return_computation(sf.col('Open'), sf.col('Close(W+1)'))).select(['Date', 'Open', 'Close(W+1)', 'Weekly_Return']).orderBy(sf.col('Date').desc())
            
        elif period == 'month' :
            month_df =  df.withColumn('month_nb', sf.month('Date'))
            year_df = month_df.withColumn('year_nb', sf.year('Date'))

            drop_df = year_df.dropDuplicates(['month_nb', 'year_nb']).select(['Date', 'Open', 'Close', 'month_nb', 'year_nb']).orderBy(sf.col('Date').desc())
            window_df = drop_df.withColumn('Close(M+1)', sf.lead('Close').over(window))
            dreturn_df = window_df.withColumn('Monthly_Return', self.return_computation(sf.col('Open'), sf.col('Close(M+1)'))).select(['Date', 'Open', 'Close(M+1)', 'Monthly_Return']).orderBy(sf.col('Date').desc())

        elif period == 'year':

            year_df = df.withColumn('year_nb', sf.year('Date'))

            drop_df = year_df.dropDuplicates(['year_nb']).select(['Date', 'Open', 'Close', 'year_nb']).orderBy(sf.col('Date').desc())
            window_df = drop_df.withColumn('Close(Y+1)', sf.lead('Close').over(window))
            dreturn_df = window_df.withColumn('Yearly_Return', self.return_computation(sf.col('Open'), sf.col('Close(Y+1)'))).select(['Date', 'Open', 'Close(Y+1)', 'Yearly_Return']).orderBy(sf.col('Date').desc())

        result = dreturn_df.fillna(0)
        return self.spark_to_pandas(result, args[0], "stock_price_variation", period) 
    
    
    
    
    
    # A refaire pour prendre les highly return de chaque stock
    def max_return(self, df : DataFrame, period : str):

        dreturn_df = self.period_return(df, period)
        max_dreturn = dreturn_df.select(sf.max("Daily_Return")).collect()[0][0]

        return max_dreturn 







    def period_return_until(self, df : DataFrame, on : DateType, until : DateType, period : str) -> DataFrame:

        filtered_df = df.filter((sf.col('Date')>= on) & (sf.col('Date') <= until))
        return self.period_return(filtered_df, period)


    def avg_return(self, df : DataFrame, period : str):

        dreturn_df = self.period_return(df, 'd').orderBy(sf.col('Date').desc())
        dreturn_df.printSchema()
        
        if period == 'w':
            title = 'Weekly'

            period_df =  dreturn_df.withColumn('week_nb', sf.weekofyear('Date'))
            year_df =  period_df.withColumn('year_nb', sf.year('Date'))
            df_grouped = year_df.groupBy(['week_nb', 'year_nb']).agg(sf.avg("Daily_Return").alias(f"{title}_Return")).orderBy(sf.col('year_nb').desc(), sf.col('week_nb').desc())

        elif period == 'm':
            title = 'Monthly'

            period_df = dreturn_df.withColumn('month_nb', sf.month('Date'))
            year_df =  period_df.withColumn('year_nb', sf.year('Date'))
            df_grouped = year_df.groupBy(['month_nb', 'year_nb']).agg(sf.avg('Daily_Return').alias(f"{title}_Return")).orderBy(sf.col('year_nb').desc(), sf.col('month_nb').desc())

        elif period == 'y' : 
            title = 'Yearly'
    
            period_df = dreturn_df.withColumn('year_nb', sf.year('Date'))
            df_grouped = period_df.groupBy('year_nb').agg(sf.avg('Daily_Return').alias(f"{title}_Return")).orderBy(sf.col('year_nb').desc())

        return df_grouped.fillna(0)


    def moving_average(self, df : DataFrame, field : str, nb_sample : int, *args):

        window = Window.orderBy('Date').rowsBetween(0, nb_sample-1)
        mov_df = df.withColumn('Moving_Average', sf.avg(field).over(window))
        result = mov_df.fillna(0)
        return self.spark_to_pandas(result, args[0], "stock_price_variation", field, nb_sample) 


    def correlation_btw_stocks(self, df1 : DataFrame, df2 : DataFrame, col1: str, col2 : str, *args):

        window_1 = Window.orderBy('Date_1')
        window_2 = Window.orderBy('Date_2')

        df1 = df1.select([sf.col(col).alias(f"{col}_1") for col in df1.columns])
        df2 = df2.select([sf.col(col).alias(f"{col}_2") for col in df2.columns])

        index_df1 = df1.withColumn('index', sf.row_number().over(window_1))
        index_df2 = df2.withColumn('index', sf.row_number().over(window_2))

        joined_df = index_df1.join(index_df2, on="index", how="inner")

        return self.values_correlation(joined_df, col1+'_1', col2+'_2')


    def rrate_computation(open_price, final_price, nb_stock):
        init_val = open_price*nb_stock
        final_val = final_price*nb_stock
        return ((final_val-init_val)/init_val)*100


    def return_rate(self, df: DataFrame, on: DateType , until: DateType, nb_stock: int):
        sub_df = df.filter((sf.col('Date') >= on) & (sf.col('Date') <= until))
        rrate_df = sub_df.withColumn('Return_Rate(%)', self.rrate_computation(sf.col('Open'), sf.col('Close'), nb_stock))
        return rrate_df

    # Bénéfice Maximum sur une période donnée
    # Retourne un Spark DataFrame

    def max_return_rate(self, df: DataFrame, on: DateType , until: DateType, nb_stock: int):
        s_df = self.return_rate(df, on, until, nb_stock)
        return s_df.select(sf.max("Return_Rate(%)")).collect()[0][0]


    def dividend_return(df : DataFrame, stocks_own : float):

        s_df_revenues = df.withColumn("Revenues", sf.col("Dividends") * stocks_own)

        s_df_revenues.show()

        return s_df_revenues

