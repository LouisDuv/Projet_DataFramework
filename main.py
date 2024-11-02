import glob
import os
import yfinance as yf

from pyspark.sql.functions import avg, month, year, day, lit
from pyspark.sql import SparkSession
import pyspark.pandas as ps

from script.DataFrame import DataframeClass

from script.business_analysis import avg_price, monthly_stock_variation, max_daily_return, return_rate, max_return_rate, avg_return
from script.exploration import values_correlation, period_btw_data, head_and_tail_40



#aapl = yf.Ticker("AAPL").history(period="5y")
#aapl.to_csv("apple.csv")
#ba = yf.Ticker("BA").history(period="5y")
#ba.to_csv("boeing.csv")
#f = yf.Ticker("F").history(period="5y")
#f.to_csv("ford.csv")
#pfe = yf.Ticker("PFE").history(period="5y")
#pfe.to_csv("pfizer.csv")
#meta = yf.Ticker("META").history(period="5y")
#meta.to_csv("meta.csv")
#am = yf.Ticker("AMZN").history(period="5y")
#am.to_csv("amazon.csv")
#mcrs = yf.Ticker("MSFT").history(period="5y")
#mcrs.to_csv("microsoft.csv")

spark = SparkSession.builder.appName("StockVariation").getOrCreate()

dataframe_obj = DataframeClass()

csv_folder_path = 'Stocks_Price'
csv_files = glob.glob(os.path.join(csv_folder_path, "*.csv"))

data_dfs = dataframe_obj.read_multiple_csv(csv_files)

result = avg_price(data_dfs[5] , "m", "Open")#dataframe_obj.perform_operation_on_each(monthly_avg_open_price)
