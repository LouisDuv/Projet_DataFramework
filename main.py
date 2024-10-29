import glob
import os

from pyspark.sql.functions import avg, month, year, day, lit
from pyspark.sql import SparkSession

from script.DataFrame import DataframeClass

from script.business_analysis import monthly_avg_price


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

spark = SparkSession.builder.appName("StockVariation").getOrCreate()

dataframe_obj = DataframeClass()

csv_folder_path = 'Stocks_Price'
csv_files = glob.glob(os.path.join(csv_folder_path, "*.csv"))

data_dfs = dataframe_obj.read_multiple_csv(csv_files)

result = monthly_avg_price(data_dfs[3], "Open")#dataframe_obj.perform_operation_on_each(monthly_avg_open_price)

print(result)
