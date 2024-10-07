import yfinance as yf
import pyspark as ps


aapl_data = yf.Ticker("AAPL")
historical_data = aapl_data.history(period = "1mo")

print(historical_data)



