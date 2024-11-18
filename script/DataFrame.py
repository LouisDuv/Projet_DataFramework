import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

import numpy as np
import glob

from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType, NumericType
from pyspark.sql import DataFrame
from pyspark.sql import Window
from typing import List
from datetime import date

from collections import Counter

class DataframeClass:

    def __init__(self, spark):
        self.spark = spark
        self.dataframes = []
        self.datasets_names = []

    def stock_chosen(self):
      print("Which stock do you want to analyze ? Give a number among the following list:\n")
      for pos, i in enumerate(self.datasets_names) :
        print(f"{pos} - {i}\n")
      stock = input("Select a stock: ")
      return int(stock)

    def read_csv(self, file_path: str) -> DataFrame:
        schema = StructType([
            StructField("Date", StringType(), True),
            StructField("Open", FloatType(), True),
            StructField("High", FloatType(), True),
            StructField("Low", FloatType(), True),
            StructField("Close", FloatType(), True),
            StructField("Volume", FloatType(), True),
            StructField("Dividends", FloatType(), True),
            StructField("Stock Splits", FloatType(), True)
        ])
        df = self.spark.read.csv(file_path, header=True, schema=schema)
        df = df.withColumn("Date", sf.col("Date").cast(DateType()))
        return df

    def read_multiple_csv(self, file_paths: List[str]) -> List[DataFrame]:
        self.dataframes = [self.read_csv(file_path) for file_path in file_paths]
        self.datasets_names = [dataset_name[13:-4] for dataset_name in file_paths]
        return self.dataframes

    def perform_operation(self, operation, *args):
        idx_dataset = self.stock_chosen()
        result = operation(self.dataframes[idx_dataset], *args)
        return result