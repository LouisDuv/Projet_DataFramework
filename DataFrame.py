from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from typing import List


class DataframeClass:
    
    def __init__(self):
        self.spark = SparkSession.builder.appName("StockAnalysis").config("spark.driver.host", "localhost").getOrCreate()
        self.dataframes = []
    
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
        df = df.withColumn("Date", col("Date").cast(DateType()))
        return df

    def read_multiple_csv(self, file_paths: List[str]) -> List[DataFrame]:
        self.dataframes = [self.read_csv(file_path) for file_path in file_paths]
        return self.dataframes

    def print_schemas(self):
        for idx, df in enumerate(self.dataframes):
            print(f"Schema of DataFrame {idx + 1}:")
            df.printSchema()

    def perform_operation_on_each(self, operation, *args):
        results = []
        for idx, df in enumerate(self.dataframes):
            result = operation(df, idx, *args)
            results.append(result)
        return results
