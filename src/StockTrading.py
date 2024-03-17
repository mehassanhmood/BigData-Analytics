import yfinance as yf
import pandas as pd
import numpy as np
import requests
import os
from pyspark.sql import SparkSession


def stock_trading():
    url = "jdbc:postgresql://localhost:5432/postgres"

    spark = SparkSession.builder.appName("cryptoData") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()


    stock_symbols = [
        'AAPL', 'MSFT', 'NVDA', 'META', 'AMZN', 'TSLA', 'GOOGL',
        'DBD', 'DSGX', 'GTLB', 'LOGI', 'CRSR',
        'LNG', 'SWN', 'APA', 'BTU', 'CL',
        'BMY', 'THC', 'TNDM',
        'MOS', 'AXTA', 'KOP',
        'SBLK', 'EME', 'DNOW',
    ]

    data = {}
    for i in stock_symbols:
        df = yf.download(i)
        df = df.reset_index()
        data[i] = df

    for i in stock_symbols:
        spark_df = spark.createDataFrame(data[i])
        spark_df.write \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", i) \
            .option("user", "postgres") \
            .option("password", "password") \
            .option("driver","org.postgresql.Driver") \
            .save()
        

    spark.stop()