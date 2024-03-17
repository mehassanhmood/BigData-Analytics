import yfinance as yf
import pandas as pd
import numpy as np
import requests
from pyspark.sql import SparkSession
import yaml



def stock_trading():

    with open("../conf.yaml", "r") as conf:
        config = yaml.safe_load(conf)

    host = config["postgres"]["host"]
    port = config["postgres"]["port"]
    password = config["postgres"]["password"]
    database = config["postgres"]["database"]
    user = config["postgres"]["user"]
    driver = config["postgres"]["driver"]
    url = f"jdbc:postgresql://{host}:{port}/{database}"

    spark = SparkSession.builder.appName("stockTrading") \
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
            .option("user", user) \
            .option("password", password) \
            .option("driver",driver) \
            .save()
        

    spark.stop()