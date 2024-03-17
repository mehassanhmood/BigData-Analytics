import fmpsdk as fmp
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import datetime
from pyspark.sql import SparkSession

load_dotenv()
fmp_key = os.getenv('fmp_key')
def stock_news():
    spark = SparkSession.builder.appName("stock_news") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.mongodb.output.uri","mongodb+srv://mehassan:passwordmongo@cluster0.pr54ed8.mongodb.net/big_data.abc") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()


    stock_symbols = [
        'AAPL', 'MSFT', 'NVDA', 'META', 'AMZN', 'TSLA', 'GOOGL',
        'DBD', 'DSGX', 'GTLB', 'LOGI', 'CRSR',
        'LNG', 'SWN', 'APA', 'BTU', 'CL',
        'BMY', 'THC', 'TNDM',
        'MOS', 'AXTA', 'KOP',
        'SBLK', 'EME', 'DNOW',
    ]


    news = fmp.stock_news(apikey=fmp_key,tickers=stock_symbols,limit=30)

    sp = spark.createDataFrame(news)

    sp.write.format("mongo") \
    .option("spark.mongodb.output.uri","mongodb+srv://mehassan:passwordmongo@cluster0.pr54ed8.mongodb.net/big_data.stock_news") \
    .save()

    spark.stop()