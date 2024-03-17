import fmpsdk as fmp
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.appName("stock_news") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.jars", "./mysql-connector-j-8.3.0.jar") \
    .getOrCreate()
stock_symbols = [
    'AAPL', 'MSFT', 'NVDA', 'META', 'AMZN', 'TSLA', 'GOOGL',
    'DBD', 'DSGX', 'GTLB', 'LOGI', 'CRSR',
    'LNG', 'SWN', 'APA', 'BTU', 'CL',
    'BMY', 'THC', 'TNDM',
    'MOS', 'AXTA', 'KOP',
    'SBLK', 'EME', 'DNOW',
]
workbench_url = "jdbc:mysql://13.92.123.83:3306/finance_out?permitMysqlSchema"
output_url = "jdbc:mysql://localhost:3306/big_data?permitMysqlSchema"
driver = "com.mysql.jdbc.Driver"

news_data = spark.read.format("jdbc") \
    .option("url",workbench_url) \
    .option("user","mysql_user") \
    .option("password",'mu!tFL^fh5kE#6z8"4dyBs') \
    .option("dbtable","historical_with_sentiment") \
    .option("driver",driver) \
    .load()

stocks = news_data.select("symbol").distinct().rdd.flatMap(lambda x : x).collect()

individual_data = {}
try :
    for i in stock_symbols:
        individual_data[i] = news_data.filter(news_data["symbol"] == i)
        individual_data[i] = individual_data[i].select("date","Close","sentiment")
        individual_data[i] = individual_data[i].withColumnRenamed("Close",i)
        if individual_data[i] != "XOM" or "ON" or "CVX":
            individual_data[i].write.format("jdbc") \
                .option("url",output_url) \
                .option("driver",driver) \
                .option("dbtable",f"{i}") \
                .option("user","root") \
                .option("password","password") \
                .save()
            print(i)
except AnalysisException as e:
    print(e)


