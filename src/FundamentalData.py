import pandas as pd
import numpy as np
import requests
import os
from dotenv import load_dotenv
import fmpsdk as fmp
from pyspark.sql import SparkSession

load_dotenv()
fmp_key = os.getenv("fmp_key")
def fundamental_data():
    try :
        spark = SparkSession.builder.appName("fin-funData").config("spark.driver.bindAddress", "0.0.0.0").getOrCreate()
        print("Connection Established")
    except ConnectionError as e:
        print(e)
        print("Spark cannot connect to a port!.")

    stock_symbols = [
        'AAPL', 'MSFT', 'NVDA', 'META', 'AMZN', 'TSLA', 'GOOGL',
        'DBD', 'DSGX', 'GTLB', 'LOGI', 'CRSR',
        'LNG', 'SWN', 'APA', 'BTU', 'CL',
        'BMY', 'THC', 'TNDM',
        'MOS', 'AXTA', 'KOP',
        'SBLK', 'EME', 'DNOW',
    ]

    try :
        data = {}
        for i in stock_symbols:
            com_income_statement = pd.DataFrame(fmp.income_statement(apikey=fmp_key, symbol=i, limit=80, period="quarter"))
            data[i] = com_income_statement
        print("Data Extracted Successfully from the API.")
    except :
        print("Could not retrieve data from the api.")

    tickers = list(data.keys())

    try :
        for i in tickers:
            df = spark.createDataFrame(data[i])
            df.write \
            .format("jdbc") \
            .option("url","jdbc:sqlserver://ZAHRA\SQLEXPRESS:61254;database=new_stock_fundamentals;trustServerCertificate=true;encrypt=true") \
            .option("dbtable",f"{i}") \
            .option("user","mehassan") \
            .option("password","password") \
            .save()
        print("Data Uploaded to the SQL server successfully.")
    except :
        print("Error while populating the database.")


    spark.stop()
