import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder.appName("crypt_cot") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()

data_base_crypto = "new_crypto_data"
data_base_cot = "cot_report"
table = "btc_data"
cot_btc_table = "BT"

mssql_url = f"jdbc:sqlserver://ZAHRA\SQLEXPRESS:61254;database={data_base_crypto};trustServerCertificate=true;encrypt=true"
mssql_cot_url = f"jdbc:sqlserver://ZAHRA\SQLEXPRESS:61254;database={data_base_cot};trustServerCertificate=true;encrypt=true"


cot_data = spark.read.format("jdbc") \
    .option("url",mssql_cot_url) \
    .option("user","mehassan") \
    .option("password",'password') \
    .option("dbtable",cot_btc_table) \
    .load()


cot_data_preprocessed = cot_data.select("date","comm_spread")
cot_data_preprocessed = cot_data_preprocessed.withColumnRenamed("comm_spread","btc_com_spread")


crypto_data = spark.read.format("jdbc") \
    .option("url",mssql_url) \
    .option("user","mehassan") \
    .option("password",'password') \
    .option("dbtable",table) \
    .load()

crypto_data_refined = crypto_data.select("date","close")
crypto_data_refined = crypto_data_refined.withColumnRenamed("close","BTC")

crypto_data_refined = crypto_data_refined.withColumn("date",col("date").cast("timestamp"))

processed_crypto_data = crypto_data_refined.join(cot_data_preprocessed,on ="date",how="left")


# code to write the data in the sql express server