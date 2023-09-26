from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

"""
Assumption- Here i have assumed that each day (after business hours we get a json file that contains specific information w.r.t the transaction. 
Each file each day, will contain transactions.
"""


def createSparkSession(app_name):

    spark = SparkSession.builder.appName(f"{app_name}").enableHiveSupport().getOrCreate()
    return spark

def read_raw_data(file_name,spark):
    #print(read_raw_data("sample_data.json").schema)  # to get schema of the json
    return spark.read.format("json").option("multiLine",True).load(f"{file_name}")

def add_current_date(df):
    #add current date and transaction date for later analysis and debugging
    return df.withColumn("load_date",current_date()).withColumn("transaction_date", to_date("timestamp"))

# Write to a table the raw data partitioned on transaction date-- This table can be in Azure as well . We should enable dynamic partitioning to
#make the pipeline idempotent

def start_ingestion():

    current_path = os.path.dirname(os.path.abspath(__file__))
    path_arr = current_path.split('\\')

    path_arr.pop()
    path_arr.append("data")

    fp="\\".join(path_arr)
    spark = createSparkSession("e-commerce-application")

    add_current_date(read_raw_data(f"{fp}\\sample_data.json",spark)).write.format("parquet").mode("overwrite")\
        .partitionBy("transaction_date").saveAsTable("raw",mode="Overwrite")

# we can change the mode later on and extract the unprocessed files only using an incremental data load with the timestamp column