from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import countDistinct
from datetime import date
import datetime
from dateutil.relativedelta import relativedelta

import logging



def date_udf(start_dt, end_dt):
    date_list = []
    diff=(end_dt-start_dt).days
    i=0
    while(i<=diff):

        new_date = (start_dt + relativedelta(days=i)).strftime("%Y-%m-%d")

        date_list.append(new_date)
        i=i+1

    print(date_list)
    return date_list




def createSparkSession(app_name):
    spark = SparkSession.builder.appName(f"{app_name}").enableHiveSupport().getOrCreate()
    return spark

def generate_date_dim(df,spark):
    """
    This should not be part of daily ETL , we can schedule it every month/quarted/year as required
     date: Current date
     day: current day
     month: current month
     year: current year
     weekday: current weekday
    """

    spark.udf.register("date_generator",date_udf,ArrayType(StringType()))

    df_exploded=df.withColumn("date_arr",expr("date_generator(start_date,end_date) date_arr"))

    df_exploded=df_exploded.select(explode("date_arr").alias("dates"))
    df_exploded=df_exploded.select(to_date("dates").alias("dates"))

    fd=df_exploded.withColumn("day",dayofmonth("dates")).withColumn("month",month("dates")).withColumn("year",year("dates"))

    try:

        fd.write.format("parquet").mode("overwrite").saveAsTable("date_dim", mode="Overwrite")
    except Exception as e:

        logging.error("Error while writing to target")


def generate_date_dimension_load(start_year, increment):

    start_year=datetime.datetime.strptime(start_year, '%m-%d-%Y').date()

    end_year = start_year + relativedelta(years=increment)

    spark=createSparkSession("date_dimension generator")

    dl=[start_year,end_year]

    df=spark.createDataFrame([dl],schema=["start_date","end_date"])


    generate_date_dim(df,spark)


