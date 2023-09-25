from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import countDistinct


def createSparkSession(app_name):

    spark = SparkSession.builder.appName(f"{app_name}").enableHiveSupport().getOrCreate()
    return spark


def display_gold_tables():
    spark=createSparkSession("load")

    print("revenue per day\n")
    spark.sql("""select * from revenue_per_day""").show(truncate=False)

    print("revenue per product category \n")
    spark.sql("""select * from revenue_per_product_category""").show(truncate=False)

    print("customer purchase history\n")
    spark.sql("""select * from customer_history""").show(truncate=False)

