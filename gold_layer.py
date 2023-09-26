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


    print("Customer Dimension\n")
    spark.sql("""select * from customer_table""").show(truncate=False)

    print("Product Dimension\n")
    spark.sql("""select * from product_table""").show(truncate=False)

    print("Date Dimension\n")
    spark.sql("select * from date_dim").show(truncate=False)

    print("Sales fact \n")
    spark.sql("select * from sales_fact").show(truncate=False)

    print("revenue per day approach 1\n")
    spark.sql("""select * from revenue_per_day_approach1""").show(truncate=False)

    print("revenue per day approach 2\n")
    spark.sql("""select * from revenue_per_day_approach2""").show(truncate=False)

    print("revenue per product category approach1 \n")
    spark.sql("""select * from revenue_per_product_category_approach1""").show(truncate=False)

    print("revenue per product category approach2 \n")
    spark.sql("""select * from revenue_per_product_category_approach2""").show(truncate=False)

    print("customer purchase history\n")
    spark.sql("""select * from customer_history""").show(truncate=False)

