from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import countDistinct

import logging


logging.basicConfig(filename='pipeline.log')


def createSparkSession(app_name):

    spark = SparkSession.builder.appName(f"{app_name}").enableHiveSupport().getOrCreate()
    return spark

def calculate_revenue_per_day(raw_df):
    """
    Calculate the revenue for each day and store in fact
    """

    r1=raw_df.select("transaction_date",explode("products").alias("exploded_products"))

    r2=r1.select("transaction_date","exploded_products.*").selectExpr("transaction_date","quantity*unitPrice value")

    f_df=r2.groupBy("transaction_date").agg(sum("value").alias("revenue"))

    #f_df.show(truncate=False)
    try:

        f_df.write.format("parquet").mode("overwrite").saveAsTable("revenue_per_day",mode="Overwrite")
    except Exception as e:

        logging.error("Error while writing to target")



def calculate_revenue_per_product_category(raw_df):
    """
    Generate Revenue per product category and store in a fact
    """

    r1=raw_df.select(explode("products").alias("exploded_products"))

    r2=r1.select("exploded_products.*").selectExpr("quantity*unitPrice value","category")

    fdf=r2.groupBy("category").agg(sum("value").alias("revenue"))

    #fdf.show(truncate=False)
    try:
        fdf.write.format("parquet").mode("overwrite").saveAsTable("revenue_per_product_category",mode="Overwrite")
    except Exception as e:
        logging.error("Error while writing to target")

def last_purchase_date(raw_df):
    """
       Generate fact for customer history
       """

    customer_id_purchases=raw_df\
        .select("transactionID","customer.*").groupBy("customerID").agg(countDistinct("transactionId").alias("no_of_purchses"))

    customer_id_last_date=raw_df\
        .select("transaction_date","customer.*").groupBy("customerID").agg(min("transaction_date").alias("last_purchase_date"))

    fd=customer_id_purchases.join(customer_id_last_date,["customerId"],"inner").select('*')

    #fd.show(truncate=False)

    fd.write.format("parquet").mode("overwrite").saveAsTable("customer_history", mode="Overwrite")

# Below code is to an example of how we can generate a data model for the same..

def generate_customer_dimension(raw_df):
    """

    customerID: Unique identifier for the customer.
    firstName: First name of the customer.
    lastName: Last name of the customer.
    email: Email address of the customer.
    phoneNumber: Phone number of the customer.
    """

    c_df=raw_df.select("customer.*").select("customerId","email","firstName","lastname","phoneNumber")

    #c_df.show(truncate=False)
    try:

        c_df.write.format("parquet").mode("overwrite").saveAsTable("customer_table", mode="Overwrite")
    except Exception as e:
        logging.error("Error while writing to target")


def generate_product_dimension(raw_df):
        """

        "productID": Unique identifier for the product
        "productName" : Name of the product
        "category": Category of the product
        "Unit Price" : Price per unit of the product
        """

        p_df=raw_df.select(explode("products").alias("exploded_prod")).\
        select("exploded_prod.*").select("productId","productName","category","unitPrice").distinct()

        #p_df.show(truncate=False)
        try:

            p_df.write.format("parquet").mode("overwrite").saveAsTable("product_table", mode="Overwrite")

        except Exception as e:
            logging.error("Error while writing to target")



"""Similar to these facts and dimensions we can generate other tables that can answer the following questions:
a) Average order value each day
b) Total Revenue per customer
"""



def start_transformation():
    spark=createSparkSession("transform")

    raw_df=spark.sql("select * from raw")


    calculate_revenue_per_day(raw_df)


    calculate_revenue_per_product_category(raw_df)

    last_purchase_date(raw_df)

    generate_customer_dimension(raw_df)

    generate_product_dimension(raw_df)
