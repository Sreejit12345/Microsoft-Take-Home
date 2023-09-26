from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import countDistinct
from generate_date_dimension import *
import logging


logging.basicConfig(filename='pipeline.log')


def createSparkSession(app_name):

    spark = SparkSession.builder.appName(f"{app_name}").enableHiveSupport().getOrCreate()
    return spark

def calculate_revenue_per_day(raw_df):
    """
    Calculate the revenue for each day (Adhoc table)
    """

    r1=raw_df.select("transaction_date",explode("products").alias("exploded_products"))

    r2=r1.select("transaction_date","exploded_products.*").selectExpr("transaction_date","quantity*unitPrice value")

    f_df=r2.groupBy("transaction_date").agg(sum("value").alias("revenue"))

    #f_df.show(truncate=False)
    try:

        f_df.write.format("parquet").mode("overwrite").saveAsTable("revenue_per_day_approach1",mode="Overwrite")
    except Exception as e:

        logging.error("Error while writing to target")



def calculate_revenue_per_product_category(raw_df):
    """
    Generate Revenue per product category (Adhoc Table)
    """

    r1=raw_df.select(explode("products").alias("exploded_products"))

    r2=r1.select("exploded_products.*").selectExpr("quantity*unitPrice value","category")

    fdf=r2.groupBy("category").agg(sum("value").alias("revenue"))

    #fdf.show(truncate=False)
    try:
        fdf.write.format("parquet").mode("overwrite").saveAsTable("revenue_per_product_category_approach1",mode="Overwrite")
    except Exception as e:
        logging.error("Error while writing to target")

def last_purchase_date(raw_df):
    """
       Generate fact for customer history (Adhoc Table)
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


def generate_sales_fact(raw_df):
    """
    transactionID (Primary Key)
    customerID (Foreign Key)
    timestamp
    paymentMethod
    cardType
    cardLast4Digits
    orderStatus
    quantity
    productId  (Foreign Key)
    """

    ffd=raw_df.select("transactionId","customer.*","timestamp","payment.*","orderstatus","products")\
    .select("transactionid","customerid","timestamp","paymentmethod","cardType","cardLast4digits","orderstatus",explode(col("products")).alias('p'))


    f=ffd.select("transactionid","customerid","timestamp","paymentmethod","cardType","cardLast4digits","orderstatus","p.*").\
        select("transactionid","customerid","timestamp","paymentmethod","cardType","cardLast4digits","orderstatus","quantity","productId")


    try:

        f.write.format("parquet").mode("overwrite").saveAsTable("sales_fact", mode="Overwrite")

    except Exception as e:
        logging.error("Error while writing to target")



def generate_rev_per_day_sec_approach(spark):
    prod_df=spark.sql("select * from product_table")
    date_df=spark.sql("select * from date_dim")
    sales_df=spark.sql("select * from sales_fact")

    #prod_df.show(truncate=False)
    #date_df.show(truncate=False)
    #sales_df.show(truncate=False)

    sales_df=sales_df.withColumn("dt",to_date("timestamp"))
    sales_joined=sales_df.join(date_df,sales_df.dt==date_df.dates,"inner").select(sales_df['*'])

    s=sales_joined.alias("s").join(prod_df.alias("p"),sales_joined.productId==prod_df.productId).select("s.dt","p.productId","p.unitPrice",
                                                                                                        "s.quantity")
    s=s.withColumn("total_val",col("unitPrice")*col("quantity"))

    s=s.groupBy("dt").agg(sum("total_val").alias("revenue"))

    try:

        s.write.format("parquet").mode("overwrite").saveAsTable("revenue_per_day_approach2", mode="Overwrite")
    except Exception as e:

        logging.error("Error while writing to target")



def generate_rev_per_prod_category_sec_approach(spark):
    prod_df = spark.sql("select * from product_table")
    date_df = spark.sql("select * from date_dim")
    sales_df = spark.sql("select * from sales_fact")

    p = sales_df.alias("s").join(prod_df.alias("p"), sales_df.productId == prod_df.productId).select("p.category",
                                                                                                             "p.unitPrice",
                                                                                                             "s.quantity")

    p = p.withColumn("total_val", col("unitPrice") * col("quantity"))
    p = p.groupBy("category").agg(sum("total_val").alias("revenue"))

    #p.show(truncate=False)

    try:

        p.write.format("parquet").mode("overwrite").saveAsTable("revenue_per_product_category_approach2", mode="Overwrite")
    except Exception as e:

        logging.error("Error while writing to target")


def start_transformation():
    spark=createSparkSession("transform")

    raw_df=spark.sql("select * from raw")

    # Adhoc analysis tables

    calculate_revenue_per_day(raw_df)

    calculate_revenue_per_product_category(raw_df)

    last_purchase_date(raw_df)

    # Generation of star schema to answer other relevant business questions

    generate_customer_dimension(raw_df) #load customer dimension

    generate_product_dimension(raw_df)  #load product dimension

    generate_date_dimension_load("01-01-2023",1)  # This should not be scheduled daily, for sample i have written it here. date dim load

    generate_sales_fact(raw_df) #fact table load

    # Thought of answering some questions with the help of the star schema i created

    generate_rev_per_day_sec_approach(spark)

    generate_rev_per_prod_category_sec_approach(spark)




