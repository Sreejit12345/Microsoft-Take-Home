from data_ingestion import *
from data_transformation import *
from generate_fake_data import *
from gold_layer import *




if(__name__=='__main__'):

    generate_sample_data()  #this will generate sample data for mock results
    start_ingestion()      # this will ingest data into a table called raw after enriching with load_date and transaction_date
    start_transformation()  #this will load the customer/product/date dimension tables and a sales fact table in a start schema format
    #It will also load adhoc tables(revenue_per_day,revenue_per_product_category,customer_purchase_history) for the questions asked in the document

    display_gold_tables()  # to display gold aggregated tables
    pass
