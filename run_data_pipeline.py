from data_ingestion import *
from data_transformation import *
from generate_fake_data import *
from gold_layer import *

if(__name__=='__main__'):

    generate_sample_data()
    start_ingestion()
    start_transformation()
    display_gold_tables()

