# Microsoft-Take-Home
This is a repo that contains a solution to the Microsoft take home assesment.

Prerequisites To Run:

Python3.7
Faker 18.13.0
py4j 0.10.7
pyspark 2.4.4

On running the run_data_pipeline.py it will do the following things in order:
1) Generate a file "sample_data.json" in the data folder of the repo with the help of faker module
2) Start the ingestion process and ingest all the raw data into a table name "raw"
3) Start the transformation process that will extract data from the raw table in step1 and generate answers to the relevant business questions
4) Show the 20 sample rows for each business question as asked in the assessment.
