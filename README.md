# Microsoft-Take-Home
This is a repo that contains a solution to the Microsoft take home assesment.

Prerequisites To Run:


Python 3.7

Faker 18.13.0

py4j 0.10.7

pyspark 2.4.4

On running the run_data_pipeline.py it will do the following things in order:
1) Generate a sample file "sample_data.json" in the data folder of the repo with the help of faker module
2) Start the ingestion process (with built in DQ check) and ingest all the raw data into a table name "raw" after enriching with transaction_date and load _date
3) Start the transformation process that will extract data from the raw table in step1 and generate adhoc analysis tables as well as a star schema(fact/dim) to answer some business questions
4) Show the 20 sample rows for each business question as asked in the assessment. (This is the gold layer).
   
I have tried to create a star schema with customer,product and date as dimension tables. The sales table is a fact table.

I have done both approaches 

1) Generate adhoc insights on raw tables
2) Normalize Schema and generate a star schema to answer specific questions


Here is a sample output on running the pipeline:

Customer Dimension

+----------+---------------------------+---------+---------+----------------------+
|customerId|email                      |firstName|lastname |phoneNumber           |
+----------+---------------------------+---------+---------+----------------------+
|22        |shafferkimberly@example.net|Steven   |Mcbride  |5422727381            |
|47        |gevans@example.org         |Jared    |Hogan    |214.713.8503x872      |
|41        |sescobar@example.com       |Michelle |Greene   |758.522.8269x92251    |
|36        |ryanbeard@example.com      |John     |Thomas   |531-708-2970x209      |
|38        |scottherring@example.net   |Cole     |West     |001-729-615-1970      |
|37        |wellsluis@example.org      |Deborah  |Williams |293.233.6754          |
|37        |gloria36@example.net       |Charles  |Cardenas |9254964227            |
|25        |sarahblanchard@example.com |Scott    |Hernandez|728.758.8554          |
|27        |tracy81@example.com        |Nathan   |Nash     |001-706-830-0822x678  |
|46        |thomas59@example.org       |Robert   |Wilson   |2045783019            |
|5         |brandon09@example.org      |Angela   |Ramos    |969-905-1011x919      |
|9         |mark58@example.org         |Todd     |Proctor  |405.829.0287          |
|12        |iramos@example.com         |Robin    |Smith    |001-694-814-7985x40806|
|1         |vduffy@example.org         |Abigail  |Rogers   |956.272.4428x985      |
|24        |ryan30@example.net         |John     |Mcdonald |001-547-494-3947x43335|
|49        |lewiswendy@example.org     |Johnny   |Nelson   |(699)484-1344x139     |
|14        |omclaughlin@example.org    |Dan      |Schaefer |(530)728-4963x992     |
|33        |lisa67@example.net         |Kyle     |Adams    |353.836.1835x8996     |
|42        |buckcrystal@example.org    |Becky    |Phillips |001-619-370-6986x698  |
|22        |qmcbride@example.org       |Renee    |Morris   |4362577647            |
+----------+---------------------------+---------+---------+----------------------+
only showing top 20 rows

Product Dimension


+------------------------------------+-----------+------------+---------+
|productId                           |productName|category    |unitPrice|
+------------------------------------+-----------+------------+---------+
|fb1fa515-3684-4c05-9c0e-4f0ea7a5ed2d|age        |its         |902      |
|8c10364d-5796-4460-b30b-3b84d9ae01a1|picture    |few         |246      |
|dd3e1e19-2cf7-4281-8657-0bc9b2976399|sound      |coach       |682      |
|10abbe85-1e40-406a-ae90-54366bca1e03|feeling    |visit       |976      |
|60a48ac5-4c12-4104-9596-8ca5db1ea8b2|assume     |probably    |774      |
|9617066e-3b91-4eac-9c29-d49242a7389f|only       |series      |319      |
|88976cea-ba28-4937-9ebb-4f42398c4f4b|lose       |trade       |319      |
|73e9a872-c4ab-45c5-b1e8-0ac1a4b233cb|crime      |heart       |439      |
|c83d6e5c-5ea7-43bc-a9e4-d02e255dc42d|production |return      |346      |
|90123c16-8e11-46c6-b61d-2b40c725667a|along      |month       |50       |
|7f5558a2-70c2-41b5-8a62-f64b3a20d1db|something  |television  |884      |
|d87006d1-d61d-400e-bb43-4a9eeb841148|clear      |organization|691      |
|f25b79c3-46f7-4686-8876-2c346821e98b|raise      |involve     |614      |
|6c4e0244-2951-44f5-aba6-2928ef2fcdcc|skill      |north       |340      |
|3c69735f-6738-4d3a-a0d0-b02f7ee5d38d|bit        |old         |715      |
|646e34c0-abc2-45df-a6e4-4f56473d51e7|cost       |ready       |584      |
|1f6ddc94-4321-4a08-abba-b8faab809fd2|choose     |organization|450      |
|07022e15-9329-4294-8814-c94b24662ee4|western    |president   |107      |
|1358af78-aac8-43ca-8b8d-0e6a8b288ccd|all        |change      |919      |
|bde7801c-e141-420a-b337-2e7d18032b8d|smile      |century     |928      |
+------------------------------------+-----------+------------+---------+
only showing top 20 rows

Date Dimension


+----------+---+-----+----+
|dates     |day|month|year|
+----------+---+-----+----+
|2023-01-01|1  |1    |2023|
|2023-01-02|2  |1    |2023|
|2023-01-03|3  |1    |2023|
|2023-01-04|4  |1    |2023|
|2023-01-05|5  |1    |2023|
|2023-01-06|6  |1    |2023|
|2023-01-07|7  |1    |2023|
|2023-01-08|8  |1    |2023|
|2023-01-09|9  |1    |2023|
|2023-01-10|10 |1    |2023|
|2023-01-11|11 |1    |2023|
|2023-01-12|12 |1    |2023|
|2023-01-13|13 |1    |2023|
|2023-01-14|14 |1    |2023|
|2023-01-15|15 |1    |2023|
|2023-01-16|16 |1    |2023|
|2023-01-17|17 |1    |2023|
|2023-01-18|18 |1    |2023|
|2023-01-19|19 |1    |2023|
|2023-01-20|20 |1    |2023|
+----------+---+-----+----+
only showing top 20 rows

Sales fact 

+------------------------------------+----------+--------------------------+-------------+---------------------------+---------------+-----------+--------+------------------------------------+
|transactionid                       |customerid|timestamp                 |paymentmethod|cardType                   |cardLast4digits|orderstatus|quantity|productId                           |
+------------------------------------+----------+--------------------------+-------------+---------------------------+---------------+-----------+--------+------------------------------------+
|9d84bca7-6e21-48c6-aba0-0041e1e9a4de|14        |2023-11-08T15:34:47.063140|Google Pay   |Diners Club / Carte Blanche|6420           |Delivered  |1       |d7552d82-0514-4b27-875a-2664072ef3f6|
|9d84bca7-6e21-48c6-aba0-0041e1e9a4de|14        |2023-11-08T15:34:47.063140|Google Pay   |Diners Club / Carte Blanche|6420           |Delivered  |1       |e3697927-dc5a-490d-b035-0fcd54c24623|
|9d84bca7-6e21-48c6-aba0-0041e1e9a4de|14        |2023-11-08T15:34:47.063140|Google Pay   |Diners Club / Carte Blanche|6420           |Delivered  |2       |23777a91-248f-4b7d-a031-1983deee3cf2|
|9d84bca7-6e21-48c6-aba0-0041e1e9a4de|14        |2023-11-08T15:34:47.063140|Google Pay   |Diners Club / Carte Blanche|6420           |Delivered  |3       |69a5682b-da30-4d8e-8f31-da99e68f3717|
|9d84bca7-6e21-48c6-aba0-0041e1e9a4de|14        |2023-11-08T15:34:47.063140|Google Pay   |Diners Club / Carte Blanche|6420           |Delivered  |3       |4cb84cd3-e207-4abf-98dd-5d4dcaa036a0|
|8b5c340b-7caa-40e0-837c-8ff624f12719|33        |2023-10-15T15:34:47.063140|PayPal       |VISA 16 digit              |0559           |Delivered  |1       |5edd6ce6-1f87-4174-9057-0f95a6437b77|
|8b5c340b-7caa-40e0-837c-8ff624f12719|33        |2023-10-15T15:34:47.063140|PayPal       |VISA 16 digit              |0559           |Delivered  |5       |0172b2c2-d2be-4df4-89d0-5532a04251e6|
|8b5c340b-7caa-40e0-837c-8ff624f12719|33        |2023-10-15T15:34:47.063140|PayPal       |VISA 16 digit              |0559           |Delivered  |3       |88976cea-ba28-4937-9ebb-4f42398c4f4b|
|8b5c340b-7caa-40e0-837c-8ff624f12719|33        |2023-10-15T15:34:47.063140|PayPal       |VISA 16 digit              |0559           |Delivered  |1       |5324fb9a-21c6-4442-9b2f-1012e49d6ef6|
|8b2909d0-57f1-4ddf-8f7d-7ba8a9034f93|42        |2023-10-15T15:34:47.063140|Google Pay   |VISA 19 digit              |6570           |Shipped    |5       |dcb79830-4cc7-4354-a258-0cdb6f90ee08|
|8b2909d0-57f1-4ddf-8f7d-7ba8a9034f93|42        |2023-10-15T15:34:47.063140|Google Pay   |VISA 19 digit              |6570           |Shipped    |4       |6901f35a-f422-4cc5-ac90-647d2288a21c|
|8b2909d0-57f1-4ddf-8f7d-7ba8a9034f93|42        |2023-10-15T15:34:47.063140|Google Pay   |VISA 19 digit              |6570           |Shipped    |3       |8bf5c940-efd0-4e68-8101-03e9b321608e|
|8b2909d0-57f1-4ddf-8f7d-7ba8a9034f93|42        |2023-10-15T15:34:47.063140|Google Pay   |VISA 19 digit              |6570           |Shipped    |1       |fead7bad-ba84-4693-8203-ec2ce7dec428|
|8b2909d0-57f1-4ddf-8f7d-7ba8a9034f93|42        |2023-10-15T15:34:47.063140|Google Pay   |VISA 19 digit              |6570           |Shipped    |1       |303c1eca-e9a3-4789-934f-adf1de2b95f7|
|a47b7737-0299-4fe5-a044-d41ca9f697fc|22        |2023-10-15T15:34:47.063140|Credit Card  |VISA 16 digit              |2770           |Delivered  |1       |aba14f69-5338-4c49-87c5-93166e487cb3|
|a47b7737-0299-4fe5-a044-d41ca9f697fc|22        |2023-10-15T15:34:47.063140|Credit Card  |VISA 16 digit              |2770           |Delivered  |3       |edc8bd4b-7535-448e-9ec2-e98c997de237|
|a47b7737-0299-4fe5-a044-d41ca9f697fc|22        |2023-10-15T15:34:47.063140|Credit Card  |VISA 16 digit              |2770           |Delivered  |3       |1cf5dac8-38e8-413f-af2c-43d46b019229|
|a47b7737-0299-4fe5-a044-d41ca9f697fc|22        |2023-10-15T15:34:47.063140|Credit Card  |VISA 16 digit              |2770           |Delivered  |5       |c28b19e5-6dc2-4348-aa5a-c4cc0c6bb811|
|a47b7737-0299-4fe5-a044-d41ca9f697fc|22        |2023-10-15T15:34:47.063140|Credit Card  |VISA 16 digit              |2770           |Delivered  |1       |e240fe77-0973-496c-9b2b-44b9b8e5621d|
|ee1e7f10-ed73-4317-95cd-4ff0626ba738|39        |2023-11-12T15:34:47.063140|Credit Card  |JCB 16 digit               |3996           |Delivered  |1       |a1575d44-ad09-4492-af53-5d4f0b5ea0fc|
+------------------------------------+----------+--------------------------+-------------+---------------------------+---------------+-----------+--------+------------------------------------+
only showing top 20 rows

revenue per day approach 1

+----------------+-------+
|transaction_date|revenue|
+----------------+-------+
|2023-10-08      |1021   |
|2023-10-25      |11660  |
|2023-10-16      |144    |
|2023-11-25      |9688   |
|2023-09-27      |2969   |
|2023-10-29      |5231   |
|2024-01-02      |239    |
|2023-12-11      |11534  |
|2023-10-13      |8907   |
|2023-12-31      |906    |
|2023-10-06      |6641   |
|2023-12-15      |11608  |
|2023-11-15      |6686   |
|2023-11-26      |3849   |
|2023-10-31      |13050  |
|2023-10-15      |24550  |
|2023-12-14      |7600   |
|2023-10-09      |620    |
|2023-10-18      |12324  |
|2023-11-23      |9906   |
+----------------+-------+
only showing top 20 rows

revenue per day approach 2

+----------+-------+
|dt        |revenue|
+----------+-------+
|2023-10-25|11660  |
|2023-10-16|144    |
|2023-10-08|1021   |
|2023-09-27|2969   |
|2023-11-25|9688   |
|2023-10-13|8907   |
|2023-12-11|11534  |
|2023-10-06|6641   |
|2023-12-31|906    |
|2023-12-15|11608  |
|2023-11-15|6686   |
|2023-10-31|13050  |
|2023-11-26|3849   |
|2023-10-15|24550  |
|2023-12-14|7600   |
|2023-10-18|12324  |
|2023-10-09|620    |
|2023-11-23|9906   |
|2023-12-28|6603   |
|2023-12-17|10030  |
+----------+-------+
only showing top 20 rows

revenue per product category approach1 

+--------------+-------+
|category      |revenue|
+--------------+-------+
|responsibility|353    |
|particularly  |1446   |
|interesting   |852    |
|generation    |2128   |
|newspaper     |4218   |
|condition     |436    |
|player        |4400   |
|discussion    |856    |
|nearly        |2379   |
|fly           |4119   |
|side          |888    |
|suddenly      |729    |
|wear          |2128   |
|environment   |1950   |
|operation     |81     |
|television    |2652   |
|positive      |988    |
|understand    |1366   |
|let           |1305   |
|short         |220    |
+--------------+-------+
only showing top 20 rows

revenue per product category approach2 

+--------------+-------+
|category      |revenue|
+--------------+-------+
|responsibility|353    |
|particularly  |1446   |
|interesting   |852    |
|generation    |2128   |
|environment   |1950   |
|suddenly      |729    |
|side          |888    |
|wear          |2128   |
|newspaper     |4218   |
|nearly        |2379   |
|fly           |4119   |
|discussion    |856    |
|player        |4400   |
|condition     |436    |
|organization  |4564   |
|let           |1305   |
|understand    |1366   |
|short         |220    |
|television    |2652   |
|positive      |988    |
+--------------+-------+
only showing top 20 rows

customer purchase history

+----------+--------------+------------------+
|customerID|no_of_purchses|last_purchase_date|
+----------+--------------+------------------+
|32        |2             |2023-11-22        |
|43        |3             |2023-09-27        |
|28        |3             |2023-09-28        |
|33        |4             |2023-10-15        |
|11        |2             |2023-10-25        |
|49        |3             |2023-10-31        |
|30        |1             |2023-10-12        |
|42        |7             |2023-10-15        |
|29        |3             |2023-10-25        |
|19        |1             |2023-09-26        |
|22        |3             |2023-10-03        |
|7         |1             |2023-11-05        |
|34        |2             |2023-10-22        |
|39        |2             |2023-10-13        |
|25        |1             |2023-12-30        |
|6         |1             |2023-12-16        |
|9         |1             |2023-10-20        |
|27        |2             |2023-10-06        |
|17        |1             |2023-11-22        |
|41        |1             |2023-11-10        |
+----------+--------------+------------------+
only showing top 20 rows


Process finished with exit code 0
