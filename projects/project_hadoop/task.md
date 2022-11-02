Preparation
1. create docker Postgres
2. Create docker Hadoop

Python
1. Ingest TR_Products.csv and TR_UserInfo.csv to PostgreSQL

Python mrjob
1. Filter TR_Products.csv that have price more than 10 --> Store result to HDFS --> Also store result to Postgres
2. Aggregate TR_OrderDetails.csv transaction quantity based on date  --> Store result to HDFS --> Also store result to Postgres
3. Aggregate TR_OrderDetails.csv transaction quantity based on date and product  --> Store result to HDFS --> Also store result to Postgres


For Python mrjob, test it in local first, then do it in Hadoop namenode container