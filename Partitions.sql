-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/accounts")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create a Hive table in an SQL notebook cell (i.e. not in the UI) which corresponds to the accounts data. To ensure
-- MAGIC the table is populated, LOCATION should be specified to be /FileStore/accounts/

-- COMMAND ----------

CREATE EXTERNAL TABLE accounts(
acct_num INT,
acct_created TIMESTAMP,
last_order TIMESTAMP,
first_name STRING,
last_name STRING,
address STRING,
city STRING,
state STRING,
zipcode STRING,
phone_number STRING,
last_click TIMESTAMP,
last_logout TIMESTAMP)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/FileStore/accounts/';

-- COMMAND ----------

CREATE TABLE accounts_with_areacode( 
acct_num INT,
first_name  STRING,
last_name STRING,
phone_number STRING,
areacode STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LOCATION '/FileStore/accounts_with_areacode';

-- COMMAND ----------

INSERT INTO accounts_with_areacode
SELECT acct_num,first_name,last_name,phone_number, substr(phone_number,1,3) AS area_code
FROM accounts;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/accounts_with_areacode")

-- COMMAND ----------

SELECT * FROM accounts_with_areacode LIMIT 10;

-- COMMAND ----------

INSERT OVERWRITE TABLE accounts_with_areacode
SELECT acct_num, first_name, last_name, phone_number,areacode
FROM accounts_with_areacode;

-- COMMAND ----------

CREATE EXTERNAL TABLE accounts_by_areacode (
acct_num INT,
first_name STRING,
last_name STRING,
phone_number STRING )
PARTITIONED BY ( areacode STRING )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/FileStore/accounts_by_areacode';

-- COMMAND ----------

DROP TABLE accounts_by_areacode

-- COMMAND ----------

CREATE EXTERNAL TABLE accounts_by_areacode (
acct_num INT,
first_name STRING,
last_name STRING,
phone_number STRING )
PARTITIONED BY ( areacode STRING )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/FileStore/accounts_by_areacode';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark import HiveContext
-- MAGIC sc = spark.sparkContext
-- MAGIC hiveContext = HiveContext(spark.sparkContext )
-- MAGIC hiveContext.setConf ( " hive.exec.dynamic.partition.mode "," nonstrict")

-- COMMAND ----------

INSERT OVERWRITE TABLE accounts_by_areacode
SELECT acct_num,first_name,last_name,phone_number,areacode
FROM accounts_with_areacode;

-- COMMAND ----------

SELECT * FROM accounts_by_areacode LIMIT 10;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/accounts_by_areacode/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC temp = dbutils.fs.ls("/FileStore/accounts_by_areacode/")
-- MAGIC print('number of files in the account_by_areacode directory is ', len(temp))
