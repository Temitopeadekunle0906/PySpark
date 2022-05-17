-- Databricks notebook source
SELECT COUNT(*) FROM zip_level_risk

-- COMMAND ----------

SELECT count_property FROM zip_level_risk ORDER BY count_property DESC;

-- COMMAND ----------

SELECT count_property FROM zip_level_risk ORDER BY count_property DESC LIMIT 10;

-- COMMAND ----------

SELECT count_property, count_fs_risk_2020_100 FROM zip_level_risk;

-- COMMAND ----------


