# Databricks notebook source
# MAGIC %fs
# MAGIC ls /databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports_us/

# COMMAND ----------

files = dbutils.fs.ls('dbfs:/databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports_us/')

# COMMAND ----------

sizeList = [x.size for x in files]

# COMMAND ----------

sizeList

# COMMAND ----------

pathList = [x.path for x in files]

# COMMAND ----------

pathList

# COMMAND ----------

filesRDD = sc.parallelize(pathList)

# COMMAND ----------

filesRDD.count()

# COMMAND ----------

csvFilesRDD = filesRDD.filter(lambda line: line.endswith('.csv'))

# COMMAND ----------

csvFilesRDD.count()

# COMMAND ----------

import re
# Assumes input MM -DD - YYYY .csv
def convert_name (filepath):
    m=re.match("^.*([0-9]{2})-([0-9]{2})-([0 -9]{4}).csv$", filepath)
    return int(m.group(3)+m.group(1)+m .group(2))

# COMMAND ----------

csvPairRDD = csvFilesRDD.map(lambda line:(convert_name(line),line))

# COMMAND ----------

csvPairRDD.take(2)

# COMMAND ----------

NewRDD1 = csvPairRDD.sortByKey(False)

# COMMAND ----------

NewRDD1.collect()

# COMMAND ----------

chosenfile = ('dbfs:/databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports_us/01-01-2021.csv')

# COMMAND ----------

DataFrame = spark.read.options(header='True').csv(chosenfile)

# COMMAND ----------

DataFrame.display()
