# Databricks notebook source
# DBTITLE 1,Simulate writing a CSV file to S3
dbutils.fs.put("s3://oetrta/lee_demo/plan_providers.csv", """
planId,providerName
5237,Anthem
5155,Anthem
984,Cigna
836,Aetna
4624,Cigna
1490,Cigna
1227,Aetna
1290,Aetna
900,Cigna
8291,Anthem
1513,Cigna
8583,Anthem
""", True)

# COMMAND ----------

display(spark.read.csv("s3://oetrta/lee_demo/plan_providers.csv", header=True))
# display(spark.read.csv("/mnt/oetrta/lee_demo/plan_providers.csv", header=True))

# COMMAND ----------

# dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS lee_demo;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS lee_demo.plan_provider_lookup
# MAGIC USING com.databricks.spark.csv
# MAGIC OPTIONS (path "s3://oetrta/lee_demo/plan_providers.csv", header "true", inferSchema "true");

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from lee_demo.plan_provider_lookup

# COMMAND ----------


