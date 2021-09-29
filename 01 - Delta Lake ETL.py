# Databricks notebook source
# MAGIC %md
# MAGIC ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Lake
# MAGIC 
# MAGIC Optimization Layer on top of blob storage for Reliability (i.e. ACID compliance) and Low Latency of Streaming + Batch data pipelines.
# MAGIC 
# MAGIC ![Delta Architecture](https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Bronze.png)

# COMMAND ----------

# DBTITLE 1,Reset database
# MAGIC %sql
# MAGIC 
# MAGIC DROP DATABASE IF EXISTS lee_demo CASCADE;
# MAGIC CREATE DATABASE lee_demo;

# COMMAND ----------

# DBTITLE 1,Reset checkpoint dir
# MAGIC %fs rm -r /mnt/lee_demo/streaming/bronze_checkpoint

# COMMAND ----------

# DBTITLE 1,Kinesis Stream Configuration
kinesisStreamName = "lee_patient_data"
kinesisRegion = "us-west-2"

kinesisDF = spark \
  .readStream \
  .format("kinesis") \
  .option("streamName", kinesisStreamName) \
  .option("initialPosition", "TRIM_HORIZON") \
  .option("region", kinesisRegion) \
  .load()

# COMMAND ----------

display(kinesisDF)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze table

# COMMAND ----------

# DBTITLE 1,Write raw kinesis data to Bronze Table
kinesisDF.writeStream \
  .outputMode("append") \
  .option("checkpointLocation", "/mnt/lee_demo/streaming/bronze_checkpoint") \
  .table("lee_demo.bronze_patient_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lee_demo.bronze_patient_data

# COMMAND ----------

# MAGIC %md ![Delta Architecture](https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Silver.png)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT string(data) as jsonData 
# MAGIC FROM lee_demo.bronze_patient_data

# COMMAND ----------

# DBTITLE 1,Define expected schema
# Also possible to read schema from a Schema Registry, or other external source

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType

jsonSchema = StructType([
    StructField("patientId", IntegerType(),True),
    StructField("planId", IntegerType(),True),
    StructField("firstName", StringType(),True),
    StructField("lastName", StringType(),True),
    StructField("companyName", StringType(),True),
    StructField("phoneNumber", StringType(),True),
    StructField("lastLogin", TimestampType(),True),
    StructField("email", StringType(),True)
  ]
)

# COMMAND ----------

from pyspark.sql.functions import from_json, col, to_date

silver_df = spark.table("lee_demo.bronze_patient_data") \
      .select(from_json(col("data").cast("string"), jsonSchema).alias("jsonData")) \
      .select("jsonData.*") \
#       .withColumn("lastLoginDate", to_date(col("lastLogin")))

display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver table

# COMMAND ----------

silver_df.write.saveAsTable("lee_demo.silver_patient_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lee_demo.silver_patient_data

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/iot-stream/data-device/

# COMMAND ----------

display(spark.read.json("/databricks-datasets/iot-stream/data-device/"))

# COMMAND ----------

# MAGIC %md ![Delta Architecture](https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Gold.png)

# COMMAND ----------

# DBTITLE 1,Define lookup table for joining
# MAGIC %run "./02 - S3 Lookup Data"

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold table (view)

# COMMAND ----------

# DBTITLE 1,Create Gold View
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW lee_demo.gold_patient_data
# MAGIC AS 
# MAGIC SELECT 
# MAGIC   pd.*,
# MAGIC   ppl.providerName
# MAGIC FROM lee_demo.silver_patient_data pd
# MAGIC LEFT JOIN lee_demo.plan_provider_lookup ppl
# MAGIC ON pd.planId = ppl.planId

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lee_demo.gold_patient_data 
# MAGIC -- where providerName is not null

# COMMAND ----------


