-- Databricks notebook source
-- MAGIC %md Full documentation here: https://docs.databricks.com/security/privacy/gdpr-delta.html

-- COMMAND ----------

SELECT * FROM lee_demo.silver_patient_data

-- COMMAND ----------

DESCRIBE HISTORY lee_demo.silver_patient_data

-- COMMAND ----------

DELETE FROM lee_demo.silver_patient_data WHERE patientId = 55089

-- COMMAND ----------

DESCRIBE HISTORY lee_demo.silver_patient_data

-- COMMAND ----------

-- MAGIC %md Use VACUUM to ensure underlying data files are removed: https://docs.databricks.com/security/privacy/gdpr-delta.html#step-3-clean-up-stale-data

-- COMMAND ----------

VACUUM lee_demo.silver_patient_data
