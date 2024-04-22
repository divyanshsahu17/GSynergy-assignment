# Databricks notebook source
# MAGIC %md
# MAGIC ## Dim Location

# COMMAND ----------

# MAGIC %run /gsynergy/utils

# COMMAND ----------

df=spark.read.option("sep","|").option("header",True).csv("/mnt/gsynergy/hier.rtlloc.dlm.gz")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("gsynergy_staging.dim_location")