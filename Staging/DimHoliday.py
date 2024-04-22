# Databricks notebook source
# MAGIC %md
# MAGIC ## Dim Holiday

# COMMAND ----------

# MAGIC %run /gsynergy/utils

# COMMAND ----------

df=spark.read.option("sep","|").option("header",True).csv("/mnt/gsynergy/hier.hldy.dlm.gz")

# COMMAND ----------

check_duplicates(df,"hldy_id")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("gsynergy_staging.dim_holiday")

# COMMAND ----------

