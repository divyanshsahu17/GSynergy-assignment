# Databricks notebook source
# MAGIC %md
# MAGIC ## Dim Date

# COMMAND ----------

# MAGIC %run /gsynergy/utils

# COMMAND ----------

df=spark.read.option("sep","|").option("header",True).csv("/mnt/gsynergy/hier.clnd.dlm.gz")

# COMMAND ----------

check_duplicates(df,"fscldt_id")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("gsynergy_staging.dim_date")