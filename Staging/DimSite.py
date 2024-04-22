# Databricks notebook source
# MAGIC %md
# MAGIC ## Dim Site

# COMMAND ----------

# MAGIC %run /gsynergy/utils

# COMMAND ----------

df=spark.read.option("sep","|").option("header",True).csv("/mnt/gsynergy/hier.possite.dlm.gz")

# COMMAND ----------

check_duplicates(df,"site_id")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("gsynergy_staging.dim_site")

# COMMAND ----------

