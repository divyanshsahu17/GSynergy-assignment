# Databricks notebook source
# MAGIC %md
# MAGIC ## Dim InvoiceStatus

# COMMAND ----------

# MAGIC %run /gsynergy/utils

# COMMAND ----------

df=spark.read.option("sep","|").option("header",True).csv("/mnt/gsynergy/hier.invstatus.dlm.gz")

# COMMAND ----------

check_duplicates(df,"code_id")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("gsynergy_staging.dim_invoice_status")

# COMMAND ----------

