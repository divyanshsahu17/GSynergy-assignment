# Databricks notebook source
# MAGIC %md
# MAGIC ## Dim InvoiceLocation

# COMMAND ----------

# MAGIC %run /gsynergy/utils

# COMMAND ----------

df=spark.read.option("sep","|").option("header",True).csv("/mnt/gsynergy/hier.invloc.dlm.gz")

# COMMAND ----------

check_duplicates(df,"loc")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("gsynergy_staging.dim_invoice_location")

# COMMAND ----------

