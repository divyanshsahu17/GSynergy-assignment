# Databricks notebook source
# MAGIC %md
# MAGIC ## Dim Price State

# COMMAND ----------

# MAGIC %run /gsynergy/utils

# COMMAND ----------

df=spark.read.option("sep","|").option("header",True).csv("/mnt/gsynergy/hier.pricestate.dlm.gz")

# COMMAND ----------

check_duplicates(df,"substate_id")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("gsynergy_staging.dim_price_state")