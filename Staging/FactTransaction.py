# Databricks notebook source
# MAGIC %md
# MAGIC ## Fact Transaction

# COMMAND ----------

# MAGIC %run /gsynergy/utils

# COMMAND ----------

df=spark.read.option("sep","|").option("header",True).csv("/mnt/gsynergy/fact.transactions.dlm.gz")

# COMMAND ----------

products=spark.sql("SELECT * FROM gsynergy_staging.dim_product")

# COMMAND ----------

# check foreign key contraints for products table
check_foreign_key_constraint(parent_df=products,child_df=df, parent_column="sku_id",child_column="sku_id")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("gsynergy_staging.fact_transaction")