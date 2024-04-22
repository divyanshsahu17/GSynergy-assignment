# Databricks notebook source
# MAGIC %md
# MAGIC ## Fact AverageCost

# COMMAND ----------

# MAGIC %run /gsynergy/utils

# COMMAND ----------

df=spark.read.option("sep","|").option("header",True).csv("/mnt/gsynergy/fact.averagecosts.dlm.gz")

# COMMAND ----------

products=spark.sql("SELECT * FROM gsynergy_staging.dim_product")

# COMMAND ----------

# check foreign key contraints for date table if dates from average_cost is missing in dim_date then filer out the missing records and keep only valid records
try:
  check_foreign_key_constraint(parent_df=products,child_df=df, parent_column="sku_id",child_column="sku_id")
except:
  df=df.join(productsl, df['sku_id'] == products['sku_id'], "left_semi")

# COMMAND ----------

# check foreign key contraints for products table
check_foreign_key_constraint(parent_df=products,child_df=df, parent_column="sku_id",child_column="sku_id")

# COMMAND ----------

dates=spark.sql("SELECT * FROM gsynergy_staging.dim_date")

# COMMAND ----------

# check foreign key contraints for date table if dates from average_cost is missing in dim_date then filer out the missing records and keep only valid records
try:
  check_foreign_key_constraint(parent_df=dates,child_df=df, parent_column="fscldt_id",child_column="fscldt_id")
except:
  df=df.join(dates, df['fscldt_id'] == dates['fscldt_id'], "left_semi")

# COMMAND ----------

check_foreign_key_constraint(parent_df=dates,child_df=df, parent_column="fscldt_id",child_column="fscldt_id")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("gsynergy_staging.fact_average_cost")