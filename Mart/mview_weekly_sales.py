# Databricks notebook source
# MAGIC %md
# MAGIC ##Final Processed Data
# MAGIC
# MAGIC Create a refined table called **mview_weekly_sales** which aggregates *sales_units*, *sales_dollars*, and *disocunt_dollars* by *pos_site_id*, *sku_id*, *fsclwk_id*, *price_substate_id* and *type*.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gsynergy_mart.mview_weekly_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gsynergy_mart.mview_weekly_sales AS
# MAGIC SELECT a.pos_site_id,
# MAGIC   a.sku_id,
# MAGIC   b.fsclwk_id,
# MAGIC   a.price_substate_id,
# MAGIC   a.type,
# MAGIC   SUM(a.sales_units) AS sales_units,
# MAGIC   SUM(a.sales_dollars) AS sales_dollars, 
# MAGIC   SUM(a.discount_dollars) AS discount_dollars
# MAGIC FROM gsynergy_staging.fact_transaction a
# MAGIC LEFT JOIN gsynergy_staging.dim_date b ON a.fscldt_id=b.fscldt_id
# MAGIC GROUP BY a.pos_site_id,
# MAGIC   a.sku_id,
# MAGIC   b.fsclwk_id,
# MAGIC   a.price_substate_id,
# MAGIC   a.type

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from gsynergy_mart.mview_weekly_sales;