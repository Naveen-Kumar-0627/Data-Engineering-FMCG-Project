# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists fmcg;
# MAGIC use catalog fmcg;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema fmcg.bronze;
# MAGIC create schema fmcg.silver;
# MAGIC create schema fmcg.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from  fmcg.gold.fact_orders;