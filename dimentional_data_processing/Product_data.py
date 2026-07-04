# Databricks notebook source
# MAGIC %md
# MAGIC ### In this notebook iam doing Medallion Architecture operations for the Dimentional table Gross Price

# COMMAND ----------

from pyspark.sql.functions import * 
from delta.tables import DeltaTable


# COMMAND ----------

# MAGIC %run /Workspace/Users/puchimanidan5777@gmail.com/FMCG/Set_ups/utilities

# COMMAND ----------

print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg","Catalog")
dbutils.widgets.text("data_source","products","Source")

# COMMAND ----------

catalog=dbutils.widgets.get("catalog")
data_source=dbutils.widgets.get("data_source")
data_path=f's3://bucket-for-fmcg-project/{data_source}/*.csv'

# COMMAND ----------

print(data_path)

# COMMAND ----------

#Reading data from aws S3
df_source=spark.read.format("csv")\
    .option("inferSchema",True)\
    .option("header",True)\
    .load(data_path)\
    .withColumn("read_timestamp",current_timestamp())\
    .select("*","_metadata.file_name","_metadata.file_size")

# COMMAND ----------

display(df_source.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Schema

# COMMAND ----------

df_source.write\
    .format('delta')\
    .mode('overwrite')\
    .option("enableChangeDataFeed",True)\
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Silver Processing

# COMMAND ----------

silver_df=spark.sql(f"select * from {catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

silver_df.display()

# COMMAND ----------

print("before dropping duplicates",silver_df.count())
silver_df=silver_df.dropDuplicates(["product_id"]) 
print("after dropping duplicates",silver_df.count())

# COMMAND ----------

silver_df.select("product_name").distinct().display()

# COMMAND ----------

silver_df=silver_df.withColumn("category",when(col("category").isNull() , None) .otherwise(initcap("category")))

# COMMAND ----------

silver_df.select("category").distinct().display()

# COMMAND ----------

# Replacing protien with Protein
silver_df=silver_df.withColumn("product_name",regexp_replace(col("product_name"),"(?i)protien","Protein"))

# COMMAND ----------

silver_df.select("category").distinct().display()

# COMMAND ----------

silver_df=silver_df.withColumn("division",
    when(col("category")=="Protien Bars","Nutrition Bars")\
    .when(col("category")=="Energy Bars","Nutrition Bars")\
    .when(col("category")=="Granola & Cereals","Breakfast Foods")\
    .when(col("category")=="Recovery Dairy","Dairy and Recovery")\
    .when(col("category")=="Healthy Snacks","Healthy Snacks")\
    .when(col("category")=="Electrolyte Mix","Hydrations")
) 


# COMMAND ----------

#creating variations by extracting 
# SportsBar Energy Bar Choco Fudge (60g) --> 60g
#SportsBar Oats Cookie Bites ChocoChip (500g) --> 500g
silver_df=silver_df.withColumn("variations",regexp_extract(col("product_name"),r"\(([^)]+)\)",1))

# COMMAND ----------

#product_id is not reliable so creating product_code using sha2 fucntion based on product_name
silver_df=silver_df.withColumn("product_code",sha2(col("product_name").cast("string"),256))\
    .withColumn("product_id",when(col("product_id").cast("string").rlike("^[0-9]+$"),col("product_id").cast("string"))\
    .otherwise(lit("9999999").cast("string"))) 

# COMMAND ----------

silver_df.display()

# COMMAND ----------

silver_df.display()

# COMMAND ----------

 silver_df.write\
    .format('delta')\
    .option('delta.enableChangeDataFeed',True)\
    .option('mergeSchema',True)\
    .mode('overwrite')\
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC # **Gold**

# COMMAND ----------

gold_source=spark.sql(f"select * from {catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

gold_source=gold_source.select("product_name","product_id","category","division","variations","product_code")

# COMMAND ----------

gold_source.write\
    .format('delta')\
    .option('delta.enableChangeDataFeed',True)\
    .option('mergeSchema',True)\
    .mode('overwrite')\
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

gold=spark.sql(f"select * from {catalog}.{gold_schema}.sb_dim_{data_source}")
display(gold)

# COMMAND ----------

parent_table=DeltaTable.forName(spark,"fmcg.gold.dim_products")
child_table=spark.sql(f"select product_code,variations as variant,division,category,product_name as product from fmcg.gold.sb_dim_products")

# COMMAND ----------

#upserting into parent table
parent_table.alias("target").merge(
    source=child_table.alias("source"),
    condition="target.product_code=source.product_code")\
    .whenMatchedUpdate(
        set={
        "target.variant":"source.variant" ,
        "target.division":"source.division",
        "target.category":"source.category",
        "target.product":"source.product" 
        }
    )\
    .whenNotMatchedInsert(
       values={
        "target.product_code":"source.product_code",
       "target.variant":"source.variant" ,
        "target.division":"source.division",
        "target.category":"source.category",
        "target.product":"source.product"  
       }
        
    ).execute()
 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fmcg.gold.dim_products;