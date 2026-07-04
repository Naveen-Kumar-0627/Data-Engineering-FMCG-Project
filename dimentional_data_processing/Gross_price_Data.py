# Databricks notebook source
# MAGIC %md
# MAGIC ### In this notebook iam doing Medallion Architecture operations for the Dimentional table Gross Price

# COMMAND ----------

from pyspark.sql.functions import * 
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run /Workspace/Users/puchimanidan5777@gmail.com/FMCG/Set_ups/utilities

# COMMAND ----------

dbutils.widgets.text("data_source","gross_price","DataSource")
dbutils.widgets.text("catalog","fmcg","Catalog")

# COMMAND ----------

catalog=dbutils.widgets.get("catalog")
data_source=dbutils.widgets.get("data_source")
data_path=f"s3://bucket-for-fmcg-project/{data_source}/*.csv"

# COMMAND ----------

print(catalog,data_source,data_path)

# COMMAND ----------

#Reading data from aws S3
df=spark.read.format('csv')\
    .option("header",True)\
    .option("inferSchema",True)\
    .load(data_path)\
    .withColumn("read_timestamp",current_timestamp())\
    .select("*","_metadata.file_name","_metadata.file_size") 
     

# COMMAND ----------

df.show()

# COMMAND ----------

#writing data into bronze gross price table
df.write\
    .format('delta')\
    .option('delta.enableChangeDataFeed',True)\
    .option('mergeSchema',True)\
    .mode('overwrite')\
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Silver Processing

# COMMAND ----------

df=spark.sql(f"select * from {catalog}.{bronze_schema}.{data_source}")
df.display()

# COMMAND ----------

df=df.withColumn(
    "month",
    coalesce(
        try_to_date(col("month"),"yyyy/MM/dd"),
        try_to_date(col("month"),"dd/MM/yyyy"),
        try_to_date(col("month"),"yyyy-MM-dd"),
        try_to_date(col("month"),"dd-MM-yyyy")
    )
) 

# COMMAND ----------

#if gross price is integer and negative then converting into positive else replacing with 0
df=df.withColumn("gross_price",
    when(col("gross_price").rlike(r'^-?\d+(\.\d+)?$'),
    when(col("gross_price").cast("double") <0 ,-1 * col("gross_price").cast("double")).otherwise(col("gross_price").cast("double"))).otherwise(0)
    )

# COMMAND ----------

#using product_code from products table 
# because we dont have correct key attribute here
df_products=spark.table("fmcg.silver.products")
joined_df=df.join(df_products.select("product_id","product_code"),on="product_id",how="inner")
df_joined=joined_df.select("product_id","product_code","month","gross_price","read_timestamp","file_name","file_size")

# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_joined.write\
    .format('delta')\
    .option('delta.enableChangeDataFeed',True)\
    .option('mergeSchema',True)\
    .mode('overwrite')\
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC # **Gold**

# COMMAND ----------

silver_df=spark.sql(f"select * from {catalog}.{silver_schema}.{data_source}")
silver_df.display()

# COMMAND ----------

gold_df=silver_df.select("product_code","month","gross_price")

# COMMAND ----------

gold_df.write\
    .format('delta')\
    .option('mergeSchema',True)\
    .option('delta.enableChangeDataFeed',True)\
    .mode('overwrite')\
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

gold_df=spark.sql(f"select * from {catalog}.{gold_schema}.sb_dim_{data_source}")
gold_df.display()

# COMMAND ----------

gold_df=gold_df.withColumn("year",year(col("month")))\
    .withColumn("is_zero",when(col("gross_price")==0,1).otherwise(0))\
    

# COMMAND ----------

 w=Window.partitionBy("product_code","year").orderBy(col("is_zero"),desc("month"))

# COMMAND ----------

#Picking the new price 
# 2025-12-01 id-A this will be selected
# 2025-11-01 id-A
gold_df=gold_df.withColumn("rnk",rank().over(w)).filter(col("rnk")==1) 

# COMMAND ----------

gold_df.filter("product_code='e91ba9d665f90254da5809bfdebe3db2be01a52f50b6fd96b57eed238392b843'").display()

# COMMAND ----------

child_table=gold_df.select("product_code","year","gross_price").withColumnRenamed("gross_price","price_inr").select("*")
child_table=child_table.withColumn("year",col("year").cast("string"))

# COMMAND ----------

#Merging into parent_table
parent_table=DeltaTable.forName(spark,"fmcg.gold.dim_gross_price") 
parent_table.alias("target").merge(
    source=child_table.alias("source"),
    condition="target.product_code=source.product_code")\
        .whenMatchedUpdate(
            set={
                "year":"source.year",
                "price_inr":"source.price_inr"
            }
        )\
        .whenNotMatchedInsert(
            values={
                "product_code":"source.product_code",
                "year":"source.year",
                "price_inr":"source.price_inr"
            }
        ).execute()



# COMMAND ----------

df=spark.sql(f"select * from fmcg.gold.dim_gross_price" )
df.display()