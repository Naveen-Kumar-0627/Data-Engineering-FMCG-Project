# Databricks notebook source
# MAGIC %md
# MAGIC ###  Full load for Fact Orders Table

# COMMAND ----------

from pyspark.sql.functions import * 
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/puchimanidan5777@gmail.com/FMCG/Set_ups/utilities

# COMMAND ----------

print(gold_schema,bronze_schema,silver_schema)

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg","Catalogs")
dbutils.widgets.text("data_source","orders","Data_sources")

# COMMAND ----------

catalog=dbutils.widgets.get("catalog")
data_source=dbutils.widgets.get("data_source")
base_path=f's3://bucket-for-fmcg-project/{data_source}'
landings=f"{base_path}/landing"
processed=f"{base_path}/processed"
print("base_path",base_path)
print("landings_path",landings)
print("processed_path",processed)

# COMMAND ----------

bronze_table=f"{catalog}.{bronze_schema}.{data_source}"
silver_table=f"{catalog}.{silver_schema}.{data_source}"
gold_table=f"{catalog}.{gold_schema}.sb_fact{data_source}"

# COMMAND ----------

#Reading data from s3
bronze_df=spark.read.format('csv')\
    .option("header",True)\
    .option("inferSchema",True)\
    .load(f"{landings}/*.csv")\
    .withColumn("read_timestamp",current_timestamp())\
    .select("*","_metadata.file_name","_metadata.file_size")
display(bronze_df)

# COMMAND ----------

bronze_df.write\
    .format('delta')\
    .option('delta.enableChangeDataFeed',True)\
    .option('mergeSchema',True)\
    .mode('overwrite')\
    .saveAsTable(f"{bronze_table}")

# COMMAND ----------

silver_df=spark.sql(f"select * from {bronze_table}")
display(silver_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Once the files are Picked then iam Moving data from Landings folder to Processed folder in S3

# COMMAND ----------

files=dbutils.fs.ls(f"{landings}")
for files_info in files:
    dbutils.fs.mv(
        files_info.path,(f"{processed}/{files_info.name}")
       )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Processing

# COMMAND ----------

silver_df=silver_df.filter(col("order_qty").isNotNull()) 

# COMMAND ----------

display(silver_df.limit(10))

# COMMAND ----------

#Removing day from order_placement_date
#"Tuesday, July 01, 2025" --> July 01,2025

silver_df=silver_df.withColumn("order_placement_date",regexp_replace(col("order_placement_date"),r"^[A-Za-z]+,\s*\d*\s*","")) 

# COMMAND ----------

display(silver_df.limit(10))

# COMMAND ----------

silver_df=silver_df.withColumn("order_placement_date",
    coalesce(
        try_to_date("order_placement_date","dd/MM/yyyy"),
        try_to_date("order_placement_date","yyyy/MM/dd"),
        try_to_date("order_placement_date","dd-MM-yyyy"),
        try_to_date("order_placement_date","yyyy-MM-dd"),
        try_to_date("order_placement_date","MMMM dd, yyyy")

    )) 

# COMMAND ----------

silver_df=silver_df.dropDuplicates(["order_id","customer_id","product_id","order_placement_date","order_qty"])

# COMMAND ----------

#if customer_id is non int then replacing with 999999
silver_df=silver_df.withColumn("customer_id",when(col("customer_id").rlike('^[0-9]+$'),col("customer_id").cast("string")).otherwise("999999")) 

# COMMAND ----------

silver_df=silver_df.withColumn("order_id",col("order_id").cast("string"))

# COMMAND ----------

silver_df.agg(
min("order_placement_date").alias("start_date"),
max("order_placement_date").alias("end_date")).show()

# COMMAND ----------

df_products=spark.sql(f"select * from {catalog}.{silver_schema}.products")

# COMMAND ----------

#picking the valid products only
df_join=silver_df.join(df_products,on="product_id",how="inner").select(silver_df["*"],df_products["product_code"])
display(df_join.limit(20))

# COMMAND ----------

#merging data into silver table
if not (spark.catalog.tableExists(silver_table)):
    df_join.write\
        .format('delta')\
        .option('mergeSchema',True)\
        .option('delta.enableChangeDataFeed',True)\
        .saveAsTable(silver_table)
else:
    silver=DeltaTable.forName(spark,silver_table)
    silver.alias("target").merge(df_join.alias("source"),
            """
            target.product_code=source.product_code and 
            target.customer_id=source.customer_id and  
            target.order_placement_date=source.order_placement_date and 
            target.order_id=source.order_id
            """
            )\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()

# COMMAND ----------

df=spark.sql(f"select * from {silver_table}")
display(df.limit(30))

# COMMAND ----------

gold_df=spark.sql(f"select order_id,order_placement_date as date,product_code,customer_id as customer_code,product_id,order_qty as sold_quantity from  {silver_table}")


# COMMAND ----------

#Mergind into gold table
if not (spark.catalog.tableExists(gold_table)):
    gold_df.write\
        .format('delta')\
        .option('delta.enableChangeDataFeed',True)\
        .option('mergeSchema',True)\
        .saveAsTable(gold_table)

else:
    gold_trg=DeltaTable.forName(spark,gold_table)
    gold_trg.alias("trg").merge(gold_df.alias('source'),
            """
            trg.order_id=source.order_id and
            trg.product_code=source.product_code and 
            trg.customer_code=source.customer_code and 
            trg.date=source.date
            """                
                                
            ).whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()

# COMMAND ----------

child_table=spark.sql(f"select * from {gold_table}")

# COMMAND ----------



# COMMAND ----------

display(child_table.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregating data and calculating  the sold quantity at monthly level for Merge into parent Table

# COMMAND ----------

child_table=child_table.withColumn("date",trunc("date","month")) 

# COMMAND ----------

child_table=child_table.groupBy("date","product_code","customer_code").agg(sum("sold_quantity").alias("sold_quantity")) 

# COMMAND ----------

child_table.display()

# COMMAND ----------

#Mergind child gold table parent gold table
parent_table=DeltaTable.forName(spark,"fmcg.gold.fact_orders")
parent_table.alias("trg").merge(
    child_table.alias("src"),
    """
    trg.product_code=src.product_code and
    trg.customer_code=src.customer_code and 
    trg.date=src.date
    """
).whenMatchedUpdateAll()\
 .whenNotMatchedInsertAll()\
 .execute()