# Databricks notebook source
# MAGIC %md
# MAGIC ### Incremental Load for Fact Orders Table

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
gold_table=f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# COMMAND ----------

#Reading data from aws S3
bronze_df=spark.read.format('csv')\
    .option('inferSchema',True)\
    .option('header',True)\
    .load(f"{landings}/*.csv")\
    .withColumn("read_timestamp",current_timestamp())\
    .select("*","_metadata.file_name","_metadata.file_size")

# COMMAND ----------

display(bronze_df.count())

# COMMAND ----------

bronze_df.display()

# COMMAND ----------

bronze_df=bronze_df.withColumn("order_qty",col("order_qty").cast("double"))

# COMMAND ----------

bronze_df.write\
    .format('delta')\
    .option('mergeSchema',True)\
    .option('delta.enableChangeDataFeed',True)\
    .mode('append')\
    .saveAsTable(f"{bronze_table}")

# COMMAND ----------

#writing new arrived data into staging table 
bronze_df.write\
    .format('delta')\
    .option('delta.enableChangeDataFeed',True)\
    .mode('overwrite')\
    .saveAsTable(f"{catalog}.{bronze_schema}.staging_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Once files are picked then moving data from landings folder into processed folder in s3

# COMMAND ----------

files=dbutils.fs.ls(f"{landings}")
for files_info in files:
    dbutils.fs.mv(files_info.path,(f"{processed}/{files_info.name}"),True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver
# MAGIC

# COMMAND ----------

df_silver=spark.sql(f"select * from fmcg.bronze.staging_orders")

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver=df_silver.filter(col("order_qty").isNotNull()) 

# COMMAND ----------

#Removing day from order_placement_date
#"Tuesday, July 01, 2025" --> July 01,2025
df_silver=df_silver.withColumn("order_placement_date",regexp_replace(col("order_placement_date"),r"^[A-Za-z]+,\s*\d*\s*",""))

# COMMAND ----------

df_silver=df_silver.withColumn("order_placement_date",
    coalesce(
        try_to_date("order_placement_date","dd-MM-yyyy"),
        try_to_date("order_placement_date","yyyy-MM-dd"),
        try_to_date("order_placement_date","dd/MM/yyyy"),
        try_to_date("order_placement_date","yyyy/MM/dd"),
        try_to_date("order_placement_date","MMMM dd, yyyy")
    ))

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver=df_silver.withColumn("customer_id",when(col("customer_id").rlike('^[0-9]+$'),col("customer_id")).otherwise("999999")) 

# COMMAND ----------

df_silver=df_silver.dropDuplicates(["order_id","order_placement_date","product_id","customer_id"]) 

# COMMAND ----------

df_products=spark.sql("select * from fmcg.silver.products")
df_products.display()

# COMMAND ----------

#picking the valid products only
df_join=df_silver.join(df_products,on="product_id",how="inner").select(df_silver["*"],df_products["product_code"])
df_join.display()

# COMMAND ----------

df_join.display()

# COMMAND ----------

df_join=df_join.withColumn("product_id",col("product_id").cast("int")) 

# COMMAND ----------

if not (spark.catalog.tableExists(silver_table)):
    df_join.write\
     .format('delta')\
     .option('delta.enableChangeDataFeed',True)\
     .option('mergeSchema',True)\
     .mode('overwrite')\
     .saveAsTable(f"{silver_table}")
else:
    silver_table=DeltaTable.forName(spark,"fmcg.silver.orders")
    silver_table.alias('trg').merge(df_join.alias("src"),
        """
        trg.customer_id=src.customer_id and
        trg.product_code=src.product_code and
        trg.order_placement_date=src.order_placement_date and
        trg.order_id=src.order_id
        """).whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
        

# COMMAND ----------

#writing cleaned data into staging table
df_join.write\
    .format('delta')\
    .option('delta.enableChangeDataFeed',True)\
    .mode('overwrite')\
    .saveAsTable("fmcg.silver.staging_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold

# COMMAND ----------

gold_df=spark.sql(f"select * from fmcg.silver.staging_orders")

# COMMAND ----------

gold_df=gold_df.withColumnRenamed("customer_id","customer_code") 

# COMMAND ----------

gold_df=gold_df.withColumnRenamed("order_qty","sold_quantity")

# COMMAND ----------

gold_df=gold_df.withColumnRenamed("order_placement_date","date")

# COMMAND ----------

#Merging into child company table
if not (spark.catalog.tableExists("fmcg.silver.staging_orders")):
    gold_df.write\
        .format('delta')\
        .option('mergeSchema',True)\
        .option('delta.enableChangeDataFeed',True)\
        .mode('overwrite')\
        .saveAsTable("fmcg.gold.sb_fact_orders")
else:
    gold_table=DeltaTable.forName(spark,"fmcg.gold.sb_fact_orders")
    gold_table.alias('trg').merge(gold_df.alias('src'),
        """
        trg.customer_code=src.customer_code and 
        trg.product_code=src.product_code and 
        trg.date=src.date and 
        trg.order_id=src.order_id
        """).whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()       

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregating data and calculating the sold quantity at monthly level for Merge into parent Table

# COMMAND ----------

#To understand this see the Architectural diagrams
new_month_df=spark.sql(f"select * from fmcg.silver.staging_orders")
new_month_df=new_month_df.withColumn("new_month",trunc(col("order_placement_date"),"MM")).select("new_month").distinct() 

# COMMAND ----------

new_month_df.display()

# COMMAND ----------

new_month_df.createOrReplaceTempView("join_df")

# COMMAND ----------

gold_join=(spark.sql(
    f"""select date,
        product_code,
        customer_code,
        sold_quantity from fmcg.gold.sb_fact_orders as g inner join join_df as n on trunc(g.date,'MM')=n.new_month 
        """
        )

)
gold_join.display()

# COMMAND ----------

gold_join=gold_join.withColumn("date",trunc("date","MM")).groupBy("date","product_code","customer_code").agg(sum("sold_quantity").alias("sold_quantity"))

# COMMAND ----------

gold_join.display()

# COMMAND ----------

#Merging the monthly aggregated data into parent table
parent_table=DeltaTable.forName(spark,"fmcg.gold.fact_orders")
parent_table.alias("trg").merge(gold_join.alias("src"),
        """
        trg.customer_code=src.customer_code and 
        trg.product_code=src.product_code and 
        trg.date=src.date  
        """).whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table fmcg.bronze.staging_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table fmcg.silver.staging_orders;