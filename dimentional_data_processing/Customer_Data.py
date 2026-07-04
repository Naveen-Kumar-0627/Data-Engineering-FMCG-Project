# Databricks notebook source
# MAGIC %md
# MAGIC ### In this notebook iam doing Medallion Architecture operations for the Dimentional table Customer 

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/puchimanidan5777@gmail.com/FMCG/Set_ups/utilities

# COMMAND ----------

print(gold_schema,bronze_schema,silver_schema)

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg","Catalogs")
dbutils.widgets.text("data_source","customer","Data_sources")

# COMMAND ----------

catalog=dbutils.widgets.get("catalog")
data_source=dbutils.widgets.get("data_source")
data_path=f's3://bucket-for-fmcg-project/{data_source}/*.csv'

# COMMAND ----------

print(data_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading data from Aws S3 and overwriting into Bronze Table

# COMMAND ----------

df=(
    spark.read.format('csv')\
    .option("header",True)\
        .option("inferScheam",True)\
            .load(data_path)\
                .withColumn("read_timestamp",F.current_timestamp())\
                    .select("*","_metadata.file_name","_metadata.file_size")


)


# COMMAND ----------

df.select("customer_id").distinct().count()

# COMMAND ----------

df.write.format("delta") \
    .mode('overwrite') \
    .option("delta.enableChangeDataFeed",True) \
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations and cleaning in Silver Layer

# COMMAND ----------

bronze_df=spark.sql(f"select * from {catalog}.{bronze_schema}.{data_source};") 
display(bronze_df)

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

bronze_df.groupBy("customer_id").count().alias("count").filter(F.col("count")>1).show()

# COMMAND ----------

print( "before drop" , bronze_df.count())
silver_df=bronze_df.dropDuplicates(['customer_id'])
print("after drop",silver_df.count())
 

# COMMAND ----------

display(silver_df.filter(F.col("customer_name")!=F.trim(F.col("customer_name"))))

# COMMAND ----------

silver_df=silver_df.withColumn("customer_name",F.trim(F.col("customer_name")))

# COMMAND ----------

silver_df.select("city").distinct().show()

# COMMAND ----------

# correcting the typo mistake in city column
city_map={
    "bengaluruu":"Bengaluru",
    "bengalore":"Bengaluru",
    "NewDelhi":"New Delhi",
    "NewDheli":"New Delhi",
     "NewDelhee":"New Delhi",
     "Hyderabadd":"Hyderabad",
     "Hyderbad":"Hyderabad"

}
allowed=["Bengaluru","New Delhi","Hyderabad"]
silver_df=(
    silver_df.replace(city_map,subset=['city'])\
    .withColumn(
        "city",F.when(F.col("city").isNull(),None)\
            .when(F.col("city").isin(allowed),F.col("city"))\
                .otherwise(None)
    )
 
)

# COMMAND ----------

#replacing invalid non integer with 999999
silver_df = silver_df.withColumn(
    "customer_id",
    F.when(
        F.col("customer_id").rlike("^[0-9]+$"),
        F.col("customer_id")
    ).otherwise(F.lit("999999"))
)

# COMMAND ----------


silver_df.select("city").distinct().show()

# COMMAND ----------

silver_df.select("customer_name").distinct().show()

# COMMAND ----------

silver_df=(silver_df.withColumn("customer_name",F.when(F.col("customer_name").isNull(),None)\
    .otherwise(F.initcap("customer_name")))
)

# COMMAND ----------

silver_df.select("customer_name").distinct().show()

# COMMAND ----------

silver_df.filter(F.col("city").isNull()).show()

# COMMAND ----------


li=["Endurance Foods","Sprintx Nutrition","Zenathlete Foods","Primefuel Nutrition","Recovery Lane"]


# COMMAND ----------

silver_df.filter(F.col("customer_name").isin(li)).display()

# COMMAND ----------

#as per above showing records every customer have mapped to among 3 cities only but someone is missed
# to map so iam doing that.
df={
    "789101": "Bengaluru",
    "789403": "New Delhi",
    "789520": "Bengaluru",
    "789521": "Hyderabad",
    "789603": "Hyderabad",
    "789420": "Bengaluru"
}
df_fix=spark.createDataFrame([(k,r) for k ,r in df.items()],["customer_id","correct_city"])

# COMMAND ----------

silver_df=(
    silver_df.join(df_fix,"customer_id","left").withColumn("city",F.coalesce("city","correct_city")).drop("correct_city")
)

# COMMAND ----------

silver_df=silver_df.withColumn("customer_id",F.col("customer_id").cast("string"))
silver_df.printSchema()

# COMMAND ----------

#adding few columns ,because we have these columns in parent table
silver_df=(silver_df.withColumn("customer",F.concat_ws("-","customer_name",F.coalesce("city",F.lit("unknown"))))\
    .withColumn("market",F.lit("india")) \
        .withColumn("platform",F.lit("Sports Bar"))\
            .withColumn("channel",F.lit("Acquisition")))

# COMMAND ----------

silver_df.display()

# COMMAND ----------

silver_df.write\
    .format('delta')\
    .option("delta.enableChangeDataFeed",True)\
    .mode("overwrite")\
    .option("mergeSchema",True)\
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

silver_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # ** Gold  Processing**

# COMMAND ----------

df_silver=spark.sql(f"select * from {catalog}.{silver_schema}.{data_source}")
# Picking required columns only 
gold_df=df_silver.select("customer_id","customer_name","customer","city","channel","market","platform")
gold_df.display()

# COMMAND ----------

#overwriting data into child company table
gold_df.write\
    .format('delta') \
    .option("delta.enableChangeDataFeed",True) \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

child_table_df=spark.table("fmcg.gold.sb_dim_customers").select(F.col("customer_id").alias("customer_code"),"customer","market","channel","platform")

# COMMAND ----------

#merging child company data with parent company data
parent_table_df=DeltaTable.forName(spark,"fmcg.gold.dim_customers")
parent_table_df.alias("target").merge(
     source=child_table_df.alias("source"),
     condition="target.customer_code=source.customer_code")\
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()   
  
 