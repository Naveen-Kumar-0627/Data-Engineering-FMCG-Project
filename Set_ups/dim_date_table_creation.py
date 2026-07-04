# Databricks notebook source
#creating dim_date table for analytics
from pyspark.sql import functions as f
start_date="2024-01-01"
end_date="2025-12-01"
df=( 
spark.sql(f""" 
           select  explode(
                 sequence(
                     to_date('{start_date}'),
                     to_date('{end_date}'),
                     interval 1 month
                 ) 
              ) as month_start
 
              """)  
)
df= ( 
df.withColumn("date_key",f.date_format("month_start","yyyyMM").cast("int")) 
.withColumn("year",f.year("month_start"))  
.withColumn("month_name",f.date_format("month_start","MMMM"))  
.withColumn("month_short",f.date_format("month_start","MMM"))  
.withColumn("quater",f.concat(f.lit("Q"),f.quarter("month_start")))  
.withColumn("year_quarter",f.concat(f.lit("-Q"),f.quarter("month_start")))
)
   
df.write \
  .mode('overwrite') \
  .format('delta') \
  .saveAsTable('fmcg.gold.dim_date')   