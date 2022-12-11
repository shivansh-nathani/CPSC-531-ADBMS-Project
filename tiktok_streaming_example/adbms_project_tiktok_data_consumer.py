# Databricks notebook source
import pyspark.sql.functions as fn
from pyspark.sql.types import StringType
from pyspark.sql.functions import *

topic="tiktok_category"

tikTokDF = (
spark
.readStream
.format("kafka")
.option("kafka.bootstrap.servers", dbutils.secrets.get("kafkasecrets","server"))  # reading kafka server details from databricks secrets. we can directly mention the details as plain string as well as place them in databricks secrets.
.option("kafka.security.protocol", "SASL_SSL")
.option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(dbutils.secrets.get("kafkasecrets","username"),dbutils.secrets.get("kafkasecrets","password")))  # reading and setting the kafka username and password key that was generated during confluent setup
.option("kafka.ssl.endpoint.identification.algorithm", "https")
.option("kafka.sasl.mechanism", "PLAIN")
.option("subscribe", topic)  # subscribing to the topic from which we want to consume data
.option("startingOffsets", "latest")  # setting offset from which data read should start. two option are there earliest and latest. earliest is from the begining and latest would be the data that is currently being written to topic
.option("failOnDataLoss", "false")
.load()
.withColumn('value', fn.col("value").cast(StringType()))  # processing the data and extracting fields required using withColumn function.
.withColumn('key', fn.col("key").cast(StringType()))
.withColumn('time', split('value',",").getItem(0))
.withColumn('category', split('value',",").getItem(1))
.withColumn('watchTime', split('value',",").getItem(2))
.withColumn('liked', split('value',",").getItem(3))
.select('key','time','category','watchTime','liked')   # selecting the fields from process data that should be present in the streaming dataframe
)

# COMMAND ----------

tikTokDF.isStreaming  # checking if the dataframe is a streaming source.

# COMMAND ----------

tikTokDF.printSchema() # printing schema of dataframe.

# COMMAND ----------

# a basic analysis performed on streaming data to get insights per user based on their interactions on various tiktok videos based on category.
metrics = tikTokDF.groupBy("key","category").agg((avg("watchTime").alias("avgWatchTime")),(count("*").alias("TotalViewed")),(sum(when(col("liked")=="1",1).otherwise(0)).alias("likes"))).sort(asc("key")).sort("key","category")

# COMMAND ----------

# writing the batched analysis to console
metrics.writeStream.format("console").outputMode("complete").start()

# COMMAND ----------

# writing streaming dataframe to s3 using writestream function which also ensures that new data is written using checkpoint concept. 
tikTokDF\
    .writeStream \
    .format("json") \
    .option("checkpointLocation", "s3://adbms.project.data/tiktok_streaming_data_checkpoint_1") \
    .option("path", "s3://adbms.project.data/tiktok_streaming_data_1") \
    .start()

# COMMAND ----------

# creating an external table based on s3 location
%sql
CREATE TABLE tiktok_streaming_data LOCATION 's3://adbms.project.data/tiktok_streaming_data'

# COMMAND ----------

#performing sql analysis on table that was created in previous step
%sql
select count(key) from tiktok_streaming_data;


# COMMAND ----------

# sql analysis on table
%sql
select avg(watchTime),count(liked),(category,liked) from tiktok_streaming_data where liked=1 group by(category,liked) order by count(liked) desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select avg(watchTime),count(liked),(category,liked) from tiktok_streaming_data where liked=1 group by(category,liked) order by count(liked) desc

# COMMAND ----------

# reading data from s3 to spark dataframe for future analysis
df = spark.read.format("s3://adbms.project.data/tiktok_streaming_data")

# COMMAND ----------


