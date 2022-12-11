# Databricks notebook source
import pyspark.sql.functions as fn
from pyspark.sql.types import StringType,IntegerType,FloatType
from pyspark.sql.functions import split

topic="energy_consumption"

streamingData = (
spark
.readStream
.format("kafka")
.option("kafka.bootstrap.servers", dbutils.secrets.get("kafkasecrets","server"))  # reading kafka server details from databricks secrets. we can directly mention the details as plain string as well as place them in databricks secrets.
.option("kafka.security.protocol", "SASL_SSL")
.option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(dbutils.secrets.get("kafkasecrets","username"),dbutils.secrets.get("kafkasecrets","password")))  # reading and setting the kafka username and password key that was generated during confluent setup
.option("kafka.ssl.endpoint.identification.algorithm", "https")
.option("kafka.sasl.mechanism", "PLAIN")
.option("subscribe", topic)                 # subscribing to the topic from which we want to consume data
.option("startingOffsets", "latest")         # setting offset from which data read should start. two option are there earliest and latest. earliest is from the begining and latest would be the data that is currently being written to topic
.option("failOnDataLoss", "false")
.load()
.withColumn('value', fn.col("value").cast(StringType()))     # processing the data and extracting fields required using withColumn function.
.withColumn('key', fn.col("key").cast(StringType()))
.withColumn('time', split('value',",").getItem(0).cast(IntegerType()))
.withColumn('usage', split('value',",").getItem(2).cast(FloatType()))
.withColumn('userId', split('value',",").getItem(1).cast(IntegerType()))
.withColumn('location', split('value',",").getItem(3).cast(StringType()))

.select('key','time','usage','userId','location')           # selecting the fields from process data that should be present in the streaming dataframe
)

# COMMAND ----------

display(spark)

# COMMAND ----------

# writing streaming dataframe to s3 using writestream function which also ensures that new data is written using checkpoint concept. 


streamingData\
.writeStream\
.format("json")\
.option("path", "s3://adbms.project.data/energy_consumption")\
.option("checkpointLocation", "s3://adbms.project.data/energy_consumption_checkpoint")\
.start()


# COMMAND ----------

dataframe = spark.read.json("s3://adbms.project.data/energy_consumption")

# COMMAND ----------

display(dataframe.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from energy_consumption

# COMMAND ----------

spark.sql("CREATE TABLE energy_consumption using json LOCATION 's3://adbms.project.data/energy_consumption'")

# COMMAND ----------

import pandas as pd

#joining weather and energy data for analysis

dataframe = spark.sql("select * from energy_consumption ")
dataframe_weather = spark.sql("select * from weather_data ")
dataframe = dataframe.withColumnRenamed("time","localtime_epoch")
dataframe.printSchema()
print(type(dataframe))
print(type(dataframe_weather))
joineddf = dataframe.join(dataframe_weather,dataframe.localtime_epoch ==dataframe_weather.localtime_epoch,'inner')
display(joineddf)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table weather_data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table weather_data

# COMMAND ----------

#sql queries

%sql
select distinct(energy_consumption.location) from weather_data inner join energy_consumption on energy_consumption.time == weather_data.localtime_epoch

# COMMAND ----------

#sql queries

%sql
select count(*) from energy_consumption_mock

# COMMAND ----------

#sql queries

%sql
select distinct(time) from  energy_consumption

# COMMAND ----------

#sql queries
%sql
select localtime_epoch from  weather_data

# COMMAND ----------


# we need to refresh table if the underlying s3 path is being updated by any streaming process
%sql
refresh table energy_consumption;
REFRESH table weather_data;

# COMMAND ----------


