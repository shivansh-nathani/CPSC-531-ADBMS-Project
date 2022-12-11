# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType,FloatType,IntegerType
from pyspark.sql.functions import col,from_json
# we are pulling weather data from Rapid API and pushing the data to Kafka. The Response from API is a nested JSON. In this spark job we process the JSON and flatten the JSON
# defining schema for location object
locationSchema = StructType([ 
    StructField("name",StringType(),True), 
    StructField("region",StringType(),True), 
    StructField("country",StringType(),True), 
    StructField("lat", FloatType(), True),
    StructField("lon", FloatType(), True),
    StructField("tz_id", StringType(), True),
    StructField("localtime_epoch", IntegerType(), True),
    StructField("localtime", StringType(), True),
   

  ])
#defining schema for current weather details
currentSchema = StructType([ 
    StructField("temp_c",FloatType(),True), 
    StructField("temp_f",FloatType(),True), 
    StructField("is_day",IntegerType(),True), 
    StructField("wind_mph", FloatType(), True),
    StructField("wind_kph", FloatType(), True),
    StructField("wind_degree", FloatType(), True),
    StructField("wind_dir", StringType(), True),
    StructField("pressure_mb", FloatType(), True),
    StructField("pressure_in", FloatType(), True),
    StructField("precip_in", FloatType(), True),
    StructField("precip_mm", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("cloud", FloatType(), True),
    StructField("feelslike_c", FloatType(), True),
    StructField("feelslike_f", FloatType(), True)

  ])

#Actual JSON with nested location JSON and current weather JSON
outerSchema = StructType([ 
    StructField("location",locationSchema,True), 
    StructField("current",currentSchema,True), 
    

  ])


# COMMAND ----------

import pyspark.sql.functions as fn
from pyspark.sql.types import StringType,IntegerType,FloatType
from pyspark.sql.functions import split

topic="weather"

streamingData = (
spark
.readStream
.format("kafka")
.option("kafka.bootstrap.servers", dbutils.secrets.get("kafkasecrets","server")) # reading kafka server details from databricks secrets. we can directly mention the details as plain string as well as place them in databricks secrets.
.option("kafka.security.protocol", "SASL_SSL")
.option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(dbutils.secrets.get("kafkasecrets","username"),dbutils.secrets.get("kafkasecrets","password")))   # reading and setting the kafka username and password key that was generated during confluent setup
.option("kafka.ssl.endpoint.identification.algorithm", "https")
.option("kafka.sasl.mechanism", "PLAIN")
.option("subscribe", topic)# subscribing to the topic from which we want to consume data
.option("startingOffsets", "latest")  # setting offset from which data read should start. two option are there earliest and latest. earliest is from the begining and latest would be the data that is currently being written to topic
.option("failOnDataLoss", "false")
.load()
 .withColumn('value', fn.col("value").cast(StringType()))    # processing the data and extracting fields required using withColumn function.
# .withColumn('city', fn.col("key").cast(StringType()))
# .withColumn('time', split('value',",").getItem(0))
# .withColumn('usage', split('value',",").getItem(2).cast(FloatType()))
.withColumn("value",from_json(col("value"),outerSchema)) 
.withColumn('key', fn.col("key").cast(StringType()))
# .withColumn("value.location",from_json(col("value.location"),locationSchema)) 
# .withColumn("value.current",from_json(col("value.current"),currentSchema)) 
             
.select('key','value.*').select('key','location.*','current.*')  #flattening the nested JSON
)

# COMMAND ----------

display(streamingData)

# COMMAND ----------

streamingData.printSchema()

# COMMAND ----------

# writing streaming dataframe to s3 using writestream function which also ensures that new data is written using checkpoint concept. 


streamingData\
.writeStream\
.format("json")\
.option("path", "s3://adbms.project.data/weather_data_mock")\
.option("checkpointLocation", "s3://adbms.project.data/weather_data_checkpoint_mock")\
.start()


# COMMAND ----------

dataframe = spark.read.json("s3://adbms.project.data/weather_data")

# COMMAND ----------

display(dataframe.count())

# COMMAND ----------

dataframe.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE weather_data_mock using json LOCATION 's3://adbms.project.data/weather_data_mock'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from weather_data_mock limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table weather_data

# COMMAND ----------


