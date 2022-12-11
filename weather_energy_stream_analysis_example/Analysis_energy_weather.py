# Databricks notebook source
import matplotlib.pyplot as plt
from pyspark.sql.types import DoubleType
import time
from pyspark.sql import functions as f
from pyspark.sql import types as t

# Import energy data into a dataframe
dataframe_energy = spark.sql("select * from energy_consumption_mock  sort by time asc limit 500")

# Import weather data into a dataframe
dataframe_weather = spark.sql("select * from weather_data_mock sort by localtime_epoch asc limit 500")

# Change the data type of usage column to Douvke
dataframe_energy.withColumn("usage", dataframe_energy.usage.cast(DoubleType()))

# Filter all records with 'Pune India' city as the key
dataframe_weather = dataframe_weather[dataframe_weather["key"] == "Pune India"]

# Join the weather and energy data based on the time column.
joined_df = dataframe_energy.join(dataframe_weather,dataframe_energy["time"] == dataframe_weather["localtime_epoch"],'inner').distinct()

# Format the time datatype so that meaningful values are displayed on the graph
joined_df = joined_df.withColumn('localtime_epoch', f.date_format(joined_df.localtime_epoch.cast(dataType=t.TimestampType()), "yyyy-MM-dd hh-mm-ss"))

#Filter for a particular date
daily_df = joined_df.filter(joined_df.localtime_epoch.contains('2022-11-06'))
display(daily_df)






# COMMAND ----------

from matplotlib.dates import DateFormatter

# Store all usage column values into y_axis_val. This will be the y-axis.
y_axis_val = [val.usage for val in joined_df.select('usage').collect()]

# Store all the temperature in Farenheit values in x_ts. This will be the x-axis.
x_ts = [val.temp_f for val in joined_df.select('temp_f').collect()]

# Creating scatter plot by inputing the x and y axis values
plt.scatter(x_ts, y_axis_val)

# Set the scatter plot size
plt.rcParams["figure.figsize"] = (20,5)

# Set the y-axis label
plt.ylabel('Energy Usage')

# Set the x-axis label
plt.xlabel('Temperature (F)')

# Set the graph title
plt.title('Energy Usage vs Temperature')

#Plot the graph.
plt.show()


#Plot graph time vs monthly energy usage

y_axis_val = [val.usage for val in joined_df.select('usage').collect()]
x_ts = [val.localtime_epoch for val in joined_df.select('localtime_epoch').collect()]
plt.plot(x_ts, y_axis_val)
plt.rcParams["figure.figsize"] = (20,5)
plt.ylabel('Energy Usage')
plt.xlabel('LocalTime')
plt.title('Monthly Energy Usage vs Time')
date_form = DateFormatter("%m-%d")
plt.gca().xaxis.set_major_formatter(date_form)
plt.xticks(rotation=90, fontweight='light',  fontsize='x-small',)
plt.show()

#Plot graph time vs daily energy usage

y_axis_val = [val.usage for val in daily_df.select('usage').collect()]
x_ts = [val.localtime_epoch for val in daily_df.select('localtime_epoch').collect()]
plt.plot(x_ts, y_axis_val)
plt.rcParams["figure.figsize"] = (20,5)
plt.ylabel('Energy Usage')
plt.xlabel('LocalTime')
plt.title('Daily Energy Usage vs Time')
plt.xticks(rotation=45, fontweight='light',  fontsize='x-small',)
plt.show()




# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator


# Create dataframes for energy consumption and weather data
energy = spark.sql("select * from energy_consumption_mock")
weather = spark.sql("select * from weather_data_mock")

# Filter records by 'Pune India' city.
weather = weather[dataframe_weather["key"] == "Pune India"]

# Join weather and energy dataframes by time attribute
joined = dataframe_energy.join(dataframe_weather,dataframe_energy["time"] == dataframe_weather["localtime_epoch"],'inner')

# Select usage attribute and label it as 'label'
data2 = joined.select(joined.temp_f,joined.usage.alias('label'))

# Split the data - 70% train and 30% split
train, test = data2.randomSplit([0.7,0.3])

# Create vector assembler to vectorize the input and output columns
assembler = VectorAssembler().setInputCols(['temp_f',]).setOutputCol('features')
train01 = assembler.transform(train)

# Select features and label columns
train02 = train01.select("features","label")

# Create Linear Regression Model
lr = LinearRegression()

# Fit the training data into the model
model = lr.fit(train02)
print(model.summary)

# Vectorize and select features and label column from test data
test01 = assembler.transform(test)
test02 = test01.select('features', 'label')
test03 = model.transform(test02)
test03.show(truncate=False)

# Evaluate the model using different metrics such as r^2 and mean square error
evaluator = RegressionEvaluator()
print(evaluator.evaluate(test03,
{evaluator.metricName: "r2"})
)

print(evaluator.evaluate(test03,
{evaluator.metricName: "mse"})
)

#Plot the graph - actual data vs predicted data
y_axis_val = [val.label for val in test03.select('label').collect()]
x_ts = [val.features for val in test03.select('features').collect()]
plt.scatter(x_ts, y_axis_val)
y_axis_val = [val.prediction for val in test03.select('prediction').collect()]
plt.scatter(x_ts, y_axis_val)
plt.rcParams["figure.figsize"] = (20,5)
plt.ylabel('Energy Usage')
plt.xlabel('Temperature (F)')
plt.title('Energy Usage vs Temperature')
plt.show()





# COMMAND ----------

from pyspark.sql.functions import col, unix_timestamp, current_date
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StringType,IntegerType,FloatType,DateType

df = (spark.read
  .format("json")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("s3://adbms.project.data/energy_consumption")
)

display(df)
houseId = df.collect()[1][0]
df=df.withColumn("time", current_date().cast("string"))
filtered_df = df.filter("key ==" + houseId)
filtered_df  = filtered_df.withColumn("time", col("time").cast("timestamp"))
display(filtered_df)

trainDF, testDF = filtered_df.randomSplit([.8, .2], seed=42)
print(f'There are {trainDF.cache().count()} records in the training dataset.')
print(f'There are {testDF.cache().count()} records in the testing dataset.')

vecAssembler = VectorAssembler(inputCols=['time', 'usage'], outputCol="vectorValues")
vecTrainDF = vecAssembler.transform(trainDF)
 
display(vecTrainDF)







# COMMAND ----------

import pandas as pd

dataframe = spark.sql("select * from energy_consumption")
dataframe_weather = spark.sql("select * from weather_data")
block_df = dataframe.toPandas()
weather_df = dataframe_weather.toPandas()
''''block_df = pandas_df.set_index("time")
block_df.index = block_df.index.astype("datetime64[ns]")'''
block_df.rename(columns={'time': 'localtime_epoch'}, inplace=True)
#display(block_df.head())
block_df = block_df[block_df["usage"] != "Null"]
block_df["usage"] = block_df["usage"].astype("float64")
block_df = block_df[block_df["key"] == "37648475" ]
#block_df.plot(y="usage", figsize=(12, 4))
#block_df.plot(kind='barh',x='location',y='usage',colormap='winter_r')
#block_df.hist('usage', bins = 100)

weather_df = weather_df[weather_df["key"] == "Fullerton US"]
#display(weather_df.head())
#weather_df.plot(y="temp_f", x="localtime", figsize=(12, 4))
#weather_df.plot(y="pressure_in", x="localtime", figsize=(12, 4))
#joined_df = block_df.join(weather_df,block_df["localtime_epoch"] == weather_df["localtime_epoch"]).show()

display(joined_df)




