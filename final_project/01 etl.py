# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# start_date = str(dbutils.widgets.get('01.start_date'))
# end_date = str(dbutils.widgets.get('02.end_date'))
# hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
# promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

# print(start_date,end_date,hours_to_forecast, promote_model)
# print("YOUR CODE HERE...")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DoubleType

# COMMAND ----------

bike_schema = StructType([
  StructField("ride_id", StringType(), True),
  StructField("rideable_type", StringType(), True),
  StructField("started_at", StringType(), True),
  StructField("ended_at", StringType(), True),
  StructField("start_station_name", StringType(), True),
  StructField("start_station_id", StringType(), True),
  StructField("end_station_name", StringType(), True),
  StructField("end_station_id", StringType(), True),
  StructField("start_lat", StringType(), True),
  StructField("start_lng", StringType(), True),
  StructField("end_lat", StringType(), True),
  StructField("end_lng", StringType(), True),
  StructField("member_casual", StringType(), True)
  
# ride_id:string
# rideable_type:string
# started_at:string
# ended_at:string
# start_station_name:string
# start_station_id:string
# end_station_name:string
# end_station_id:string
# start_lat:string
# start_lng:string
# end_lat:string
# end_lng:string
# member_casual:string
  # more fields as needed
])

# COMMAND ----------

# rawbike_df = spark.readStream.format('csv')\
#     .schema(bike_schema)\
#     .option('sep',',')\
#     .load(BIKE_TRIP_DATA_PATH)
# display(rawbike_df)

# COMMAND ----------

rawbike_df = spark.readStream.format('csv')\
    .schema(bike_schema)\
    .option('sep',',')\
    .option("latestFirst", "true")\
    .csv(BIKE_TRIP_DATA_PATH)
display(rawbike_df)

# COMMAND ----------

from pyspark.sql.functions import input_file_name

# COMMAND ----------

# bikeinfo_df=rawbike_df
# displaybikeinfo_df
df_with_filename = rawbike_df.withColumn("filename", input_file_name())

# COMMAND ----------

bronze_bike_query=df_with_filename.writeStream \
  .format("console") \
  .option("checkpointLocation", f'{GROUP_DATA_PATH}/_checkpoint') \
  .start(f'{GROUP_DATA_PATH}/bike_hist')
# bronze_bike_query.awaitTermination()

# COMMAND ----------

bronze_bike_query.status

# COMMAND ----------

  bronze_bike_query.stop()
# display(bronze_bike_query)

# COMMAND ----------

# streamingQuery = (spark.readStream
#                   .format("csv")
#                   .option("header", "true")
#                   .option("path", f'{GROUP_DATA_PATH})\
#                   .load()\
#                   .writeStream\
#                   .format("delta")\
#                   .option("checkpointLocation", f'{GROUP_DATA_PATH}/_checkpoint')\
#                   .option("path", f'{GROUP_DATA_PATH}')\
#                   .start())

# if not streamingQuery.isActive:
#     streamingQuery.start()

# COMMAND ----------

weather_schema = StructType([
  StructField("dt", IntegerType(), True),
  StructField("temp", DoubleType(), True),
  StructField("feels_like", DoubleType(), True),
  StructField("pressure", IntegerType(), True),
  StructField("humidity", IntegerType(), True),
  StructField("dew_point", DoubleType(), True),
  StructField("uvi", DoubleType(), True),
  StructField("clouds", IntegerType(), True),
  StructField("visibility", IntegerType(), True),
  StructField("wind_speed", DoubleType(), True),
  StructField("wind_deg", IntegerType(), True),
  StructField("pop", DoubleType(), True),
  StructField("snow_1h", DoubleType(), True),
  StructField("id", IntegerType(), True),
  StructField("main", StringType(), True),
  StructField("description", StringType(), True),
  StructField("icon", StringType(), True),
  StructField("loc", StringType(), True),
  StructField("lat", DoubleType(), True),
  StructField("lon", DoubleType(), True),  
  StructField("timezone", StringType(), True),
  StructField("timezone_offset", IntegerType(), True),  
# dt:integer
# temp:double
# feels_like:double
# pressure:integer
# humidity:integer
# dew_point:double
# uvi:double
# clouds:integer
# visibility:integer
# wind_speed:double
# wind_deg:integer
# pop:double
# snow_1h:double
# id:integer
# main:string
# description:string
# icon:string
# loc:string
# lat:double
# lon:double
# timezone:string
# timezone_offset:integer
  # more fields as needed
])

# COMMAND ----------

rawweather_df = spark.readStream.format("csv")\
    .option("header", "true") \
    .schema(weather_schema)\
    .option("path", "dbfs:/FileStore/tables/raw/weather/*.csv") \
    .load()

# COMMAND ----------

# display(rawweather_df)

# COMMAND ----------

query=rawweather_df.writeStream \
  .outputMode("append")\
  .format("delta") \
  .trigger(processingTime="1 second")\
  .option("checkpointLocation", f'{GROUP_DATA_PATH}') \
  .start(f'{GROUP_DATA_PATH}\weather_data')

# COMMAND ----------

query.stop()

# COMMAND ----------

# from delta.tables import DeltaTable

# COMMAND ----------

# from pyspark.sql import SparkSession

# COMMAND ----------

# spark = SparkSession.builder.appName("DeltaTableMove").getOrCreate()

# COMMAND ----------

# delta_table = DeltaTable.forPath(BRONZE_STATION_INFO_PATH, "dbfs:/FileStore/tables/bronze_station_info.delta")
# ALTER TABLE bronze_station_info SET LOCATION 'dbfs:/FileStore/tables/G07/'
stationinfo_delta_table = spark.read.load(BRONZE_STATION_INFO_PATH)

# COMMAND ----------

display(stationinfo_delta_table)

# COMMAND ----------

stationinfo_delta_table.write.format("parquet").save(f'{GROUP_DATA_PATH}/bronze_station_info')

# COMMAND ----------

stationstatus_delta_table = spark.read.load(BRONZE_STATION_STATUS_PATH)

# COMMAND ----------

display(stationstatus_delta_table)

# COMMAND ----------

stationstatus_delta_table.write.format("parquet").save(f'{GROUP_DATA_PATH}/bronze_station_status')

# COMMAND ----------

nycweather_delta_table = spark.read.load(BRONZE_NYC_WEATHER_PATH)

# COMMAND ----------

display(nycweather_delta_table)

# COMMAND ----------

nycweather_delta_table.write.format("parquet").save(f'{GROUP_DATA_PATH}/bronze_weather')

# COMMAND ----------

# spark.sql(f"ALTER TABLE delta.`{stationinfo_delta_table}` SET LOCATION 'dbfs:/FileStore/tables/G07/'")

# COMMAND ----------

files=dbutils.fs.ls(f'{BRONZE_NYC_WEATHER_PATH}')
# files=dbutils.fs.ls(f'{GROUP_DATA_PATH}/bronze_station_info')

# COMMAND ----------

count=0
for file in files:
    count+=1
print(count)
#     print(file.name)

# COMMAND ----------

# stationinfo_df.write.format("delta").save("dbfs:/FileStore/tables/G07/")

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
