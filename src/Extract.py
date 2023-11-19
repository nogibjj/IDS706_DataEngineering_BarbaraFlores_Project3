# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

my_db=pd.read_csv("../data/UniversalTopSpotifySongs.csv")
spark_db=spark.createDataFrame(my_db)


# COMMAND ----------

file_path=f'/FileStore/tables/{"UniversalTopSpotifySongs"}.csv'
spark_db.write.mode('overwrite').csv(file_path)

# COMMAND ----------

file_path = '/FileStore/tables/UniversalTopSpotifySongs.delta'
spark_db.write.format("delta").mode("overwrite").save(file_path)

# COMMAND ----------

from pyspark.sql import SparkSession
spark=SparkSession.builder.getOrCreate()

# COMMAND ----------

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField, LongType

schema = StructType([
  StructField("id", LongType(),  True),
  StructField("spotify_id", StringType(),  True),
  StructField("name", StringType(),  True),
  StructField("artists", StringType(),  True),
  StructField("daily_rank", LongType(),  True),
  StructField("daily_movement", LongType(),  True),
  StructField("weekly_movement", LongType(),  True),
  StructField("country", StringType(),  True),
  StructField("snapshot_date", StringType(),  True),
  StructField("popularity", LongType(),  True),
  StructField("is_explicit", StringType(),  True),
  StructField("duration_ms", LongType(),  True),
  StructField("album_name", StringType(),  True),
  StructField("album_release_date", StringType(),  True),
  StructField("danceability", DoubleType(),  True),
  StructField("energy", DoubleType(),  True),
  StructField("key", LongType(),  True),
  StructField("loudness", DoubleType(),  True),
  StructField("mode", LongType(),  True),
  StructField("speechiness", DoubleType(),  True),
  StructField("acousticness", DoubleType(),  True),
  StructField("instrumentalness", DoubleType(),  True),
  StructField("liveness", DoubleType(),  True),

])

csv_file_path="dbfs:/FileStore/tables/UniversalTopSpotifySongs.csv"
UniversalTopSpotifySongs=spark.read.csv(csv_file_path, header=True, schema=schema)

# COMMAND ----------

UniversalTopSpotifySongs.select("spotify_id","name","artists","daily_movement","weekly_movement","album_name","album_release_date","daily_rank").show()


# COMMAND ----------

UniversalTopSpotifySongs.write.mode("overwrite").saveAsTable("RawUniversalTopSpotifySongs")
