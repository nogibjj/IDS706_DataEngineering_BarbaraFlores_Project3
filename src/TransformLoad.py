# Databricks notebook source
# MAGIC %md
# MAGIC ### Delta Table

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql import SparkSession
# MAGIC from pyspark.sql.functions import upper, date_format
# MAGIC spark = SparkSession.builder.appName("Delta").getOrCreate()
# MAGIC
# MAGIC delta_file_path = '/FileStore/tables/UniversalTopSpotifySongs.delta'
# MAGIC delta_df = spark.read.format("delta").load(delta_file_path)
# MAGIC
# MAGIC
# MAGIC clean_delta_df = (delta_df
# MAGIC     .select(
# MAGIC         "id",
# MAGIC         "spotify_id",
# MAGIC         upper("name").alias("upper_name"),
# MAGIC         upper("artists").alias("upper_artists"),
# MAGIC         "daily_rank",
# MAGIC         "daily_movement",
# MAGIC         "weekly_movement",
# MAGIC         "country",
# MAGIC         "snapshot_date",
# MAGIC         "popularity",
# MAGIC         "is_explicit",
# MAGIC         "duration_ms",
# MAGIC         upper("album_name").alias("album_name"),
# MAGIC         date_format("album_release_date", "yyyy-MM-dd").alias("album_release_date"),
# MAGIC         "danceability",
# MAGIC         "energy",
# MAGIC         "key",
# MAGIC         "loudness",
# MAGIC         "mode",
# MAGIC         "speechiness",
# MAGIC         "acousticness",
# MAGIC         "instrumentalness"
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC clean_delta_df.write.format("delta").mode("overwrite").save('/FileStore/tables/CleanUniversalTopSpotifySongs.delta')
# MAGIC clean_delta_df.show()
