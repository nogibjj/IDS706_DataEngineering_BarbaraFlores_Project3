-- Databricks notebook source
-- MAGIC %md
-- MAGIC Let's see which artists have the highest presence in the top 50 of Spotify during the evaluation period

-- COMMAND ----------

SELECT 
  upper_artists, 
  COUNT(*) AS records_count
FROM CleanUniversalTopSpotifySongs
GROUP BY upper_artists
ORDER BY records_count DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's check the top Spotify songs for November 5, 2023.

-- COMMAND ----------

SELECT 
  country, 
  upper_artists,
  upper_name,
  daily_rank,
  snapshot_date
FROM CleanUniversalTopSpotifySongs
WHERE country = "US" AND snapshot_date = "2023-11-05"
ORDER BY daily_rank DESC


-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import seaborn as sns
-- MAGIC
-- MAGIC df_s = spark.sql("SELECT upper_artists, COUNT(*) AS records_count FROM CleanUniversalTopSpotifySongs GROUP BY upper_artists ORDER BY records_count DESC LIMIT 5 ")
-- MAGIC db_p=df_s.toPandas()
-- MAGIC
-- MAGIC db_p = df_s.toPandas()
-- MAGIC
-- MAGIC plt.figure(figsize=(12, 6))
-- MAGIC sns.barplot(x='upper_artists', y='records_count', data=db_p)
-- MAGIC
-- MAGIC plt.xticks(rotation=45, ha='right')
-- MAGIC
-- MAGIC
-- MAGIC plt.xlabel('Artist')
-- MAGIC plt.ylabel('Number of Records')
-- MAGIC plt.title('Number of Records per Artist')
-- MAGIC
-- MAGIC plt.tight_layout()
-- MAGIC plt.show()
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_s2 = spark.sql("SELECT upper_artists, snapshot_date, mean(daily_rank) as mean_daily_rank FROM CleanUniversalTopSpotifySongs WHERE upper_artists in ('TAYLOR SWIFT','BAD BUNNY','DOJA CAT','BIZARRAP, MILO J','TATE MCRAE','IÑIGO QUINTERO') GROUP BY upper_artists, snapshot_date")
-- MAGIC db_p2 = df_s2.toPandas()
-- MAGIC db_p2

-- COMMAND ----------

SELECT 
  upper_artists,
  snapshot_date,
  mean(daily_rank) as mean_daily_rank
FROM CleanUniversalTopSpotifySongs
WHERE upper_artists in ("TAYLOR SWIFT","BAD BUNNY","DOJA CAT","BIZARRAP, MILO J","TATE MCRAE","IÑIGO QUINTERO")
GROUP BY upper_artists, snapshot_date

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import seaborn as sns
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC
-- MAGIC db_p2['snapshot_date_short'] = db_p2['snapshot_date']
-- MAGIC
-- MAGIC plt.figure(figsize=(12, 6))
-- MAGIC sns.lineplot(x='snapshot_date_short', y='mean_daily_rank', hue='upper_artists', data=db_p2)
-- MAGIC
-- MAGIC plt.xticks(rotation=45, ha='right')
-- MAGIC plt.legend( loc='upper left')
-- MAGIC
-- MAGIC plt.xlabel('Snapshot Date')
-- MAGIC plt.ylabel('Mean Daily Rank')
-- MAGIC plt.title('Mean Daily Rank Over Time for Top 5 Artists')
-- MAGIC
-- MAGIC plt.tight_layout()
-- MAGIC plt.show()
-- MAGIC
