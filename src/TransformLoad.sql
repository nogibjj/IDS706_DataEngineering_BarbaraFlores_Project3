-- Databricks notebook source
CREATE OR REPLACE TABLE CleanUniversalTopSpotifySongs AS
SELECT
  id
  ,spotify_id
  ,UPPER(name) AS upper_name
  ,UPPER(artists) AS upper_artists
  ,daily_rank
  ,daily_movement
  ,weekly_movement
  ,country
  ,snapshot_date
  ,popularity
  ,is_explicit
  ,duration_ms
  ,UPPER(album_name) AS album_name
  ,DATE(album_release_date) AS album_release_date
  ,danceability
  ,energy
  ,key
  ,loudness
  ,mode
  ,speechiness
  ,acousticness
  ,instrumentalness
FROM RawUniversalTopSpotifySongs

-- COMMAND ----------

SELECT * FROM CleanUniversalTopSpotifySongs
