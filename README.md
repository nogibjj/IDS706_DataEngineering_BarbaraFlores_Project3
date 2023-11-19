[![Format](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/actions/workflows/format.yml/badge.svg)](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/actions/workflows/format.yml)
[![Install](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/actions/workflows/install.yml/badge.svg)](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/actions/workflows/install.yml)
[![Test](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/actions/workflows/test.yml/badge.svg)](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/actions/workflows/test.yml)
[![Lint](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/actions/workflows/test.yml/badge.svg)](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/actions/workflows/Lint.yml)



IDS706_DataEngineering_BarbaraFlores_Project3
# 📂  Databricks ETL Pipeline

The main goal of this project is to develop a meticulously documented Databricks notebook that performs ETL operations, ensuring effective use of Delta Lake for data storage, and employing Spark SQL for the necessary transformations.

## 🎥 Video Tutorial
The following [YouTube link](https://youtu.be/7IdvxX5F508) shows a clear, concise walkthrough and demonstration of the project


## 📊 Database

For this project, we will utilize the [Top Spotify Songs in 73 Countries](https://www.kaggle.com/datasets/asaniczka/top-spotify-songs-in-73-countries-daily-updated/) dataset. This dataset provides a comprehensive view of the top songs trending in over 70 countries, offering valuable insights into the dynamics of the music industry. It includes a wide range of information on the most popular songs in the world, such as unique Spotify identifiers, song names, artists, daily rankings, daily movement in rankings, and more.

This dataset comprises 25 variables (columns) and was extracted on 2023-11-05. In total, it contains 72.959 records.

## 🚀 Delta Lake

For this project, we chose to utilize Delta Lake as our data storage system. Delta Lake provides key benefits that enhance the integrity and efficiency of our ETL pipeline. Among its advantages are support for atomic transactions, ensuring data consistency during write operations; the ability to perform incremental writes and version data for easy tracking and recovery; and optimized performance for both read and write operations, crucial for handling large volumes of data efficiently. The implementation of Delta Lake reflects our commitment to the quality and reliability of our pipeline, ensuring data consistency and accessibility over time.

![01.png](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/main/images/01.png)

![02.png](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/main/images/02.png)

![03.png](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/main/images/03.png)

![04.png](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/main/images/04.png)

![05.png](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/main/images/05.png)

![06.png](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/main/images/06.png)

![07.png](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/main/images/07.png)

![08.png](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/main/images/08.png)

![09.png](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/main/images/09.png)

![10.png](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/main/images/10.png)

![11.png](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/main/images/11.png)

![12.png](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/main/images/12.png)

![13.png](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/main/images/13.png)

![14.png](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/main/images/14.png)


