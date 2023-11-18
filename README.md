[![Format](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/actions/workflows/format.yml/badge.svg)](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/actions/workflows/format.yml)
[![Lint](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/actions/workflows/lint.yml/badge.svg)](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/actions/workflows/lint.yml)
[![Install](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/actions/workflows/install.yml/badge.svg)](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/actions/workflows/install.yml)
[![Test](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/actions/workflows/test.yml/badge.svg)](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Project3/actions/workflows/test.yml)



IDS706_DataEngineering_BarbaraFlores_Project3
# 📂  Databricks ETL Pipeline

The main goal of this project is to develop a meticulously documented Databricks notebook that performs ETL operations, ensuring effective use of Delta Lake for data storage, and employing Spark SQL for the necessary transformations.


## 📌 References

This project is guided by the official [Databricks Data Pipeline documentation](https://docs.databricks.com/en/getting-started/data-pipeline-get-started.html). Please refer to this documentation for detailed information and best practices.

## 🎵 Dataset

In this project, we will use a dataset suggested as an example in the Databricks documentation. This dataset is a subset of the Million Song Dataset, containing features and metadata for contemporary music tracks. You can access this dataset in the [sample datasets](https://docs.databricks.com/en/dbfs/databricks-datasets.html#databricks-datasets-databricks-datasets) included in the Databricks workspace.

## 🛠️ Project Workflow Steps
The steps carried out to execute this project are:

### Step 1: Create a cluster
For the data processing and analysis conducted in this example, a cluster was established to provide the essential computing resources for command execution. This cluster creation is fundamental for efficiently running Databricks commands, ensuring seamless operations throughout the data pipeline.

![00](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Miniproject11/main/images/00.png)

### Step 2: Explore the source data
To learn how to use the Databricks interface to explore raw source data, commands from Databricks Utilities and PySpark were executed in a notebook. The goal was to examine both the source data and associated artifacts.

Data exploration took place in the notebook [Explore songs data.py](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Miniproject11/blob/main/Explore%20songs%20data.py).

![01](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Miniproject11/main/images/01.png)

### Step 3: Ingest Raw Data

In this step, the raw data is loaded into a table using the script [Ingest songs data.py](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Miniproject11/blob/main/Ingest%20songs%20data.py). This process makes the data accessible for subsequent processing. However, during our data exploration, we observed that the header is not stored with the data. Therefore, it becomes necessary to explicitly define the schema, as demonstrated in the following example.

![02](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Miniproject11/main/images/02.png)

### Step 4: Prepare the raw data
During this stage, the preparation of raw songs data for analysis took place. The process, detailed in [Prepare_songs_data.sql](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Miniproject11/blob/main/Prepare_songs_data.sql), involved filtering out unnecessary columns and introducing a new field with a timestamp for the creation of each new record.

![03](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Miniproject11/main/images/03.png)

### Step 5: Query the transformed data
In this stage, the processing pipeline is extended by adding queries to analyze the songs data. These queries leverage the prepared records generated in the preceding step. The SQL queries utilized for this analysis can be found in [Analyze songs data.sql](https://github.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Miniproject11/blob/main/Analyze%20songs%20data.sql).

![04](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Miniproject11/main/images/04.png)


![05](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Miniproject11/main/images/05.png)

### Step 6: Create a Databricks job to run the pipeline

This step involves creating a Databricks job to automate the execution of the data ingestion, processing, and analysis steps.

![06](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Miniproject11/main/images/06.png)

![07](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Miniproject11/main/images/07.png)

### Step 7: Schedule the data pipeline job

In this stage, a schedule is established for the Databricks job responsible for executing the entire data pipeline in an automated and regular manner.

![08](https://raw.githubusercontent.com/nogibjj/IDS706_DataEngineering_BarbaraFlores_Miniproject11/main/images/08.png)


