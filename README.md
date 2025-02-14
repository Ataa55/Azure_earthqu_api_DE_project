# Azure Earthquake Data Engineering Cloud Project
![_Azure_DateBricks_api_eTe_project](https://github.com/user-attachments/assets/aa735d7e-3fb0-44a6-97db-4d8fc14d9672)
## Project Overview

This project involves extracting earthquake data from the USGS Earthquake API, landing it within Azure Data Lake Storage Gen2, processing it using Azure Data Factory, and Databricks. The processed data is then made available for analytics using Azure Synapse Analytics.

## Data Extraction

The data is extracted from the following API:
[USGS Earthquake API](https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2014-01-01&endtime=2014-01-02)

This API returns detailed data about earthquakes that occurred within the specified date range. Using Azure Data Factory (ADF), the data is dynamically extracted by passing parameters for the start and end dates.

![eartqu_api_adf_pipeline](https://github.com/user-attachments/assets/12054e60-f03e-4b4c-ad42-8794d02448ce)


## Data Ingestion

Azure Data Factory writes the extracted data to the Azure Data Lake Storage Gen2 (ADLS Gen2) bronze bucket in JSON format. The bronze layer holds the raw data in JSON format. Before loading the data to the bronze layer, the bucket is cleared to ensure only fresh new data is present.

## Data Processing

### Bronze to Silver

Using PySpark within Databricks, the `Bronze_to_silver` notebook processes the data by accessing the nested structures in the JSON, exploding arrays to extract the required data, and transforming it into a flat tabular format. This processed data is then written to the silver layer in Parquet format, overwriting the existing data to ensure only the newest data is present.

### Silver to Gold

The `Silver_to_gold` notebook reads the silver data, adds columns to enrich the data and derive more insights for the business, and appends the new data to the existing table in the gold layer. The gold layer contains all the data and is written as a Delta table, which is managed and stored under the Hive metastore.

## Data Analytics

The data in the gold layer is ready for analytics. Azure Synapse Analytics is used to query the data in the gold layer, as it is connected to the ADLS Gen2 service.

## Loading Approach

The loading approach for the gold layer is incremental, as only new data for the last 24 hours is extracted from the API. To ensure data freshness, only data with a creation date greater than the creation date of the gold layer is loaded into the silver layer.

## Handling Pipeline Failures

If the pipeline fails while loading data to the gold layer, Delta tables ensure ACID compliance, so there is no need to worry about partial data writes. Delta Lake supports atomicity, meaning all or none of the data is written. If the pipeline fails, no data is written, and no commit is made to the Delta log.

However, as a precaution, the `on_failure` notebook removes all data with the maximum creation_date, as this represents the last entered data before the failure. This step prevents duplicates before rerunning the pipeline.


## Automation

The entire pipeline is automated using triggers in Azure Data Factory. These triggers ensure that the data extraction, ingestion, and processing steps are executed at scheduled intervals, providing a seamless and efficient workflow.
---

This documentation provides an overview of the project's data extraction, ingestion, processing, and analytics workflow. It also outlines the approach for handling pipeline failures to ensure data integrity.
