
# Wajbah Data Platform

## Overview
Wajbah is a comprehensive cloud-native data platform built on Microsoft Azure. It is designed to ingest, process, store, and visualize operational data efficiently. The pipeline implements a Medallion Architecture (Bronze/Silver) pattern to ensure data quality and accessibility for business intelligence and analytics.

## Architecture

![Infrastructure Diagram](Wajbah%20Data%20Platforom.drawio.png)

The platform follows a linear Extract-Load-Transform-Load (ELTL) workflow:

1.  **Ingestion:** Data is extracted from the Operational Database using Azure Data Factory.
2.  **Raw Storage (Bronze):** Raw data is loaded into Azure Data Lake Storage Gen2.
3.  **Transformation:** Azure Databricks (Spark) processes and cleans the data using **Delta Live Tables**.
4.  **Refined Storage (Silver):** Transformed data is stored back into Azure Data Lake Storage Gen2.
5.  **Serving:** Azure Synapse Analytics acts as the serving layer using **External Tables** for cost-efficiency.
6.  **Visualization:** Insights are delivered via Power BI and Looker Studio.

## Technology Stack
### Cloud Provider
* **Microsoft Azure**

### Core Services
* **Orchestration & Ingestion:** Azure Data Factory (ADF)
* **Storage:** Azure Data Lake Storage Gen2 (ADLS Gen2)
* **Processing:** Azure Databricks (Apache Spark / Delta Live Tables)
* **Data Warehousing:** Azure Synapse Analytics (SQL Pools)
* **Visualization:** Microsoft Power BI, Looker Studio

## Key Architectural Decisions

### 1. Delta Live Tables (DLT) for Streaming Pipelines
We utilize **Delta Live Tables** within Azure Databricks for the transformation layer.
* **Streaming & Micro-batching:** DLT allows us to treat data as a continuous stream, enabling near real-time data availability without managing complex state or checkpoints manually.
* **Reliability:** It automatically handles infrastructure complexity, recovery, and dependency management.
* **Quality Constraints:** DLT enables us to define quality expectations directly in the pipeline code, preventing bad data from polluting downstream tables.

### 2. Synapse External Tables (Decoupled Storage & Compute)
Instead of traditionally loading all data into Synapse's internal storage, we leverage **External Tables** pointing directly to the Silver layer in ADLS Gen2.
* **Cost Reduction:** We avoid "Double Storage" costs. Data is stored cheaply in ADLS Gen2 and is not duplicated into Synapse storage.
* **Pay-for-Compute:** We leverage Synapse SQL Pools primarily as a compute engine. This model aligns with a modern data lakehouse approach where the Data Lake is the single source of truth, and the warehouse is the high-performance query engine.

## Pipeline Stages

### 1. Extract (Ingestion)
* **Source:** Operational Database (On-premise or Cloud).
* **Tool:** Azure Data Factory.
* **Action:** ADF pipelines connect to the source system and perform raw data ingestion.
* **Destination:** The raw data is landed in the `Bronze` container of the Data Lake.

### 2. Load (Bronze Layer)
* **Format:** Parquet/Avro/Delta (depending on specific implementation).
* **Purpose:** Acts as a landing zone for raw, immutable history of data.

### 3. Transform (Processing)
* **Tool:** Azure Databricks (Delta Live Tables).
* **Logic:**
    * Reads data from the Bronze layer using streaming inputs.
    * Applies data cleansing, validation, and schema enforcement via DLT expectations.
    * Performs business logic transformations.
* **Destination:** Processed data is written to the `Silver` container.

### 4. Storage (Silver Layer)
* **Format:** Delta Lake format is recommended for ACID transactions and time travel capabilities.
* **Purpose:** Contains "Enterprise Truth" data—clean, filtered, and augmented.

### 5. Load (Serving Layer)
* **Tool:** Azure Synapse Analytics (Serverless or Dedicated SQL Pools).
* **Action:** External Tables are defined over the `Silver` Delta Lake files.
* **Mechanism:** Synapse reads directly from ADLS Gen2 during query time. This eliminates the need for a heavy `COPY` command and keeps data storage consolidated.

### 6. Visualization
* **Power BI:** Connects to Azure Synapse for interactive dashboards and reporting.
* **Looker Studio:** Utilized for ad-hoc reporting and alternative visualization needs.

## Cost Optimization Summary
**Technology**	     **Benefit**
ADLS Gen2	        Low-cost, scalable storage
Delta Live Tables	  Automated optimization and incremental processing
Databricks	        Compute used only during execution
Synapse Serverless  Pay only per query
External Tables	  No double storage, no warehouse compute

Overall savings:

**Compute:** from 11 USD/hour to 1.10 USD/hour

**Storage:** from 23 USD/TB to 0.50 USD/TB
