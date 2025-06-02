# 📊 Loan Application Analytics Pipeline — Spring Financial

## 🚀 Project Overview

This project demonstrates a robust data ingestion and correction pipeline for **Spring Financial**, built using **Snowflake**, **S3**, and **Snowpipe**. It tracks loan applications and customer demographic data while handling real-world scenarios like **file-level data correction**.

---

## 🎯 Objective

To design a production-grade analytics pipeline that:

- Ingests loan application and customer demographic data from AWS S3 into Snowflake using **Snowpipe**.
- Automatically captures file-level metadata (e.g., `METADATA$FILENAME`, `METADATA$START_SCAN_TIME`) during ingestion.
- Supports **post-ingestion data correction**, allowing removal and re-processing of faulty files.

---

## 🧩 Components

| Component          | Description                                                                 |
|--------------------|-----------------------------------------------------------------------------|
| **Snowflake**      | Cloud data warehouse for ingestion, storage, and querying.                  |
| **AWS S3**         | File storage for raw `.csv` data files.                                     |
| **Snowpipe**       | Auto-ingestion of files from S3 to Snowflake tables.                        |
| **Staging Tables** | Temporary tables to hold ingested data with metadata for audit/correction.  |
| **Metadata Columns** | Tracks `METADATA$FILENAME`, `METADATA$START_SCAN_TIME`, etc.              |

---

## 🏗️ Pipeline Workflow

```mermaid
graph TD;
    A[S3: Upload Loan Data] --> B[Snowpipe: Auto Ingest];
    B --> C[Staging Table in Snowflake];
    C --> D[Persist to Final Table];
    D --> E[Audit / Validate Ingestion];
    E --> F[Identify Faulty Files];
    F --> G[Delete Faulty Records];
    G --> H[Upload Corrected Files to S3];
    H --> B

---
![image](https://github.com/user-attachments/assets/db010668-d252-4c59-a651-0312d301fa2e)

