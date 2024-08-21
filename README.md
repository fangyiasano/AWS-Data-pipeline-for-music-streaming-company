# Building Automated Data Pipelines with Apache Airflow

## Project Overview

This project leverages Apache Airflow to automate and optimize the monitoring of ETL pipelines within a music streaming company’s data warehouse on Amazon Redshift. The goal is to create high-quality, dynamic data pipelines that facilitate effective monitoring, support seamless data backfilling, and enforce stringent data quality checks.

Key operations include extracting JSON-formatted user activity logs and song metadata from S3, transforming this data into a structured format in Redshift, and performing comprehensive post-load quality checks to ensure data integrity and reliability.

## Features

**Dynamic and Reusable Tasks**: Using Airflow's DAGs to create flexible and modular tasks.
**Data Quality Assurance**: Implementing checks to validate the integrity and accuracy of the data warehouse after ETL operations.
**Backfill Capability**: Designed to handle backfills effectively, ensuring data completeness for historical analysis.
**Comprehensive Monitoring**: Utilizing Airflow's UI to monitor pipeline operations and performance.

## Datasets

Two main datasets are used in this project:

**Log Data**: JSON logs that capture user activity on the music streaming company’s app.
**Song Data**: JSON metadata that provides information about the songs users listen to.

## Components

### 1. DAG Configuration

- **Default Parameters**:
  - No dependencies on past runs.
  - Tasks retry 3 times on failure.
  - Retries happen every 5 minutes.
  - Catchup is turned off.
  - No email notifications on retries.

- **Task Dependencies**: 
  The tasks are linked to form a coherent data flow as outlined in the image below.
![dag_graph](https://github.com/user-attachments/assets/9e1eb3a5-6d35-4161-9707-25f7832d3914)

### 2. Custom Operators

- **Stage Operator**: 
  This operator is responsible for loading JSON files from S3 into Amazon Redshift. It generates and executes a SQL COPY statement based on the parameters you provide and can differentiate between different types of JSON files. Additionally, it supports loading timestamped files based on execution time, allowing for the backfilling of historical data.

- **Fact and Dimension Operators**: 
 These operators are used for data transformations. You can provide them with a SQL statement, a target database, and an optional target table. For dimension tables, it uses the truncate-insert method to update data and allows switching between different insert modes. For fact tables, it supports appending new data without removing existing records.

### 3. Data Quality Checks

- **Data Quality Operator**:
 This operator is used to check the quality of the data. It validates the data using SQL test cases and expected results. If the results do not meet expectations, it raises an exception and initiates task retries and eventual failure.

## Prerequisites
Ensure you have the following installed and configured:

- Python 3.6 or higher
- Apache Airflow 2.x
- Access to AWS services including S3 and Redshift

## Implementation

### 1. Initiating the Airflow Web Server
### 2. Configuring connections in the Airflow Web Server UI
After logging in, go to Admin > Connections to set up the necessary connections, such as aws_credentials and redshift. Be sure to launch your Redshift cluster through the AWS console. Once these steps are complete, trigger your DAG to verify that all tasks execute successfully.
