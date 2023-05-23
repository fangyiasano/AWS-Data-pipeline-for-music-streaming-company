# Building Automated Data Pipelines with Apache Airflow

## Project Overview

The focus for this project was on utilizing Apache Airflow to orchestrate and monitor data warehouse ETL pipelines, ensuring smooth and efficient data flow from source to storage, and maintaining the high quality of data for subsequent analysis.

## Diving Into the Data

The project revolves around two JSON data sources - logs detailing user activity in the app and metadata about songs users listen to. This data is hosted on Amazon S3 and the objective is to process and store this data in a Redshift data warehouse.

## Approach & Implementation

### 1. Data Analysis & Transfer

Initially, I spent time understanding the structure and contents of the two JSON data sources. Subsequently, I created my own S3 directories and transferred the data from the provided sources to my own S3 bucket using AWS CLI. 

### 2. Airflow DAG Configuration

The next step was to configure the Directed Acyclic Graph (DAG) in Apache Airflow. This involved setting parameters to control the DAG's behavior such as enabling task retries on failures, ensuring no dependency on past runs, and turning off catchup.

### 3. Airflow Operators

The crux of the project involved creating four custom Airflow operators to automate the ETL process:

- **Stage Operator**: This operator was responsible for loading the JSON files from S3 to Redshift.
- **Fact and Dimension Operators**: I developed these operators to handle data transformations and loading using SQL queries.
- **Data Quality Operator**: To ensure the integrity of the data, this operator performed checks on the transformed data.

### 4. Data Quality Checks

Finally, to ensure the quality of data, I incorporated tests to catch any discrepancies in the datasets after the ETL process was complete.

## Conclusion & Learning

This project was a deep dive into building automated, robust, and dynamic data pipelines using Apache Airflow. Not only did I get to develop reusable tasks and operators, but I also had the chance to ensure data quality through thorough checks.
