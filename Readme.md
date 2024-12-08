# Project Title: Restaurant and Weather Data Enrichment

## Overview
This project demonstrates how to join restaurant data with weather data using Apache Spark. It includes data preprocessing, geohashing, enrichment using an external API, and saving the enriched data to disk in Parquet format.

## Steps Completed
1. **Data Preprocessing**: Handling invalid lat/lng and using OpenCage API for missing data.
2. **Geohashing**: Generating a 4-character geohash for restaurant locations.
3. **Data Enrichment**: Joining restaurant and weather data based on geohash.
4. **Output**: Saving the enriched data as Parquet files with partitioning.

## Requirements
- Apache Spark 3.4.0 or above
- Java 8 or above
- Maven for dependency management

## Setup and Run
1. Set up your Spark environment.
2. Download the input CSV and Parquet files.
3. Add necessary dependencies to `pom.xml` or use a suitable build tool.
4. Run the project using your IDE or Maven.

## Results
The final output is saved in the local file system in Parquet format, with partitioning by geohash.

## Repository Link

