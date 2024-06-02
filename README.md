# README

##  [DEMO](https://khiibaedu-my.sharepoint.com/:v:/g/personal/a_iqbal_24529_khi_iba_edu_pk/Efb2xQivzuVJtqZPvjBOnOoBElC1ygtvvyYuviDRi9HAVA?e=tHfuhL) 

## Introduction

This project involves setting up and populating a dummy OLTP (Online Transaction Processing) database and then transforming and loading data into an OLAP (Online Analytical Processing) or Data Warehouse for analysis. The main focus is on the flight operations data, which will allow dimensional analysis of various flight operations.

## Instructions

### 1. Database Setup

- **PostgreSQL** is the DBMS used for both OLTP and Data Warehouse databases.
- The OLTP database should be created based on the data provided in the `spaghetti` folder.
- The Data Warehouse will store the transformed and cleaned data for analytical purposes.

### 2. Python Script Configuration

- The ETL process is implemented in the `python_scripts/ETL` script. You need to update the database connection credentials for both source (OLTP) and destination (Data Warehouse) databases.
- Ensure the PostgreSQL server is running and accessible.

### 3. OLTP Data Insertion

- Use the `spaghetti_data_final.sql` script along with instructions from `spaghetti_related_info` to generate and insert random data into the OLTP database.
- Focus on populating the `flight` table as it is crucial for dimensional analysis.

### 4. Data Cleaning and Transformation

- The data extracted from the OLTP database is cleaned and transformed using the Python script. This includes handling missing values, converting data types, and ensuring consistency.

### 5. Creating Dimensions and Facts

- The script will create dimension and fact tables in the Data Warehouse:
  - **dimAircraft**: Stores information about aircraft.
  - **dimRoute**: Stores information about routes.
  - **dimFlight**: Stores information about flights.
  - **dimAirport**: Stores information about airports.
  - **dimDateTime**: Stores information about date and time components.
  - **flight_operations**: The fact table that stores detailed flight operation data.

### 6. Data Loading

- The cleaned and transformed data is loaded into the respective dimension and fact tables in the Data Warehouse.
- Ensure the relationships between dimensions and facts are correctly established via foreign keys.

### 7. Snapshot for Analysis

- The `DW.pbix` file should be connected to both `flight_operations_snapshot_old` and `flight_operations_snapshot_new` for analysis.
- Ensure that the Power BI file is properly configured to use these data sources.

### 8. Execution

- Run the Python script to perform the ETL process.
- Validate the data in the Data Warehouse to ensure it matches the expected structure and content.
- Use Power BI to analyze the data and generate insights.

## Important Notes

- Make sure to have all necessary Python libraries installed (`psycopg2`, `pandas`).
- Follow best practices for data handling and ensure data privacy and security.
- Regularly backup the databases to prevent data loss.

## Conclusion

By following these instructions, you will be able to set up a dummy OLTP database, perform ETL operations to populate a Data Warehouse, and use Power BI to analyze the flight operations data. 