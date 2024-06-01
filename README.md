# README


## click [here](https://khiibaedu-my.sharepoint.com/:v:/g/personal/a_iqbal_24529_khi_iba_edu_pk/Efb2xQivzuVJtqZPvjBOnOoBElC1ygtvvyYuviDRi9HAVA?e=tHfuhL) to view the testing of pipeline.
## The spaghetti folder contains the data in OLTP. Our task is to create a dummy OLTP with data (refer to the spaghetti folder).

### 1. The `python_scripts/ETL` file has the pipeline code. You need to change the credentials of the source and destination databases (OLAP or Data Warehouse).
### 2. The `DW.pbix` should be connected to both `flight_operations_snapshot_old` and `flight_operations_snapshot_new`.
### 3. Insert data into OLTP. In this data mart case, adding data to the flight table is of value because its purpose is to allow dimensional analysis of flight operations which depend on the flight table. If data is inserted in the aircraft table, but there isn't a flight that used that aircraft, the aircraft's data won't be reflected in `dimAircraft`.
### 4. Use the `spaghetti_data_final.sql` script and instructions in `spaghetti_related_info` to generate random data.
### 5. The DBMS is PostgreSQL.

## Please ensure all instructions are followed correctly.
