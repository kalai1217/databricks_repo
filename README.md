# databricks_repo
_This repo contains databricks Assignment_

## Assignment_1

### source_to_bronze/schema.py

Defines custom schemas for Employee, Department, and Country datasets.
Describes the structure and data types of each column in the datasets.

### source_to_bronze/utils.py

Outlines common utility functions used across notebooks:
- Reading CSV files with custom schemas.
- Writing DataFrames to DBFS as CSV files.
- Obtaining the current date for data lineage.

### source_to_bronze/employee_source_to_bronze.py (Driver Notebook)

- Reads CSV files (Employee, Department, Country) using the defined schemas.
- Writes the DataFrames to DBFS as separate CSV files (employee.csv, department.csv, country.csv) in the source_to_bronze folder.

### bronze_to_silver/employee_bronze_to_silver.py

- Reads the CSV files written in the previous step from DBFS (/source_to_bronze).
- Uses different read methods (e.g., option("header", True), inferSchema) to explore options.
- Converts camelCase column names to snake_case using a User-Defined Function (UDF).
- Adds a load_date column with the current date for data lineage.
- Writes the transformed DataFrame as a Delta table to the silver layer with the following details:
  - Database name: Employee_info
  - Table name: dim_employee
  - Location: /silver/Employee_info/dim_employee.delta

### silver_to_gold/employee_silver_to_gold.py

- Reads the Delta table created in the previous step (/silver/Employee_info/dim_employee.delta).
- Performs aggregations and joins to fulfill the following requirements:
  - Find the salary of each department in descending order.
  - Find the number of employees in each department located in each country.
  - List the department names along with their corresponding country names.
  - Calculate the average age of employees in each department.
- Adds an at_load_date column with the current date for data lineage.
- Writes the resulting DataFrame as a Delta table to the gold layer with the following details:
  - Database name: Replace with your chosen database name (e.g., employee_data)
  - Table name: fact_employee
  - Location: /gold/employee_data/fact_employee.delta
  - Write mode: Overwrite existing data, replacing rows with the same at_load_date.

## Assignment_2

### Task Description:

- API: [https://reqres.in/api/users?page=2](https://reqres.in/api/users?page=2)
- Drop "page", "per_page", "total", "total_pages", and complete block of support.
- Fetch the data from the given API by passing the parameter as a page and retrieve the data until the data is empty.
- Read the DataFrame with a custom schema.
- Flatten the DataFrame.
- Derive a new column from email as `site_address` with values(reqres.in).
- Add `load_date` with the current date.
- Write the DataFrame to location in DBFS as `/db_name/table_name` with:
  - DB_name as site_info
  - Table_name as person_info with Delta format and overwrite mode.
