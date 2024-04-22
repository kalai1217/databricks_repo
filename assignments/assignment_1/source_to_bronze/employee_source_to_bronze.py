# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

# read all csv

employee_path = "dbfs:/FileStore/assignments/assignment_1/resources/Employee_Q1.csv"
department_path = "dbfs:/FileStore/assignments/assignment_1/resources/Department_Q1.csv"
country_path= "dbfs:/FileStore/assignments/assignment_1/resources/Country_Q1.csv"

country_df=read_csv_data(country_path)
country_df.display()
department_df=read_csv_data(department_path)
department_df.display()
employee_df=read_csv_data(employee_path)
employee_df.display()


# COMMAND ----------

# write all csv files

write_csv_file(country_df,'dbfs:/FileStore/assignments/assignment_1/source_to_bronze/country_df.csv')
write_csv_file(department_df,'dbfs:/FileStore/assignments/assignment_1/source_to_bronze/department_df.csv')
write_csv_file(employee_df,'dbfs:/FileStore/assignments/assignment_1/source_to_bronze/employee_df.csv')

# COMMAND ----------

employee_df= change_column_case_to_snake_case(employee_df).display()
department_df=change_column_case_to_snake_case(department_df).display()
country_df=change_column_case_to_snake_case(country_df).display()

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS Employee_info")

# COMMAND ----------

write_delta_table(employee_df,"Employee_info","dim_employee","EmployeeID","/silver/Employee_info/dim_employee")
