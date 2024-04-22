# Databricks notebook source
# MAGIC %run /Users/kalaiarasan.j@diggibyte.com/assignments/assignment_1/source_to_bronze/utils

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

employee_custom_schema = StructType([
    StructField('EmployeeID', IntegerType(), True),
    StructField('EmployeeName', StringType(), True),
    StructField('Department', StringType(), True),
    StructField('Country', StringType(), True),
    StructField('Salary', IntegerType(), True),
    StructField('Age', IntegerType(), True)
])

# COMMAND ----------

country_custom_schema = StructType([
    StructField('CountryCode', StringType(), True),
    StructField('CountryName', StringType(), True)
])

# COMMAND ----------

department_custom_schema = StructType([
    StructField('DepartmentID', StringType(), True),
    StructField('DepartmentName', StringType(), True)
])

# COMMAND ----------

employee_csv_path = '''dbfs:/FileStore/assignments/assignment_1/source_to_bronze/employee_df.csv/part-00000-tid-6103212535810671775-8064e420-0dbc-4c94-8620-af7f9d179e51-70-1-c000.csv'''
country_csv_path = '''dbfs:/FileStore/assignments/assignment_1/source_to_bronze/country_df.csv/part-00000-tid-5901953138618445503-d99ef960-168a-4cb9-b869-a936e301d364-66-1-c000.csv'''
department_csv_path = '''dbfs:/FileStore/assignments/assignment_1/source_to_bronze/department_df.csv/part-00000-tid-6078041443976848379-7a79f1ff-78f6-405e-8bac-929dafc479b3-69-1-c000.csv'''

# COMMAND ----------

employee_df = read_with_custom_schema(employee_csv_path,employee_custom_schema)
country_df = read_with_custom_schema_format(country_csv_path, country_custom_schema)
department_df = read_with_custom_schema(department_csv_path, department_custom_schema)

# COMMAND ----------


employee_snake_case_df = change_column_case_to_snake_case(employee_df)
department_snake_case_df = change_column_case_to_snake_case(department_df)
country_snake_case_df = change_column_case_to_snake_case(country_df)


# COMMAND ----------

employee_with_date_df = add_current_date(employee_snake_case_df)
department_with_date_df = add_current_date(department_snake_case_df)
country_with_date_df = add_current_date(country_snake_case_df)


# COMMAND ----------

# spark.sql('create database employee_info')
spark.sql('use employee_info')



# COMMAND ----------

employee_df.write.option('path', 'dbfs:/FileStore/assignments/question1/silver/employee_info/dim_employee').saveAsTable('dim_employee')
