# Databricks notebook source
from pyspark.sql.functions import desc, count, avg

# COMMAND ----------

# MAGIC %run /Users/kalaiarasan.j@diggibyte.com/assignments/assignment_1/source_to_bronze/utils
# MAGIC

# COMMAND ----------

# MAGIC %run /Users/kalaiarasan.j@diggibyte.com/assignments/assignment_1/bronze_to_silver/employee_bronze_to_silver/

# COMMAND ----------

employee_df = spark.read.format("delta").load('dbfs:/FileStore/assignments/question1/silver/employee_info/dim_employee')
display(employee_df)

# COMMAND ----------

salary_of_department = employee_with_date_df.orderBy(desc('salary'))
display(salary_of_department.select('department', 'salary'))

# COMMAND ----------

employee_count = employee_df.groupBy("department", "country").agg(count("employeeid").alias("employee_count"))
display(employee_count)

# COMMAND ----------

department_join_df = employee_with_date_df.join(department_with_date_df, employee_with_date_df.department == department_with_date_df.department_i_d, "inner")
country_join_df = department_join_df.join(country_with_date_df, department_join_df.country == country_with_date_df.country_code, "inner")
department_with_country = country_join_df.select('department_name', 'country_name')
display(department_with_country)

# COMMAND ----------

avg_age_employee = employee_with_date_df.groupBy('department').agg(avg("age").alias('avg_age'))
display(avg_age_employee)

# COMMAND ----------

employee_with_date_df.write.format("parquet").mode("overwrite").option("replaceWhere", "load_date = '2024-04-16'").save("/FileStore/assignments/gold/employee/table_name")

# COMMAND ----------

test_df = spark.read.parquet('dbfs:/FileStore/assignments/gold/employee/table_name/')
display(test_df)