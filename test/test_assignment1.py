# Databricks notebook source
# MAGIC %run /Users/kalaiarasan.j@diggibyte.com/assignments/assignment_1/source_to_bronze/utils

# COMMAND ----------

# MAGIC %run /Users/kalaiarasan.j@diggibyte.com/assignments/assignment_1/source_to_bronze/schmea
# COMMAND ----------

import unittest
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from datetime import datetime

# COMMAND ----------


class test_assignment1(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("PySpark Assignment1 Testcase").getOrCreate()
        
    def test_read_csv(self):   
        emp_path = 'dbfs:/FileStore/resources/Employee_Q1.csv'
        dep_path = 'dbfs:/FileStore/resources/Department_Q1.csv'
        country_path = 'dbfs:/FileStore/resources/Country_Q1.csv' 
        emp_df = read_csv(emp_path) 
        dep_df = read_csv(dep_path)
        country_path = read_csv(country_path)
        self.assertTrue(emp_df.count() >0)  
        self.assertTrue(dep_df.count() >0)
        self.assertTrue(country_path.count() >0)

    def test_custom_schema_read_csv(self):
        test_input_schema = "Emp_Id INT, Emp_Name STRING, department STRING, country STRING, salary INT, age INT"
        expected_output_schema = StructType([
           StructField("Emp_Id", IntegerType(), True),
           StructField("Emp_Name", StringType(), True),
           StructField("department", StringType(), True),
           StructField("country", StringType(), True),
           StructField("salary", IntegerType(), True),
           StructField("age", IntegerType(), True)
         ])
      

        result_df = custom_schema_read_csv(emp_path, test_input_schema)

        # Assert that the schema of the resulting DataFrame matches the expected schema
        self.assertEqual(result_df.schema, expected_output_schema)

    def test_camel_to_snake(self):
        emp_df = custom_schema_read_csv('dbfs:/FileStore/resources/Employee_Q1.csv',employee_schema)
        result_df = camel_to_snake(emp_df)

        # Assert that column names are converted to snake case
        expected_columns = ["emp_id", "emp_name", "department" , "country" , "salary" , "age"]
        self.assertEqual(result_df.columns, expected_columns)

    def test_current_date_df(self):
        
        emp_df = custom_schema_read_csv('dbfs:/FileStore/resources/Employee_Q1.csv',employee_schema)

        # Apply the function on the  DataFrame
        result_df = current_date_df(emp_df)

        # Assert that the 'load_date' column is added and contains the current date
        current_date_value = datetime.now().date()
        self.assertTrue("load_date" in result_df.columns)
        self.assertEqual(result_df.select("load_date").first()[0], current_date_value)

    def test_salary_of_each_department(self):
        emp_df = custom_schema_read_csv('dbfs:/FileStore/resources/Employee_Q1.csv',employee_schema)

        # Apply the function on the sample DataFrame
        result_df = salary_of_each_department(emp_df)

        # Assert that the resulting DataFrame contains the expected columns
        expected_columns = ["department", "total_salary"]
        self.assertEqual(result_df.columns, expected_columns)
    def test_employee_count(self):
        # Create a sample DataFrame for testing
        emp_df = custom_schema_read_csv('dbfs:/FileStore/resources/Employee_Q1.csv',employee_schema)
        # Apply the function on the sample DataFrame
        result_df = employee_count(emp_df)

        # Assert that the resulting DataFrame contains the expected columns
        expected_columns = ["department", "country", "count"]
        self.assertEqual(result_df.columns, expected_columns)

    def test_list_of_department(self):
        emp_df = custom_schema_read_csv('dbfs:/FileStore/resources/Employee_Q1.csv',employee_schema)

        # Apply the function on the DataFrame
        result_df = list_the_department(emp_df)

        # Assert that the resulting DataFrame contains the expected columns
        expected_columns = ["department", "country"]
        self.assertEqual(result_df.columns, expected_columns)
        
    def test_avg_age(self):
        emp_df = custom_schema_read_csv('dbfs:/FileStore/resources/Employee_Q1.csv',employee_schema)

        # Apply the function on the DataFrame
        result_df = avg_age(emp_df)

        # Assert that the resulting DataFrame contains the expected columns
        expected_columns = ["department", "average_age"]
        self.assertEqual(result_df.columns, expected_columns)

    suite = unittest.TestLoader().loadTestsFromTestCase(test_assignment1)
    unittest.TextTestRunner(verbosity=1).run(suite)
