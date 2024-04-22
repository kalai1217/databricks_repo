# Databricks notebook source
# MAGIC %run /Users/kalaiarasan.j@diggibyte.com/assignments/assignment_2/

# COMMAND ----------

import unittest
import requests
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, MapType

class assignment2(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("PySpark Assignment").getOrCreate()

    def assertion_create_df(self):
        response_data = requests.get('https://reqres.in/api/users?page=2')
        data = response_data.json()
        json_schema = StructType([
            StructField("page", IntegerType(), True),
            StructField("per_page", IntegerType(), True),
            StructField("total", IntegerType(), True),
            StructField("total_pages", IntegerType(), True),
            StructField("data", StructType([
                StructField("id", IntegerType(), True),
                StructField("email", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("avatar", StringType(), True)
            ]), True),
            StructField("support", StructType([
                StructField("url", StringType(), True),
                StructField("text", StringType(), True)
            ]), True)
        ])
        expected_df = spark.createDataFrame([data],schema=json_schema)
        actual_df = create_df(data,json_schema)
        self.assertEqual( actual_df ,expected_df)

    def assertion_col_drop(self):
        response_data = requests.get('https://reqres.in/api/users?page=2')
        json_data = response_data.json()
        data_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("email", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("avatar", StringType(), True)
        ])
        custom_schema = StructType([
            StructField("page", IntegerType(), True),
            StructField("per_page", IntegerType(), True),
            StructField("total", IntegerType(), True),
            StructField("total_pages", IntegerType(), True),
            StructField("data", ArrayType(data_schema), True),
            StructField("support", MapType(StringType(), StringType()), True)
        ])
        actual_df = spark.createDataFrame([json_data], custom_schema)
        expected_df = actual_df.drop('page', 'per_page', 'total', 'total_pages', 'support')
        result_df = col_drop(actual_df)
        self.assertEqual( actual_df ,expected_df) 


    

suite = unittest.TestLoader().loadTestsFromTestCase(assignment2)
unittest.TextTestRunner(verbosity=1).run(suite)
