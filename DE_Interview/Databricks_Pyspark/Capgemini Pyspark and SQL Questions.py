# Databricks notebook source
# Data Engineer Interview Questions on Spark and SQL in Capgemini

'''
1. You have a DataFrame df_sonar with the following schema:

 |-- Sonar_id: string (nullable = true)
 |-- Created_date: date (nullable = true)
Write a PySpark transformation that:

Adds a new column New_col1 that contains True if Created_date is NULL, otherwise False.
Creates a column Category based on the Sonar_id pattern:
If Sonar_id starts with 'A', assign 'Hardware'.
If Sonar_id starts with 'B', assign 'Network'.
If Sonar_id starts with 'C', assign 'OS'.
Otherwise, assign NULL
'''
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import *
from datetime import date 

# Create Spark session with necessary configurations
spark = SparkSession.builder \
  .appName("Sonar") \
  .getOrCreate()

data=[("A123", date(2024, 1, 15), "Active", "New York"),
    ("B456", date(2023, 12, 10), "Inactive", "Los Angeles"),
    ("C789", None, "Active", "Chicago"),
    ("A234", date(2022, 6, 30), "Inactive", "Houston"),
    ("B567", date(2021, 9, 20), "Active", "Phoenix"),
    ("C890", None, "Inactive", "Philadelphia"),
    ("A345", date(2023, 11, 5), "Active", "San Diego"),
    ("B678", date(2020, 8, 14), "Inactive", "Dallas"),
    ("C901", None, "Active", "San Francisco"),
    ("A456", date(2019, 3, 27), "Inactive", "Seattle")]

columns=["Sonar_id", "Created_date", "Status", "Location"]

df_sonar=spark.createDataFrame(data, columns)
#1. If the Created_date is null then create new column Created_Date_Null_Check
df_sonar=df_sonar.withColumn('Created_date_Null_Check',df_sonar['Created_date'].isNull())
df_sonar.show()
'''2. Creates a column Category based on the Sonar_id pattern:
If Sonar_id starts with 'A', assign 'Hardware'.
If Sonar_id starts with 'B', assign 'Network'.
If Sonar_id starts with 'C', assign 'OS'.
Otherwise, assign NULL'''
df_sonar=df_sonar.withColumn("Category",when(df_sonar["Sonar_id"].like("A%"),"Hardware").when(df_sonar["Sonar_id"].like("B%"),"Network").when(df_sonar["Sonar_id"].like("C%"),"OS").otherwise(None))#We can use col("Sonar_id")
df_sonar.show()


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data Engineer Interview Questions on Spark and SQL in Capgemini
# MAGIC /*3. In oracle SQL Adds a new column New_col1 that contains True if Created_date is NULL, otherwise False.
# MAGIC Creates a column Category based on the Sonar_id pattern:
# MAGIC If Sonar_id starts with 'A', assign 'Hardware'.
# MAGIC If Sonar_id starts with 'B', assign 'Network'.
# MAGIC If Sonar_id starts with 'C', assign 'OS'.
# MAGIC Otherwise, assign NULL*/
# MAGIC
# MAGIC -- Create the table
# MAGIC --sonar_info_csv got created by uploading csv file in dbfs and thorugh notebook it got created view then saved in permanent table scripts saved locally "Creation of Table in DBFS using notebook"
# MAGIC select nvl2(created_date,False,True) as Create_Date_Null_Check, CASE when sonar_id like 'A%' then 'Hardware' 
# MAGIC when sonar_id like 'B%' then 'Network' when sonar_id like 'B%' then 'OS' else 'None' END AS Category from `sonar_info_csv`
# MAGIC
# MAGIC

# COMMAND ----------

#4. Upload the json file and read it
'''
Write a query in PySpark to:
Select name, dob, age, gender, email, phone, and address.
Filter the dataset to only include records where add_type is 'Permanent'.
Display all columns from the address field for records with a Permanent address.
''' 
from pyspark.sql.functions import col

df_json = spark.read.option("mode", "DROPMALFORMED").json("dbfs:/FileStore/personal_data_add.json")
# After reading, print schema
df_json.printSchema()

# Check the corrupt records
df_json.filter("_corrupt_record IS NOT NULL").show(truncate=False)

# After reading, print schema
df_json.printSchema()


#df_json_flat = df_json.select("name", "dob", "age", "gender", "email", "phone", "address.*") \
                     #.filter(df_json.add_type == 'Permanent')

#df_json_flat.display()

