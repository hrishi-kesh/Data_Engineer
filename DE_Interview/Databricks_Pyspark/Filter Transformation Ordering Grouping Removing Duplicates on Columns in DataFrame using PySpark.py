# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# Add column in Dataframe using Pyspark
from pyspark.sql.functions import *
# File location and type
file_location = "/FileStore/tables/sample_student.csv"
file_type = "csv"

# CSV options
infer_schema = "true" # if false it will give all schema to string by default
first_row_is_header = "true" #if false then it will auto add header
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

#df=df.withColumn("Country",lit("India")) #lit is using for default column value
#df2=df.withColumn("Salary",col("ID")*col("Age")).withColumn("Country",lit("India")) #showing salary by multiplying of id and age
df2=df.select(col("ID"),col("Name"),col("Age"),lit("India").alias("Country"))
df2.show()
display(df) #tabular format
df.show() #dataframe format
#df.collect()
df.take(2) #showing two records
df.printSchema()



# COMMAND ----------

# Filter DataFrame using PySpark

from pyspark.sql.functions import *
# File location and type
file_location = "/FileStore/tables/sample_student.csv"
file_type = "csv"

# CSV options
infer_schema = "true" # if false it will give all schema to string by default
first_row_is_header = "true" #if false then it will auto add header
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

df=df.select(col("ID"),col("Name"),col("Age"),lit("India").alias("Country"))

df.show()
#df.filter(df.ID==2).show()#df.filter(col("ID")==1).show()
#df.filter((col("ID")==1) | (col("Name")=="Shyam")).show() #we can use |(or) &(and)
df.filter(col("ID")!=1).show()



# COMMAND ----------

# Before sorting I want this one row to be inserted
#Create a new row (as a DataFrame).
#Union this new row with the existing DataFrame.
#Write the updated DataFrame to the existing .csv file in DBFS.
#Download the updated .csv file.

from pyspark.sql.functions import * #to assess col option
from pyspark.sql import Row  #to add new row
# File location and type
file_location = "/FileStore/tables/sample_student.csv"
file_type = "csv"

# CSV options
infer_schema = "true" # if false it will give all schema to string by default
first_row_is_header = "true" #if false then it will auto add header
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

df=df.select(col("ID"),col("Name"),col("Age"),lit("India").alias("Country"))

df.show()

dbutils.fs.ls('dbfs:/FileStore/tables/sample_student.csv')#to see file at location
new_row = Row(ID=1, Name="Hrishikesh", Age=20, Country="India")
#new_row_df=spark.createDataFrame([new_row])# Convert the new row into a DataFrame
#df_updated=df.union(new_row_df) # Union the new row with the existing DataFrame
output_path = 'dbfs:/FileStore/tables/sample_student.csv'
df_updated.write.mode("overwrite").csv(output_path, header=True)# Write the updated DataFrame to the existing CSV file in DBFS (use mode='append' to add data and mode("overwrite"): Will overwrite the entire file)
df_updated.show()
df_updated.display() #in this option it will show the download option as well
df.show()

#to generate url link to download file Host_name-"https://community.cloud.databricks.com/" then "files/" then "tables/sample_student.csv" we can ignore FileStore then authentication key-"?o=1658314575761879"
#Url to download:- https://community.cloud.databricks.com/files/tables/sample_student.csv?o=1658314575761879


# COMMAND ----------

# Sort a DataFrame using PySpark

from pyspark.sql.functions import * #to assess col option
from pyspark.sql import Row  #to add new row
# File location and type
file_location = "/FileStore/tables/sample_student-2.csv"
file_type = "csv"

# CSV options
infer_schema = "true" # if false it will give all schema to string by default
first_row_is_header = "true" #if false then it will auto add header
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

df.display() #in this option it will show the download option as well
df.sort(df.ID.desc()).show() #df.orderBy(df.ID.desc()).show()
df.orderBy(df.ID.desc(),df.Age.desc()).show()#we can use col("ID") instead of df.ID


# COMMAND ----------

# Remove Duplicates in DataFrame

from pyspark.sql.functions import * #to assess col option
from pyspark.sql import Row  #to add new row
# File location and type
file_location = "/FileStore/tables/sample_student_dup.csv"
file_type = "csv"

# CSV options
infer_schema = "true" # if false it will give all schema to string by default
first_row_is_header = "true" #if false then it will auto add header
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

df.display() #in this option it will show the download option as well
df.distinct().display() #df.dropDuplicates().display()
# The basic diff bw them is shown below as we can see according to column list in dropDuplicates and we can't pass the parameter in distinct
df.dropDuplicates(["ID","Name"]).display()

# COMMAND ----------

#GroupBY in DataFrame using PySpark

from pyspark.sql.functions import * #to assess col option

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.load('dbfs:/FileStore/tables/sample_student_groupBy.csv', inferSchema='True',header=True,format='csv')

df.display() #in this option it will show the download 
df.groupBy("ID","Name").max("Marks").display() #Here we can't use multiple aggregate min max and other agg in same statement. To overcome this we can use agg func
df.groupBy("ID","Name").agg(max("Marks").alias("Max_Marks"),min("Marks")).display() # Here with aggregate we can use alias


