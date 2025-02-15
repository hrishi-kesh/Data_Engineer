# Databricks notebook source
'''1. How to ingest and load incremental data in S3 from different tables with different columns using Databricks
Solution: Databricks can handle incremental data loading using Delta Lake’s MERGE operation. This allows you to upsert (update and insert) new or changed data.
'''

'''
1. Simple Loading (Overwrite or Append)
What it does: Simply loads CSV into a Delta table (overwrite or append).
Use case: Full load (no need to track changes or match records).
'''
# Load the CSV file
#csv_data = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/MOCK_DATA.csv")

# Write the data to Delta format
#csv_data.write.format("delta").mode("overwrite").save("dbfs:/FileStore/table_a")

# Load the CSV file
csv_data = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/sample_student.csv")

# Write the data to Delta format
csv_data.write.format("delta").mode("overwrite").save("dbfs:/FileStore/table_b")


# COMMAND ----------

'''
2. Incremental Loading with Delta MERGE (Upsert)
What it does: Loads new or changed data into Delta tables. It updates existing records if a match is found and inserts new records if no match exists.

Use case: For incremental data ingestion (updating existing records or inserting new ones).

How to Implement:
Steps to Perform Incremental Loading Using MERGE:
Load the Incremental Data:
Load new records from a CSV file.

Load the Existing Delta Table:
Ensure the existing data is in Delta format.

Perform MERGE:
Use Delta Lake’s MERGE to upsert data.
'''

from delta.tables import DeltaTable

# Step 1: Load Incremental Data (e.g., from CSV)
incremental_data = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/MOCK_DATA.csv")

# Step 2: Check if Delta table exists
delta_path = "dbfs:/FileStore/table_a"

if DeltaTable.isDeltaTable(spark, delta_path):
    # Step 3: Load the existing Delta table
    table_a_target = DeltaTable.forPath(spark, delta_path)

    # Step 4: Perform the MERGE (Upsert)
    table_a_target.alias("target").merge(
        incremental_data.alias("source"),
        "target.id = source.id"  # Matching condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

else:
    # Step 5: If Delta table doesn't exist, initialize it
    incremental_data.write.format("delta").mode("overwrite").save(delta_path)


# COMMAND ----------

#To see the data and structure of delta table
# Load the Delta table
delta_table = spark.read.format("delta").load("dbfs:/FileStore/table_a")
# Show the data
delta_table.show()
# Print the schema of the Delta table
delta_table.printSchema()



# COMMAND ----------

'''
3. How to link one notebook to another in Databricks
Solution:
You can link one Databricks notebook to another using the dbutils.notebook.run() method. This allows a parent notebook to call another notebook and pass parameters.

#parent notebook
# Call child notebook and pass parameters
result = dbutils.notebook.run("/path/to/child_notebook", 300, {"param1": "value1"})

# Print result from child notebook
print(f"Result from child notebook: {result}")
Child Notebook (child_notebook):

# Get parameters passed from parent
param1 = dbutils.widgets.get("param1")

# Perform some task
print(f"Received parameter: {param1}")

# Return result to parent
dbutils.notebook.exit("Success")
'''

# COMMAND ----------

#3. How to read and load  the nested json file in databricks

# Step 1: Load the Nested JSON Data
nested_json_df = spark.read.format("json").load("dbfs:/FileStore/BANK_DATA.json")

# Step 2: Display the Data
nested_json_df.show(truncate=False)

# Step 3: Print the Schema to Understand the Nested Structure
nested_json_df.printSchema()

