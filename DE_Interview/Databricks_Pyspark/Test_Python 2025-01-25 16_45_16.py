# Databricks notebook source
spark.sql("create database if not exists Practice1")

# COMMAND ----------

spark.sql("use Practice1")

# COMMAND ----------

spark.sql("""create table student(id int, name string)""")

# COMMAND ----------

Spark.sql("""insert into student 
select 1 as id, 'Hrishikesh' as name""")

# COMMAND ----------

spark.sql("select * from student").show() #collect() it will show content and metadata as but show() only show content of the dataframe and take(n) we can pass the parameter for no. of rows
