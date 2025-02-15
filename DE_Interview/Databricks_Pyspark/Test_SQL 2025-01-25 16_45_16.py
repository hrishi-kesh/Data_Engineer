# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists Practice

# COMMAND ----------

# MAGIC %sql
# MAGIC use Practice

# COMMAND ----------

# MAGIC %sql
# MAGIC create table student(id int, name string)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into student 
# MAGIC select 1 as id, 'Hrishikesh' as name

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from student
