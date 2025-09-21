# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS spark_catalog.bronze
# MAGIC LOCATION 'dbfs/FileStore/Ampev/bronze/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS spark_catalog.silver
# MAGIC LOCATION 'dbfs/FileStore/Ampev/silver/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS spark_catalog.gold
# MAGIC LOCATION 'dbfs/FileStore/Ampev/gold/';
