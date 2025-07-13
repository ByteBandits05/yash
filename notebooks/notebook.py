# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks 5-Second Sleep Notebook
# MAGIC 
# MAGIC This notebook demonstrates a simple 5-second sleep operation in Databricks.
# MAGIC It imports the time module and uses `time.sleep(5)` to pause execution for 5 seconds.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Libraries
# MAGIC 
# MAGIC Import the time module to use the sleep function.

# COMMAND ----------

# MAGIC %python
# Import the time module
import time
print("Time module imported successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sleep for 5 Seconds
# MAGIC 
# MAGIC Use `time.sleep(5)` to pause execution for 5 seconds. This is useful for:
# MAGIC - Testing long-running processes
# MAGIC - Adding delays between operations
# MAGIC - Simulating processing time in demonstrations

# COMMAND ----------

# MAGIC %python
# Sleep for 5 seconds
print("Starting 5-second sleep...")
print(f"Start time: {time.strftime('%H:%M:%S')}")

# Sleep for 5 seconds
time.sleep(5)

print(f"End time: {time.strftime('%H:%M:%S')}")
print("Sleep completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Information
# MAGIC 
# MAGIC This notebook can be imported directly into Databricks workspace.
# MAGIC The sleep operation will pause the execution for exactly 5 seconds.
