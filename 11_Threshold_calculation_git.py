# Databricks notebook source
# MAGIC %md
# MAGIC ## just check git hub updates main branch

# COMMAND ----------

# MAGIC %run /Workspace/Users/dr0138@att.com/list_of_columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Date widgets for threshold and validation

# COMMAND ----------

# MAGIC %md
# MAGIC Just to testing purpose where I only need to run this notebook otherwise to run job we have widgets

# COMMAND ----------

dbutils.widgets.text("threshold_start_date", "")
threshold_start_date = dbutils.widgets.get('threshold_start_date')

dbutils.widgets.text("threshold_end_date", "")
threshold_end_date = dbutils.widgets.get('threshold_end_date')

dbutils.widgets.text("validation_start_date", "")
validation_start_date = dbutils.widgets.get('validation_start_date')

dbutils.widgets.text("validation_end_date", "")
validation_end_date = dbutils.widgets.get('validation_end_date')

# COMMAND ----------

# MAGIC %md
# MAGIC #### import important libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import DataFrame
# from pyspark.sql.functions import col, count, when, countDistinct
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Path for threshold

# COMMAND ----------

# threshold_path = 'abfss://tb4840@blackbirdproddatastore.dfs.core.windows.net/backup_runs/run_date=2024-05-21'
threshold_path = 'abfss://tb4840@blackbirdproddatastore.dfs.core.windows.net/latest_refreshs/run_date=2024-05-30'
threshold_df=spark.read.format("delta").load(threshold_path)

# COMMAND ----------

display(threshold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filter data based on threshold date

# COMMAND ----------

threshold_df_filter= threshold_df.filter((col('call_date')>=threshold_start_date) & (col('call_date')<=threshold_end_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Path for validation

# COMMAND ----------

# validation_path = 'abfss://tb4840@blackbirdproddatastore.dfs.core.windows.net/latest_refreshs/run_date=2024-05-30'
validation_path = 'abfss://tb4840@blackbirdproddatastore.dfs.core.windows.net/latest_refreshs/run_date=2024-07-15'
validation_df=spark.read.format("delta").load(validation_path)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Filter data based on validation date

# COMMAND ----------

validation_df_filter= validation_df.filter((col('call_date')>=validation_start_date) & (col('call_date')<=validation_end_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate threshold value for null count

# COMMAND ----------

## NULL COUNT

def find_threshold_value_from_col_null_count(col_name):
  null_count_by_date = threshold_df_filter.groupBy('call_date') \
                       .agg(count(when(col(col_name).isNull(), True)).alias('null_count'))

  mean_null_count = null_count_by_date.select(mean("null_count")).collect()[0][0]
  stddev_null_count = null_count_by_date.select(stddev('null_count')).collect()[0][0]

  z_score = null_count_by_date.withColumn("z_score", (col("null_count") - mean_null_count) / stddev_null_count)
  threshold = 2.5
  null_count_by_date_after_removing_outliers = z_score.filter(col("z_score").between(-threshold, threshold))

  null_count_after_outliers_mean = null_count_by_date_after_removing_outliers.select(mean('null_count')).collect()[0][0]
  null_count_after_outliers_stddev = null_count_by_date_after_removing_outliers.select(stddev('null_count')).collect()[0][0]

  if null_count_after_outliers_mean == None:
    null_count_after_outliers_mean = 0
  if null_count_after_outliers_stddev == None:
    null_count_after_outliers_stddev = 0

  threshold_upper = null_count_after_outliers_mean + 2.5 * null_count_after_outliers_stddev

  null_count_by_date2 = validation_df_filter.groupBy("call_date") \
  .agg(count("*").alias("total_count"),count(when(col(col_name).isNull(), 1)).alias(col_name+"_null_count")
  ) \
  .withColumn("null_percentage",round((col(col_name+"_null_count") / col("total_count")) * 100, 2)
  ) \
  .withColumn((col_name+
      "_null_count_with_percentage"),
      concat(col(col_name+"_null_count"),lit(" ("),col("null_percentage"),lit("%)"))).select("call_date", col_name+"_null_count_with_percentage",(col_name+"_null_count"))

  result_df_null = null_count_by_date2.withColumn(col_name + '_null_count_is_within_threshold', when(col(col_name +"_null_count") > threshold_upper, False).otherwise(True)).select('call_date',(col_name+'_null_count_with_percentage'),(col_name + '_null_count_is_within_threshold'))

  return (result_df_null)


# COMMAND ----------

## TOTAL COUNT

def find_threshold_value_from_col_total_count(col_name):
  total_count_by_date = threshold_df_filter.groupBy('call_date') \
                       .agg(count(when(col(col_name).isNotNull(), True)).alias('total_count'))

  mean_total_count = total_count_by_date.select(mean("total_count")).collect()[0][0]
  stddev_total_count = total_count_by_date.select(stddev('total_count')).collect()[0][0]

  z_score = total_count_by_date.withColumn("z_score", (col("total_count") - mean_total_count) / stddev_total_count)
  threshold = 2.5
  total_count_by_date_after_removing_outliers = z_score.filter(col("z_score").between(-threshold, threshold))

  total_count_after_outliers_mean = total_count_by_date_after_removing_outliers.select(mean('total_count')).collect()[0][0]
  total_count_after_outliers_stddev = total_count_by_date_after_removing_outliers.select(stddev('total_count')).collect()[0][0]

  if total_count_after_outliers_mean == None:
    total_count_after_outliers_mean = 0
  if total_count_after_outliers_stddev == None:
    total_count_after_outliers_stddev = 0

  threshold_upper = total_count_after_outliers_mean + 2.5 * total_count_after_outliers_stddev 

  total_count_by_date2 = validation_df_filter.groupBy('call_date') \
      .agg(count(when(col(col_name).isNotNull(), True)).alias(col_name +'_total_count')) \
      .orderBy('call_date')

  result_df_total = total_count_by_date2.withColumn(col_name + '_total_count_is_within_threshold', when(col(col_name +"_total_count") > threshold_upper, False).otherwise(True))

  return (result_df_total)


# COMMAND ----------

## DISTINCT COUNT

def find_threshold_value_from_col_distinct_count(col_name):
  # print('col_name',col_name)
  distinct_count_by_date = threshold_df_filter.filter(col(col_name).isNotNull()) \
                        .groupBy('call_date') \
                        .agg(countDistinct(col_name).alias('distinct_count'))

  mean_distinct_count = distinct_count_by_date.select(mean("distinct_count")).collect()[0][0]
  stddev_distinct_count = distinct_count_by_date.select(stddev('distinct_count')).collect()[0][0]

  if stddev_distinct_count == 0:
    stddev_distinct_count=1

  z_score = distinct_count_by_date.withColumn("z_score", (col("distinct_count") - mean_distinct_count) / stddev_distinct_count)
  threshold = 2.5
  distinct_count_by_date_after_removing_outliers = z_score.filter(col("z_score").between(-threshold, threshold))

  distinct_count_after_outliers_mean = distinct_count_by_date_after_removing_outliers.select(mean('distinct_count')).collect()[0][0]
  distinct_count_after_outliers_stddev = distinct_count_by_date_after_removing_outliers.select(stddev('distinct_count')).collect()[0][0]

  if distinct_count_after_outliers_mean == None:
    distinct_count_after_outliers_mean = 0
  if distinct_count_after_outliers_stddev == None:
    distinct_count_after_outliers_stddev = 0

  threshold_upper = distinct_count_after_outliers_mean + 2.5 * distinct_count_after_outliers_stddev

  distinct_count_by_date2 = validation_df_filter.filter(col(col_name).isNotNull()) \
                        .groupBy('call_date') \
                        .agg(countDistinct(col_name).alias(col_name +'_distinct_count')) \
                        .orderBy('call_date')

  result_df_distinct = distinct_count_by_date2.withColumn(col_name + '_distinct_count_is_within_threshold', when(col(col_name +"_distinct_count") <= threshold_upper, True).otherwise(False))

  return (result_df_distinct)


# COMMAND ----------

# MAGIC %md
# MAGIC #### List of column on which we apply threshold & validation

# COMMAND ----------

# MAGIC %md
# MAGIC #### Merge all output columns

# COMMAND ----------

const_list_of_col= ['customer_products_IPBB_BIZ','customer_products_IPBB_OOF','has_hsia','destination','has_gigapower','days_since_upgradechangeplanfeature']

list_of_col = list_of_columns['columns_list'] + const_list_of_col

# COMMAND ----------

# MAGIC %md
# MAGIC ## SMTP for email

# COMMAND ----------

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP

# COMMAND ----------

# MAGIC %md
# MAGIC ### Email format

# COMMAND ----------

html_content = f"""
  <!DOCTYPE html>
  <html>
  <head>
    <style>
      /* Global container styles */
      body {{
        font-family: Arial, sans-serif;
        background-color: #f5f5f5;
        margin: 0;
        padding: 0;
      
      }}

      .email-container {{
        max-width: 600px;
        margin: 0 auto;
        padding: 20px;
        background-color: #fff;
        box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.1);
        
      }}

      /* Header styles */
      .header {{
        background-color: #3498db;
        color: #fff;
        text-align: center;
        padding: 20px;
      }}

      /* Table styles */
      table {{
        margin-bottom: 10px;
        width: 100%;
      }}

      table, th, td {{
      
        border-style: solid;
        border-collapse : collapse;
        border-color: rgb(12, 13, 69);
  
      }}

      th, td {{
        padding: 2px;
        text-align: left;
      }}

      th {{
        background-color: #f2f2f2;
        color: #202020; 
      }}

    </style>
  
  """
  #333

# COMMAND ----------

# MAGIC %md
# MAGIC Variable to sending email

# COMMAND ----------

count_for_email = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Heart of the code - Calling all functions

# COMMAND ----------

final_df = validation_df_filter.groupBy('call_date').count().orderBy('call_date').select('call_date')

for col_in_train_data in list_of_col[:]:

  null_df = find_threshold_value_from_col_null_count(col_in_train_data) 
  # final_df = final_df.join(null_df, 'call_date', 'outer')
  false_df_null = null_df.filter(col(col_in_train_data + '_null_count_is_within_threshold') == False).select('call_date',col_in_train_data+'_null_count_with_percentage')
  if false_df_null.count() > 0:
    count_for_email = 1
    final_df = final_df.join(false_df_null, 'call_date', 'inner')
    html_content += false_df_null.toPandas().to_html()
    html_content += f""" 
    <br><br>
    """

  total_df = find_threshold_value_from_col_total_count(col_in_train_data) 
  # final_df = final_df.join(total_df, 'call_date', 'outer')
  false_df_total = total_df.filter(col(col_in_train_data + '_total_count_is_within_threshold') == False)
  if false_df_total.count() > 0:
    count_for_email = 1
    final_df = final_df.join(false_df_total, 'call_date', 'inner')
    html_content += false_df_total.toPandas().to_html()
    html_content += f""" 
    <br><br>
    """

  distinct_df = find_threshold_value_from_col_distinct_count(col_in_train_data) 
  # final_df = final_df.join(distinct_df, 'call_date', 'outer')
  false_df_distinct = distinct_df.filter(col(col_in_train_data + '_distinct_count_is_within_threshold') == False)
  if false_df_distinct.count() > 0:
    count_for_email = 1
    final_df = final_df.join(false_df_distinct, 'call_date', 'inner')
    html_content += false_df_distinct.toPandas().to_html()
    html_content += f""" 
    <br><br>
    """

# display(final_df)


# COMMAND ----------

display(final_df)

# COMMAND ----------

if count_for_email == 1 :

  subject = "Alert : Below are the columns that violated the threshold value"

if count_for_email == 0 :

  subject = 'Not any violation found'

# COMMAND ----------

fromAddr = "validation_alert@att.onmicrosoft.com"
recipients1 = "dr0138@att.com"
recipients2 =  "dr0138@att.com"
# recipients2 = 'validation_alert@att.onmicrosoft.com'

msgRoot = MIMEMultipart('related')
msgRoot['From'] = fromAddr
msgRoot['To'] = recipients1
msgRoot['Cc'] = recipients2
msgRoot['Subject'] = subject

msgText = MIMEText(html_content, 'html')
msgRoot.attach(msgText)

server = SMTP('smtp.it.att.com:25')
server.ehlo()
server.starttls()
server.sendmail(msgRoot['From'], [recipients1, recipients2], msgRoot.as_string()) 
server.close()

# COMMAND ----------


