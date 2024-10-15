# Databricks notebook source
# Instantiate Config Variable
if 'config' not in locals().keys():
  config = {}

# COMMAND ----------

# Configure Database and Volume
config['catalog'] = 'genai_anomaly_detection'
config['schema'] = 'anomaly_detection'
config['volume'] = 'source_data'
config['vol_data_landing'] = f"/Volumes/{config['catalog']}/{config['schema']}/{config['volume']}"

# COMMAND ----------

# create catalog if not exists
spark.sql('create catalog if not exists {0}'.format(config['catalog']));

# COMMAND ----------

# set current catalog context
spark.sql('USE CATALOG {0}'.format(config['catalog']));

# COMMAND ----------

# create database if not exists
spark.sql('create database if not exists {0}'.format(config['schema']));

# COMMAND ----------

# set current datebase context
spark.sql('USE {0}'.format(config['schema']));

# COMMAND ----------

# Create the volume
spark.sql(f"CREATE VOLUME IF NOT EXISTS {config['volume']}");

# COMMAND ----------

import os
import requests

# Define the URL of the file you want to download
git_url = 'https://raw.githubusercontent.com/databricks-solutions/databricks-blogposts/main/genai-anomaly-detection/purchase_vendors_products_synthetic.csv'

# Define the local path where you want to save the file
local_path = config['vol_data_landing'] + '/purchase_vendors_products_synthetic.csv'


# Check if the file exists
if os.path.isfile(local_path):
    print('file already exists')
else:
    # Download the file using requests library
    response = requests.get(git_url)
    # Save the downloaded file to the local path
    with open(local_path, 'wb') as file:
        file.write(response.content)