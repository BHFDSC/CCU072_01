# Databricks notebook source
# MAGIC %md # CCU072_01-D04-skinny
# MAGIC  
# MAGIC **Description** This notebook creates the skinny patient table, which includes key patient characteristics, using demographics table updated monthly by BHF data science team.
# MAGIC  
# MAGIC **Authors** Wen Shi
# MAGIC
# MAGIC **Reviewed** 12/03/2025
# MAGIC
# MAGIC **Acknowledgements**  BHF data science team.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #0. Setup

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
import pandas as pd
import numpy as np

import re
import io
import datetime

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

print("Matplotlib version: ", matplotlib.__version__)
print("Seaborn version: ", sns.__version__)
_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow:  {_datetimenow}")

# COMMAND ----------

# MAGIC %md # 1. Parameters

# COMMAND ----------

# MAGIC %run "./CCU072_01-D01-parameters"

# COMMAND ----------

# MAGIC %md # 2. Data

# COMMAND ----------

tmp = spark.table(path_tmp_demo)

# COMMAND ----------

tmp.printSchema()

# COMMAND ----------

# MAGIC %md # 3. Skinny

# COMMAND ----------

# rename column names and select columns wanted
tmp = (tmp.withColumnRenamed('person_id','PERSON_ID')
          .withColumnRenamed('sex','SEX')

          .withColumnRenamed('date_of_birth','DOB')
          .withColumnRenamed('ethnicity_18_group','ETHNIC_CAT')
          .withColumnRenamed('date_of_death','DOD')
          .select('PERSON_ID','SEX','DOB','ETHNIC_CAT','death_flag','DOD','in_gdppr','gdppr_min_date')
        )


# COMMAND ----------

# change sex values
tmp = tmp.withColumn('SEX',f.when(f.col('SEX')=='M',1).when(f.col('SEX')=='F',2).otherwise(f.col('SEX')))

# COMMAND ----------

# MAGIC %md # 4. Save

# COMMAND ----------

outName = f'{proj}_skinny'.lower()

tmp.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{outName}')
