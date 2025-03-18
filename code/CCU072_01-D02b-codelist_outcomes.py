# Databricks notebook source
# MAGIC %md # CCU072_01-D02b-codelist_outcomes
# MAGIC
# MAGIC **Description** This notebook load, check and combine the outcome code lists.
# MAGIC
# MAGIC **Author(s)** Wen Shi
# MAGIC
# MAGIC **Last Reviewed** 12/03/2025
# MAGIC
# MAGIC **Acknowledgements** Code lists were compiled by Elias Allara, Elena Raffetti, William Whitely, et al.

# COMMAND ----------

# MAGIC %md # 0. Setup

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import *
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

# MAGIC %md # 2. Check code lists

# COMMAND ----------

dcode = spark.table(path_out_dcode_name)
icd = spark.table(path_out_dcode_icd10)
sno = spark.table(path_out_dcode_snomed)
epcc = spark.table(path_out_dcode_epcc)
dcode.printSchema()
icd.printSchema()
sno.printSchema()
epcc.printSchema()

# COMMAND ----------

count_var(dcode,'dcode')
count_var(icd,'dcode')
count_var(sno,'dcode')
count_var(epcc,'dcode')

# COMMAND ----------

dcode.agg(*[f.count(f.when(f.isnull(col),col)).alias(col) for col in dcode.columns]).show()
icd.agg(*[f.count(f.when(f.isnull(col),col)).alias(col) for col in icd.columns]).show()
sno.agg(*[f.count(f.when(f.isnull(col),col)).alias(col) for col in sno.columns]).show()
epcc.agg(*[f.count(f.when(f.isnull(col),col)).alias(col) for col in epcc.columns]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Combine dcode, icd and snomed code lists

# COMMAND ----------

codesall = dcode.select('dcode').join(icd.select('dcode','icd10_code'),'dcode','left').join(sno.select('dcode','snomed_code'),'dcode','left')


# COMMAND ----------

codesall.filter((codesall.dcode.rlike('\W'))|(codesall.icd10_code.rlike('\W'))|(codesall.snomed_code.rlike('\W'))).show()

# COMMAND ----------

codesall = codesall.distinct()

# COMMAND ----------

codesall.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_out_codelist_outcome_v3')

# COMMAND ----------

codesall = spark.table(path_out_codelist_outcome_v3)


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #4. EPCC codelist

# COMMAND ----------

epcc2 = epcc.select('dcode','epcc_code').distinct()


# COMMAND ----------

epcc2.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_out_codelist_epcc')