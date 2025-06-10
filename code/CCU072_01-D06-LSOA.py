# Databricks notebook source
# MAGIC %md # CCU072_01-D06-LSOA
# MAGIC  
# MAGIC **Description** This notebook creates the covariates - region,deprivation based on LSOA using multi-sourced LSOA table updated monthly by BHF data science centre.
# MAGIC
# MAGIC **Authors** Wen Shi
# MAGIC
# MAGIC **Notes** 12/03/2025
# MAGIC
# MAGIC **Acknowledgements** BHF data science centre team.

# COMMAND ----------

# MAGIC %md
# MAGIC # 0. Setup

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

# DBTITLE 1,Libraries
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
from datetime import date

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

lsoa_mult = spark.table(path_tmp_lsoa)

# COMMAND ----------

# -----------------------------------------------------------------------------
# LSOA Curated Data
# -----------------------------------------------------------------------------
lsoa_region = spark.table(path_cur_lsoa_region)
lsoa_imd    = spark.table(path_cur_lsoa_imd)



# COMMAND ----------

icb= spark.table(path_cur_lsoa_icb)

# COMMAND ----------

# display(lsoa_region)

# COMMAND ----------

# display(lsoa_imd)

# COMMAND ----------

lsoa_mult.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #3. Apply rules to conflicted nearest LSOA

# COMMAND ----------

for i in period:
    tmp = (lsoa_mult
           .withColumnRenamed('person_id','PERSON_ID')
           .withColumn('lsoa_diff',f.datediff(f.col('record_date') ,f.lit(date(i,1,1))))
            .withColumn('lsoa_diff_abs',f.abs('lsoa_diff'))
            .withColumn('lsoa_record_window',f.when((f.col('lsoa_diff') >=0) & (f.col('lsoa_diff') <=365),1).when(f.col('lsoa_diff') <0,2).otherwise(3))
            .distinct()
        )
    _win =(Window
            .partitionBy('PERSON_ID')
            .orderBy('lsoa_record_window','lsoa_diff_abs')
            )
    tmp = tmp.withColumn('_rank',f.dense_rank().over(_win)).where('_rank ==1').drop('_rank')
    tmp = (tmp.pandas_api().groupby('PERSON_ID').agg('mode').reset_index()
            .to_spark().withColumn('_rownum', f.row_number().over(Window.partitionBy('PERSON_ID').orderBy('lsoa_diff_abs')))
            .where('_rownum = 1').drop('_rownum')
            
          )
    print('='*20,'Year', i,'='*20)
    count_var(tmp,'PERSON_ID');print()
    tab(tmp,'lsoa_record_window');print()
    
    outName = f'{proj}_tmp_rank_mode_lsoa_year{i}'.lower()
    tmp.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{outName}')

# COMMAND ----------

# MAGIC %md # 4. Add region & imd
# MAGIC

# COMMAND ----------

######if mapped to icb, run section 5 directly##########

# for i in period:
#   tmp0 = spark.table(f'{path_tmp_rank_lsoa}{i}')
#   tmp0 = (tmp0
#         .withColumn('LSOA_1',f.substring(f.col('lsoa'),1,1))
#         .withColumnRenamed('lsoa','LSOA')
#         )
#   tmp1 = merge(tmp0, lsoa_region, ['LSOA'], validate='m:1', keep_results=['both', 'left_only'])
        

#   tmp2 = (
#     tmp1
#     .withColumn('region',
#                 f.when(f.col('LSOA_1') == 'W', 'Wales')
#                 .when(f.col('LSOA_1') == 'S', 'Scotland')
#                 .otherwise(f.col('region'))
#               )
#     .drop('_merge')
#   )


#   tmp3 = merge(tmp2, lsoa_imd, ['LSOA'], validate='m:1', keep_results=['both', 'left_only'])
  
#   outName = f'{proj}_rank_mode_lsoa_year{i}'.lower()
#   tmp3.select('PERSON_ID','region','IMD_2019_DECILES').write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{outName}')

#   # check
#   tmp = spark.table(f'{dsa}.{outName}')
#   assert tmp.count()==tmp.select('PERSON_ID').distinct().count()
#   # count_var(tmp, 'PERSON_ID'); print()
#   tab(tmp,'region');print()
#   tab(tmp,'IMD_2019_DECILES');print()


 


# COMMAND ----------

# MAGIC %md
# MAGIC #5. Add ICB & imd

# COMMAND ----------

for i in period:
  tmp0 = spark.table(f'{path_tmp_rank_lsoa}{i}')
  tmp0 = (tmp0
        .withColumn('LSOA_1',f.substring(f.col('lsoa'),1,1))
        )
  tmp1 = merge(tmp0, lsoa_region, ['LSOA'], validate='m:1', keep_results=['both', 'left_only'])
        

  tmp2 = (
    tmp1
    .withColumn('region2',
                f.when(f.col('LSOA_1') == 'W', 'Wales')
                .when(f.col('LSOA_1') == 'S', 'Scotland')
                .otherwise(f.col('region'))
              )
    .drop('_merge','region')
  )
  tmp3 = merge(tmp2, icb, ['lsoa'], validate='m:1', keep_results=['both', 'left_only'])
        

  tmp3 = tmp3.drop('_merge')
  


  tmp4 = merge(tmp3, lsoa_imd, ['lsoa'], validate='m:1', keep_results=['both', 'left_only'])
  
  outName = f'{proj}_lsoa_icb_year{i}'.lower()
  tmp3.select('PERSON_ID','region','region2','IMD_2019_DECILES').write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{outName}')

  # check
  tmp = spark.table(f'{dsa}.{outName}')
  assert tmp.count()==tmp.select('PERSON_ID').distinct().count()
  # count_var(tmp, 'PERSON_ID'); print()
  tab(tmp,'region');print()
  tab(tmp,'IMD_2019_DECILES');print()


 
