# Databricks notebook source
# MAGIC %md
# MAGIC # CCU072_01-D10-calculation_metrics_month_weight
# MAGIC
# MAGIC **Description** This notebook calculate monthly prevalence, incidence, 30-d mortality and recurrence incorporating personal weights.
# MAGIC
# MAGIC **Authors** Wen Shi
# MAGIC
# MAGIC **Last Reviewed**17/03/2025
# MAGIC
# MAGIC **Acknowledgements** Tom Bolton

# COMMAND ----------

# MAGIC %md
# MAGIC # 0. Setup

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

#import the pyspaprk module
# from pyspark.sql.functions import col,lit,when

import re
import io
import datetime 

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns
import textwrap as txt
from timeit import default_timer as timer

print("Matplotlib version: ", matplotlib.__version__)
print("Seaborn version: ", sns.__version__)
_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow:  {_datetimenow}")

# COMMAND ----------

# MAGIC %run "./CCU072_01-D01-parameters"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Calculation

# COMMAND ----------

for i in period:
    list_months = []
    list_cols = []
    
    tmp = spark.table(f'{dsa}.{proj}_tmp8_all_year{i}_recur_weight_age1')
    
    
    if i != period[-1]:
        for v in month:      
            tmp = (
                    tmp
                    .withColumn(f'CENSOR_DATE_END_prev_{i}_{v}', f.to_date(f.lit(f'{i}-{v}-15')))
                    .withColumn(f'pop_{i}_{v}', f.when(((f.col('DOD').isNull())|(f.col('DOD')>=f.col(f'CENSOR_DATE_END_prev_{i}_{v}'))) & (f.col('DOB')<=f.col(f'CENSOR_DATE_END_prev_{i}_{v}')) , 1).otherwise(0)) 
                    .withColumn(f'flag_prev_{i}_{v}',f.when( (f.col(f'pop_{i}_{v}') == 1) & (f.col(f'prev') <= f.col(f'CENSOR_DATE_END_prev_{i}_{v}')),f.lit(1)).otherwise(f.lit(0)))
            )
            if v in [1,3,5,7,8,10,12]:
                tmp = (tmp
                        .withColumn(f'CENSOR_DATE_START_{i}_{v}',f.when(f.col('DOB') > f'{i}-{v}-31',None).when(f.col('DOB')<=f'{i}-{v}-1',f.to_date(f.lit(f'{i}-{v}-1'))).otherwise(f.col('DOB')))
                        .withColumn(f'CENSOR_DATE_END_inc_{i}_{v}',f.when((f.col('DOD') > f'{i}-{v}-31')|(f.col('DOD').isNull()),f.to_date(f.lit(f'{i}-{v}-31'))).when(f.col('DOD')<f'{i}-{v}-1',None).otherwise(f.col('DOD'))))
                
                
            elif v in [4,6,9,11]:
                tmp = (tmp
                              .withColumn(f'CENSOR_DATE_START_{i}_{v}',f.when(f.col('DOB') > f'{i}-{v}-30',None).when(f.col('DOB')<=f'{i}-{v}-1',f.to_date(f.lit(f'{i}-{v}-1'))).otherwise(f.col('DOB')))
                              .withColumn(f'CENSOR_DATE_END_inc_{i}_{v}',f.when((f.col('DOD') > f'{i}-{v}-30')|(f.col('DOD').isNull()), f.to_date(f.lit(f'{i}-{v}-30'))).when(f.col('DOD')<f'{i}-{v}-1',None).otherwise(f.col('DOD'))))
            elif i%4==0 and v==2:   
                tmp = (tmp
                              .withColumn(f'CENSOR_DATE_START_{i}_{v}',f.when(f.col('DOB') > f'{i}-{v}-29',None).when(f.col('DOB')<=f'{i}-{v}-1',f.to_date(f.lit(f'{i}-{v}-1'))).otherwise(f.col('DOB')))
                              .withColumn(f'CENSOR_DATE_END_inc_{i}_{v}',f.when((f.col('DOD') > f'{i}-{v}-29')|(f.col('DOD').isNull()), f.to_date(f.lit(f'{i}-{v}-29'))).when(f.col('DOD')<f'{i}-{v}-1',None).otherwise(f.col('DOD'))))
            elif i%4!=0 and v==2:
                tmp = (tmp
                              .withColumn(f'CENSOR_DATE_START_{i}_{v}',f.when(f.col('DOB') > f'{i}-{v}-28',None).when(f.col('DOB')<=f'{i}-{v}-1',f.to_date(f.lit(f'{i}-{v}-1'))).otherwise(f.col('DOB')))
                              .withColumn(f'CENSOR_DATE_END_inc_{i}_{v}',f.when((f.col('DOD') > f'{i}-{v}-28')|(f.col('DOD').isNull()), f.to_date(f.lit(f'{i}-{v}-28'))).when(f.col('DOD')<f'{i}-{v}-1',None).otherwise(f.col('DOD'))))
            list_months += [f'{i}_{v}']
            tmp = (tmp
                    .withColumn(f'nohistory_{i}_{v}',f.when( (f.col(f'CENSOR_DATE_START_{i}_{v}').isNotNull())&(f.col(f'CENSOR_DATE_END_inc_{i}_{v}').isNotNull()) & ((f.col('prev') >= f.col(f'CENSOR_DATE_START_{i}_{v}'))|(f.col('prev').isNull())),f.lit(1)).otherwise(f.lit(0)))
                    .withColumn(f'FU_START_{i}_{v}',f.when(f.col(f'nohistory_{i}_{v}')==0,None).otherwise(f.col(f'CENSOR_DATE_START_{i}_{v}')))
                    .withColumn(f'date_inc_{i}_{v}', f.when((f.col(f'nohistory_{i}_{v}')==1)&(f.col('inc_all') >= f.col(f'FU_START_{i}_{v}')) & (f.col('inc_all') <= f.col(f'CENSOR_DATE_END_inc_{i}_{v}')), f.col('inc_all')).otherwise(f.lit(None)))
                    .withColumn(f'flag_inc_{i}_{v}', f.when(f.col(f'date_inc_{i}_{v}').isNotNull(), f.lit(1)).otherwise(f.lit(0)))
                    .withColumn(f'FU_END_{i}_{v}', f.least(f.col(f'date_inc_{i}_{v}'), f.col(f'CENSOR_DATE_END_inc_{i}_{v}')))
                    .withColumn(f'DIFF_{i}_{v}',f.datediff(f'FU_END_{i}_{v}',f'FU_START_{i}_{v}'))
                    
                    .withColumn(f'flag_inc_fatal_{i}_{v}',f.when(f.col(f'date_inc_{i}_{v}').isNotNull(),f.lit(1)).otherwise(f.lit(0)))
                    .withColumn(f'flag_fatal_{i}_{v}',f.when(f.datediff('DOD',f'date_inc_{i}_{v}')<=30,f.lit(1)).otherwise(f.lit(0)))
                    .withColumn(f'date_inc_recur_{i}_{v}',f.when((f.datediff('DOD',f'date_inc_{i}_{v}')>30)|(f.col('DOD').isNull()),f.col(f'date_inc_{i}_{v}')).otherwise(None))
                    
                    .withColumn(f'date_recur_{i}_{v}',f.when((f.col('recur_strokemi')<=end_date)&(f.datediff('recur_strokemi',f'date_inc_recur_{i}_{v}')>30)&(f.datediff('recur_strokemi',f'date_inc_recur_{i}_{v}')<=365),f.col('recur_strokemi')).otherwise(None)) 
                    .withColumn(f'flag_recur_{i}_{v}',f.when(f.col(f'date_recur_{i}_{v}').isNotNull(),f.lit(1)).otherwise(f.lit(0)))
                    .withColumn(f'FU_END_recur_{i}_{v}', f.least(f.col(f'date_recur_{i}_{v}'),f.col('DOD'),f.col(f'date_inc_recur_{i}_{v}')+365,f.lit(end_date)))
                    .withColumn(f'DIFF_recur_{i}_{v}',f.datediff(f'FU_END_recur_{i}_{v}',f'date_inc_recur_{i}_{v}'))
                     )
            list_cols += [f'pop_{i}_{v}',f'flag_prev_{i}_{v}',f'flag_inc_{i}_{v}',f'DIFF_{i}_{v}',f'flag_inc_fatal_{i}_{v}',f'flag_fatal_{i}_{v}',f'flag_recur_{i}_{v}',f'DIFF_recur_{i}_{v}']
    else:
        for v in range(1,idx_end_month+1):
            tmp = (
                    tmp
                    .withColumn(f'CENSOR_DATE_END_prev_{i}_{v}', f.to_date(f.lit(f'{i}-{v}-15')))
                    .withColumn(f'pop_{i}_{v}', f.when(((f.col('DOD').isNull())|(f.col('DOD')>=f.col(f'CENSOR_DATE_END_prev_{i}_{v}'))) & (f.col('DOB')<=f.col(f'CENSOR_DATE_END_prev_{i}_{v}')) , 1).otherwise(0)) 
                    .withColumn(f'flag_prev_{i}_{v}',f.when( (f.col(f'pop_{i}_{v}') == 1) & (f.col(f'prev') <= f.col(f'CENSOR_DATE_END_prev_{i}_{v}')),f.lit(1)).otherwise(f.lit(0)))
            )
            if v in [1,3,5,7,8,10,12]:
                tmp = (tmp
                        .withColumn(f'CENSOR_DATE_START_{i}_{v}',f.when(f.col('DOB') > f'{i}-{v}-31',None).when(f.col('DOB')<=f'{i}-{v}-1',f.to_date(f.lit(f'{i}-{v}-1'))).otherwise(f.col('DOB')))
                        .withColumn(f'CENSOR_DATE_END_inc_{i}_{v}',f.when((f.col('DOD') > f'{i}-{v}-31')|(f.col('DOD').isNull()),f.to_date(f.lit(f'{i}-{v}-31'))).when(f.col('DOD')<f'{i}-{v}-1',None).otherwise(f.col('DOD'))))
                
                
            elif v in [4,6,9,11]:
                tmp = (tmp
                              .withColumn(f'CENSOR_DATE_START_{i}_{v}',f.when(f.col('DOB') > f'{i}-{v}-30',None).when(f.col('DOB')<=f'{i}-{v}-1',f.to_date(f.lit(f'{i}-{v}-1'))).otherwise(f.col('DOB')))
                              .withColumn(f'CENSOR_DATE_END_inc_{i}_{v}',f.when((f.col('DOD') > f'{i}-{v}-30')|(f.col('DOD').isNull()), f.to_date(f.lit(f'{i}-{v}-30'))).when(f.col('DOD')<f'{i}-{v}-1',None).otherwise(f.col('DOD'))))
            elif i%4==0 and v==2:   
                tmp = (tmp
                              .withColumn(f'CENSOR_DATE_START_{i}_{v}',f.when(f.col('DOB') > f'{i}-{v}-29',None).when(f.col('DOB')<=f'{i}-{v}-1',f.to_date(f.lit(f'{i}-{v}-1'))).otherwise(f.col('DOB')))
                              .withColumn(f'CENSOR_DATE_END_inc_{i}_{v}',f.when((f.col('DOD') > f'{i}-{v}-29')|(f.col('DOD').isNull()), f.to_date(f.lit(f'{i}-{v}-29'))).when(f.col('DOD')<f'{i}-{v}-1',None).otherwise(f.col('DOD'))))
            elif i%4!=0 and v==2:
                tmp = (tmp
                              .withColumn(f'CENSOR_DATE_START_{i}_{v}',f.when(f.col('DOB') > f'{i}-{v}-28',None).when(f.col('DOB')<=f'{i}-{v}-1',f.to_date(f.lit(f'{i}-{v}-1'))).otherwise(f.col('DOB')))
                              .withColumn(f'CENSOR_DATE_END_inc_{i}_{v}',f.when((f.col('DOD') > f'{i}-{v}-28')|(f.col('DOD').isNull()), f.to_date(f.lit(f'{i}-{v}-28'))).when(f.col('DOD')<f'{i}-{v}-1',None).otherwise(f.col('DOD'))))
            list_months += [f'{i}_{v}']
            tmp = (tmp
                    .withColumn(f'nohistory_{i}_{v}',f.when( (f.col(f'CENSOR_DATE_START_{i}_{v}').isNotNull())&(f.col(f'CENSOR_DATE_END_inc_{i}_{v}').isNotNull()) & ((f.col('prev') >= f.col(f'CENSOR_DATE_START_{i}_{v}'))|(f.col('prev').isNull())),f.lit(1)).otherwise(f.lit(0)))
                    .withColumn(f'FU_START_{i}_{v}',f.when(f.col(f'nohistory_{i}_{v}')==0,None).otherwise(f.col(f'CENSOR_DATE_START_{i}_{v}')))
                    .withColumn(f'date_inc_{i}_{v}', f.when((f.col(f'nohistory_{i}_{v}')==1)&(f.col('inc_all') >= f.col(f'FU_START_{i}_{v}')) & (f.col('inc_all') <= f.col(f'CENSOR_DATE_END_inc_{i}_{v}')), f.col('inc_all')).otherwise(f.lit(None)))
                    .withColumn(f'flag_inc_{i}_{v}', f.when(f.col(f'date_inc_{i}_{v}').isNotNull(), f.lit(1)).otherwise(f.lit(0)))
                    .withColumn(f'FU_END_{i}_{v}', f.least(f.col(f'date_inc_{i}_{v}'), f.col(f'CENSOR_DATE_END_inc_{i}_{v}')))
                    .withColumn(f'DIFF_{i}_{v}',f.datediff(f'FU_END_{i}_{v}',f'FU_START_{i}_{v}'))
                    
                    .withColumn(f'flag_inc_fatal_{i}_{v}',f.when(f.col(f'date_inc_{i}_{v}').isNotNull(),f.lit(1)).otherwise(f.lit(0)))
                    .withColumn(f'flag_fatal_{i}_{v}',f.when(f.datediff('DOD',f'date_inc_{i}_{v}')<=30,f.lit(1)).otherwise(f.lit(0)))
                    .withColumn(f'date_inc_recur_{i}_{v}',f.when((f.datediff('DOD',f'date_inc_{i}_{v}')>30)|(f.col('DOD').isNull()),f.col(f'date_inc_{i}_{v}')).otherwise(None))
                    
                    .withColumn(f'date_recur_{i}_{v}',f.when((f.col('recur_strokemi')<=end_date)&(f.datediff('recur_strokemi',f'date_inc_recur_{i}_{v}')>30)&(f.datediff('recur_strokemi',f'date_inc_recur_{i}_{v}')<=365),f.col('recur_strokemi')).otherwise(None)) 
                    .withColumn(f'flag_recur_{i}_{v}',f.when(f.col(f'date_recur_{i}_{v}').isNotNull(),f.lit(1)).otherwise(f.lit(0)))
                    .withColumn(f'FU_END_recur_{i}_{v}', f.least(f.col(f'date_recur_{i}_{v}'),f.col('DOD'),f.col(f'date_inc_recur_{i}_{v}')+365,f.lit(end_date)))
                    .withColumn(f'DIFF_recur_{i}_{v}',f.datediff(f'FU_END_recur_{i}_{v}',f'date_inc_recur_{i}_{v}'))
                     )
            list_cols += [f'pop_{i}_{v}',f'flag_prev_{i}_{v}',f'flag_inc_{i}_{v}',f'DIFF_{i}_{v}',f'flag_inc_fatal_{i}_{v}',f'flag_fatal_{i}_{v}',f'flag_recur_{i}_{v}',f'DIFF_recur_{i}_{v}']
    
    tmp0 = (
            tmp
            .groupBy(['dcode'])                              
            .agg(
                *[f.sum(f.col(col)*f.col('weight')).alias(col) for col in list_cols ] 
                )
          )

    tmp0.write.mode('overwrite').option('overwriteSchema','true').saveAsTable(f'{dsa}.{proj}_tmp0_monthstd_wide_all_year{i}')
    tmp0 = spark.table(f'{dsa}.{proj}_tmp0_monthstd_wide_all_year{i}')
    print(f'tmp0 for year {i} saved')

    tmp1 = (tmp0
            .select('dcode',*[col for col in list_cols],*[(f.col(f'flag_prev_{monx}')/f.col(f'pop_{monx}')*100).alias(f'prev_{monx}') for monx in list_months],*[(f.col(f'flag_inc_{monx}')/f.col(f'DIFF_{monx}')*365.25*100000).alias(f'inc_{monx}') for monx in list_months],*[(f.col(f'flag_fatal_{monx}')/f.col(f'flag_inc_fatal_{monx}')*100).alias(f'fatal_{monx}') for monx in list_months],*[(f.col(f'flag_recur_{monx}')/f.col(f'DIFF_recur_{monx}')*365.25*100000).alias(f'recur_{monx}') for monx in list_months])
          )
            
        
    tmp2 = (tmp1.unpivot(['dcode'],[f'flag_prev_{monx}' for monx in list_months]+[f'flag_inc_{monx}' for monx in list_months]+[f'flag_fatal_{monx}' for monx in list_months]+[f'flag_recur_{monx}' for monx in list_months],'n_name','numerator')
                .withColumn('metric',f.regexp_extract('n_name','flag_(prev|inc|fatal|recur)_(\d{4})_(\d+)',1))

                
                .withColumn('year',f.regexp_extract('n_name','flag_(prev|inc|fatal|recur)_(\d{4})_(\d+)',2))
                .withColumn('month',f.regexp_extract('n_name','flag_(prev|inc|fatal|recur)_(\d{4})_(\d+)',3))
                .drop('n_name')
            )
    

    tmp3 = (tmp1.unpivot(['dcode'],[f'pop_{monx}' for monx in list_months]+[f'DIFF_{monx}' for monx in list_months]+[f'flag_inc_fatal_{monx}' for monx in list_months]+[f'DIFF_recur_{monx}' for monx in list_months],'n_name','denominator')
            .withColumn('metric',f.regexp_extract('n_name','(pop|DIFF|flag_inc_fatal|DIFF_recur)_(\d{4})_(\d+)',1))
            .withColumn('year',f.regexp_extract('n_name','(pop|DIFF|flag_inc_fatal|DIFF_recur)_(\d{4})_(\d+)',2))
            .withColumn('month',f.regexp_extract('n_name','(pop|DIFF|flag_inc_fatal|DIFF_recur)_(\d{4})_(\d+)',3))
            .drop('n_name')
            .withColumn('metric',f.when(f.col('metric')=="pop",f.lit('prev')).when(f.col('metric')=="DIFF",f.lit('inc')).when(f.col('metric')=="flag_inc_fatal",f.lit('fatal')).when(f.col('metric')=="DIFF_recur",f.lit('recur')))
            .withColumn('denominator',f.when(f.col('metric').isin(["inc",'recur']),f.col('denominator')/365.25/100000).otherwise(f.col('denominator')))       
          )
   
    tmp4 = (tmp1.unpivot(['dcode'],[f'prev_{monx}'for monx in list_months]+[f'inc_{monx}' for monx in list_months]+[f'fatal_{monx}' for monx in list_months]+[f'recur_{monx}' for monx in list_months],'n_name','result')
                .withColumn('metric',f.regexp_extract('n_name','(prev|inc|fatal|recur)_(\d{4})_(\d+)',1))
              
                .withColumn('year',f.regexp_extract('n_name','(prev|inc|fatal|recur)_(\d{4})_(\d+)',2))
                .withColumn('month',f.regexp_extract('n_name','(prev|inc|fatal|recur)_(\d{4})_(\d+)',3))
                .drop('n_name')
            )
     

    tmpc =(tmp2.join(tmp3,['dcode','metric','year','month']).join(tmp4,['dcode','metric','year','month'])
                .withColumn('year',f.col('year').astype('int'))
                .withColumn('month',f.col('month').astype('int'))
            )
    

    print(f'All monthly weighted metrics for all phenos in year{i} completed') 
    
    tmpc.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp_all_year{i}_allpos_weight_age1')



# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Combine and save

# COMMAND ----------

k=0
for i in period:
  k +=1
  tmp = spark.table(f'{dsa}.{proj}_tmp_all_year{i}_allpos_weight_age1')
  if k==1:tmpc = tmp
  else: tmpc = tmpc.unionByName(tmp)
tmpc.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_results_month_weight_age1')
