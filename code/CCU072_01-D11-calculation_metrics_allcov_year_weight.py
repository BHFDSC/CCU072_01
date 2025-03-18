# Databricks notebook source
# MAGIC %md
# MAGIC # CCU072_01-D11-calculation_metrics_allcov_year_weight
# MAGIC
# MAGIC **Description** This notebook calculate yearly prevalence, incidence, 30-d mortality and recurrence for single subgroup and combinations of subgroups. Results incorporated personal weights.
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
# MAGIC #1. Maximally adjusted

# COMMAND ----------

for i in period:
    list_months = []
    list_cols = []
    
    tmp = spark.table(f'{dsa}.{proj}_tmp8_all_year{i}_recur_ethnic_region_weightage1')
    
    
    if i != period[-1]:
             
        tmp = (
                tmp
                .withColumn(f'CENSOR_DATE_END_prev_{i}', f.to_date(f.lit(f'{i}-06-30')))
                .withColumn(f'pop_{i}', f.when(((f.col('DOD').isNull())|(f.col('DOD')>=f.col(f'CENSOR_DATE_END_prev_{i}'))) & (f.col('DOB')<=f.col(f'CENSOR_DATE_END_prev_{i}')) , 1).otherwise(0)) 
                .withColumn(f'flag_prev_{i}',f.when( (f.col(f'pop_{i}') == 1) & (f.col(f'prev') <= f.col(f'CENSOR_DATE_END_prev_{i}')),f.lit(1)).otherwise(f.lit(0)))
            )
            
        tmp = (tmp
                .withColumn(f'CENSOR_DATE_START_{i}',f.when(f.col('DOB') > f'{i}-12-31',None).when(f.col('DOB')<=f'{i}-01-01',f.to_date(f.lit(f'{i}-01-01'))).otherwise(f.col('DOB')))
                .withColumn(f'CENSOR_DATE_END_inc_{i}',f.when((f.col('DOD') > f'{i}-12-31')|(f.col('DOD').isNull()),f.to_date(f.lit(f'{i}-12-31'))).when(f.col('DOD')<f'{i}-01-01',None).otherwise(f.col('DOD'))))
                
                
            
        list_months += [f'{i}']
        tmp = (tmp
                .withColumn(f'nohistory_{i}',f.when( (f.col(f'CENSOR_DATE_START_{i}').isNotNull())&(f.col(f'CENSOR_DATE_END_inc_{i}').isNotNull()) & ((f.col('prev') >= f.col(f'CENSOR_DATE_START_{i}'))|(f.col('prev').isNull())),f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'FU_START_{i}',f.when(f.col(f'nohistory_{i}')==0,None).otherwise(f.col(f'CENSOR_DATE_START_{i}')))
                .withColumn(f'date_inc_{i}', f.when((f.col(f'nohistory_{i}')==1)&(f.col('inc_all') >= f.col(f'FU_START_{i}')) & (f.col('inc_all') <= f.col(f'CENSOR_DATE_END_inc_{i}')), f.col('inc_all')).otherwise(f.lit(None)))
                .withColumn(f'flag_inc_{i}', f.when(f.col(f'date_inc_{i}').isNotNull(), f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'FU_END_{i}', f.least(f.col(f'date_inc_{i}'), f.col(f'CENSOR_DATE_END_inc_{i}')))
                .withColumn(f'DIFF_{i}',f.datediff(f'FU_END_{i}',f'FU_START_{i}'))
                
                .withColumn(f'flag_inc_fatal_{i}',f.when(f.col(f'date_inc_{i}').isNotNull(),f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'flag_fatal_{i}',f.when(f.datediff('DOD',f'date_inc_{i}')<=30,f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'date_inc_recur_{i}',f.when((f.datediff('DOD',f'date_inc_{i}')>30)|(f.col('DOD').isNull()),f.col(f'date_inc_{i}')).otherwise(None))
                
                .withColumn(f'date_recur_{i}',f.when((f.col('recur_strokemi')<=end_date)&(f.datediff('recur_strokemi',f'date_inc_recur_{i}')>30)&(f.datediff('recur_strokemi',f'date_inc_recur_{i}')<=365),f.col('recur_strokemi')).otherwise(None)) 
                .withColumn(f'flag_recur_{i}',f.when(f.col(f'date_recur_{i}').isNotNull(),f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'FU_END_recur_{i}', f.least(f.col(f'date_recur_{i}'),f.col('DOD'),f.col(f'date_inc_recur_{i}')+365,f.lit(end_date)))
                .withColumn(f'DIFF_recur_{i}',f.datediff(f'FU_END_recur_{i}',f'date_inc_recur_{i}'))
                    )
        list_cols += [f'pop_{i}',f'flag_prev_{i}',f'flag_inc_{i}',f'DIFF_{i}',f'flag_inc_fatal_{i}',f'flag_fatal_{i}',f'flag_recur_{i}',f'DIFF_recur_{i}']
    else:
      if end_date >= datetime.date(i,6,30):
        tmp = (
                tmp
                .withColumn(f'CENSOR_DATE_END_prev_{i}', f.to_date(f.lit(f'{i}-06-30')))
                .withColumn(f'pop_{i}', f.when(((f.col('DOD').isNull())|(f.col('DOD')>=f.col(f'CENSOR_DATE_END_prev_{i}'))) & (f.col('DOB')<=f.col(f'CENSOR_DATE_END_prev_{i}')) , 1).otherwise(0)) 
                .withColumn(f'flag_prev_{i}',f.when( (f.col(f'pop_{i}') == 1) & (f.col(f'prev') <= f.col(f'CENSOR_DATE_END_prev_{i}')),f.lit(1)).otherwise(f.lit(0)))
            )
      else:
        tmp = (tmp.withColumn(f'pop_{i}',f.lit(None))
                   .withColumn(f'flag_prev_{i}',f.lit(None))
                    )
      if idx_end_month in [1,3,5,7,8,10,12]:
        tmp = (tmp
                .withColumn(f'CENSOR_DATE_START_{i}',f.when(f.col('DOB') > f'{i}-{idx_end_month}-31',None).when(f.col('DOB')<=f'{i}-01-01',f.to_date(f.lit(f'{i}-01-01'))).otherwise(f.col('DOB')))
                .withColumn(f'CENSOR_DATE_END_inc_{i}',f.when((f.col('DOD') > f'{i}-{idx_end_month}-31')|(f.col('DOD').isNull()),f.to_date(f.lit(f'{i}-{idx_end_month}-31'))).when(f.col('DOD')<f'{i}-01-01',None).otherwise(f.col('DOD'))))
                
                
      elif idx_end_month in [4,6,9,11]:
        tmp = (tmp
                .withColumn(f'CENSOR_DATE_START_{i}',f.when(f.col('DOB') > f'{i}-{idx_end_month}-30',None).when(f.col('DOB')<=f'{i}-01-01',f.to_date(f.lit(f'{i}-01-01'))).otherwise(f.col('DOB')))
                .withColumn(f'CENSOR_DATE_END_inc_{i}',f.when((f.col('DOD') > f'{i}-{idx_end_month}-30')|(f.col('DOD').isNull()),f.to_date(f.lit(f'{i}-{idx_end_month}-30'))).when(f.col('DOD')<f'{i}-01-01',None).otherwise(f.col('DOD'))))
      elif i%4==0 and idx_end_month==2:   
        tmp = (tmp
                .withColumn(f'CENSOR_DATE_START_{i}',f.when(f.col('DOB') > f'{i}-{idx_end_month}-29',None).when(f.col('DOB')<=f'{i}-01-01',f.to_date(f.lit(f'{i}-01-01'))).otherwise(f.col('DOB')))
                .withColumn(f'CENSOR_DATE_END_inc_{i}',f.when((f.col('DOD') > f'{i}-{idx_end_month}-29')|(f.col('DOD').isNull()),f.to_date(f.lit(f'{i}-{idx_end_month}-29'))).when(f.col('DOD')<f'{i}-01-01',None).otherwise(f.col('DOD'))))
      elif i%4!=0 and idx_end_month==2:
        tmp = (tmp
                .withColumn(f'CENSOR_DATE_START_{i}',f.when(f.col('DOB') > f'{i}-{idx_end_month}-28',None).when(f.col('DOB')<=f'{i}-01-01',f.to_date(f.lit(f'{i}-01-01'))).otherwise(f.col('DOB')))
                .withColumn(f'CENSOR_DATE_END_inc_{i}',f.when((f.col('DOD') > f'{i}-{idx_end_month}-28')|(f.col('DOD').isNull()),f.to_date(f.lit(f'{i}-{idx_end_month}-28'))).when(f.col('DOD')<f'{i}-01-01',None).otherwise(f.col('DOD'))))
      list_months += [f'{i}']
      tmp = (tmp
                .withColumn(f'nohistory_{i}',f.when( (f.col(f'CENSOR_DATE_START_{i}').isNotNull())&(f.col(f'CENSOR_DATE_END_inc_{i}').isNotNull()) & ((f.col('prev') >= f.col(f'CENSOR_DATE_START_{i}'))|(f.col('prev').isNull())),f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'FU_START_{i}',f.when(f.col(f'nohistory_{i}')==0,None).otherwise(f.col(f'CENSOR_DATE_START_{i}')))
                .withColumn(f'date_inc_{i}', f.when((f.col(f'nohistory_{i}')==1)&(f.col('inc_all') >= f.col(f'FU_START_{i}')) & (f.col('inc_all') <= f.col(f'CENSOR_DATE_END_inc_{i}')), f.col('inc_all')).otherwise(f.lit(None)))
                .withColumn(f'flag_inc_{i}', f.when(f.col(f'date_inc_{i}').isNotNull(), f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'FU_END_{i}', f.least(f.col(f'date_inc_{i}'), f.col(f'CENSOR_DATE_END_inc_{i}')))
                .withColumn(f'DIFF_{i}',f.datediff(f'FU_END_{i}',f'FU_START_{i}'))
                
                .withColumn(f'flag_inc_fatal_{i}',f.when(f.col(f'date_inc_{i}').isNotNull(),f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'flag_fatal_{i}',f.when(f.datediff('DOD',f'date_inc_{i}')<=30,f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'date_inc_recur_{i}',f.when((f.datediff('DOD',f'date_inc_{i}')>30)|(f.col('DOD').isNull()),f.col(f'date_inc_{i}')).otherwise(None))
                
                .withColumn(f'date_recur_{i}',f.when((f.col('recur_strokemi')<=end_date)&(f.datediff('recur_strokemi',f'date_inc_recur_{i}')>30)&(f.datediff('recur_strokemi',f'date_inc_recur_{i}')<=365),f.col('recur_strokemi')).otherwise(None)) 
                .withColumn(f'flag_recur_{i}',f.when(f.col(f'date_recur_{i}').isNotNull(),f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'FU_END_recur_{i}', f.least(f.col(f'date_recur_{i}'),f.col('DOD'),f.col(f'date_inc_recur_{i}')+365,f.lit(end_date)))
                .withColumn(f'DIFF_recur_{i}',f.datediff(f'FU_END_recur_{i}',f'date_inc_recur_{i}'))
                    )
      list_cols += [f'pop_{i}',f'flag_prev_{i}',f'flag_inc_{i}',f'DIFF_{i}',f'flag_inc_fatal_{i}',f'flag_fatal_{i}',f'flag_recur_{i}',f'DIFF_recur_{i}']

    tmp0 = (
            tmp
            .groupBy(['dcode','age','sex','ethnicity','region','deprivation','comorbidity'])                              
            .agg(
                *[f.sum(f.col(col)*f.col('weight')).alias(col) for col in list_cols ] 
                )
          )

    tmp0.write.mode('overwrite').option('overwriteSchema','true').saveAsTable(f'{dsa}.{proj}_tmp0_wide_all_year{i}')
    tmp0 = spark.table(f'{dsa}.{proj}_tmp0_wide_all_year{i}')
    print(f'tmp0 for year {i} saved')

    tmp1 = (tmp0
            .select('dcode','age','sex','ethnicity','region','deprivation','comorbidity',*[col for col in list_cols],*[(f.col(f'flag_prev_{monx}')/f.col(f'pop_{monx}')*100).alias(f'prev_{monx}') for monx in list_months],*[(f.col(f'flag_inc_{monx}')/f.col(f'DIFF_{monx}')*365.25*100000).alias(f'inc_{monx}') for monx in list_months],*[(f.col(f'flag_fatal_{monx}')/f.col(f'flag_inc_fatal_{monx}')*100).alias(f'fatal_{monx}') for monx in list_months],*[(f.col(f'flag_recur_{monx}')/f.col(f'DIFF_recur_{monx}')*365.25*100000).alias(f'recur_{monx}') for monx in list_months])
          )
            
        
    tmp2 = (tmp1.unpivot(['dcode','age','sex','ethnicity','region','deprivation','comorbidity'],[f'flag_prev_{monx}' for monx in list_months]+[f'flag_inc_{monx}' for monx in list_months]+[f'flag_fatal_{monx}' for monx in list_months]+[f'flag_recur_{monx}' for monx in list_months],'n_name','numerator')
                .withColumn('metric',f.regexp_extract('n_name','flag_(prev|inc|fatal|recur)_(\d{4})',1))

                
                .withColumn('year',f.regexp_extract('n_name','flag_(prev|inc|fatal|recur)_(\d{4})',2))
                .drop('n_name')
            )
    

    tmp3 = (tmp1.unpivot(['dcode','age','sex','ethnicity','region','deprivation','comorbidity'],[f'pop_{monx}' for monx in list_months]+[f'DIFF_{monx}' for monx in list_months]+[f'flag_inc_fatal_{monx}' for monx in list_months]+[f'DIFF_recur_{monx}' for monx in list_months],'n_name','denominator')
            .withColumn('metric',f.regexp_extract('n_name','(pop|DIFF|flag_inc_fatal|DIFF_recur)_(\d{4})',1))
            .withColumn('year',f.regexp_extract('n_name','(pop|DIFF|flag_inc_fatal|DIFF_recur)_(\d{4})',2))
            .drop('n_name')
            .withColumn('metric',f.when(f.col('metric')=="pop",f.lit('prev')).when(f.col('metric')=="DIFF",f.lit('inc')).when(f.col('metric')=="flag_inc_fatal",f.lit('fatal')).when(f.col('metric')=="DIFF_recur",f.lit('recur')))
            .withColumn('denominator',f.when(f.col('metric').isin(["inc",'recur']),f.col('denominator')/365.25/100000).otherwise(f.col('denominator')))       
          )
    
    tmp4 = (tmp1.unpivot(['dcode','age','sex','ethnicity','region','deprivation','comorbidity'],[f'prev_{monx}'for monx in list_months]+[f'inc_{monx}' for monx in list_months]+[f'fatal_{monx}' for monx in list_months]+[f'recur_{monx}' for monx in list_months],'n_name','result')
                .withColumn('metric',f.regexp_extract('n_name','(prev|inc|fatal|recur)_(\d{4})',1))
              
                .withColumn('year',f.regexp_extract('n_name','(prev|inc|fatal|recur)_(\d{4})',2))
                .drop('n_name')
            )
    

    tmpc =(tmp2.join(tmp3,['dcode','age','sex','ethnicity','region','deprivation','comorbidity','metric','year']).join(tmp4,['dcode','age','sex','ethnicity','region','deprivation','comorbidity','metric','year'])
                .withColumn('year',f.col('year').astype('int'))
            )
    

    print(f'All metrics for all phenos adjusted for all covs weighted to ONS pop in year{i} completed') 
    
    tmpc.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp_all_year{i}_allpos_allcovadj_year_weight_age1')



# COMMAND ----------

k = 0
for i in period:
  k +=1
  tmp = spark.table(f'{dsa}.{proj}_tmp_all_year{i}_allpos_allcovadj_year_weight_age1')
  if k==1:tmpc = tmp
  else: tmpc = tmpc.unionByName(tmp)
tmpc.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_results_allcovadj_year_weight_age1_ethnicregion')
# tmpc = spark.table(f'{dsa}.{proj}_results_allcovadj_year_weight')
# display(tmpc)

# COMMAND ----------

tmpc = spark.table(f'{dsa}.{proj}_results_allcovadj_year_weight_age1_ethnicregion')
tmpc = tmpc.withColumn('CHUNK',f.floor(f.rand()*10)+1)
tmpc.select('CHUNK').distinct().show()
tmpc.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_results_allcovadj_year_weight_age1_ethnicregion')

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Minimally adjusted

# COMMAND ----------

for i in period:
    list_months = []
    list_cols = []
    
    tmp = spark.table(f'{dsa}.{proj}_tmp8_all_year{i}_recur_ethnic_region_weightage1')
    
    
    if i != period[-1]:
             
        tmp = (
                tmp
                .withColumn(f'CENSOR_DATE_END_prev_{i}', f.to_date(f.lit(f'{i}-06-30')))
                .withColumn(f'pop_{i}', f.when(((f.col('DOD').isNull())|(f.col('DOD')>=f.col(f'CENSOR_DATE_END_prev_{i}'))) & (f.col('DOB')<=f.col(f'CENSOR_DATE_END_prev_{i}')) , 1).otherwise(0)) 
                .withColumn(f'flag_prev_{i}',f.when( (f.col(f'pop_{i}') == 1) & (f.col(f'prev') <= f.col(f'CENSOR_DATE_END_prev_{i}')),f.lit(1)).otherwise(f.lit(0)))
            )
            
        tmp = (tmp
                .withColumn(f'CENSOR_DATE_START_{i}',f.when(f.col('DOB') > f'{i}-12-31',None).when(f.col('DOB')<=f'{i}-01-01',f.to_date(f.lit(f'{i}-01-01'))).otherwise(f.col('DOB')))
                .withColumn(f'CENSOR_DATE_END_inc_{i}',f.when((f.col('DOD') > f'{i}-12-31')|(f.col('DOD').isNull()),f.to_date(f.lit(f'{i}-12-31'))).when(f.col('DOD')<f'{i}-01-01',None).otherwise(f.col('DOD'))))
                
                
            
        list_months += [f'{i}']
        tmp = (tmp
                .withColumn(f'nohistory_{i}',f.when( (f.col(f'CENSOR_DATE_START_{i}').isNotNull())&(f.col(f'CENSOR_DATE_END_inc_{i}').isNotNull()) & ((f.col('prev') >= f.col(f'CENSOR_DATE_START_{i}'))|(f.col('prev').isNull())),f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'FU_START_{i}',f.when(f.col(f'nohistory_{i}')==0,None).otherwise(f.col(f'CENSOR_DATE_START_{i}')))
                .withColumn(f'date_inc_{i}', f.when((f.col(f'nohistory_{i}')==1)&(f.col('inc_all') >= f.col(f'FU_START_{i}')) & (f.col('inc_all') <= f.col(f'CENSOR_DATE_END_inc_{i}')), f.col('inc_all')).otherwise(f.lit(None)))
                .withColumn(f'flag_inc_{i}', f.when(f.col(f'date_inc_{i}').isNotNull(), f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'FU_END_{i}', f.least(f.col(f'date_inc_{i}'), f.col(f'CENSOR_DATE_END_inc_{i}')))
                .withColumn(f'DIFF_{i}',f.datediff(f'FU_END_{i}',f'FU_START_{i}'))
                
                .withColumn(f'flag_inc_fatal_{i}',f.when(f.col(f'date_inc_{i}').isNotNull(),f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'flag_fatal_{i}',f.when(f.datediff('DOD',f'date_inc_{i}')<=30,f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'date_inc_recur_{i}',f.when((f.datediff('DOD',f'date_inc_{i}')>30)|(f.col('DOD').isNull()),f.col(f'date_inc_{i}')).otherwise(None))
                
                .withColumn(f'date_recur_{i}',f.when((f.col('recur_strokemi')<=end_date)&(f.datediff('recur_strokemi',f'date_inc_recur_{i}')>30)&(f.datediff('recur_strokemi',f'date_inc_recur_{i}')<=365),f.col('recur_strokemi')).otherwise(None)) 
                .withColumn(f'flag_recur_{i}',f.when(f.col(f'date_recur_{i}').isNotNull(),f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'FU_END_recur_{i}', f.least(f.col(f'date_recur_{i}'),f.col('DOD'),f.col(f'date_inc_recur_{i}')+365,f.lit(end_date)))
                .withColumn(f'DIFF_recur_{i}',f.datediff(f'FU_END_recur_{i}',f'date_inc_recur_{i}'))
                    )
        list_cols += [f'pop_{i}',f'flag_prev_{i}',f'flag_inc_{i}',f'DIFF_{i}',f'flag_inc_fatal_{i}',f'flag_fatal_{i}',f'flag_recur_{i}',f'DIFF_recur_{i}']
    else:
      if end_date >= datetime.date(i,6,30):
         tmp = (
                tmp
                .withColumn(f'CENSOR_DATE_END_prev_{i}', f.to_date(f.lit(f'{i}-06-30')))
                .withColumn(f'pop_{i}', f.when(((f.col('DOD').isNull())|(f.col('DOD')>=f.col(f'CENSOR_DATE_END_prev_{i}'))) & (f.col('DOB')<=f.col(f'CENSOR_DATE_END_prev_{i}')) , 1).otherwise(0)) 
                .withColumn(f'flag_prev_{i}',f.when( (f.col(f'pop_{i}') == 1) & (f.col(f'prev') <= f.col(f'CENSOR_DATE_END_prev_{i}')),f.lit(1)).otherwise(f.lit(0)))
            )
      else:
        tmp = (tmp.withColumn(f'pop_{i}',f.lit(None))
                  .withColumn(f'flag_prev_{i}',f.lit(None))
               )
      if idx_end_month in [1,3,5,7,8,10,12]:
        tmp = (tmp
                .withColumn(f'CENSOR_DATE_START_{i}',f.when(f.col('DOB') > f'{i}-{idx_end_month}-31',None).when(f.col('DOB')<=f'{i}-01-01',f.to_date(f.lit(f'{i}-01-01'))).otherwise(f.col('DOB')))
                .withColumn(f'CENSOR_DATE_END_inc_{i}',f.when((f.col('DOD') > f'{i}-{idx_end_month}-31')|(f.col('DOD').isNull()),f.to_date(f.lit(f'{i}-{idx_end_month}-31'))).when(f.col('DOD')<f'{i}-01-01',None).otherwise(f.col('DOD'))))
                
                
      elif idx_end_month in [4,6,9,11]:
        tmp = (tmp
                .withColumn(f'CENSOR_DATE_START_{i}',f.when(f.col('DOB') > f'{i}-{idx_end_month}-30',None).when(f.col('DOB')<=f'{i}-01-01',f.to_date(f.lit(f'{i}-01-01'))).otherwise(f.col('DOB')))
                .withColumn(f'CENSOR_DATE_END_inc_{i}',f.when((f.col('DOD') > f'{i}-{idx_end_month}-30')|(f.col('DOD').isNull()),f.to_date(f.lit(f'{i}-{idx_end_month}-30'))).when(f.col('DOD')<f'{i}-01-01',None).otherwise(f.col('DOD'))))
      elif i%4==0 and idx_end_month==2:   
        tmp = (tmp
                .withColumn(f'CENSOR_DATE_START_{i}',f.when(f.col('DOB') > f'{i}-{idx_end_month}-29',None).when(f.col('DOB')<=f'{i}-01-01',f.to_date(f.lit(f'{i}-01-01'))).otherwise(f.col('DOB')))
                .withColumn(f'CENSOR_DATE_END_inc_{i}',f.when((f.col('DOD') > f'{i}-{idx_end_month}-29')|(f.col('DOD').isNull()),f.to_date(f.lit(f'{i}-{idx_end_month}-29'))).when(f.col('DOD')<f'{i}-01-01',None).otherwise(f.col('DOD'))))
      elif i%4!=0 and idx_end_month==2:
        tmp = (tmp
                .withColumn(f'CENSOR_DATE_START_{i}',f.when(f.col('DOB') > f'{i}-{idx_end_month}-28',None).when(f.col('DOB')<=f'{i}-01-01',f.to_date(f.lit(f'{i}-01-01'))).otherwise(f.col('DOB')))
                .withColumn(f'CENSOR_DATE_END_inc_{i}',f.when((f.col('DOD') > f'{i}-{idx_end_month}-28')|(f.col('DOD').isNull()),f.to_date(f.lit(f'{i}-{idx_end_month}-28'))).when(f.col('DOD')<f'{i}-01-01',None).otherwise(f.col('DOD'))))
      list_months += [f'{i}']
      tmp = (tmp
                .withColumn(f'nohistory_{i}',f.when( (f.col(f'CENSOR_DATE_START_{i}').isNotNull())&(f.col(f'CENSOR_DATE_END_inc_{i}').isNotNull()) & ((f.col('prev') >= f.col(f'CENSOR_DATE_START_{i}'))|(f.col('prev').isNull())),f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'FU_START_{i}',f.when(f.col(f'nohistory_{i}')==0,None).otherwise(f.col(f'CENSOR_DATE_START_{i}')))
                .withColumn(f'date_inc_{i}', f.when((f.col(f'nohistory_{i}')==1)&(f.col('inc_all') >= f.col(f'FU_START_{i}')) & (f.col('inc_all') <= f.col(f'CENSOR_DATE_END_inc_{i}')), f.col('inc_all')).otherwise(f.lit(None)))
                .withColumn(f'flag_inc_{i}', f.when(f.col(f'date_inc_{i}').isNotNull(), f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'FU_END_{i}', f.least(f.col(f'date_inc_{i}'), f.col(f'CENSOR_DATE_END_inc_{i}')))
                .withColumn(f'DIFF_{i}',f.datediff(f'FU_END_{i}',f'FU_START_{i}'))
                
                .withColumn(f'flag_inc_fatal_{i}',f.when(f.col(f'date_inc_{i}').isNotNull(),f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'flag_fatal_{i}',f.when(f.datediff('DOD',f'date_inc_{i}')<=30,f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'date_inc_recur_{i}',f.when((f.datediff('DOD',f'date_inc_{i}')>30)|(f.col('DOD').isNull()),f.col(f'date_inc_{i}')).otherwise(None))
                
                .withColumn(f'date_recur_{i}',f.when((f.col('recur_strokemi')<=end_date)&(f.datediff('recur_strokemi',f'date_inc_recur_{i}')>30)&(f.datediff('recur_strokemi',f'date_inc_recur_{i}')<=365),f.col('recur_strokemi')).otherwise(None)) 
                .withColumn(f'flag_recur_{i}',f.when(f.col(f'date_recur_{i}').isNotNull(),f.lit(1)).otherwise(f.lit(0)))
                .withColumn(f'FU_END_recur_{i}', f.least(f.col(f'date_recur_{i}'),f.col('DOD'),f.col(f'date_inc_recur_{i}')+365,f.lit(end_date)))
                .withColumn(f'DIFF_recur_{i}',f.datediff(f'FU_END_recur_{i}',f'date_inc_recur_{i}'))
                    )
      list_cols += [f'pop_{i}',f'flag_prev_{i}',f'flag_inc_{i}',f'DIFF_{i}',f'flag_inc_fatal_{i}',f'flag_fatal_{i}',f'flag_recur_{i}',f'DIFF_recur_{i}']

    tmp0 = tmp.unpivot(['dcode','weight']+list_cols,['age','sex','ethnicity','region','deprivation','comorbidity'],'group','value')

    tmp0 = (
            tmp0
            .groupBy(['dcode','group','value'])                              
            .agg(
                *[f.sum(f.col(col)*f.col('weight')).alias(col) for col in list_cols ] 
                )
          )

    tmp0.write.mode('overwrite').option('overwriteSchema','true').saveAsTable(f'{dsa}.{proj}_tmp0_wide_all_year{i}')
    tmp0 = spark.table(f'{dsa}.{proj}_tmp0_wide_all_year{i}')
    print(f'tmp0 for year {i} saved')

    tmp1 = (tmp0
            .select('dcode','group','value',*[col for col in list_cols],*[(f.col(f'flag_prev_{monx}')/f.col(f'pop_{monx}')*100).alias(f'prev_{monx}') for monx in list_months],*[(f.col(f'flag_inc_{monx}')/f.col(f'DIFF_{monx}')*365.25*100000).alias(f'inc_{monx}') for monx in list_months],*[(f.col(f'flag_fatal_{monx}')/f.col(f'flag_inc_fatal_{monx}')*100).alias(f'fatal_{monx}') for monx in list_months],*[(f.col(f'flag_recur_{monx}')/f.col(f'DIFF_recur_{monx}')*365.25*100000).alias(f'recur_{monx}') for monx in list_months])
          )
            
        
    tmp2 = (tmp1.unpivot(['dcode','group','value'],[f'flag_prev_{monx}' for monx in list_months]+[f'flag_inc_{monx}' for monx in list_months]+[f'flag_fatal_{monx}' for monx in list_months]+[f'flag_recur_{monx}' for monx in list_months],'n_name','numerator')
                .withColumn('metric',f.regexp_extract('n_name','flag_(prev|inc|fatal|recur)_(\d{4})',1))

                
                .withColumn('year',f.regexp_extract('n_name','flag_(prev|inc|fatal|recur)_(\d{4})',2))
                .drop('n_name')
            )
    

    tmp3 = (tmp1.unpivot(['dcode','group','value'],[f'pop_{monx}' for monx in list_months]+[f'DIFF_{monx}' for monx in list_months]+[f'flag_inc_fatal_{monx}' for monx in list_months]+[f'DIFF_recur_{monx}' for monx in list_months],'n_name','denominator')
            .withColumn('metric',f.regexp_extract('n_name','(pop|DIFF|flag_inc_fatal|DIFF_recur)_(\d{4})',1))
            .withColumn('year',f.regexp_extract('n_name','(pop|DIFF|flag_inc_fatal|DIFF_recur)_(\d{4})',2))
            .drop('n_name')
            .withColumn('metric',f.when(f.col('metric')=="pop",f.lit('prev')).when(f.col('metric')=="DIFF",f.lit('inc')).when(f.col('metric')=="flag_inc_fatal",f.lit('fatal')).when(f.col('metric')=="DIFF_recur",f.lit('recur')))
            .withColumn('denominator',f.when(f.col('metric').isin(["inc",'recur']),f.col('denominator')/365.25/100000).otherwise(f.col('denominator')))       
          )
    
    tmp4 = (tmp1.unpivot(['dcode','group','value'],[f'prev_{monx}'for monx in list_months]+[f'inc_{monx}' for monx in list_months]+[f'fatal_{monx}' for monx in list_months]+[f'recur_{monx}' for monx in list_months],'n_name','result')
                .withColumn('metric',f.regexp_extract('n_name','(prev|inc|fatal|recur)_(\d{4})',1))
              
                .withColumn('year',f.regexp_extract('n_name','(prev|inc|fatal|recur)_(\d{4})',2))
                .drop('n_name')
            )
    

    tmpc =(tmp2.join(tmp3,['dcode','group','value','metric','year']).join(tmp4,['dcode','group','value','metric','year'])
                .withColumn('year',f.col('year').astype('int'))
            )
    

    print(f'All metrics for all phenos adjusted for one cov weighted to ONS pop in year{i} completed') 
    
    tmpc.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp_all_year{i}_allpos_onecovadj_year_weight_age1')



# COMMAND ----------

k = 0
for i in period:
  k +=1
  tmp = spark.table(f'{dsa}.{proj}_tmp_all_year{i}_allpos_onecovadj_year_weight_age1')
  if k==1:tmpc = tmp
  else: tmpc = tmpc.unionByName(tmp)
tmpc.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_results_onecovadj_year_weight_age1_ethnicregion')
