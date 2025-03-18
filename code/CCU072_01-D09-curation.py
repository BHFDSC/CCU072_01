# Databricks notebook source
# MAGIC %md
# MAGIC # CCU072_01-D09-curation
# MAGIC
# MAGIC **Description** This notebook extracts date indicators needed for calculating prevalence, incidence, 30-d mortality and recurrence. The indicators are combined with cohort data for group wise calculation.
# MAGIC
# MAGIC **Authors** Wen Shi
# MAGIC
# MAGIC **Last Reviewed**12/03/2025
# MAGIC
# MAGIC **Acknowledgements** Tom Bolton

# COMMAND ----------

# MAGIC %md
# MAGIC #0. Setup

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

# MAGIC %md
# MAGIC #1. Parameters

# COMMAND ----------

# MAGIC %run "./CCU072_01-D01-parameters"

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Data

# COMMAND ----------

# -----------------------------------------------------------------------------
# import datasets
# -----------------------------------------------------------------------------

spark.sql(f"""REFRESH TABLE {path_cur_hes_apc_long}""")
hes_apc_long = spark.table(path_cur_hes_apc_long)
spark.sql(f"""REFRESH TABLE {path_cur_deaths_long}""")
deaths_long  = spark.table(path_cur_deaths_long)
spark.sql(f"""REFRESH TABLE {path_cur_gdppr}""")
gdppr   = spark.table(path_cur_gdppr)

spark.sql(f"""REFRESH TABLE {path_cur_minap}""")
spark.sql(f"""REFRESH TABLE {path_cur_ssnap}""")
spark.sql(f"""REFRESH TABLE {path_cur_nhfa}""")
spark.sql(f"""REFRESH TABLE {path_cur_nacrm}""")
spark.sql(f"""REFRESH TABLE {path_cur_nchda}""")
minap = spark.table(path_cur_minap)
ssnap = spark.table(path_cur_ssnap)
nhfa = spark.table(path_cur_nhfa)
nacrm = spark.table(path_cur_nacrm)
nchda = spark.table(path_cur_nchda)






# COMMAND ----------

codesall = spark.table(path_out_codelist_outcome_v3)
outcomes = codesall.select('dcode').distinct().toPandas()['dcode']

# COMMAND ----------

hescips = spark.table(path_cur_hescips)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Curate datasets

# COMMAND ----------

# MAGIC %md
# MAGIC #Skip section 4 if adjusting for hescips

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1. Combine

# COMMAND ----------

# # combine all data sources and extract relevant records
# outcode = codesall.select('dcode',f.col('icd10_code').alias('code')).distinct()
# outcode_sno = codesall.select('dcode',f.col('snomed_code').alias('code')).distinct()

# _hes_apc = hes_apc_long.join(f.broadcast(outcode),'code')
# _hes_apc = _hes_apc.select(['PERSON_ID', 'EPISTART','DIAG_POSITION','dcode']).withColumnRenamed('EPISTART', 'DATE')

# _death = deaths_long.join(f.broadcast(outcode),'code').where(f.col('DIAG_POSITION') == 'UNDERLYING').select('PERSON_ID','DATE','dcode')

# _gdppr = gdppr.join(f.broadcast(outcode_sno),'code').select('PERSON_ID','DATE','dcode')

# tmp1 = (
# _gdppr
# .withColumn('DIAG_POSITION', f.lit(None))
# .withColumn('source', f.lit('gdppr'))
# .unionByName(_hes_apc.withColumn('source', f.lit('hes_apc')))
# .unionByName(_death.withColumn('DIAG_POSITION', f.lit(None)).withColumn('source', f.lit('deaths')))
# .unionByName(minap).unionByName(ssnap).unionByName(nhfa).unionByName(nacrm).unionByName(nchda)
# .where((f.col('PERSON_ID').isNotNull())&(f.col('DATE').isNotNull()))
# )
# tmp1.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp1_all')
# tmp1 = spark.table(f'{dsa}.{proj}_tmp1_all')

# COMMAND ----------

# # check date validity, add date2 for calculate recurrence
# for i in period:
#     print(f"'{'year'+str(i):=^80}")
#     spark.sql(f"""REFRESH TABLE {path_out_cohort}{i}""")
#     cohort = spark.table(f'{path_out_cohort}{i}')
#     tmp2 = (cohort.join(tmp1,'PERSON_ID','left')
#                   .withColumn('DATE',f.when((f.col('DATE')<f.col('DOB'))|(f.col('DATE')>f.col('DOD')),None).otherwise(f.col('DATE')))
            
#             )
#     tmp3 = tmp2.join(tmp2.where(f.col('dcode').isin(['X411_2','X433_0'])).select('PERSON_ID',f.col('DATE').alias('date2'),f.col('source').alias('source2'),f.col('DIAG_POSITION').alias('pos2')),'PERSON_ID','left')

#     tmp3.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp3_all_year{i}')


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2. Add flags and save

# COMMAND ----------

# for i in period:
#     print(f"'{'year'+str(i):=^80}")
#     spark.sql(f"""REFRESH TABLE {path_out_cohort}{i}""")
#     spark.sql(f"""REFRESH TABLE {dsa}.{proj}_tmp3_all_year{i}""")

#     cohort = spark.table(f'{path_out_cohort}{i}')
#     tmp3 = spark.table(f'{dsa}.{proj}_tmp3_all_year{i}')
#     tmp4 = (tmp3
#             .withColumn('prev', f.when((~f.col('source').isin(['deaths'])), f.col('DATE')).otherwise(f.lit(None)))
#             .withColumn('inc_all', f.when(f.col('source').isin(['gdppr', 'deaths','minap','ssnap','nhfa','nacrm','nchda','hes_apc']) , f.col('DATE')).otherwise(f.lit(None)))
#             .withColumn('recur_strokemi',f.when(((f.col('source2').isin(['gdppr', 'deaths','minap','ssnap','nhfa','nacrm','nchda']))|((f.col('source2').isin(['hes_apc']))&(f.col('pos2')=='1')))&(f.datediff('date2','inc_all')>30)&(f.datediff('date2','inc_all')<=365),f.col('date2')).otherwise(None))
#             )
#     print('Adding flags done') 

#     tmp5 = (tmp4
#             .groupBy(['PERSON_ID','dcode'])
#             .agg(*[f.min(col).alias(col) for col in ['prev','inc_all','recur_strokemi']])
#         )
   

#     tmp5.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp5_all_year{i}')
#     print('tmp5 saved')
#     tmp5 = spark.table(f'{dsa}.{proj}_tmp5_all_year{i}')

    
#     tmp6=cohort.select('PERSON_ID','DOB','DOD').crossJoin(codesall.select('dcode').distinct())

#     tmp7 = tmp6.join(tmp5,['PERSON_ID','dcode'],'left')
#     tmp7.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp7_all_year{i}')
#     tmp7 = spark.table(f'{dsa}.{proj}_tmp7_all_year{i}')
#     assert tmp7.count() ==cohort.count()*len(outcomes)
#     print('tmp7 checked')
    


# COMMAND ----------

# MAGIC %md
# MAGIC #5. Combine curated dataset with covariates

# COMMAND ----------

# MAGIC %md
# MAGIC #Skip section 5 if using 19 ethnicity and icb region

# COMMAND ----------

# for i in period:
#     spark.sql(f"""REFRESH TABLE {path_out_cohort}{i}""")
#     cohort = spark.table(f'{path_out_cohort}{i}')
#     cohort = (cohort
#               .withColumn('ethnicity',
#                           f.when(f.col('ETHNIC_CAT')=='White','White')
#                            .when(f.col('ETHNIC_CAT')=='Asian or Asian British','Asian')
#                            .when(f.col('ETHNIC_CAT')=='Black, Black British, Caribbean or African','Black')
#                            .when(f.col('ETHNIC_CAT')=='Mixed or multiple ethnic groups','Mixed')
#                            .when(f.col('ETHNIC_CAT')=='Other ethnic group','Other')
#                            .otherwise('Missing')
#                         )
#               .withColumn('deprivation',
#                           f.when(f.col('IMD_2019_DECILES').isin([1,2]),'1_2_most_deprived')
#                           .when(f.col('IMD_2019_DECILES').isin([3,4]),'3_4')
#                           .when(f.col('IMD_2019_DECILES').isin([5,6]),'5_6')
#                           .when(f.col('IMD_2019_DECILES').isin([7,8]),'7_8')
#                           .when(f.col('IMD_2019_DECILES').isin([9,10]),'9_10_least_deprived'))
#               .withColumn('sex',f.when(f.col('SEX')=='1','Male').otherwise('Female'))
#               .withColumn('age',f.floor(f.datediff(f.lit(f'{i}-01-01'),'DOB')/365.25))
#               .withColumn('age',f.when(f.col('age')<=19,'0_19')
#                         .when((f.col('age')>=20)&(f.col('age')<=29),'20_29')
#                         .when((f.col('age')>=30)&(f.col('age')<=39),'30_39')
#                         .when((f.col('age')>=40)&(f.col('age')<=49),'40_49')
#                         .when((f.col('age')>=50)&(f.col('age')<=59),'50_59')
#                         .when((f.col('age')>=60)&(f.col('age')<=69),'60_69')
#                         .when((f.col('age')>=70)&(f.col('age')<=79),'70_79')
#                         .when((f.col('age')>=80)&(f.col('age')<=89),'80_89')
#                         .when(f.col('age')>=90,'90_over'))
#     )
#     cohort.select('PERSON_ID','age','sex','ethnicity','region','deprivation','comorbidity').write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_out_cohort_relevel_year{i}')

# COMMAND ----------

# for i in period:
#   print(f'{i:=^80}')
#   spark.sql(f"""REFRESH TABLE {dsa}.{proj}_tmp7_all_year{i}""")
#   out = spark.table(f'{dsa}.{proj}_tmp7_all_year{i}')
#   spark.sql(f"""REFRESH TABLE {dsa}.{proj}_out_cohort_relevel_year{i}""")
#   cov = spark.table(f'{dsa}.{proj}_out_cohort_relevel_year{i}')
#   combine = out.join(cov,'PERSON_ID')
#   combine.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp8_all_year{i}')

# COMMAND ----------

# MAGIC %md
# MAGIC #6. Curate datasets by data sources

# COMMAND ----------

# MAGIC %md
# MAGIC #Skip section 6 if adjusting for hescips

# COMMAND ----------

# uniqSource = spark.createDataFrame([('gdppr',),('deaths',),('minap',),('ssnap',),('nhfa',),('nacrm',),('nchda',),('hes_apc',)],['source'])
# for i in period:
#     print(f"'{'year'+str(i):=^80}")
#     spark.sql(f"""REFRESH TABLE {path_out_cohort}{i}""")
#     spark.sql(f"""REFRESH TABLE {dsa}.{proj}_tmp3_all_year{i}""")

#     cohort = spark.table(f'{path_out_cohort}{i}')
#     tmp3 = spark.table(f'{dsa}.{proj}_tmp3_all_year{i}')
#     tmp4 = (tmp3
#             .withColumn('prev', f.when((~f.col('source').isin(['deaths'])), f.col('DATE')).otherwise(f.lit(None)))
#             .withColumn('inc_all', f.when(f.col('source').isin(['gdppr', 'deaths','minap','ssnap','nhfa','nacrm','nchda','hes_apc']) , f.col('DATE')).otherwise(f.lit(None)))
#             .withColumn('recur_strokemi',f.when(((f.col('source2').isin(['gdppr', 'deaths','minap','ssnap','nhfa','nacrm','nchda']))|((f.col('source2').isin(['hes_apc']))&(f.col('pos2')=='1')))&(f.datediff('date2','inc_all')>30)&(f.datediff('date2','inc_all')<=365),f.col('date2')).otherwise(None))
#             )
#     print('Adding flags done') 

#     tmp5 = (tmp4
#             .groupBy(['PERSON_ID','dcode','source'])
#             .agg(*[f.min(col).alias(col) for col in ['prev','inc_all','recur_strokemi']])
#         )
   

#     tmp5.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp5_all_year{i}')
#     print('tmp5 saved')
#     tmp5 = spark.table(f'{dsa}.{proj}_tmp5_all_year{i}')

    
#     tmp6=cohort.select('PERSON_ID','DOB','DOD').crossJoin(codesall.select('dcode').distinct()).crossJoin(uniqSource)
   

#     tmp7 = tmp6.join(tmp5,['PERSON_ID','dcode','source'],'left')
#     tmp7.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp7_all_year{i}_source')
#     tmp7 = spark.table(f'{dsa}.{proj}_tmp7_all_year{i}_source')
#     assert tmp7.count() ==cohort.count()*len(outcomes)*8
#     print('tmp7 checked')
    


# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Curate data for adjusting for hescips

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1. Add discharge date for hesapc

# COMMAND ----------

outcode = codesall.select('dcode',f.col('icd10_code').alias('code')).distinct()
outcode_sno = codesall.select('dcode',f.col('snomed_code').alias('code')).distinct()

_hes_apc = hes_apc_long.join(f.broadcast(outcode),'code')
_hes_apc = (_hes_apc
            .select(['PERSON_ID', 'EPISTART','DIAG_POSITION','dcode','EPIKEY'])
            .withColumnRenamed('EPISTART', 'DATE')
            .withColumnRenamed('EPIKEY','epikey')
            .join(hescips,'epikey','left')
            
           )
_hes_apc.where((f.col('PERSON_ID').isNotNull())&(f.col('DATE').isNotNull())).where(f.col('dis_date').isNull()).count()

# COMMAND ----------

tmp = _hes_apc.where((f.col('PERSON_ID').isNotNull())&(f.col('DATE').isNotNull()))
tmp.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp_disdate')

# COMMAND ----------

tmp = spark.table(f'{dsa}.{proj}_tmp_disdate')

# COMMAND ----------

tmp.where(f.col('dis_date').isNull()).select(f.min('DATE'),f.max('DATE'),f.countDistinct('epikey')).show()

# COMMAND ----------

tmp.where(f.col('DATE')>='2020-01-01').where(f.col('dis_date').isNull()).select(f.min('DATE'),f.max('DATE'),f.countDistinct('epikey')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##7.2. Combine

# COMMAND ----------

# # combine all data sources and extract relevant records

_hes_apc = _hes_apc.drop('epikey')


_death = deaths_long.join(f.broadcast(outcode),'code').where(f.col('DIAG_POSITION') == 'UNDERLYING').select('PERSON_ID','DATE','dcode')

_gdppr = gdppr.join(f.broadcast(outcode_sno),'code').select('PERSON_ID','DATE','dcode')

tmp1 = (
_gdppr
.withColumn('DIAG_POSITION', f.lit(None))
.withColumn('source', f.lit('gdppr'))
.withColumn('dis_date',f.lit(None))
.unionByName(_hes_apc.withColumn('source', f.lit('hes_apc')))
.unionByName(_death.withColumn('DIAG_POSITION', f.lit(None)).withColumn('source', f.lit('deaths')).withColumn('dis_date',f.lit(None)))
.unionByName(minap.withColumn('dis_date',f.lit(None))
)
.unionByName(ssnap.withColumn('dis_date',f.lit(None))
)
.unionByName(nhfa.withColumn('dis_date',f.lit(None))
)
.unionByName(nacrm.withColumn('dis_date',f.lit(None))
)
.unionByName(nchda.withColumn('dis_date',f.lit(None))
)
.where((f.col('PERSON_ID').isNotNull())&(f.col('DATE').isNotNull()))
)
tmp1.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp1_all')
tmp1 = spark.table(f'{dsa}.{proj}_tmp1_all')

# COMMAND ----------

# check date validity, add date2 for calculate recurrence

for i in period:
    print(f"'{'year'+str(i):=^80}")
    spark.sql(f"""REFRESH TABLE {path_out_cohort}{i}""")
    cohort = spark.table(f'{path_out_cohort}{i}')
    tmp2 = (cohort.join(tmp1,'PERSON_ID','left')
                  .withColumn('DATE',f.when((f.col('DATE')<f.col('DOB'))|(f.col('DATE')>f.col('DOD')),None).otherwise(f.col('DATE')))
            
            )
    tmp3 = tmp2.join(tmp2.where(f.col('dcode').isin(['X411_2','X433_0'])).select('PERSON_ID',f.col('DATE').alias('date2'),f.col('source').alias('source2'),f.col('DIAG_POSITION').alias('pos2')),'PERSON_ID','left')

    tmp3.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp3_all_year{i}')


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.3. Create flags

# COMMAND ----------

for i in period:
    print(f"'{'year'+str(i):=^80}")
    spark.sql(f"""REFRESH TABLE {path_out_cohort}{i}""")
    spark.sql(f"""REFRESH TABLE {dsa}.{proj}_tmp3_all_year{i}""")

    cohort = spark.table(f'{path_out_cohort}{i}')
    tmp3 = spark.table(f'{dsa}.{proj}_tmp3_all_year{i}')
    tmp4 = (tmp3
            .withColumn('prev', f.when((~f.col('source').isin(['deaths'])), f.col('DATE')).otherwise(f.lit(None)))
            .withColumn('inc_all', f.when(f.col('source').isin(['gdppr', 'deaths','minap','ssnap','nhfa','nacrm','nchda','hes_apc']) , f.col('DATE')).otherwise(f.lit(None)))
            .withColumn('recur_strokemi',f.when(((f.col('source2').isin(['gdppr', 'deaths','minap','ssnap','nhfa','nacrm','nchda']))|((f.col('source2').isin(['hes_apc']))&(f.col('pos2')=='1')))&(f.datediff('date2','inc_all')>30)&(f.datediff('date2','inc_all')<=365)&((f.col('source').isin(['gdppr', 'deaths','minap','ssnap','nhfa','nacrm','nchda']))|((f.col('source').isin(['hes_apc']))&(f.col('date2')>f.col('dis_date')))),f.col('date2')).otherwise(None))
            )
    print('Adding flags done') 

    tmp5_1 = (tmp4
            .groupBy(['PERSON_ID','dcode'])
            .agg(*[f.min(col).alias(col) for col in ['prev','inc_all']])
        )
    

    tmp5_2 = tmp5_1.join(tmp4.select('PERSON_ID','dcode','inc_all','recur_strokemi'),['PERSON_ID','dcode','inc_all'],'left')
    tmp5_2 = (tmp5_2.groupBy(['PERSON_ID','dcode','prev','inc_all'])
                    .agg(f.min('recur_strokemi').alias('recur_strokemi'))
            )
    tmp5_2.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp5_all_year{i}')
    print('tmp5 saved')
    tmp5_2 = spark.table(f'{dsa}.{proj}_tmp5_all_year{i}')
    assert tmp5_2.groupBy(['PERSON_ID','dcode']).agg(f.count('*').alias('n')).where('n!=1').count()==0
    
    tmp6=cohort.select('PERSON_ID','DOB','DOD').crossJoin(codesall.select('dcode').distinct())

    tmp7 = tmp6.join(tmp5_2,['PERSON_ID','dcode'],'left')
    tmp7.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp7_all_year{i}_recur')
    tmp7 = spark.table(f'{dsa}.{proj}_tmp7_all_year{i}_recur')
    assert tmp7.count() ==cohort.count()*len(outcomes)
    print('tmp7 checked')
    


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.4. By source

# COMMAND ----------

uniqSource = spark.createDataFrame([('gdppr',),('deaths',),('minap',),('ssnap',),('nhfa',),('nacrm',),('nchda',),('hes_apc',)],['source'])
for i in period:
    print(f"'{'year'+str(i):=^80}")
    spark.sql(f"""REFRESH TABLE {path_out_cohort}{i}""")
    spark.sql(f"""REFRESH TABLE {dsa}.{proj}_tmp3_all_year{i}""")

    cohort = spark.table(f'{path_out_cohort}{i}')
    tmp3 = spark.table(f'{dsa}.{proj}_tmp3_all_year{i}')
    tmp4 = (tmp3
            .withColumn('prev', f.when((~f.col('source').isin(['deaths'])), f.col('DATE')).otherwise(f.lit(None)))
            .withColumn('inc_all', f.when(f.col('source').isin(['gdppr', 'deaths','minap','ssnap','nhfa','nacrm','nchda','hes_apc']) , f.col('DATE')).otherwise(f.lit(None)))
            .withColumn('recur_strokemi',f.when(((f.col('source2').isin(['gdppr', 'deaths','minap','ssnap','nhfa','nacrm','nchda']))|((f.col('source2').isin(['hes_apc']))&(f.col('pos2')=='1')))&(f.datediff('date2','inc_all')>30)&(f.datediff('date2','inc_all')<=365)&((f.col('source').isin(['gdppr', 'deaths','minap','ssnap','nhfa','nacrm','nchda']))|((f.col('source').isin(['hes_apc']))&(f.col('date2')>f.col('dis_date')))),f.col('date2')).otherwise(None))
            )
    print('Adding flags done') 

    tmp5_1 = (tmp4
            .groupBy(['PERSON_ID','dcode','source'])
            .agg(*[f.min(col).alias(col) for col in ['prev','inc_all']])
        )
    tmp5_2 = tmp5_1.join(tmp4.select('PERSON_ID','dcode','source','inc_all','recur_strokemi'),['PERSON_ID','dcode','source','inc_all'],'left')
    tmp5_2 = (tmp5_2.groupBy(['PERSON_ID','dcode','source','prev','inc_all'])
                    .agg(f.min('recur_strokemi').alias('recur_strokemi'))
            )
   

    tmp5_2.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp5_all_year{i}')
    print('tmp5 saved')
    tmp5_2 = spark.table(f'{dsa}.{proj}_tmp5_all_year{i}')
    assert tmp5_2.groupBy(['PERSON_ID','dcode','source']).agg(f.count('*').alias('n')).where('n!=1').count()==0

    
    tmp6=cohort.select('PERSON_ID','DOB','DOD').crossJoin(codesall.select('dcode').distinct()).crossJoin(uniqSource)
   

    tmp7 = tmp6.join(tmp5_2,['PERSON_ID','dcode','source'],'left')
    tmp7.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp7_all_year{i}_source_recur')
    tmp7 = spark.table(f'{dsa}.{proj}_tmp7_all_year{i}_source_recur')
    assert tmp7.count() ==cohort.count()*len(outcomes)*8
    print('tmp7 checked')
    


# COMMAND ----------

# MAGIC %md
# MAGIC #8. Using 19 ethnic groups and 42 icb regions

# COMMAND ----------

for i in period:
    spark.sql(f"""REFRESH TABLE {path_out_cohort}{i}""")
    cohort = spark.table(f'{path_out_cohort}{i}')
    cohort = (cohort
              .withColumn('ethnicity',
                          f.when(f.col('ETHNIC_CAT').isNull(),'Missing')
                           .otherwise(f.col('ETHNIC_CAT'))
                        )
              .withColumn('deprivation',
                          f.when(f.col('IMD_2019_DECILES').isin([1,2]),'1_2_most_deprived')
                          .when(f.col('IMD_2019_DECILES').isin([3,4]),'3_4')
                          .when(f.col('IMD_2019_DECILES').isin([5,6]),'5_6')
                          .when(f.col('IMD_2019_DECILES').isin([7,8]),'7_8')
                          .when(f.col('IMD_2019_DECILES').isin([9,10]),'9_10_least_deprived'))
              .withColumn('sex',f.when(f.col('SEX')=='1','Male').otherwise('Female'))
              .withColumn('age',f.floor(f.datediff(f.lit(f'{i}-01-01'),'DOB')/365.25))
              .withColumn('age',f.when(f.col('age')<=19,'0_19')
                        .when((f.col('age')>=20)&(f.col('age')<=29),'20_29')
                        .when((f.col('age')>=30)&(f.col('age')<=39),'30_39')
                        .when((f.col('age')>=40)&(f.col('age')<=49),'40_49')
                        .when((f.col('age')>=50)&(f.col('age')<=59),'50_59')
                        .when((f.col('age')>=60)&(f.col('age')<=69),'60_69')
                        .when((f.col('age')>=70)&(f.col('age')<=79),'70_79')
                        .when((f.col('age')>=80)&(f.col('age')<=89),'80_89')
                        .when(f.col('age')>=90,'90_over'))
    )
    cohort.select('PERSON_ID','age','sex','ethnicity','region','deprivation','comorbidity').write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_out_cohort_relevel_ethnic_region_year{i}')

# COMMAND ----------

cohort.select(f.count_distinct('ethnicity'),f.count_distinct('region')).show()

# COMMAND ----------

for i in period:
  print(f'{i:=^80}')
  spark.sql(f"""REFRESH TABLE {dsa}.{proj}_tmp7_all_year{i}_recur""")
  out = spark.table(f'{dsa}.{proj}_tmp7_all_year{i}_recur')
  spark.sql(f"""REFRESH TABLE {path_out_cohort_relevel_ethnic_region}{i}""")
  cov = spark.table(f'{path_out_cohort_relevel_ethnic_region}{i}')
  combine = out.join(cov,'PERSON_ID')
  combine.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp8_all_year{i}_recur_ethnic_region')

# COMMAND ----------

# MAGIC %md
# MAGIC #9. Add personal weight

# COMMAND ----------

ons = spark.table(f'{dsa}.{proj}_cur_ons_age1')

# COMMAND ----------

for i in period:
    print(f'{i:=^80}')
    spark.sql(f"""REFRESH TABLE {path_out_cohort}{i}""")
    coh = spark.table(f'{path_out_cohort}{i}')
    tmp0 = (coh.withColumn('sex',f.when(f.col('SEX')=='1','Male').otherwise('Female'))
              .withColumn('age',f.floor(f.datediff(f.lit(f'{i}-01-01'),'DOB')/365.25))
              .withColumn('age',f.when(f.col('age')<0,0)
                            .when(f.col('age')>90,90)
                            .otherwise(f.col('age'))
                        )
              .withColumn('region',f.col('region2'))
        )
    
    tmp1 = tmp0.groupBy(['age','sex','region']).agg(f.count('PERSON_ID').alias('n_id'))
    tmp2 = ons.where(f.col('year')==i).drop('year')
    tmp3 = tmp1.join(tmp2,['age','sex','region'])
    tmp3 = tmp3.withColumn('weight',f.col('n')/f.col('n_id'))
    tmpc = tmp0.join(tmp3.drop('n','n_id'),['age','sex','region'])
    assert tmpc.count()==coh2.count()
       

    tmpc.select('PERSON_ID','weight').write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_out_weight_age1_year{i}')
    tmpc = spark.table(f'{dsa}.{proj}_out_weight_age1_year{i}')
    tmpc.select(f.col('weight')).describe().show()

# COMMAND ----------

for i in period:
    tmp = spark.table(f'{dsa}.{proj}_out_weight_age1_year{i}')
    print(tmp.where(f.col('weight').isNull()).count())

# COMMAND ----------

for i in period:
  print(f'{i:=^80}')
  out = spark.table(f'{dsa}.{proj}_tmp7_all_year{i}_source_recur')
  tmp = spark.table(f'{dsa}.{proj}_out_weight_age1_year{i}')
  tmpc = out.join(tmp,'PERSON_ID')
  assert tmpc.count() ==out.count()
  tmpc.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp7_all_year{i}_source_recur_weightage1')


# COMMAND ----------

for i in period:
    tmp = spark.table(f'{dsa}.{proj}_tmp8_all_year{i}_recur_ethnic_region')
    tmp2 = spark.table(f'{dsa}.{proj}_out_weight_age1_year{i}')
    tmpc = tmp.join(tmp2,'PERSON_ID')
    assert tmpc.count()==tmp.count()
    tmpc.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp8_all_year{i}_recur_ethnic_region_weightage1')