# Databricks notebook source
# MAGIC %md # CCU072_01-D03-curated_data
# MAGIC
# MAGIC **Description** This notebook creates the curated tables.
# MAGIC
# MAGIC **Authors** Wen Shi
# MAGIC
# MAGIC **Reviewed** 12/03/2025
# MAGIC
# MAGIC **Acknowledgements** Tom Bolton and previous CVD-COVID-UK/COVID-IMPACT projects.

# COMMAND ----------

# MAGIC %md
# MAGIC #0. Setup

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

gdppr = extract_batch_from_archive(parameters_df_datasets,'gdppr')
# codelist
codelist = spark.table(path_out_codelist_outcome_v3)

# COMMAND ----------

hes_apc = extract_batch_from_archive(parameters_df_datasets, 'hes_apc')
deaths  = extract_batch_from_archive(parameters_df_datasets, 'deaths')
geog    = spark.table(path_ref_geog)
imd     = spark.table(path_ref_imd)


# COMMAND ----------

minap  = extract_batch_from_archive(parameters_df_datasets, 'minap')
nhfa = extract_batch_from_archive(parameters_df_datasets, 'nhfa')
nhfa_v5 = extract_batch_from_archive(parameters_df_datasets, 'nhfa_v5')
ssnap = extract_batch_from_archive(parameters_df_datasets, 'ssnap')
nacrm = extract_batch_from_archive(parameters_df_datasets, 'nacrm')
nchda = extract_batch_from_archive(parameters_df_datasets, 'nchda')

epcc = spark.table(path_out_codelist_epcc)

# COMMAND ----------

hescips = spark.table(path_tmp_hescips)

# COMMAND ----------

ons = spark.table(path_ref_ons)

# COMMAND ----------

# MAGIC %md
# MAGIC #3. GDPPR

# COMMAND ----------

codelist_sno = codelist.select('dcode','snomed_code').withColumnRenamed('snomed_code','code').distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Practice registration N

# COMMAND ----------

# note: pragmatic approach - number of individuals ever registered with the GP practice
prac = (
  gdppr
  .select('NHS_NUMBER_DEID', 'PRACTICE')  
  .groupBy('PRACTICE')
  .agg(f.countDistinct('NHS_NUMBER_DEID').alias('PRACTICE_N'))
  .where(f.col('PRACTICE').isNotNull())  
)

# temp save
out_name = f'{proj}_tmp_check_codelist_practice_prac_n'
prac.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{out_name}')
prac = spark.table(f'{dsa}.{proj}_tmp_check_codelist_practice_prac_n')


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2. Code assignment by practice, date, code

# COMMAND ----------

# codelist match
tmp1 = (
  gdppr
  .select('NHS_NUMBER_DEID', 'PRACTICE', 'DATE', 'CODE')
  .join(f.broadcast(codelist_sno), on='CODE', how='inner')
  .where(
    (f.col('NHS_NUMBER_DEID').isNotNull())
    & (f.col('PRACTICE').isNotNull())
    & (f.col('DATE').isNotNull())
  )
)

# temp save
out_name = f'{proj}_tmp_check_codelist_practice_tmp1'
tmp1.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{out_name}')
tmp1 = spark.table(f'{dsa}.{proj}_tmp_check_codelist_practice_tmp1')

# COMMAND ----------

# for each name/code/term in the codelist, calculate the number of distinct individuals by practice and date
tmp1a = (
  tmp1
  .groupBy( 'CODE', 'PRACTICE', 'DATE')
  .agg(f.countDistinct('NHS_NUMBER_DEID').alias('n'))
)

# proportion of individuals ever registered at the practice with the name/code/term on a given date
# note: dates shown for codes with at least one codelist match
# note: all 'both' => every practice in gdppr has at least one code in codelist (e.g., never smoker)
tmp2 = (
  merge(tmp1a, prac, ['PRACTICE'], validate='m:1', broadcast_right=1, assert_results=['both', 'right_only'], keep_results=['both'], indicator=0)
  .withColumn('PRACTICE_prop', f.round(f.col('n')/f.col('PRACTICE_N'), 3))
  .select( 'CODE', 'PRACTICE', 'DATE', 'n', 'PRACTICE_N', 'PRACTICE_prop')
)

# temp save
out_name = f'{proj}_tmp_check_codelist_practice_tmp2'
tmp2.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{out_name}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3. Inspect unusual code assignment

# COMMAND ----------

# inspect

tmp2 = spark.table(f'{dsa}.{proj}_tmp_check_codelist_practice_tmp2')
display(tmp2.orderBy(f.desc('PRACTICE_prop')))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4. Save after removal of unusual code assignment

# COMMAND ----------

#Remove the first record related diagnoses
_gdppr = (gdppr.withColumn('flag',f.when((f.col('CODE')=='195080001')&(f.col('PRACTICE')=='P81683')&(f.col('DATE')=='2023-02-08'),f.lit(1)).otherwise(f.lit(0)))
                .where('flag=0').drop('flag')
        )

# COMMAND ----------

_gdppr = _gdppr.where((f.col('CODE').isNotNull()) | (f.col('CODE') != '')).select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'),f.coalesce('DATE','RECORD_DATE').alias('DATE'),'CODE')

# COMMAND ----------

outName = f'{proj}_cur_gdppr'
_gdppr.write.mode('overwrite').option('overwriteSchema', True).saveAsTable(f'{dsa}.{outName}')

# COMMAND ----------

# MAGIC %md # 4. HES_APC

# COMMAND ----------

# MAGIC %md ## 4.1. Create

# COMMAND ----------

_hes_apc = hes_apc\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')\
  .select(['PERSON_ID', 'EPIKEY', 'EPISTART','PROCODE3'] + [col for col in list(hes_apc.columns) if re.match(r'^DIAG_(3|4)_\d\d$', col)])\
  .orderBy('PERSON_ID', 'EPIKEY')  


# COMMAND ----------


# reshape twice, tidy, and remove records with missing code
hes_apc_long = reshape_wide_to_long_multi(_hes_apc, i=['PERSON_ID', 'EPIKEY', 'EPISTART','PROCODE3'], j='POSITION', stubnames=['DIAG_4_', 'DIAG_3_'])
hes_apc_long = reshape_wide_to_long_multi(hes_apc_long, i=['PERSON_ID', 'EPIKEY', 'EPISTART', 'PROCODE3', 'POSITION'], j='DIAG_DIGITS', stubnames=['DIAG_'])\
  .withColumnRenamed('POSITION', 'DIAG_POSITION')\
  .withColumn('DIAG_POSITION', f.regexp_replace('DIAG_POSITION', r'^[0]', ''))\
  .withColumn('DIAG_DIGITS', f.regexp_replace('DIAG_DIGITS', r'[_]', ''))\
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'X$', ''))\
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'[.,\-\s]', ''))\
  .withColumnRenamed('DIAG_', 'CODE')\
  .where((f.col('CODE').isNotNull()) & (f.col('CODE') != ''))\
  .orderBy(['PERSON_ID', 'EPIKEY', 'DIAG_DIGITS', 'DIAG_POSITION'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2. Code assignment by hospital

# COMMAND ----------

codelist_icd = codelist.select('dcode','icd10_code').withColumnRenamed('icd10_code','code').distinct()
# count_var(codelist_icd,'code')

# no of patients by orgnization and date
tmp1_1 = hes_apc.groupBy('PROCODE3','EPISTART').agg(f.count_distinct('PERSON_ID_DEID').alias('Hospital_n'))

#no patients by organization
tmp1_2 = hes_apc.groupBy('PROCODE3').agg(f.count_distinct('PERSON_ID_DEID').alias('Hospital_n'))

# COMMAND ----------

tmp2 = (hes_apc_long.join(f.broadcast(codelist_icd), ['CODE'])
                    .groupBy(['CODE','PROCODE3','EPISTART'])
                    .agg(f.count_distinct('PERSON_ID').alias('n'))
)

# COMMAND ----------

#check
tmp3_2 = tmp1_2.join(tmp2,'PROCODE3').withColumn('prop',f.round(f.col('n')/f.col('Hospital_n'),3))
display(tmp3_2.orderBy(f.desc('prop')))

# COMMAND ----------

tmp3_1 = tmp1_1.join(tmp2,['PROCODE3','EPISTART']).withColumn('prop',f.round(f.col('n')/f.col('Hospital_n'),3))
display(tmp3_1.orderBy(f.desc('prop')))
#No obvious abnormal code assignment

# COMMAND ----------

# MAGIC %md ## 4.3. Check code

# COMMAND ----------

count_var(hes_apc_long, 'PERSON_ID'); print()
count_var(hes_apc_long, 'EPIKEY'); print()

# check removal of trailing X
tmpt = hes_apc_long\
  .where(f.col('CODE').rlike('X'))\
  .withColumn('flag', f.when(f.col('CODE').rlike('^X.*'), 1).otherwise(0))
tmpt = tab(tmpt, 'flag'); print()

# COMMAND ----------

# MAGIC %md ## 4.4. Save

# COMMAND ----------

outName = f'{proj}_cur_hes_apc_all_years_long'.lower()  
hes_apc_long.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{outName}')

# COMMAND ----------

# MAGIC %md # 5. Deaths

# COMMAND ----------

# MAGIC %md ## 5.1. Create and check

# COMMAND ----------


# setting recommended by the data wranglers to avoid the following error message:
# SparkUpgradeException: [INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER] 
# You may get a different result due to the upgrading to Spark >= 3.0:
# Caused by: DateTimeParseException: Text '2009029' could not be parsed at index 6
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# check
count_var(deaths, 'DEC_CONF_NHS_NUMBER_CLEAN_DEID')
assert dict(deaths.dtypes)['REG_DATE'] == 'string'
assert dict(deaths.dtypes)['REG_DATE_OF_DEATH'] == 'string'

# define window for the purpose of creating a row number below as per the skinny patient table
_win = Window\
  .partitionBy('PERSON_ID')\
  .orderBy(f.desc('REG_DATE'), f.desc('REG_DATE_OF_DEATH'), f.desc('S_UNDERLYING_COD_ICD10'))

# rename ID
# remove records with missing IDs
# reformat dates
# reduce to a single row per individual as per the skinny patient table
# select columns required
# rename column ahead of reshape below
# sort by ID
deaths_out = deaths\
  .withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'PERSON_ID')\
  .where(f.col('PERSON_ID').isNotNull())\
  .withColumn('REG_DATE', f.to_date(f.col('REG_DATE'), 'yyyyMMdd'))\
  .withColumn('REG_DATE_OF_DEATH', f.to_date(f.col('REG_DATE_OF_DEATH'), 'yyyyMMdd'))\
  .withColumn('_rownum', f.row_number().over(_win))\
  .where(f.col('_rownum') == 1)\
  .select(['PERSON_ID', 'REG_DATE', 'REG_DATE_OF_DEATH', 'S_UNDERLYING_COD_ICD10'] + [col for col in list(deaths.columns) if re.match(r'^S_COD_CODE_\d(\d)*$', col)])\
  .withColumnRenamed('S_UNDERLYING_COD_ICD10', 'S_COD_CODE_UNDERLYING')\
  .orderBy('PERSON_ID')

# check
count_var(deaths_out, 'PERSON_ID')
count_var(deaths_out, 'REG_DATE_OF_DEATH')
count_var(deaths_out, 'S_COD_CODE_UNDERLYING')

# single row deaths 
deaths_out_sing = deaths_out

# remove records with missing DOD
deaths_out = deaths_out\
  .where(f.col('REG_DATE_OF_DEATH').isNotNull())\
  .drop('REG_DATE')

# check
count_var(deaths_out, 'PERSON_ID')

# reshape
# add 1 to diagnosis position to start at 1 (c.f., 0) - will avoid confusion with HES long, which start at 1
# rename 
# remove records with missing cause of death
deaths_out_long = reshape_wide_to_long(deaths_out, i=['PERSON_ID', 'REG_DATE_OF_DEATH'], j='DIAG_POSITION', stubname='S_COD_CODE_')\
  .withColumn('DIAG_POSITION', f.when(f.col('DIAG_POSITION') != 'UNDERLYING', f.concat(f.lit('SECONDARY_'), f.col('DIAG_POSITION'))).otherwise(f.col('DIAG_POSITION')))\
  .withColumnRenamed('S_COD_CODE_', 'CODE4')\
  .where(f.col('CODE4').isNotNull())\
  .withColumnRenamed('REG_DATE_OF_DEATH', 'DATE')\
  .withColumn('CODE3', f.substring(f.col('CODE4'), 1, 3))
deaths_out_long = reshape_wide_to_long(deaths_out_long, i=['PERSON_ID', 'DATE', 'DIAG_POSITION'], j='DIAG_DIGITS', stubname='CODE')\
  .withColumn('CODE', f.regexp_replace('CODE', r'[.,\-\s]', ''))
  
# check
count_var(deaths_out_long, 'PERSON_ID')  
tmpt = tab(deaths_out_long, 'DIAG_POSITION', 'DIAG_DIGITS', var2_unstyled=1) 
tmpt = tab(deaths_out_long, 'CODE')  


# COMMAND ----------

# MAGIC %md ## 5.2. Save

# COMMAND ----------

outName = f'{proj}_cur_deaths_archive_sing'.lower()
deaths_out_sing.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{outName}')

# COMMAND ----------

outName = f'{proj}_cur_deaths_archive_long'.lower()
deaths_out_long.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{outName}')

# COMMAND ----------

# MAGIC %md # 6. LSOA region lookup

# COMMAND ----------

# MAGIC %md ## 6.1. Create

# COMMAND ----------

spark.sql(f"""
  -- CREATE or replace global temporary view {proj}_lsoa_region_lookup AS
  create table {dsa}.{proj}_lsoa_region_lookup as 
  with curren_chd_geo_listings as (
    SELECT * 
    FROM {path_ref_geog}
    --WHERE IS_CURRENT = 1
  ),
  lsoa_auth as (
    SELECT e01.geography_code as lsoa_code, e01.geography_name lsoa_name, 
      e02.geography_code as msoa_code, e02.geography_name as msoa_name, 
      e0789.geography_code as authority_code, e0789.geography_name as authority_name,
      e0789.parent_geography_code as authority_parent_geography
    FROM curren_chd_geo_listings e01
    LEFT JOIN curren_chd_geo_listings e02 on e02.geography_code = e01.parent_geography_code
    LEFT JOIN curren_chd_geo_listings e0789 on e0789.geography_code = e02.parent_geography_code
    WHERE e01.geography_code like 'E01%' and e02.geography_code like 'E02%'
  ),
  auth_county as (
    SELECT lsoa_code, lsoa_name,
           msoa_code, msoa_name,
           authority_code, authority_name,
           e10.geography_code as county_code, e10.geography_name as county_name,
           e10.parent_geography_code as parent_geography
    FROM lsoa_auth
    LEFT JOIN dss_corporate.ons_chd_geo_listings e10 on e10.geography_code = lsoa_auth.authority_parent_geography
    WHERE LEFT(authority_parent_geography,3) = 'E10'
  ),
  auth_met_county as (
    SELECT lsoa_code, lsoa_name,
           msoa_code, msoa_name,
           authority_code, authority_name,
           NULL as county_code, NULL as county_name,           
           lsoa_auth.authority_parent_geography as region_code
    FROM lsoa_auth
    WHERE LEFT(authority_parent_geography,3) = 'E12'
  ),
  lsoa_region_code as (
    SELECT lsoa_code, lsoa_name,
           msoa_code, msoa_name,
           authority_code, authority_name,
           county_code, county_name, 
           auth_county.parent_geography as region_code
    FROM auth_county
    UNION ALL
    SELECT lsoa_code, lsoa_name,
           msoa_code, msoa_name,
           authority_code, authority_name,
           county_code, county_name, 
           region_code 
    FROM auth_met_county
  ),
  lsoa_region as (
    SELECT lsoa_code, lsoa_name,
           msoa_code, msoa_name,
           authority_code, authority_name,
           county_code, county_name, 
           region_code, e12.geography_name as region_name 
    FROM lsoa_region_code
    LEFT JOIN dss_corporate.ons_chd_geo_listings e12 on lsoa_region_code.region_code = e12.geography_code
  )
  SELECT * FROM lsoa_region
""")

# COMMAND ----------

tmp1 = spark.table(f'{dsa}.{proj}_lsoa_region_lookup')

# COMMAND ----------

# MAGIC %md ## 6.2. Check

# COMMAND ----------

# check duplicates
w1 = Window\
  .partitionBy('lsoa_code')\
  .orderBy('lsoa_name')
w2 = Window\
  .partitionBy('lsoa_code')
tmp2 = tmp1\
  .withColumn('_rownum', f.row_number().over(w1))\
  .withColumn('_rownummax', f.count('lsoa_code').over(w2))\
  .where(f.col('_rownummax') > 1)
display(tmp2)
# duplicates are a result of an authority name change

# COMMAND ----------

tmp2 = tmp1\
  .withColumn('_rownum', f.row_number().over(w1))\
  .where(f.col('_rownum') == 1)\
  .select('lsoa_code', 'lsoa_name', 'region_code', 'region_name')\
  .withColumnRenamed('lsoa_code', 'LSOA')\
  .withColumnRenamed('region_name', 'region')

count_var(tmp2, 'LSOA'); print()
tmpt = tab(tmp2, 'region'); print()

# COMMAND ----------

# MAGIC %md
# MAGIC ##6.3. Save

# COMMAND ----------

outName = f'{proj}_cur_lsoa_region_lookup'.lower()
tmp2.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{outName}')

# COMMAND ----------

# MAGIC %md # 7. LSOA IMD lookup

# COMMAND ----------

# MAGIC %md ## 7.1. Create and check

# COMMAND ----------

# check
print(imd.toPandas().head(5)); print()
count_var(imd, 'LSOA_CODE_2011'); print()
tmpt = tab(imd, 'DECI_IMD', 'IMD_YEAR', var2_unstyled=1); print()

# tidy
tmp1 = imd\
  .where(f.col('IMD_YEAR') == 2019)\
  .select('LSOA_CODE_2011', 'DECI_IMD')\
  .withColumnRenamed('LSOA_CODE_2011', 'LSOA')\
  .withColumn('IMD_2019_QUINTILES',
    f.when(f.col('DECI_IMD').isin([1,2]), 1)\
     .when(f.col('DECI_IMD').isin([3,4]), 2)\
     .when(f.col('DECI_IMD').isin([5,6]), 3)\
     .when(f.col('DECI_IMD').isin([7,8]), 4)\
     .when(f.col('DECI_IMD').isin([9,10]), 5)\
     .otherwise(None)\
  )\
  .withColumnRenamed('DECI_IMD', 'IMD_2019_DECILES')

# check
tmpt = tab(tmp1, 'IMD_2019_DECILES', 'IMD_2019_QUINTILES', var2_unstyled=1); print()
print(tmp1.toPandas().head(5)); print()

# COMMAND ----------

# MAGIC %md
# MAGIC ##7.2. Save

# COMMAND ----------

outName = f'{proj}_cur_lsoa_imd_lookup'.lower()
tmp1.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{outName}')

# COMMAND ----------

# MAGIC %md
# MAGIC #8. NICOR_MINAP

# COMMAND ----------

minap = (minap.withColumn('ARRIVAL_AT_HOSPITAL',f.to_date('ARRIVAL_AT_HOSPITAL'))
             .withColumn('ARRIVAL_AMBULANCE',f.to_date('ARRIVAL_AMBULANCE'))
             .withColumn('ARRIVAL_1ST_RESPONDER',f.to_date('ARRIVAL_1ST_RESPONDER'))
             .withColumn('ARRIVAL_NON_INTERVENTION_HOS',f.to_date('ARRIVAL_NON_INTERVENTION_HOS'))
             .withColumn('CALL_FOR_HELP',f.to_date('CALL_FOR_HELP'))
             .withColumn('SYMPTOM_ONSET',f.to_date('SYMPTOM_ONSET'))
             .withColumnRenamed('NHS_NUMBER_DEID','PERSON_ID')
)


# COMMAND ----------

minap.select(f.substring('DISCHARGE_DIAGNOSIS',1,2)).distinct().show()

# COMMAND ----------

#extract main diagnosis - MI
tmpc = (minap.where(f.substring('DISCHARGE_DIAGNOSIS',1,2).isin(['1.','4.']))
      .select('PERSON_ID',f.least('ARRIVAL_AT_HOSPITAL','ARRIVAL_AMBULANCE','ARRIVAL_1ST_RESPONDER','ARRIVAL_NON_INTERVENTION_HOS','CALL_FOR_HELP','SYMPTOM_ONSET').alias('DATE'))
      .withColumn('DIAG_POSITION',f.lit('main'))
      .withColumn('source',f.lit('minap'))
      .withColumn('dcode',f.lit('X411_2'))
      
)
print(f"{str('MI'):-^80}")
count_var(tmpc,'PERSON_ID')

# main diagnosis - Unstable angina
tmp = (minap.where(f.substring('DISCHARGE_DIAGNOSIS',1,1).isin(['5']))
      .select('PERSON_ID',f.least('ARRIVAL_AT_HOSPITAL','ARRIVAL_AMBULANCE','ARRIVAL_1ST_RESPONDER','ARRIVAL_NON_INTERVENTION_HOS','CALL_FOR_HELP','SYMPTOM_ONSET').alias('DATE'))
      .withColumn('DIAG_POSITION',f.lit('main'))
      .withColumn('source',f.lit('minap'))
      .withColumn('dcode',f.lit('X411_1'))
      
)
tmpc = tmpc.unionByName(tmp)
print(f"{'unstable angina':-^80}")
count_var(tmp,'PERSON_ID')

# comorbidity - heart failure NOS
tmp = (minap.where((f.substring('HEART_FAILURE',1,1)=='1')&(f.substring('LVEF',1,1).isin(['8','9'])))
        .select('PERSON_ID',f.least('ARRIVAL_AT_HOSPITAL','ARRIVAL_AMBULANCE','ARRIVAL_1ST_RESPONDER','ARRIVAL_NON_INTERVENTION_HOS','CALL_FOR_HELP','SYMPTOM_ONSET').alias('DATE'))
        .withColumn('DIAG_POSITION',f.lit('comorbidity'))
        .withColumn('source',f.lit('minap'))
        .withColumn('dcode',f.lit('X428_2'))
        )
tmpc = tmpc.unionByName(tmp)
print(f"{'heart failure NOS':-^80}")
count_var(tmp,'PERSON_ID')

#comorbidity - Heart Failure with reduced EF
tmp = (minap.where((f.substring('HEART_FAILURE',1,1)=='1')&(f.substring('LVEF',1,1).isin(['2','3'])))
        .select('PERSON_ID',f.least('ARRIVAL_AT_HOSPITAL','ARRIVAL_AMBULANCE','ARRIVAL_1ST_RESPONDER','ARRIVAL_NON_INTERVENTION_HOS','CALL_FOR_HELP','SYMPTOM_ONSET').alias('DATE'))
        .withColumn('DIAG_POSITION',f.lit('comorbidity'))
        .withColumn('source',f.lit('minap'))
        .withColumn('dcode',f.lit('XHFREF'))
        )
tmpc = tmpc.unionByName(tmp)
print(f"{'Heart Failure with reduced EF':-^80}")
count_var(tmp,'PERSON_ID')


#comorbidity - Atrial fibrillation and flutter
tmp = (minap.where((f.substring('TC_ATRIAL_TACHYCARDIA',1,1)=='1'))
        .select('PERSON_ID',f.least('ARRIVAL_AT_HOSPITAL','ARRIVAL_AMBULANCE','ARRIVAL_1ST_RESPONDER','ARRIVAL_NON_INTERVENTION_HOS','CALL_FOR_HELP','SYMPTOM_ONSET').alias('DATE'))
        .withColumn('DIAG_POSITION',f.lit('comorbidity'))
        .withColumn('source',f.lit('minap'))
        .withColumn('dcode',f.lit('X427_2'))
        )
tmpc = tmpc.unionByName(tmp)
print(f"{'Atrial fibrillation and flutter':-^80}")
count_var(tmp,'PERSON_ID')

#comorbidity - Ventricular fibrillation and flutter
tmp = (minap.where((f.substring('TC_VENTRICULAR_TACHYCARDIA',1,1)=='3'))
        .select('PERSON_ID',f.least('ARRIVAL_AT_HOSPITAL','ARRIVAL_AMBULANCE','ARRIVAL_1ST_RESPONDER','ARRIVAL_NON_INTERVENTION_HOS','CALL_FOR_HELP','SYMPTOM_ONSET').alias('DATE'))
        .withColumn('DIAG_POSITION',f.lit('comorbidity'))
        .withColumn('source',f.lit('minap'))
        .withColumn('dcode',f.lit('X427_41'))
        )
tmpc = tmpc.unionByName(tmp)
print(f"{'Ventricular fibrillation and flutter':-^80}")
count_var(tmp,'PERSON_ID')



# COMMAND ----------

tmpc = tmpc.where(f.col('PERSON_ID').isNotNull()).distinct()
count_var(tmpc,'PERSON_ID')

# COMMAND ----------

tmpc.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_cur_minap')

# COMMAND ----------

# MAGIC %md
# MAGIC #9. NHFA

# COMMAND ----------

nhfa2 = nhfa.select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'),'14_00_HEART_FAILURE',
                   f.to_date('15_13_DATE_OF_REVIEW_APPOINTMENT').alias('15_13_DATE_OF_REVIEW_APPOINTMENT'),
                   f.to_date('2_00_DATE_OF_VISIT').alias('2_00_DATE_OF_VISIT'),
                   f.col('9_23_ECHO_OR_OTHER_GOLD_STANDARD_TEST_E_G_MRI_NUCLEAR_SCAN_OR_ANGIOGRAM'))


# COMMAND ----------

nhfa2.select(f.substring('9_23_ECHO_OR_OTHER_GOLD_STANDARD_TEST_E_G_MRI_NUCLEAR_SCAN_OR_ANGIOGRAM',1,2)).distinct().show()

# COMMAND ----------

# main diag heart failure NOS
tmpc = (nhfa2.where((f.substring('14_00_HEART_FAILURE',1,1)=='1')&(f.substring('9_23_ECHO_OR_OTHER_GOLD_STANDARD_TEST_E_G_MRI_NUCLEAR_SCAN_OR_ANGIOGRAM',1,2).isin(['8.','9.','10'])))
        .select('PERSON_ID',
                f.least('15_13_DATE_OF_REVIEW_APPOINTMENT','2_00_DATE_OF_VISIT').alias('DATE'))
        .withColumn('DIAG_POSITION',f.lit('main'))
        .withColumn('source',f.lit('nhfa'))
        .withColumn('dcode',f.lit('X428_2'))
        
)
print(f"{'heart failure NOS':-^80}")
count_var(tmpc,'PERSON_ID')

# main diag Heart failure, reduced ejection fraction
tmp = (nhfa2.where((f.substring('14_00_HEART_FAILURE',1,1)=='1')&(f.substring('9_23_ECHO_OR_OTHER_GOLD_STANDARD_TEST_E_G_MRI_NUCLEAR_SCAN_OR_ANGIOGRAM',1,2)=='1.'))
        .select('PERSON_ID',
                f.least('15_13_DATE_OF_REVIEW_APPOINTMENT','2_00_DATE_OF_VISIT').alias('DATE'))
        .withColumn('DIAG_POSITION',f.lit('main'))
        .withColumn('source',f.lit('nhfa'))
        .withColumn('dcode',f.lit('XHFREF'))
       
)
print(f"{'Heart failure, reduced ejection fraction from nhfa':-^80}")
count_var(tmp,'PERSON_ID')

tmpc = tmpc.unionByName(tmp)


# COMMAND ----------

nhfa_v5_2 = nhfa_v5.select('14_00_CONFIRMED_DIAGNOSIS_OF_HEART_FAILURE',
                            '9_23_ECHO_OR_OTHER_GOLD_STANDARD_TEST_E_G_MRI_RECORDED_WITHIN_12_MONTHS_OF_ADMISSION',
                            '9_24_LVEF_MOST_RECENT_EITHER_MEASURED_OR_VISUALLY_ESTIMATED_',
                            f.col('PERSON_ID_DEID').alias('PERSON_ID'),
                            f.to_date(f.col('15_13_DATE_OF_REVIEW_APPOINTMENT')).alias('15_13_DATE_OF_REVIEW_APPOINTMENT'),
                            f.to_date(f.col('2_00_DATE_OF_ADMISSION')).alias('2_00_DATE_OF_ADMISSION'))

# COMMAND ----------

nhfa_v5_2.select(f.substring('9_23_ECHO_OR_OTHER_GOLD_STANDARD_TEST_E_G_MRI_RECORDED_WITHIN_12_MONTHS_OF_ADMISSION',1,2)).distinct().show()

# COMMAND ----------

# main diag Heart failure, reduced ejection fraction
tmp = (nhfa_v5_2.where((f.substring('14_00_CONFIRMED_DIAGNOSIS_OF_HEART_FAILURE',1,1)==1)&((f.substring('9_23_ECHO_OR_OTHER_GOLD_STANDARD_TEST_E_G_MRI_RECORDED_WITHIN_12_MONTHS_OF_ADMISSION',1,2)=='1.'))|(f.substring('9_24_LVEF_MOST_RECENT_EITHER_MEASURED_OR_VISUALLY_ESTIMATED_',1,1).isin(['3','4'])))
        .select('PERSON_ID',
                f.least('15_13_DATE_OF_REVIEW_APPOINTMENT','2_00_DATE_OF_ADMISSION').alias('DATE'))
        .withColumn('DIAG_POSITION',f.lit('main'))
        .withColumn('source',f.lit('nhfa'))
        .withColumn('dcode',f.lit('XHFREF'))
       
)
print(f"{'Heart failure, reduced ejection fraction from nhfa_v5':-^80}")
count_var(tmp,'PERSON_ID')

tmpc = tmpc.unionByName(tmp)

# main diag heart failure NOS
tmp  = (nhfa_v5_2.where((f.substring('14_00_CONFIRMED_DIAGNOSIS_OF_HEART_FAILURE',1,1)==1)&(f.substring('9_23_ECHO_OR_OTHER_GOLD_STANDARD_TEST_E_G_MRI_RECORDED_WITHIN_12_MONTHS_OF_ADMISSION',1,2).isin(['8.','9.','10'])))
        .select('PERSON_ID',
                f.least('15_13_DATE_OF_REVIEW_APPOINTMENT','2_00_DATE_OF_ADMISSION').alias('DATE'))
        .withColumn('DIAG_POSITION',f.lit('main'))
        .withColumn('source',f.lit('nhfa'))
        .withColumn('dcode',f.lit('X428_2'))
       
)
print(f"{'Heart failure,nos':-^80}")
count_var(tmp,'PERSON_ID')

tmpc = tmpc.unionByName(tmp)

# COMMAND ----------

tmpc = tmpc.where(f.col('PERSON_ID').isNotNull()).distinct()

count_var(tmpc,'PERSON_ID')

# COMMAND ----------

tmpc.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_cur_nhfa')


# COMMAND ----------

# MAGIC %md
# MAGIC #10. SSNAP

# COMMAND ----------

ssnap2 =ssnap.select(f.col('Person_ID_DEID').alias('PERSON_ID'),
                     f.to_date('S1ONSETDATETIME').alias('S1ONSETDATETIME'),
                     f.to_date('S1FIRSTARRIVALDATETIME').alias('S1FIRSTARRIVALDATETIME'),
                     f.to_date('S1FIRSTSTROKEUNITARRIVALDATETIME').alias('S1FIRSTSTROKEUNITARRIVALDATETIME'),
                     'S1DIAGNOSIS','S2STROKETYPE','S2COMATRIALFIBRILLATION','S7DISCHARGEATRIALFIBRILLATION'
                     )

# COMMAND ----------

tmpc = (ssnap2.where((f.col('S1DIAGNOSIS')=='S')&(f.col('S2STROKETYPE')=='PIH'))
              .select('PERSON_ID',
                      f.least('S1ONSETDATETIME','S1FIRSTARRIVALDATETIME','S1FIRSTSTROKEUNITARRIVALDATETIME').alias('DATE'))
              .withColumn('DIAG_POSITION',f.lit('main'))
              .withColumn('source',f.lit('ssnap'))
              .withColumn('dcode',f.lit('XICH'))
              
)
print(f"{'Intracerebral haemorrhage':-^80}")
count_var(tmpc,'PERSON_ID')

# COMMAND ----------

tmp = (ssnap2.where((f.col('S1DIAGNOSIS')=='S')&(f.col('S2STROKETYPE')=='I'))
              .select('PERSON_ID',
                      f.least('S1ONSETDATETIME','S1FIRSTARRIVALDATETIME','S1FIRSTSTROKEUNITARRIVALDATETIME').alias('DATE'))
              .withColumn('DIAG_POSITION',f.lit('main'))
              .withColumn('source',f.lit('ssnap'))
              .withColumn('dcode',f.lit('X433_0'))
              
)
print(f"{'Ischaemic stroke':-^80}")
count_var(tmp,'PERSON_ID')
tmpc = tmpc.unionByName(tmp)

tmp = (ssnap2.where((f.col('S2COMATRIALFIBRILLATION')=='Y')|(f.col('S7DISCHARGEATRIALFIBRILLATION')=='Y'))
              .select('PERSON_ID',
                      f.least('S1ONSETDATETIME','S1FIRSTARRIVALDATETIME','S1FIRSTSTROKEUNITARRIVALDATETIME').alias('DATE'))
              .withColumn('DIAG_POSITION',f.lit('comorbidity'))
              .withColumn('source',f.lit('ssnap'))
              .withColumn('dcode',f.lit('X427_2'))
              
)
print(f"{'atrial fibrillation':-^80}")
count_var(tmp,'PERSON_ID')
tmpc = tmpc.unionByName(tmp)


# COMMAND ----------

tmpc = tmpc.where(f.col('PERSON_ID').isNotNull()).distinct()

count_var(tmpc,'PERSON_ID')

# COMMAND ----------

tmpc.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_cur_ssnap')

# COMMAND ----------

# MAGIC %md
# MAGIC #11. NACRM

# COMMAND ----------

nacrm2 = nacrm.select(f.to_date('3_01_PROCEDURE_DATE').alias('3_01_PROCEDURE_DATE'),
                      f.col('PERSON_ID_DEID').alias('PERSON_ID'),'3_12_ABLATION_PROCEDURES','2_01_UNDERLYING_HEART_DISEASE')

# COMMAND ----------

nacrm2.select(f.substring('2_01_UNDERLYING_HEART_DISEASE',1,3)).distinct().show(50)

# COMMAND ----------

tmpc = (nacrm2.where(f.substring('3_12_ABLATION_PROCEDURES',1,2).isin(['5A','15']))
              .select('PERSON_ID',
                      f.col('3_01_PROCEDURE_DATE').alias('DATE')
                    )
              .withColumn('DIAG_POSITION',f.lit('main'))
              .withColumn('source',f.lit('nacrm'))
              .withColumn('dcode',f.lit('X427_2'))
        )
print(f"{'atrial fibrillation':-^80}")
count_var(tmpc,'PERSON_ID')

tmp = (nacrm2.where(f.substring('3_12_ABLATION_PROCEDURES',1,2)=='14')
              .select('PERSON_ID',
                      f.col('3_01_PROCEDURE_DATE').alias('DATE')
                    )
              .withColumn('DIAG_POSITION',f.lit('main'))
              .withColumn('source',f.lit('nacrm'))
              .withColumn('dcode',f.lit('X427_12'))
                     )

print(f"{'paroxysmal ventricular tachycardia':-^80}")
count_var(tmp,'PERSON_ID')
tmpc = tmpc.unionByName(tmp)

tmp = (nacrm2.where(f.substring('3_12_ABLATION_PROCEDURES',1,2)=='17')
              .select('PERSON_ID',
                      f.col('3_01_PROCEDURE_DATE').alias('DATE')
                    )
              .withColumn('DIAG_POSITION',f.lit('main'))
              .withColumn('source',f.lit('nacrm'))
              .withColumn('dcode',f.lit('X426_2'))
                     )

print(f"{'Atrioventricular_AV_block':-^80}")
count_var(tmp,'PERSON_ID')
tmpc = tmpc.unionByName(tmp)


tmp = (nacrm2.where(f.substring('2_01_UNDERLYING_HEART_DISEASE',1,2)=='G2')
              .select('PERSON_ID',
                      f.col('3_01_PROCEDURE_DATE').alias('DATE')
                    )
              .withColumn('DIAG_POSITION',f.lit('main'))
              .withColumn('source',f.lit('nacrm'))
              .withColumn('dcode',f.lit('X420_1'))
                     )
print(f"{'myocarditis':-^80}")
count_var(tmp,'PERSON_ID')
tmpc = tmpc.unionByName(tmp)

# COMMAND ----------

tmpc = tmpc.where(f.col('PERSON_ID').isNotNull()).distinct()

count_var(tmpc,'PERSON_ID')

# COMMAND ----------

tmpc.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_cur_nacrm')

# COMMAND ----------

# MAGIC %md
# MAGIC #12. NCHDA

# COMMAND ----------

nchda2 = (nchda.withColumn('diag',f.explode(f.split('2_01_DIAGNOSIS1',';')))
              .withColumn('CODE',f.regexp_extract('diag','^(\w+).',1))  
              .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'),'CODE',f.least(f.to_date('3_01_DATE_OF_VISIT'),f.col('4_01_DATE_OF_DISCHARGE')).alias('DATE')
            )
        )

# COMMAND ----------

nchda2.where(f.length('CODE')!=6).count()


# COMMAND ----------

nchda2.withColumn('first',f.substring('CODE',1,1)).where(f.col('first').rlike('[^0-9]')).select('first').distinct().show()


# COMMAND ----------

nchda3 = nchda2.where(f.col('PERSON_ID').isNotNull()).distinct()
# count_var(nchda3,'PERSON_ID')

# COMMAND ----------

nchda4 = epcc.join(nchda3,epcc.epcc_code==nchda3.CODE)
# count_var(nchda4,'dcode')

# COMMAND ----------

count_var(nchda4,'PERSON_ID')

# COMMAND ----------

nchda4 = nchda4.withColumn('DIAG_POSITION',f.lit('main')).withColumn('source',f.lit('nchda')).drop('CODE','epcc_code')
nchda4.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_cur_nchda')

# COMMAND ----------

# MAGIC %md
# MAGIC #13. HESCIPS

# COMMAND ----------

hescips.printSchema()

# COMMAND ----------

hescips = hescips.withColumn('dis_date',f.coalesce('cips_disdate','cips_epiend'))

# COMMAND ----------

hescips.where(f.col('dis_date').isNull()).count()

# COMMAND ----------

hescips.groupBy('epikey').agg(f.countDistinct('dis_date').alias('n')).where('n!=1').count()

# COMMAND ----------

hescips.groupBy('p_spell_id').agg(f.countDistinct('dis_date').alias('n')).where('n!=1').count()

# COMMAND ----------

hescips2 = hescips.select('epikey','dis_date').distinct()
hescips2.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_cur_hescips')

# COMMAND ----------

# MAGIC %md
# MAGIC #14. ONS

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14.1. Create and check

# COMMAND ----------

ons.printSchema()

# COMMAND ----------

ons.select(f.max('AGE_LOWER'),f.min('AGE_LOWER'),f.max('AGE_UPPER'),f.min('AGE_UPPER')).show()

# COMMAND ----------

ons.select('GENDER').distinct().show()

# COMMAND ----------

ons1 = ons.where((f.col('YEAR_OF_COUNT').isin(['2020','2021','2022','2023']))&(f.col('GEOGRAPHIC_SUBGROUP_CODE').isin(['E12000001','E12000002','E12000003','E12000004','E12000005','E12000006','E12000007','E12000008','E12000009'])))

# COMMAND ----------

ons1 = (ons1.withColumn('age',f.when(f.col('AGE_LOWER')<=19,'0_19')
                        .when((f.col('AGE_LOWER')>=20)&(f.col('AGE_LOWER')<=29),'20_29')
                        .when((f.col('AGE_LOWER')>=30)&(f.col('AGE_LOWER')<=39),'30_39')
                        .when((f.col('AGE_LOWER')>=40)&(f.col('AGE_LOWER')<=49),'40_49')
                        .when((f.col('AGE_LOWER')>=50)&(f.col('AGE_LOWER')<=59),'50_59')
                        .when((f.col('AGE_LOWER')>=60)&(f.col('AGE_LOWER')<=69),'60_69')
                        .when((f.col('AGE_LOWER')>=70)&(f.col('AGE_LOWER')<=79),'70_79')
                        .when((f.col('AGE_LOWER')>=80)&(f.col('AGE_LOWER')<=89),'80_89')
                        .when(f.col('AGE_LOWER')==90,'90_over'))

                      )


# COMMAND ----------

ons2 = ons1.groupBy(['YEAR_OF_COUNT','GEOGRAPHIC_SUBGROUP_CODE','GENDER','age']).agg(f.sum('POPULATION_COUNT').alias('n'))


# COMMAND ----------

ons2 = ons2.withColumnRenamed('YEAR_OF_COUNT','year').withColumnRenamed('GEOGRAPHIC_SUBGROUP_CODE','region').withColumnRenamed('GENDER','sex')

# COMMAND ----------

ons2 = (ons2.withColumn('year',f.col('year').astype('int'))
         .withColumn('region',f.when(f.col('region')=='E12000001','North East')
                                .when(f.col('region')=='E12000002','North West')
                                .when(f.col('region')=='E12000003','Yorkshire and The Humber')
                                .when(f.col('region')=='E12000004','East Midlands')
                                .when(f.col('region')=='E12000005','West Midlands')
                                .when(f.col('region')=='E12000006','East of England')
                                .when(f.col('region')=='E12000007','London')
                                .when(f.col('region')=='E12000008','South East')
                                .when(f.col('region')=='E12000009','South West')
                     )
         .withColumn('sex',f.when(f.col('sex')=='M','Male').when(f.col('sex')=='F','Female'))

        )

# COMMAND ----------

display(ons2)

# COMMAND ----------

ons2.write.mode('overwrite').option('overwriteSchema','true').saveAsTable(f'{dsa}.{proj}_cur_ons')

# COMMAND ----------

ons = spark.table(path_cur_ons)
tmp = ons.where('year=2023')

# COMMAND ----------

tmp.count()

# COMMAND ----------

tmp = tmp.withColumn('year',f.lit(2024))
tmp.printSchema()

# COMMAND ----------

tmp2  =ons.unionByName(tmp)
assert tmp2.count()==ons.count()+162
assert tmp2.select('year').distinct().count()==5

# COMMAND ----------

tmp2.write.mode('overwrite').option('overwriteSchema','true').saveAsTable(f'{dsa}.{proj}_cur_ons')

# COMMAND ----------

# MAGIC %md
# MAGIC ##14.2. Create by single age group

# COMMAND ----------

ons1 = ons.where((f.col('YEAR_OF_COUNT').isin(['2020','2021','2022','2023']))&(f.col('GEOGRAPHIC_SUBGROUP_CODE').isin(['E12000001','E12000002','E12000003','E12000004','E12000005','E12000006','E12000007','E12000008','E12000009']))).select(f.col('YEAR_OF_COUNT').alias('year').astype('int'),f.col('AGE_LOWER').alias('age'),f.col('GENDER').alias('sex'),f.col('GEOGRAPHIC_SUBGROUP_CODE').alias('region'),f.col('POPULATION_COUNT').alias('n'))


ons2 = (ons1
         .withColumn('region',f.when(f.col('region')=='E12000001','North East')
                                .when(f.col('region')=='E12000002','North West')
                                .when(f.col('region')=='E12000003','Yorkshire and The Humber')
                                .when(f.col('region')=='E12000004','East Midlands')
                                .when(f.col('region')=='E12000005','West Midlands')
                                .when(f.col('region')=='E12000006','East of England')
                                .when(f.col('region')=='E12000007','London')
                                .when(f.col('region')=='E12000008','South East')
                                .when(f.col('region')=='E12000009','South West')
                     )
         .withColumn('sex',f.when(f.col('sex')=='M','Male').when(f.col('sex')=='F','Female'))

        )

tmp = ons2.where('year=2023')
tmp = tmp.withColumn('year',f.lit(2024))

tmp2  =ons2.unionByName(tmp)
assert tmp2.count()==ons2.count()+1638
assert tmp2.select('year').distinct().count()==5
tmp2.write.mode('overwrite').option('overwriteSchema','true').saveAsTable(f'{dsa}.{proj}_cur_ons_age1')


# COMMAND ----------

# MAGIC %md
# MAGIC #15. LSOA ICB lookup

# COMMAND ----------

icb = spark.table(path_ref_ons_lsoa_icb)

# COMMAND ----------

icb=icb.withColumn('icb_name', f.regexp_extract('ICB22NM', '^NHS (.+) Integrated Care Board$', 1))

# COMMAND ----------

icb = icb.select('LSOA11CD','LSOA11NM','ICB22CD','icb_name')

# COMMAND ----------

icb = icb.withColumnRenamed('LSOA11CD','lsoa').withColumnRenamed('LSOA11NM','lsoa_name').withColumnRenamed('ICB22CD','icb').withColumnRenamed('icb_name','region')
icb.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_cur_lsoa_icb')