# Databricks notebook source
# MAGIC %md # CCU072_01-D08-comorbidity_quan
# MAGIC
# MAGIC **Description** This notebook creates the Charlson Comorbidity Index-Quan update for each person in our cohort, which are defined before baseline date for each calendar year using HES APC. 
# MAGIC
# MAGIC **Authors** Wen Shi
# MAGIC
# MAGIC **Last Reviewed** 12/03/2025
# MAGIC
# MAGIC **Acknowledgement** Mehrdad Mizani and previous CVD-COVID-UK/COVID-IMPACT projects.
# MAGIC

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

# MAGIC %md # 1 Parameters

# COMMAND ----------

# MAGIC %run "./CCU072_01-D01-parameters"

# COMMAND ----------

# MAGIC %md # 2. Data

# COMMAND ----------

icd_quan = spark.table(path_out_codelist_charlson_quan)
spark.sql(f"""REFRESH TABLE {path_cur_hes_apc_long}""")
hes_apc_long = spark.table(path_cur_hes_apc_long)



# COMMAND ----------

# The weights will be used later in CCI calculator 
quan_weight_dic = {
    "mi": 0,
    "chf": 2,
    "pvd": 0,
    "cevd": 0,
    "dementia": 2,
    "cpd": 1,
    "rheumd": 1,
    "pud": 0,
    "diabwc": 1,
    "hp": 2,
    "rend": 1,
    "msld": 4,
    "metacanc": 6,
    "aids": 4,
    "diab": 0,
    "canc": 2,
    "mld": 2
}

# COMMAND ----------

# MAGIC %md # 3. Prepare and save

# COMMAND ----------

for i in period:
    spark.sql(f"""REFRESH TABLE {dsa}.{proj}_out_inc_exc_cohort_year{i}""")
    cohort = spark.table(f'{dsa}.{proj}_out_inc_exc_cohort_year{i}')
    print(f'{"year"+str(i):-^80}')
    count_var(cohort,'PERSON_ID')
    individual_censor_dates = (
        cohort
        .select('PERSON_ID', 'DOB')
        .withColumnRenamed('DOB', 'CENSOR_DATE_START')
        .withColumn('CENSOR_DATE_END', f.to_date(f.lit(f'{i}-1-1')))
        )
    
    # clean hes_apc
    hes_apc_long_prepared = (
        hes_apc_long.select('PERSON_ID', f.col('EPISTART').alias('DATE'), 'CODE')
                    .join(individual_censor_dates, on='PERSON_ID', how='inner')
    )

    
    hes_apc_long_prepared = (
    hes_apc_long_prepared
    .where((f.col('DATE') < f.col('CENSOR_DATE_END'))&(f.col('DATE') >= f.col('CENSOR_DATE_START')))
    )
    
    hes_apc_long_prepared.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp_comorbid_hes_apc_year{i}')
    hes_apc_long_prepared = spark.table(f'{dsa}.{proj}_tmp_comorbid_hes_apc_year{i}')

   

    # dictionary - dataset, codelist, and ordering in the event of tied records
    dict_hx_char = {
    'hes_apc': ['hes_apc_long_prepared', 'icd_quan',  1]
   
    }

    # run codelist match and codelist match summary functions
    hx_char, hx_char_1st, hx_char_1st_wide = codelist_match(dict_hx_char, _name_prefix=f'cov_hx_char_')

    hx_char_1st_wide.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_tmp_comorbid_hx_char_1st_wide_year{i}')

    cci_df = (
        spark.table(f'{dsa}.{proj}_tmp_comorbid_hx_char_1st_wide_year{i}')
        .drop("CENSOR_DATE_START", "CENSOR_DATE_END")
        )

   
    columns = ["mi", "chf", "pvd", "cevd", "dementia", "cpd", "rheumd", "pud", "diab", "diabwc", "hp", "rend",
           "mld", "msld", "canc", "metacanc", "aids"]
    assert len(columns) == 17
    for index, item in enumerate(columns):
        cci_df = (
            cci_df
            .withColumnRenamed(f'''cov_hx_char_{item}_flag''', item)
            .drop(f'''cov_hx_char_{item}_date''')
            .withColumn(item, f.when(f.col(item).isNull(), f.lit(0)).otherwise(f.col(item)))
        )
    
    # Quan CCI calculator
    df = cci_df
    # Mapping of comorbidities to Quan weights
    map_all = quan_weight_dic


    # Add columns to store weights
    col_list = map_all.keys()
    prefix = "cci"
    new_col_list = [f'''{prefix}_{x}'''  for x in col_list]
    for col, new_col in zip(col_list, new_col_list):
        df = df.withColumn(new_col, f.when(
            f.col(col) == 1, f.lit(map_all.get(col))).otherwise(f.lit(0)))
    #Add another set of columns for mutual-exclusive Quan (original Quan)
    prefix_mutual_exclusive = "mex"
    no_mex_col_list = [x for x in new_col_list if x not in ["cci_diab", "cci_canc", "cci_mld"]]
    mex_only_list = ["mex_cci_diab", "mex_cci_canc", "mex_cci_mld"]
    # Add columns for mild conditions of mutually-exclusive comorbidity pairs
    df = df.withColumn("mex_cci_diab", f.when(f.col("cci_diabwc") != 0, f.lit(
            0)).otherwise(f.col("cci_diab")))
    df= df.withColumn("mex_cci_canc", f.when(f.col("cci_metacanc") != 0, f.lit(
            0)).otherwise(f.col("cci_canc")))
    df= df.withColumn("mex_cci_mld", f.when(f.col("cci_msld") != 0, f.lit(
            0)).otherwise(f.col("cci_mld")))
    mex_col_list= ["mex_cci_diab", "mex_cci_canc", "mex_cci_mld"]
    new_col_list_mex = no_mex_col_list+mex_col_list


    # Calculate CCI 

    # Comment out the next line to have Quan wiht no mutual-exclusivity condition applied
    df = (df.withColumn("cci_index", f.expr('+'.join(new_col_list_mex)))
            .drop(*new_col_list)
            .drop(*new_col_list_mex)
            .drop(*col_list)
    )
    
    cohort_cci = (cohort.join(df,'PERSON_ID','left')
                    .withColumn('comorbidity',f.when((f.col('cci_index').isNull())|(f.col('cci_index')==0),f.lit('0'))
                                                .when((f.col('cci_index')>=1)&(f.col('cci_index')<=2),f.lit('1_2'))
                                                .when((f.col('cci_index')>=3)&(f.col('cci_index')<=4),f.lit('3_4'))
                                                .when((f.col('cci_index')>=5),f.lit('5_over')))
                )
    cohort_cci.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dsa}.{proj}_out_inc_exc_cohort_year{i}') 
    cohort_cci = spark.table(f'{dsa}.{proj}_out_inc_exc_cohort_year{i}') 
    tab(cohort_cci,'comorbidity')
    

        
            
                    
                        
                    