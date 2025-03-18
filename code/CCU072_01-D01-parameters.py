# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # CCU072_01-D01-parameters
# MAGIC
# MAGIC **Description** This notebook defines a set of parameters, which is loaded in each notebook in the pipeline, so that helper functions and parameters are consistently available.
# MAGIC
# MAGIC **Authors** Wen Shi
# MAGIC
# MAGIC **Last Reviewed** 12/03/2025
# MAGIC
# MAGIC **Acknowledgements**  Previous CVD-COVID-UK/COVID-IMPACT projects.

# COMMAND ----------

# MAGIC %run "/Shared/SHDS/common/functions"

# COMMAND ----------

import pyspark.sql.functions as f
import pandas as pd
import re
import datetime

# COMMAND ----------

# -----------------------------------------------------------------------------
# Project
# -----------------------------------------------------------------------------
proj = 'ccu072_01'


# -----------------------------------------------------------------------------
# Databases
# -----------------------------------------------------------------------------
db = 'dars_nic_391419_j3w9t'
dbc = f'dars_nic_391419_j3w9t_collab'
dsa = f'dsa_391419_j3w9t_collab'
# -----------------------------------------------------------------------------
# Timeline parameters
# -----------------------------------------------------------------------------
tmp_archived_on = '2024-10-01'
end_date = datetime.datetime.strptime(tmp_archived_on,'%Y-%m-%d').date()-datetime.timedelta(days=91)
idx_end_date = end_date - datetime.timedelta(days=61)
idx_end_month = idx_end_date.month
period = range(2020,idx_end_date.year+1)
month = range(1,13)
# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
# data frame of datasets
data = [
    ['deaths',  dbc, f'deaths_{db}_archive',            tmp_archived_on, 'DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'REG_DATE_OF_DEATH']
  , ['gdppr',   dbc, f'gdppr_{db}_archive',             tmp_archived_on, 'NHS_NUMBER_DEID',                'DATE']
  , ['hes_apc', dbc, f'hes_apc_all_years_archive',      tmp_archived_on, 'PERSON_ID_DEID',                 'EPISTART'] 
  , ['minap',   dbc, f'nicor_minap_{db}_archive',      '2023-12-27', 'NHS_NUMBER_DEID',                 ''] 
  , ['nhfa',   dbc, f'nicor_hf_{db}_archive',      '2023-08-31', 'NHS_NUMBER_DEID',                 ''] 
  , ['nhfa_v5',   dbc, f'nicor_hf_v5_{db}_archive',      '2023-08-31', 'PERSON_ID_DEID',                 ''] 
  , ['ssnap',   dbc, f'ssnap_{db}_archive',      '2024-09-02', 'Person_ID_DEID',                 ''] 
  , ['nacrm',   dbc, f'nicor_crm_eps_{db}_archive',     '2023-08-31', 'PERSON_ID_DEID',                 ''] 
  , ['nchda',   dbc, f'nicor_congenital_{db}_archive',     '2023-08-31', 'NHS_NUMBER_DEID',                 ''] 

]
parameters_df_datasets = pd.DataFrame(data, columns = ['dataset', 'database', 'table', 'archived_on', 'idVar', 'dateVar'])
print('parameters_df_datasets:\n', parameters_df_datasets.to_string())
  
# note: the below is largely listed in order of appearance within the pipeline:  

# reference tables
path_ref_bhf_phenotypes  = 'bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127'
path_ref_map_ctv3_snomed = 'dss_corporate.read_codes_map_ctv3_to_snomed'
path_ref_geog            = 'dss_corporate.ons_chd_geo_listings'
path_ref_imd             = 'dss_corporate.english_indices_of_dep_v02'
path_ref_gp_refset       = 'dss_corporate.gpdata_snomed_refset_full'
path_ref_gdppr_refset    = 'dss_corporate.gdppr_cluster_refset'
path_ref_icd10           = 'dss_corporate.icd10_group_chapter_v01'
path_ref_ons             = 'dss_corporate.ons_population_v2'
path_ref_ons_lsoa_icb    = 'dss_corporate.cvdp_lsoa_icb_nhser' 

# curated tables
path_cur_gdppr             = f'{dsa}.{proj}_cur_gdppr'
path_cur_hes_apc_long      = f'{dsa}.{proj}_cur_hes_apc_all_years_long'
path_cur_hes_apc_oper_long = f'{dsa}.{proj}_cur_hes_apc_all_years_archive_oper_long'
path_cur_deaths_long       = f'{dsa}.{proj}_cur_deaths_archive_long'
path_cur_deaths_sing       = f'{dsa}.{proj}_cur_deaths_archive_sing'
path_cur_lsoa_region       = f'{dsa}.{proj}_cur_lsoa_region_lookup'
path_cur_lsoa_imd          = f'{dsa}.{proj}_cur_lsoa_imd_lookup'
path_cur_minap             = f'{dsa}.{proj}_cur_minap'
path_cur_ssnap             = f'{dsa}.{proj}_cur_ssnap'
path_cur_nacrm             = f'{dsa}.{proj}_cur_nacrm'
path_cur_nhfa              = f'{dsa}.{proj}_cur_nhfa'
path_cur_nchda             = f'{dsa}.{proj}_cur_nchda'
path_cur_hescips           = f'{dsa}.{proj}_cur_hescips'
path_cur_ons               = f'{dsa}.{proj}_cur_ons_age1'
path_cur_lsoa_icb          = f'{dsa}.{proj}_cur_lsoa_icb'
path_cur_gdppr_weight      = f'{dsa}.{proj}_cur_gdppr_weight'  

# temporary tables
path_tmp_demo                  = f"{dsa}.hds_curated_assets__demographics_{re.sub('-','_',tmp_archived_on)}"

path_tmp_quality_assurance_hx_1st_wide  = f'{dsa}.{proj}_tmp_quality_assurance_hx_1st_wide'
path_tmp_quality_assurance_hx_1st       = f'{dsa}.{proj}_tmp_quality_assurance_hx_1st'
path_tmp_quality_assurance_qax          = f'{dsa}.{proj}_tmp_quality_assurance_qax'
path_tmp_lsoa                           =f"{dsa}.hds_curated_assets__lsoa_multisource_{re.sub('-','_',tmp_archived_on)}"
path_tmp_rank_lsoa                      =f'{dsa}.{proj}_tmp_rank_mode_lsoa_year'
path_tmp_ethnicity                      = f"{dsa}.hds_curated_assets__ethnicity_multisource_{re.sub('-','_',tmp_archived_on)}"

path_tmp_cohort                         = f'{dbc}.{proj}_tmp_cohort'
path_tmp_flow_inc_exc                   = f'{dbc}.{proj}_tmp_flow_inc_exc'
path_tmp_covariates_hes_apc             = f'{dbc}.{proj}_tmp_covariates_hes_apc'
path_tmp_hescips               = f"{dsa}.hds_curated_assets__hes_apc_cips_episodes_{re.sub('-','_',tmp_archived_on)}"

# out tables
path_out_codelist_comorbidity       = f'{dbc}.{proj}_out_codelist_comorbidity'
path_out_codelist_quality_assurance = f'{dsa}.{proj}_out_codelist_quality_assurance'
path_out_codelist_charlson_quan     = f'{dsa}.{proj}_codelist_charlson_quan_icd'
path_out_dcode_name                 = f'{dsa}.{proj}_dcode_name'
path_out_dcode_icd10                = f'{dsa}.{proj}_dcode_icd10'
path_out_dcode_snomed               = f'{dsa}.{proj}_dcode_snomed'
path_out_dcode_epcc                 = f'{dsa}.{proj}_dcode_epcc'
# path_out_codelist_outcome           = f'{dsa}.{proj}_out_codelist_outcome'
# path_out_codelist_outcome_v2        = f'{dsa}.{proj}_out_codelist_outcome_v2'
path_out_codelist_outcome_v3        = f'{dsa}.{proj}_out_codelist_outcome_v3'
path_out_codelist_epcc              = f'{dsa}.{proj}_out_codelist_epcc'
# path_out_codelist_epcc_old          = f'{dsa}.{proj}_out_code_epcc'

path_out_skinny                     = f'{dsa}.{proj}_skinny'
path_out_quality_assurance          = f'{dsa}.{proj}_quality_assurance'
path_out_lsoa                       = f'{dsa}.{proj}_rank_mode_lsoa_year'
path_out_cohort                     = f'{dsa}.{proj}_out_inc_exc_cohort_year'
path_out_flow                       = f'{dsa}.{proj}_out_flow_inc_exc_year'
path_out_cohort_relevel             = f'{dsa}.{proj}_out_cohort_relevel_year' 
path_out_icb                        = f'{dsa}.{proj}_lsoa_icb_year'
path_out_cohort_relevel_ethnic_region  = f'{dsa}.{proj}_out_cohort_relevel_ethnic_region_year' 

param_path_ref_icd10                = 'dss_corporate.icd10_group_chapter_v01'




# COMMAND ----------

# function to extract the batch corresponding to the pre-defined archived_on date from the archive for the specified dataset
from pyspark.sql import DataFrame
def extract_batch_from_archive(_df_datasets: DataFrame, _dataset: str):
  
  # get row from df_archive_tables corresponding to the specified dataset
  _row = _df_datasets[_df_datasets['dataset'] == _dataset]
  
  # check one row only
  assert _row.shape[0] != 0, f"dataset = {_dataset} not found in _df_datasets (datasets = {_df_datasets['dataset'].tolist()})"
  assert _row.shape[0] == 1, f"dataset = {_dataset} has >1 row in _df_datasets"
  
  # create path and extract archived on
  _row = _row.iloc[0]
  _path = _row['database'] + '.' + _row['table']  
  _archived_on = _row['archived_on']  
  print(_path + ' (archived_on = ' + _archived_on + ')')
  


  # extract batch
  _tmp = spark.table(_path)\
    .where(f.col('archived_on') == _archived_on)  
  
  # check number of records returned
  _tmp_records = _tmp.count()
  print(f'  {_tmp_records:,} records')
  assert _tmp_records > 0, f"number of records == 0"

  # return dataframe
  return _tmp