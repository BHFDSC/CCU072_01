# Databricks notebook source
cat("Initialising\n")
suppressMessages(library(tidyverse))
#suppressMessages(library(dplyr))      # Data manipulation
#suppressMessages(library(readr))      # Convert dataframe to delimited string
suppressMessages(library(lubridate))  # Semester function
suppressMessages(library(SparkR))     # Databricks wrapper functions for R
suppressMessages(library(data.table)) # Clean CSV output

# Output limits
min_n <- 10
round_n <- 5
max_n_rows <- 1000

# Split "x" vector in chunks of length "n"
#chunk2 <- function(x,n) split(x, cut(seq_along(x), n, labels = FALSE)) 
chunk2 <- function(x, n){
  split(x, ceiling(seq_along(x)/n))
}

# In print, show 1000 columns and no scientific notation
options(width = 1000, scipen = 0)

# Output folder is a subfolder of the current directory
folder <- getwd() # "/Workspace/Repos/ea431@medschl.cam.ac.uk/ccu013_03/results"
#folder <- "/Workspace/Repos/ea431@medschl.cam.ac.uk/ccu072_01/results"
folder <- "/Workspace/Users/ea431@medschl.cam.ac.uk/ccu072_01/results"

# Output table
tab1_output_file <- "ccu072_01_tab1_results.csv"

# Set value for metrics
prevalence_value <- "prev"

# Set labels for metrics
prevalence_label <- "Prevalence"

dsa <- "dsa_391419_j3w9t_collab"

table_name <- "ccu072_01_results_month_weight_age1" # "ccu072_01_results_month_recur" #"ccu072_01_results_all_month_gp_denominator"
table_name <- paste0(dsa,".",table_name)
cat("Opening main results table",table_name,"\n")
data_all <- SparkR::tableToDF(table_name)
data_all <- collect(data_all)
print(addmargins(table(data_all$metric)))
print(head(data_all))
cat(dim(data_all),"\n")

table_name <- "ccu072_01_results_onecovadj_year_weight_age1_ethnicregion" #"ccu072_01_results_onecovadj_year_weight_age1" #"ccu072_01_results_year_onecovadj_recur_ethnic_region" #"ccu072_01_results_all_month_gp_denominator"
table_name <- paste0(dsa,".",table_name)
cat("Opening subgroup results table",table_name,"\n")
data_group <- SparkR::tableToDF(table_name)
data_group <- collect(data_group)
print(addmargins(table(data_group$metric)))
print(head(data_group))
cat(dim(data_group),"\n")

# COMMAND ----------

data_all_n <- data_all[data_all$metric==prevalence_value,] %>% dplyr::group_by(year) %>% dplyr::summarise(n=max(denominator)) %>% as.data.frame
data_all_n$group <- "all"
data_all_n$value <- "-"
print(data_all_n)

# COMMAND ----------

data_group_n <- data_group[data_group$metric==prevalence_value,] %>% dplyr::group_by(year, group, value) %>% dplyr::summarise(n=max(denominator)) %>% as.data.frame
print(data_group_n)

# COMMAND ----------

data_n <- rbind(data_all_n, data_group_n)
cat(dim(data_n))

# Remove counts <10
data_n$n[data_n$n<min_n] <- NA
# Round to nearest 5
data_n$n <- round_n*round(data_n$n/round_n)
  
print(data_n)

# Store to file for export
chunks <- chunk2(1:nrow(data_n), max_n_rows-2)
i <- 1
for(chunk in chunks){
  df <- data_n[chunk,]
  chunk_output_file <- paste0(gsub("\\.csv","", tab1_output_file),"_",sprintf("%06d", i),".csv")
  chunk_output_file <- paste0(folder,"/",chunk_output_file)
  cat("\t",chunk_output_file,"\n")
  cat("\t\t",head(chunk),"...",tail(chunk),"\n")
  cat("\t\t",dim(df),"\n")
  fwrite(df, chunk_output_file)
  i <- i + 1
}

# COMMAND ----------

display(as.DataFrame(data_n))