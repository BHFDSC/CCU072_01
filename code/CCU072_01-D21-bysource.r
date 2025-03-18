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
source_subgroup_output_file <- "ccu072_01_source_subgroup_collapsed_results.csv"

# Set value for metrics
prevalence_value <- "prev"
incidence_value <- "inc"
fatality_value <- "fatal"
recurrence_value <- "recur"

# Set labels for metrics
prevalence_label <- "Prevalence"
incidence_label <- "Incidence"
fatality_label <- "30-day case fatality"
recurrence_label <- "Post-event 1-year incidence"

# Set date groups
dsa <- "dsa_391419_j3w9t_collab"

table_name <- "ccu072_01_results_month_bysource_recur" #"ccu072_01_results_all_month_gp_denominator"
table_name <- paste0(dsa,".",table_name)
cat("Opening main results table",table_name,"\n")
data_all <- SparkR::tableToDF(table_name)
data_all <- collect(data_all)
print(addmargins(table(data_all$metric)))
print(head(data_all))
cat(dim(data_all),"\n")

# COMMAND ----------

cat("Rounding estimates\n")
# Remove counts <10
data_all$numerator[data_all$numerator<min_n] <- NA
# Round to nearest 5
data_all$numerator <- round_n*round(data_all$numerator/round_n)
print(head(data_all))
cat(dim(data_all),"\n")

cat("Removing NAs\n")
data_all <- data_all[!is.na(data_all$result),]
print(head(data_all))
cat(dim(data_all),"\n")

cat("Counting sources\n")
print(addmargins(table(data_all$source)))
cat("\n")

# COMMAND ----------

# Check last date
data_all$date <- paste0(data_all$year,"-",sprintf("%02d", data_all$month),"-01")
data_all$date <- as.Date(data_all$date)
data_all %>% dplyr::group_by(metric) %>% dplyr::summarize(min = min(date), mean = mean(date), median = median(date), max = max(date),)

# Set end display date
end_date <- max(data_all$date)
incidence_end_date <- end_date
prevalence_end_date <- end_date
fatality_end_date <- end_date-30
recurrence_end_date <- end_date-366

cat("Incidence end date:",as.character(incidence_end_date),"\n")
cat("Prevalence end date:",as.character(prevalence_end_date),"\n")
cat("Fatality end date:",as.character(fatality_end_date),"\n")
cat("Recurrence end date:",as.character(recurrence_end_date),"\n")

new_data_all <- data.frame()
for(metric in unique(data_all$metric)){
  cat(metric,"\n")
  df <- data_all[data_all$metric==metric,]
  cat("\t",dim(df),"\n")
  if(metric==incidence_value){
    df <- df[df$date<=incidence_end_date,]
  } else if(metric==prevalence_value){
    df <- df[df$date<=prevalence_end_date,]
  } else if(metric==fatality_value){
    df <- df[df$date<=fatality_end_date,]
  } else if(metric==recurrence_value){
    df <- df[df$date<=recurrence_end_date,]
  }
  cat("\t",dim(df),"\n")
  new_data_all <- rbind(new_data_all, df)
  remove(df)
}
data_all <- new_data_all
remove(new_data_all)
print(data_all %>% dplyr::group_by(metric) %>% dplyr::summarize(min = min(date), mean = mean(date), median = median(date), max = max(date)))
print(data_all %>% dplyr::group_by(metric) %>% dplyr::summarize(min = min(denominator, na.rm=T), max = max(denominator, na.rm=T)) %>% mutate_if(is.numeric,~round(.,digits=4)))

# COMMAND ----------

cat("Collapse all values to same column & remove unnecessary columns\n")
data_compact <- data_all
data_compact <- data_compact[with(data_compact, order(data_compact$dcode, data_compact$date, data_compact$metric, data_compact$source)),]
print(head(data_compact))
cat(dim(data_compact),"\n")

data_compact$result <- paste0(data_compact$metric,"@numerator=",data_compact$numerator,"#result=",data_compact$result)
data_compact <- data_compact[,c("dcode","dcode_name","date","source","result")]
print(head(data_compact))
cat(dim(data_compact),"\n")

data_compact <- data_compact %>% dplyr::group_by(dcode,date,source) %>% dplyr::mutate(result=paste0(result,collapse="###")) %>% dplyr::distinct() %>% as.data.frame() 
data_compact <- data_compact[,c("dcode","dcode_name","date","source","result")]
print(head(data_compact))
cat(dim(data_compact),"\n\n")

cat("Transform long to wide\n")
data_compact_wide <- data_compact %>% pivot_wider(names_from=c(source), values_from=c(result), names_glue="{source}") %>% as.data.frame()   #dispersion #raw_proportion,beta,se
#print(head(data_unadjusted_wide[,1:10]),row.names=F)
cat(dim(data_compact_wide),"\n")
cat(names(data_compact_wide)[1:10],"\n")
display(as.DataFrame(data_compact_wide))
cat("\n")

# COMMAND ----------

# Store to file for export
chunks <- chunk2(1:nrow(data_compact_wide), max_n_rows-2)
i <- 1
for(chunk in chunks){
  df <- data_compact_wide[chunk,]
  chunk_output_file <- paste0(gsub("\\.csv","", source_subgroup_output_file),"_",sprintf("%06d", i),".csv")
  chunk_output_file <- paste0(folder,"/",chunk_output_file)
  cat("\t",chunk_output_file,"\n")
  cat("\t\t",head(chunk),"...",tail(chunk),"\n")
  cat("\t\t",dim(df),"\n")
  fwrite(df, chunk_output_file)
  i <- i + 1
}