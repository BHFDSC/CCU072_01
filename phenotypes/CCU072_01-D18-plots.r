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
fig1_output_file <- "ccu072_01_fig1_notcollapsed_results.csv"
fig2_output_file <- "ccu072_01_fig2_collapsed_results.csv"

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
date_group_names <- c("COVID-19,\nno vaccination",
                      "Beginning of\nvaccination\nprogramme",
                      "Lifting of\nCOVID-19\nrestrictions")
date_group_dates <- as.Date(c("2020-12-08", # https://www.bmj.com/content/372/bmj.n421
                              "2022-04-01"))   # https://www.gov.uk/government/news/prime-minister-sets-out-plan-for-living-with-covid

#db = 'dars_nic_391419_j3w9t'
#dbc = f'dars_nic_391419_j3w9t_collab'
#dsa = f'dsa_391419_j3w9t_collab'
dsa <- "dsa_391419_j3w9t_collab"

table_name <- "ccu072_01_results_month_weight_age1" #"ccu072_01_results_month_recur" #"ccu072_01_results_all_month_gp_denominator"
#table_name <- "ccu072_01_inc_year_ons" #"ccu072_01_results_all_month_gp_denominator"
table_name <- paste0(dsa,".",table_name)
cat("Opening main results table",table_name,"\n")
data_all <- SparkR::tableToDF(table_name)
data_all <- collect(data_all)
print(addmargins(table(data_all$metric)))
print(head(data_all))
cat(dim(data_all),"\n")

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
print(data_all %>% dplyr::group_by(metric) %>% dplyr::summarize(min = min(denominator), max = max(denominator)) %>% mutate_if(is.numeric,~round(.,digits=4)))

# COMMAND ----------

display(as.DataFrame(data_all))

# COMMAND ----------

# Quick counts for Rob, 15/1/2025
options(scipen = 999)
data_rob <- data_all
data_rob$denominator <- round_n*round(data_rob$denominator/round_n)
print(data_rob[data_rob$metric%in%c(prevalence_value),] %>% dplyr::group_by(date) %>% dplyr::summarise(min_denominator=min(denominator), max_denominator=max(denominator)) %>% as.data.frame())
range(data_rob$denominator[data_rob$metric==prevalence_value])

# COMMAND ----------

head(data_all)

# COMMAND ----------

# Format dataframe
data <- data_all
data$result <- as.numeric(data$result)
data$group <- "All"
data$value <- "-"
#data$month <- as.numeric(data$month)
#data$year <- as.numeric(data$year)
data$numerator <- as.numeric(data$numerator)
data$denominator <- as.numeric(data$denominator)
data$metric[data$metric==prevalence_value] <- prevalence_label
data$metric[data$metric==incidence_value] <- incidence_label
data$metric[data$metric==fatality_value] <- fatality_label
data$metric[data$metric==recurrence_value] <- recurrence_label
data <- data[,c("group","value","metric","dcode","date","numerator","denominator","result")]
#data$dcode_name <- gsub("_"," ",data$dcode_name)
data <- data[data$metric%in%c(incidence_label, prevalence_label, fatality_label, recurrence_label),]
data$metric <- ordered(data$metric, levels=c(incidence_label, prevalence_label, fatality_label, recurrence_label))
print(addmargins(table(data$metric)))
print(head(data))
cat(dim(data),"\n")

# COMMAND ----------

# Input data for Figure 1

group <- "All"
fig1_data <- data.frame()
for(metric in levels(data$metric)){
  #metric <- levels(data$metric)[2]
  cat(metric,"\n")
  df <- data[data$group==group & data$metric==metric,]
  df$year <- NULL
  df$mont <- NULL
  df$denominator <- NULL
  # Remove counts <10
  df$numerator[df$numerator<min_n] <- NA
  # Round to nearest 5
  df$numerator <- round_n*round(df$numerator/round_n)
  
  fig1_data <- dplyr::bind_rows(fig1_data, df)
  remove(df)
}
print(head(fig1_data))
cat(dim(fig1_data),"\n")

fig1_data <- fig1_data %>% pivot_wider(names_from = metric, values_from = c(numerator, result))
print(head(fig1_data))
cat(dim(fig1_data),"\n")

# COMMAND ----------

# Input data for Figure 1

group <- "All"
fig1_data <- data.frame()
for(metric in levels(data$metric)){
  #metric <- levels(data$metric)[2]
  cat(metric,"\n")
  df <- data[data$group==group & data$metric==metric,]
  df$year <- NULL
  df$mont <- NULL
  df$denominator <- NULL
  # Remove counts <10
  df$numerator[df$numerator<min_n] <- NA
  # Round to nearest 5
  df$numerator <- round_n*round(df$numerator/round_n)
  
  fig1_data <- dplyr::bind_rows(fig1_data, df)
  remove(df)
}
print(head(fig1_data))
cat(dim(fig1_data),"\n")

fig1_data <- fig1_data %>% pivot_wider(names_from = metric, values_from = c(numerator, result))
print(head(fig1_data))
cat(dim(fig1_data),"\n")

# Store to file for export
chunks <- chunk2(1:nrow(fig1_data), max_n_rows-2)
i <- 1
for(chunk in chunks){
  df <- fig1_data[chunk,]
  chunk_output_file <- paste0(gsub("\\.csv","", fig1_output_file),"_",sprintf("%06d", i),".csv")
  chunk_output_file <- paste0(folder,"/",chunk_output_file)
  cat("\t",chunk_output_file,"\n")
  cat("\t\t",head(chunk),"...",tail(chunk),"\n")
  cat("\t\t",dim(df),"\n")
  fwrite(df, chunk_output_file)
  i <- i + 1
}

# COMMAND ----------

display(as.DataFrame(fig1_data))

# COMMAND ----------

# Input data for Figure 2

group <- "All"
fig2_data <- data.frame()
for(metric in levels(data$metric)){
  #metric <- levels(data$metric)[1]
  cat(metric,"\n")
  df <- data[data$group==group & data$metric==metric,]
  #df$date <- as.Date(paste0(df$year,"-",sprintf("%02d", df$month),"-01"))
  
  # Specify plot-specific groups
  date_groups <- list(c(min(df$date), date_group_dates[1]-1),
                      c(date_group_dates[1], date_group_dates[2]-1), 
                      c(date_group_dates[2], max(df$date)))
  names(date_groups) <- date_group_names
  
  # Aggregate
  df$date_group <- NA
  df$new_date <- NA
  for(date_group in names(date_groups)){
    #date_group <- names(date_groups)[1]
    df$date_group[df$date>=date_groups[[date_group]][1] & df$date<=date_groups[[date_group]][2]] <- date_group
    df$new_date[df$date_group==date_group] <- as.character(mean(date_groups[[date_group]]))
    # if(date_group==date_groups[length(date_groups)]){
    #   df$new_date[df$date_group==date_group] <- as.character(mean(date_groups[[date_group]])+30)
    # }
  }
  initial_df <- df
  df$date <- as.Date(df$new_date)
  #print(head(df))
  df <- df %>% dplyr::group_by(dcode, date_group, date) %>% dplyr::summarise(result=sum(numerator, na.rm=T)/sum(denominator, na.rm = T), numerator=sum(numerator, na.rm=T)) %>% dplyr::as_data_frame()
  if(metric==prevalence_label){
    df$result <- df$result*100
  }
  df$metric <- metric
  # Remove counts <10
  df$numerator[df$numerator<min_n] <- NA
  # Round to nearest 5
  df$numerator <- round_n*round(df$numerator/round_n)
  df$date_group <- ordered(df$date_group, levels=names(date_groups))
  fig2_data <- rbind(fig2_data, df)
  remove(df)
}
print(head(fig2_data))
cat(dim(fig2_data),"\n")

# Store to file for export
chunks <- chunk2(1:nrow(fig2_data), max_n_rows-2)
i <- 1
for(chunk in chunks){
  df <- fig2_data[chunk,]
  chunk_output_file <- paste0(gsub("\\.csv","", fig2_output_file),"_",sprintf("%06d", i),".csv")
  chunk_output_file <- paste0(folder,"/",chunk_output_file)
  cat("\t",chunk_output_file,"\n")
  cat("\t\t",head(chunk),"...",tail(chunk),"\n")
  cat("\t\t",dim(df),"\n")
  fwrite(df, chunk_output_file)
  i <- i + 1
}
