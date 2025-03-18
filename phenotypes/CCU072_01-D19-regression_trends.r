# Databricks notebook source
cat("Initialising\n")
suppressMessages(library(tidyverse))
suppressMessages(library(SparkR))     # Databricks wrapper functions for R
suppressMessages(library(data.table)) # Clean CSV output

# Output limits
min_n <- 10
round_n <- 5
max_n_rows <- 1000

# Split "x" vector in chunks of length "n"
chunk2 <- function(x, n){
  split(x, ceiling(seq_along(x)/n))
}

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

# In print, show 1000 columns and no scientific notation
options(width = 1000, scipen = 0)

# Set date groups #1
date_group_1_names <- c("COVID-19,\nno vaccination",
                      "Beginning of\nvaccination\nprogramme",
                      "Lifting of\nCOVID-19\nrestrictions")
date_group_1_names <- gsub("\n"," ",date_group_1_names)
date_group_1_dates <- as.Date(c("2020-12-08", # https://www.bmj.com/content/372/bmj.n421
                              "2022-04-01"))   # https://www.gov.uk/government/news/prime-minister-sets-out-plan-for-living-with-covid

# Set date groups #2
date_group_2_names <- c("Before first COVID-19 wave",
                      "During first COVID-19 wave",
                      "After first COVID-19 wave and before last year",
                      "In the last year")
date_group_2_dates <- as.Date(c("2020-03-01", # Beginning of first lockdown
                              "2020-08-01")) # Easing of lockdown measures: August 3, eat out to help out; 14th August, reopening of outdoor theatres

# Output folder is a subfolder of the current directory
folder <- getwd()
folder <- "/Workspace/Users/ea431@medschl.cam.ac.uk/ccu072_01/results"

# Output table
cat_trend_output_file <- "ccu072_01_cat_trend_results.csv"

dsa <- "dsa_391419_j3w9t_collab"

table_name <- "ccu072_01_results_month_weight_age1" #"ccu072_01_results_month_recur"
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

# COMMAND ----------

cat("\n")
data_all$metric[data_all$metric==prevalence_value] <- prevalence_label
data_all$metric[data_all$metric==incidence_value] <- incidence_label
data_all$metric[data_all$metric==fatality_value] <- fatality_label
data_all$metric[data_all$metric==recurrence_value] <- recurrence_label
print(addmargins(table(data_all$metric)))
print(head(data_all))
cat(dim(data_all),"\n")

all_results <- data.frame()
for(dcode in unique(data_all$dcode)){

  for(metric in unique(data_all$metric[data_all$dcode==dcode])){
    #metric <- levels(data_all$metric)[1]
    cat(dcode, metric,"\n")
    df <- data_all[data_all$dcode==dcode & data_all$metric==metric & !is.na(data_all$date),]
    cat("\t",dim(df),"\n")
  
    # Create date groups 
    for(group in 1:2){
      date_group_dates <- get(paste0("date_group_",group,"_dates"))
      date_group_names <- get(paste0("date_group_",group,"_names"))
      date_group_var <-  "date_group" # paste0("date_group_",group)
      if(group==1){
        date_groups <- list(c(min(df$date), date_group_dates[1]-1),
                          c(date_group_dates[1], date_group_dates[2]-1), 
                          c(date_group_dates[2], max(df$date)))
      } else if(group==2){
        maxdate_yearbefore <- as.POSIXlt(max(df$date))
        maxdate_yearbefore$year <- maxdate_yearbefore$year-1
        maxdate_yearbefore$mon <- maxdate_yearbefore$mon+1
        date_group_dates <- c(date_group_dates,  as.Date(maxdate_yearbefore))
        date_groups <- list(c(min(df$date), date_group_dates[1]-1),
                          c(date_group_dates[1], date_group_dates[2]-1), 
                          c(date_group_dates[2], date_group_dates[3]-1), 
                          c(date_group_dates[3], max(df$date)))
      }
      # Add dates to name
      new_date_group_names <- vector()
      i <- 1
      for(level in date_group_names){
        new_date_group_names <- c(new_date_group_names, paste0(level, " (",
                                                               paste(sub("-[^-]+$", "",date_groups[[i]]), collapse=" to "),
                                                               ")"))
        i <- i + 1
      }
      date_group_names <- new_date_group_names
      names(date_groups) <- date_group_names
      print(date_groups)
      names(date_groups) <- 1:(length(date_group_names))
      df[[date_group_var]] <- NA
      for(date_group in names(date_groups)){
        df[[date_group_var]][df$date>=date_groups[[date_group]][1] & df$date<=date_groups[[date_group]][2]] <- date_group
      }
      #df[[date_group_var]] <- as.factor(as.numeric(ordered(df[[date_group_var]], date_group_names)))
      print(addmargins(table(df$date, df[[date_group_var]], useNA="ifany")))
      cat("Reference level:",date_group_names[1],"\n")

      summarised_df <- df %>% dplyr::group_by(across(date_group_var)) %>% dplyr::summarise(raw_numerator=sum(numerator,na.rm=T), raw_denominator=sum(denominator,na.rm=T)) %>% as.data.frame() # dplyr::group_by(year) %>% 
      summarised_df$raw_proportion <- summarised_df$raw_numerator / summarised_df$raw_denominator
      for(level in 1:(length(date_group_names))){
          summarised_df$var_name[summarised_df[[date_group_var]]==level] <- date_group_names[level]
      }
      summarised_df[[date_group_var]] <- NULL
      print(summarised_df)

      tryCatch({
        m <- glm(as.formula(paste0("numerator ~ ",date_group_var," + offset(log(denominator))")), family=quasipoisson, data=df)
        df_results <- as.data.frame(summary(m)$coef)
        df_results$var <- row.names(df_results)
        row.names(df_results) <- NULL
        df_results$dcode <- dcode
        #df_results$dcode_name <- unique(df$dcode_name)
        df_results$metric <- metric
        df_results$date_group <- group
        df_results_ci <- as.data.frame(exp(confint(m)))
        df_results_ci$var <- row.names(df_results_ci)
        df_results <- merge(df_results, df_results_ci)
        print(df_results)
        for(level in 2:(length(date_group_names))){
          df_results$var_name[df_results$var==paste0(date_group_var,level)] <- date_group_names[level]
          df_results$var_name_pcchange[df_results$var==paste0(date_group_var,level)] <- paste(date_group_names[level],"vs",date_group_names[1])
        }
        df_results$var_name[df_results$var=="(Intercept)"] <- date_group_names[1]
        print(df_results)
        df_results <- merge(df_results, summarised_df, all.x=T)
        print(df_results)
        df_results$beta <- df_results$Estimate
        df_results$pcchange <- (exp(df_results$Estimate)-1)*100
        df_results$pcchange_p <- df_results$`Pr(>|t|)`
        df_results$pcchange_l95ci <- (df_results$`2.5 %`-1)*100
        df_results$pcchange_u95ci <- (df_results$`97.5 %`-1)*100
        intercept <- df_results$Estimate[df_results$var=="(Intercept)"]
        df_results$intercept <- intercept
        for(var in df_results$var){
          if(var=="(Intercept)"){
            df_results$intercept_plus_beta[df_results$var==var] <- intercept
          } else{
            df_results$intercept_plus_beta[df_results$var==var] <- intercept + df_results$Estimate[df_results$var==var]
          }
        }
        df_results$exp_intercept_plus_beta <- exp(df_results$intercept_plus_beta)
        df_results <- df_results[,c("dcode","metric","date_group","var_name_pcchange","pcchange","pcchange_l95ci","pcchange_u95ci","pcchange_p","var_name","intercept_plus_beta","exp_intercept_plus_beta","raw_numerator","raw_proportion")]
        #df_results <- df_results[!is.na(df_results$var_name),]
        row.names(df_results) <- NULL
        #print(df_results)
        all_results <- rbind(all_results, df_results)
        },
        error = function(cond) {
        },
        warning = function(cond) {
        },
        finally = {
        }
      )      
    }    
  }
}

# COMMAND ----------

print(head(all_results))
cat(dim(all_results),"\n")
display(as.DataFrame(all_results))

# COMMAND ----------

print(head(all_results))
cat(dim(all_results),"\n")

cat("Suppress small counts and round counts to nearest 5\n")
# Remove counts <10
all_results$raw_numerator[all_results$raw_numerator<min_n] <- NA
# Round to nearest 5
all_results$raw_numerator <- round_n*round(all_results$raw_numerator/round_n)
print(head(all_results),row.names=F)
cat(dim(all_results),"\n")
cat("\n")

# Store to file for export
chunks <- chunk2(1:nrow(all_results), max_n_rows-2)
i <- 1
for(chunk in chunks){
  df <- all_results[chunk,]
  chunk_output_file <- paste0(gsub("\\.csv","", cat_trend_output_file),"_",sprintf("%06d", i),".csv")
  chunk_output_file <- paste0(folder,"/",chunk_output_file)
  cat("\t",chunk_output_file,"\n")
  cat("\t\t",head(chunk),"...",tail(chunk),"\n")
  cat("\t\t",dim(df),"\n")
  fwrite(df, chunk_output_file)
  i <- i + 1
}
