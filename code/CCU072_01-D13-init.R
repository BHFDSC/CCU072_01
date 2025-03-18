cat("Initialising\n")
options(width = 1000)
suppressMessages(library(tidyverse))
suppressMessages(library(dplyr))      # Data manipulation
suppressMessages(library(readr))      # Convert dataframe to delimited string
suppressMessages(library(data.table)) # Clean CSV output
suppressMessages(library(parallel))
cat("\n")

input_folder <- "/db-mnt/databricks/rstudio_collab/CCU072_01/Data"
output_folder <- "/db-mnt/databricks/rstudio_collab/CCU072_01/Output"
output_file <- "ccu072_01_detailed_adjusted_subgroup_results.csv"

n_cores <- 50 #detectCores() - 1

# Output limits
min_n <- 10
round_n <- 5
max_n_rows <- 1000

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

cat("Input files\n")
table_names_df <- data.frame(analysis=c("unadjusted","fullyadjusted"),
                             #table_name=c("Results_year_onecovadj_recur_ethnic_region.csv","Results_year_allcovadj_recur_ethnic_region.csv"))
                             table_name=c("Results_year_onecovadj_weight_age1_ethnicregion.csv","Results_year_allcovadj_weight_age1_ethnicregion.csv"))
table_names_df$analysis <- as.character(table_names_df$analysis)
table_names_df$table_name <- paste0(input_folder,"/",as.character(table_names_df$table_name))
print(table_names_df)
cat("\n")

# Returns pretty numbers
format.numbers <- function(number, digits=2, scientific=T, integer.digits=11, correct.small.number=F){
  debug <- F
  lower_numberthreshold <- 1/10^(digits)
  if(debug) cat(number,digits,lower_numberthreshold,"\n")
  if(debug) cat(number,"\t")
  number <- as.numeric(number)
  if(length(number)>0){
    if(!is.na(number) & !is.null(number)){
      # If N smaller than 0.001 [for digits = 3], 0.0001 [for digits = 4] etc, then use scientific notation
      if(debug) cat(number, nchar(as.character(round(number))), "\n")
      if((abs(number)<lower_numberthreshold | nchar(as.character(round(number)))>integer.digits) & scientific ==T){
        if(number==0 & correct.small.number){
          paste0("<",trimws(sprintf(paste0("%",integer.digits,".",digits,"e"),as.numeric(.Machine$double.xmin))))
        } else{
          trimws(sprintf(paste0("%",integer.digits,".",digits,"e"),as.numeric(number)))
        }
      } else{
        # Otherwise, just show first [n=digits] numbers after point
        if(number==0 & correct.small.number){
          paste0("<",trimws(sprintf(paste0("%",integer.digits,".",digits,"f"),as.numeric(number))))
        }
        trimws(sprintf(paste0("%",integer.digits,".",digits,"f"),as.numeric(number)))
      }
    } else{
      as.character(number)
    }
  } else {
    as.character(number)
  }
}
format.numbers <- Vectorize(format.numbers)

# Get pretty 95%CI from raw regression estimates, handling binary outcomes as well
# Uses z-test by default. In the future, implement t-test.
# Requires format.numbers()
get95ci <- function(beta, se, digits=2, exp=F, alpha=0.05, test="z", scientific=F){
  z <- qnorm(1-alpha/2)
  ul=beta + z*se
  ll=beta - z*se
  if(exp){
    beta <- exp(beta)
    ul <- exp(ul)
    ll <- exp(ll)
  }
  beta_formatted <- format.numbers(beta, digits, scientific=scientific)
  ul_formatted <- format.numbers(ul, digits, scientific=scientific)
  ll_formatted <- format.numbers(ll, digits, scientific=scientific)
  remove(beta, ul, ll)
  return(paste0(beta_formatted," (",ll_formatted," to ",ul_formatted,")"))  
}
get95ci <- Vectorize(get95ci)