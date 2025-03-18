cat("Initialising\n")

#library(openxlsx)
library(readxl)
library(tidyverse)
library(data.table)
library(gridExtra)
library(openxlsx)

homefolder <- "/home/elias/Downloads/000_IMPORTANT_DATA/000_large_data/Papers_Cam/COVID-19/CVD-COVID-UK_COVID-IMPACT/ccu072_01 burden of cardiovascular diseases/0_analysis/"
inputfolder <- paste0(homefolder,"/input")
outputfolder <- paste0(homefolder,"/output")


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