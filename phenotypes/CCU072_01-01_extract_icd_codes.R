folder <- "/home/elias/Downloads/000_IMPORTANT_DATA/000_large_data/Papers_Cam/COVID-19/CVD-COVID/burden of cardiovascular diseases/analysis"
setwd(folder)

library(utils)
library(data.table)

# Source: https://www.cms.gov/medicare/coding/icd10/downloads/2018-icd-10-code-descriptions.zip from https://www.cms.gov/Medicare/Coding/ICD10/2018-ICD-10-CM-and-GEMs

data <- read.fwf(paste0(folder,"/icd10cm_codes_2018.txt"), widths = c(8, 100000))
names(data) <- c("code","name")

# Select CVD data
cvd_data <- data[startsWith(data$code,"I"),]

# Store codes for CVD data
fwrite(cvd_data, file=paste0(folder,"/cvd_icd10_codes.csv"))