suppressMessages(source("0_init.R"))

# Add ICD-10 codes to initial version
dcode_data <- fread(paste0(outputfolder,"/ccu072_01_dcode_name_group_DONOTDELETE_2024-06-05.csv"))
dcode_data$initial_dcode_name <- dcode_data$dcode_name
dcode_data[,c("dcode_name","dcode_group")] <- NULL
dcode_icd10 <- fread(paste0(outputfolder,"/ccu072_01_dcode_icd10.csv"))
dcode_icd10$initial_icd10_code_name <- paste0(dcode_icd10$icd10_code," [",dcode_icd10$icd10_code_name,"]")
dcode_icd10 <- dcode_icd10 %>% group_by(dcode) %>% summarise(initial_icd10_code_name = paste(sort(initial_icd10_code_name), collapse = "; ")) %>% as.data.frame()
initial_data <- merge(dcode_data, dcode_icd10)
remove(dcode_icd10, dcode_data)
# Make new version more compact
new_data <- read.xlsx(paste0(outputfolder,"/ccu072_01_dcode_icd10_WW_consolidated_formatted_consensus_2024-07-29.xlsx"))
new_data$final_dcode <- new_data$dcode_final
new_data$final_dcode_name <- new_data$dcode_name_final
new_data$final_dcode_group <- new_data$dcode_group_final
new_data <- new_data[new_data$dcode_name_final!="[remove]",]
new_data$final_icd10_code_name <- paste0(new_data$icd10_code, " [",new_data$icd10_code_name,"]")
new_data$final_dcode_source <- ""
for(final_dcode in unique(new_data$final_dcode)){
  new_data$final_dcode_source[new_data$final_dcode==final_dcode] <- paste(sort(unique(new_data$dcode[new_data$final_dcode==final_dcode])), collapse = "; ")
}
new_data <- new_data %>% group_by(final_dcode, final_dcode_name, final_dcode_source, final_dcode_group) %>% summarise(final_icd10_code_name = paste(sort(final_icd10_code_name), collapse = "; ")) %>% as.data.frame()
# Merge initial and final data
merged_data <- merge(initial_data, new_data, all = T, by.x = "dcode", by.y="final_dcode")
merged_data$dcode_added_removed_kept <- ifelse(is.na(merged_data$initial_dcode_name), "Added", 
                                         ifelse(is.na(merged_data$final_dcode_name), "Removed", "Kept"))
merged_data$name_change <- ifelse(merged_data$initial_dcode_name!=merged_data$final_dcode_name, "Name change", NA)
merged_data$icd10_change <- ifelse(merged_data$initial_icd10_code_name!=merged_data$final_icd10_code_name, "ICD-10 change", NA)
merged_data <- merged_data %>% relocate(final_dcode_source, .after = last_col()) %>% as.data.frame()
write.xlsx(merged_data, paste0(outputfolder,"/ccu072_01_dcode_icd10_WW_consolidated_formatted_consensus_2024-07-29_differences_2024-08-21.xlsx"))

# Load dcode-ICD10 mappings
# I ended up using a file I had obtained at the beginning of our project from Centers for Medicare & Medicaid Services ('icd10cm_codes_2018.txt') from https://www.cms.gov/medicare/coding-billing/icd-10-codes/2018-icd-10-cm-gem (specifically, '2018 Code Descriptions in Tabular Order (ZIP)': https://www.cms.gov/medicare/coding/icd10/downloads/2018-icd-10-code-descriptions.zip)
# For backup: info from Elena: https://www.nber.org/research/data/icd-9-cm-and-icd-10-cm-and-icd-10-pcs-crosswalk-or-general-equivalence-mappings
dcode_icd10 <- fread(paste0(outputfolder,"/ccu072_01_dcode_icd10.csv"))
dcode_icd10$icd10_code_nchar <- nchar(dcode_icd10$icd10_code)
dcode_icd10$mapped_to_dcode <- 1
summary(dcode_icd10$icd10_code_nchar)
# Add dcode names and groups
dcode_name_group <- fread(paste0(outputfolder,"/ccu072_01_dcode_name_group.csv"))
dcode_icd10 <- merge(dcode_name_group, dcode_icd10)
# Collapse dcodes and names, transforming the dataset into ICD10-dcode mapping (rather than the reverse)
dcode_icd10 <- dcode_icd10[,c("dcode","dcode_name","icd10_code","icd10_code_name")]
dcode_icd10$dcode_and_dcode_name <- paste0(dcode_icd10$dcode, " (", dcode_icd10$dcode_name, ")")
icd10_dcode <- dcode_icd10 %>%
  group_by(icd10_code) %>%
  summarize(dcode_and_dcode_name = paste(unique(dcode_and_dcode_name), collapse = "; "))
# Re-add ICD10 names
dcode_icd10_compact <- dcode_icd10[,c("icd10_code","icd10_code_name")]
dcode_icd10_compact <- dcode_icd10_compact[!duplicated(dcode_icd10_compact),]
icd10_dcode <- merge(icd10_dcode, dcode_icd10_compact)
icd10_dcode <- icd10_dcode[,c("icd10_code","icd10_code_name","dcode_and_dcode_name")]
icd10_dcode$icd10_code_mapped <- 1
remove(dcode_icd10_compact)

# Load full list of ICD10
all_icd10 <- read.fwf(paste0(inputfolder,"/icd10cm_codes_2018.txt"), widths=c(8,10000))
names(all_icd10) <- c("icd10_code","icd10_code_name")
all_icd10$icd10_code <- trimws(all_icd10$icd10_code)
# Select ICD10 codes in the I chapter
i_icd10 <- all_icd10[grepl("^I",all_icd10$icd10_code),]
i_icd10$icd10_code_nchar <- nchar(i_icd10$icd10_code)
i_icd10 <- i_icd10[i_icd10$icd10_code_nchar<=4,]
summary(i_icd10$icd10_code_nchar)
i_icd10$icd10_code_nchar <- NULL
# Merge with ICD10 codes mapped to dcodes
i_icd10_checks <- merge(i_icd10, icd10_dcode[,c("icd10_code","dcode_and_dcode_name","icd10_code_mapped")], all.x = T)
i_icd10_checks$icd10_code_mapped[is.na(i_icd10_checks$icd10_code_mapped)] <- 0
table(i_icd10_checks$icd10_code_mapped)

# Store final file
fwrite(i_icd10_checks, paste0(outputfolder,"/ccu072_01_i_icd10_checks.csv"))