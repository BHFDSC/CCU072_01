suppressMessages(source("0_init.R"))

# Load version after checking with Chris Tomlinson (31st May)
latest_data <- read.xlsx(paste0(outputfolder,"/ccu072_01_dcode_icd10_WW_consolidated_formatted_consensus_2024-07-29_differences_2024-08-21_newgroups_2025-01-24.xlsx"))
latest_data$dcode_name <- latest_data$final_dcode_name
latest_data$dcode_group <- latest_data$final_dcode_group
latest_data$icd10_code_name <- latest_data$final_icd10_code_name
latest_data <- latest_data[latest_data$dcode_added_removed_kept!="Removed",c("dcode","dcode_name","dcode_group","icd10_code_name")]

# Names and groups
dcode_name <- latest_data[,c("dcode","dcode_name","dcode_group")]
dcode_name <- dcode_name[with(dcode_name, order(dcode_group, dcode_name)),]
fwrite(dcode_name, paste0(outputfolder,"/ccu072_01_dcode_name_group.csv"))
ordered_dcodes <- dcode_name$dcode

# Expand & export ICD10
dcode_icd10 <- latest_data[,c("dcode","icd10_code_name")]
dcode_icd10 <- dcode_icd10 %>% separate_longer_delim(cols = icd10_code_name, delim = ";")
dcode_icd10$icd10_code_name <- trimws(dcode_icd10$icd10_code_name)
dcode_icd10 <- dcode_icd10 %>% separate(icd10_code_name, into = c("icd10_code", "icd10_code_name"), sep = "\\s", extra = "merge")
dcode_icd10$icd10_code_name <- gsub("\\[|\\]","", dcode_icd10$icd10_code_name)
dcode_icd10 <- dcode_icd10[!duplicated(dcode_icd10),]
dcode_icd10$dcode <- ordered(dcode_icd10$dcode, levels=ordered_dcodes)
dcode_icd10 <- dcode_icd10[with(dcode_icd10, order(dcode_icd10$dcode, dcode_icd10$icd10_code)),]
fwrite(dcode_icd10, paste0(outputfolder,"/ccu072_01_dcode_icd10.csv"))

# Add dcode names/groups to ICD-10 codes for Tom/Fiona/paper and Supplementary Tables
dcode_icd10_name <- merge(dcode_name, dcode_icd10)
names(dcode_icd10_name)[names(dcode_icd10_name) == "dcode_name"] <- "diagnosis"
names(dcode_icd10_name)[names(dcode_icd10_name) == "dcode_group"] <- "group"
fwrite(dcode_icd10_name, paste0(outputfolder,"/ccu072_01_dcode_icd10_name.csv"))
dcode_icd10_name_suppltable <- dcode_icd10_name
dcode_icd10_name_suppltable$dcode <- NULL
dcode_icd10_name_suppltable <- dcode_icd10_name_suppltable[with(dcode_icd10_name_suppltable, order(dcode_icd10_name_suppltable$diagnosis, dcode_icd10_name_suppltable$icd10_code)),]
fwrite(dcode_icd10_name_suppltable, paste0(outputfolder,"/ccu072_01_dcode_icd10_name_suppltable.csv"))

############### MAPPING TO SNOMED CODES ###############

# Add SNOMED codes (Elias/Luigi)
dcode_snomed_mar9 <- read_xlsx("/home/elias/Downloads/000_IMPORTANT_DATA/000_large_data/Papers_Cam/COVID-19/CVD-COVID-UK_COVID-IMPACT/ccu072_01 burden of cardiovascular diseases/phenotyping/4_snomed/from_luigi_elias/2024-03-09_phecode_snomed_mapping_EA_LBmerged_difference_formatted_final_WW.xlsx")
dcode_snomed_mar9 <- dcode_snomed_mar9[dcode_snomed_mar9$final_selection==1,]
dcode_snomed_mar9$dcode <- dcode_snomed_mar9$phecode
dcode_snomed_mar9$snomed_code <- as.character(as.numeric(dcode_snomed_mar9$conceptId))
dcode_snomed_mar9$snomed_code_name <- dcode_snomed_mar9$term
dcode_snomed_mar9$dcode <- ordered(dcode_snomed_mar9$dcode, levels=ordered_dcodes)
dcode_snomed_mar9 <- dcode_snomed_mar9[with(dcode_snomed_mar9, order(dcode_snomed_mar9$dcode, dcode_snomed_mar9$snomed_code)),]
dcode_snomed_mar9 <- dcode_snomed_mar9[,c("dcode","snomed_code","snomed_code_name")]

# Add SNOMED codes (Elias/Luigi)
dcode_snomed_jul8 <- read_xlsx("/home/elias/Downloads/000_IMPORTANT_DATA/000_large_data/Papers_Cam/COVID-19/CVD-COVID-UK_COVID-IMPACT/ccu072_01 burden of cardiovascular diseases/phenotyping/4_snomed/from_luigi_elias/2024-07-08_dcode_snomed_mapping_EA_LBmerged_difference_formatted_final_WW.xlsx")
dcode_snomed_jul8 <- dcode_snomed_jul8[dcode_snomed_jul8$final_selection==1,]
dcode_snomed_jul8$dcode <- dcode_snomed_jul8$Dcode
dcode_snomed_jul8$snomed_code <- as.character(as.numeric(dcode_snomed_jul8$conceptId))
dcode_snomed_jul8$snomed_code_name <- dcode_snomed_jul8$term
dcode_snomed_jul8$dcode <- ordered(dcode_snomed_jul8$dcode, levels=ordered_dcodes)
dcode_snomed_jul8 <- dcode_snomed_jul8[with(dcode_snomed_jul8, order(dcode_snomed_jul8$dcode, dcode_snomed_jul8$snomed_code)),]
dcode_snomed_jul8 <- dcode_snomed_jul8[,c("dcode","snomed_code","snomed_code_name")]

# Remove decodes that have been updated
dcode_snomed_mar9_kept <- dcode_snomed_mar9[!dcode_snomed_mar9$dcode%in%unique(dcode_snomed_jul8$dcode), ]

# Collate all d-codes (Elias/Luigi)
dcode_snomed_eliasluigi <- rbind(dcode_snomed_mar9_kept, dcode_snomed_jul8)

# Add collapsed decode after emails from Will (24/7/2024 and 14/8/2024)
# X442_8	Aneurysm and dissection of other artery: from X442_4; X442_8; XREMAPI729
# XCMP		Cardiomyopathy: from X425_1; X425_8; XREMAPI515
dcode_snomed_aug27 <- read_xlsx("/home/elias/Downloads/000_IMPORTANT_DATA/000_large_data/Papers_Cam/COVID-19/CVD-COVID-UK_COVID-IMPACT/ccu072_01 burden of cardiovascular diseases/phenotyping/4_snomed/from_luigi_elias/2024-08-27_dcode_snomed_mapping_after_WW_comments.xlsx")
collapsed_dcodes <- unique(dcode_snomed_aug27$old_Dcode)
dcode_snomed_aug27 <- dcode_snomed_aug27[dcode_snomed_aug27$final_selection==1,]
dcode_snomed_aug27$dcode <- dcode_snomed_aug27$Dcode
dcode_snomed_aug27$snomed_code <- as.character(as.numeric(dcode_snomed_aug27$conceptId))
dcode_snomed_aug27$snomed_code_name <- dcode_snomed_aug27$term
dcode_snomed_aug27$dcode <- ordered(dcode_snomed_aug27$dcode, levels=ordered_dcodes)
dcode_snomed_aug27 <- dcode_snomed_aug27[with(dcode_snomed_aug27, order(dcode_snomed_aug27$dcode, dcode_snomed_aug27$snomed_code)),]
dcode_snomed_aug27 <- dcode_snomed_aug27[,c("dcode","snomed_code","snomed_code_name")]

# Collate all d-codes (Elias/Luigi)
dcode_snomed_eliasluigi <- dcode_snomed_eliasluigi[!dcode_snomed_eliasluigi$dcode%in%collapsed_dcodes,]
dcode_snomed_eliasluigi <- rbind(dcode_snomed_eliasluigi, dcode_snomed_aug27)
dcode_snomed_eliasluigi <- dcode_snomed_eliasluigi[!duplicated(dcode_snomed_eliasluigi),]

# Add SNOMED codes (Elena)
elena_mapping <- fread("/home/elias/Downloads/000_IMPORTANT_DATA/000_large_data/Papers_Cam/COVID-19/CVD-COVID-UK_COVID-IMPACT/ccu072_01 burden of cardiovascular diseases/phenotyping/4_snomed/from_elena/elena_phenotypes_dcode_mapping.csv", data.table = F)
dcode_snomed_elena <- data.frame()
for(elena_phenotype in elena_mapping$elena_phenotype){
  #elena_phenotype <- elena_mapping$elena_phenotype[20]
  elena_data <- read_xlsx("/home/elias/Downloads/000_IMPORTANT_DATA/000_large_data/Papers_Cam/COVID-19/CVD-COVID-UK_COVID-IMPACT/ccu072_01 burden of cardiovascular diseases/phenotyping/4_snomed/from_elena/CCU018_02_phenotyping_codelists_to_use_in CCU076_EA_LB_WW.xlsx", sheet = elena_phenotype)
  elena_data <- elena_data[elena_data$terminology=="SNOMED",]
  elena_data <- elena_data[elena_data$final_selection==1,]
  elena_data$dcode <- elena_mapping$dcode[elena_mapping$elena_phenotype==elena_phenotype]
  elena_data$snomed_code <- as.character(elena_data$code)
  elena_data$snomed_code_name <- elena_data$term
  elena_data <- elena_data[,c("dcode","snomed_code","snomed_code_name")]
  dcode_snomed_elena <- rbind(dcode_snomed_elena, elena_data)
  remove(elena_data)
}

# Collate all d-codes (Elias/Luigi + Elena)
dcode_snomed <- rbind(dcode_snomed_eliasluigi, dcode_snomed_elena)
dcode_snomed <- dcode_snomed[!duplicated(dcode_snomed),]

# Ensure we only keep the dcodes we need
dcode_snomed <- dcode_snomed[dcode_snomed$dcode%in%dcode_name$dcode,]
dcode_snomed$dcode <- ordered(dcode_snomed$dcode, levels=ordered_dcodes)
dcode_snomed <- dcode_snomed[with(dcode_snomed, order(dcode_snomed$dcode, dcode_snomed$snomed_code)),]
length(unique(dcode_snomed$dcode))
print(dcode_name[!dcode_name$dcode%in%unique(dcode_snomed$dcode),])
fwrite(dcode_snomed, paste0(outputfolder,"/ccu072_01_dcode_snomed.csv"))

# Add dcode names/groups to ICD-10 codes for Tom/Fiona/paper and Supplementary Tables
dcode_snomed_name <- merge(dcode_name, dcode_snomed)
names(dcode_snomed_name)[names(dcode_snomed_name) == "dcode_name"] <- "diagnosis"
names(dcode_snomed_name)[names(dcode_snomed_name) == "dcode_group"] <- "group"
fwrite(dcode_snomed_name, paste0(outputfolder,"/ccu072_01_dcode_snomed_name.csv"))
dcode_snomed_name$dcode <- NULL
dcode_snomed_name$snomed_code <- as.character(dcode_snomed_name$snomed_code)
dcode_snomed_name <- dcode_snomed_name[with(dcode_snomed_name, order(dcode_snomed_name$diagnosis, dcode_snomed_name$snomed_code)),]
fwrite(dcode_snomed_name, paste0(outputfolder,"/ccu072_01_dcode_snomed_name_suppltable.csv"))


############### MAPPING TO EPCC CODES ###############

# first run these scripts:
# i) /home/elias/Downloads/000_IMPORTANT_DATA/000_large_data/Papers_Cam/COVID-19/CVD-COVID-UK_COVID-IMPACT/ccu072_01 burden of cardiovascular diseases/phenotyping/5_specialist_registers/1_epcc_icd10_mapping.R
# ii) /home/elias/Downloads/000_IMPORTANT_DATA/000_large_data/Papers_Cam/COVID-19/CVD-COVID-UK_COVID-IMPACT/ccu072_01 burden of cardiovascular diseases/phenotyping/5_specialist_registers/2_epcc_merge_codes_for_final_selection.R

# 0. Prepare EPCC codes from the NCHDA registry (https://www.aepc.org/european-paediatric-cardiac-coding) for manual curation by mapping them to d-codes via ICD-10
epcc_icd10 <- fread("/home/elias/Downloads/000_IMPORTANT_DATA/000_large_data/Papers_Cam/COVID-19/CVD-COVID-UK_COVID-IMPACT/ccu072_01 burden of cardiovascular diseases/phenotyping/5_specialist_registers/epcc_icd10_mapping.csv", data.table = F)
dcode_epcc <- merge(dcode_icd10, epcc_icd10, all.x = T)
dcode_epcc <- dcode_epcc[!is.na(dcode_epcc$epcc_code),]
dcode_epcc <- dcode_epcc[,c("dcode","epcc_code","epcc_code_name")]
dcode_epcc <- dcode_epcc[!duplicated(dcode_epcc),]
dcode_epcc$dcode <- ordered(dcode_epcc$dcode, levels=ordered_dcodes)
dcode_epcc <- dcode_epcc[with(dcode_epcc, order(dcode_epcc$dcode, dcode_epcc$epcc_code)),]
#dcode_epcc <- merge(dcode_name, dcode_epcc)
##fwrite(dcode_epcc, paste0(outputfolder,"/ccu072_01_dcode_epcc_all_automatic.csv"))

# 1. Prepare EPCC table for manual curation: add ICD-10 codes to dcode-EPCC mapping table
# 1.1. Create compact ICD-10 table
dcode_icd10_name$icd10_code_full <- paste0(dcode_icd10_name$icd10_code," (",dcode_icd10_name$icd10_code_name,")")
dcode_icd10_name_compact <- data.frame()
for(dcode in unique(dcode_icd10_name$dcode)){
  #dcode <- unique(dcode_icd10_name$dcode)[1]
  dcode_icd10_name_compact <- rbind(dcode_icd10_name_compact, data.frame(dcode=dcode,
                                                                         dcode_name=unique(dcode_icd10_name$diagnosis[dcode_icd10_name$dcode==dcode]),
                                                                         dcode_icd10_full=paste(sort(dcode_icd10_name$icd10_code_full[dcode_icd10_name$dcode==dcode]),collapse = "; ")))
}

# 1.2. Merge dcode-EPCC with compact ICD-10 table
dcode_epcc_icd10 <- merge(dcode_epcc, dcode_icd10_name_compact)

# 2. Add filter column, sort column order and save
dcode_epcc_icd10$selection <- NA
dcode_epcc_icd10 <- dcode_epcc_icd10[, c("dcode","dcode_name","dcode_icd10_full","epcc_code","epcc_code_name","selection")]
##writexl::write_xlsx(dcode_epcc_icd10, paste0("/home/elias/Downloads/000_IMPORTANT_DATA/000_large_data/Papers_Cam/COVID-19/CVD-COVID-UK_COVID-IMPACT/ccu072_01 burden of cardiovascular diseases/phenotyping/5_specialist_registers/dcode_epcc_manualselection_EA.xlsx"))
##writexl::write_xlsx(dcode_epcc_icd10, paste0("/home/elias/Downloads/000_IMPORTANT_DATA/000_large_data/Papers_Cam/COVID-19/CVD-COVID-UK_COVID-IMPACT/ccu072_01 burden of cardiovascular diseases/phenotyping/5_specialist_registers/dcode_epcc_manualselection_LB.xlsx"))

# 3. Load EPCC mapping file post manual curation
dcode_epcc_post_curation <- read_xlsx("/home/elias/Downloads/000_IMPORTANT_DATA/000_large_data/Papers_Cam/COVID-19/CVD-COVID-UK_COVID-IMPACT/ccu072_01 burden of cardiovascular diseases/phenotyping/5_specialist_registers/dcode_epcc_manualselection_EA_LBmerged_final.xlsx")
dcode_epcc_post_curation <- dcode_epcc_post_curation[dcode_epcc_post_curation$final==1,]
dcode_epcc_post_curation <- dcode_epcc_post_curation[,c("dcode","epcc_code","epcc_code_name")]
dcode_epcc_post_curation <- dcode_epcc_post_curation[with(dcode_epcc_post_curation, order(dcode_epcc_post_curation$dcode, dcode_epcc_post_curation$epcc_code)),]
fwrite(dcode_epcc_post_curation, paste0(outputfolder,"/ccu072_01_dcode_epcc.csv"))

# Prepare for Supplementary Tables
dcode_epcc_post_curation_suppltable <- merge(dcode_name, dcode_epcc_post_curation)

dcode_epcc_post_curation_suppltable$dcode <- NULL
dcode_epcc_post_curation_suppltable <- dcode_epcc_post_curation_suppltable[with(dcode_epcc_post_curation_suppltable,
                                                                                order(dcode_epcc_post_curation_suppltable$diagnosis, dcode_epcc_post_curation_suppltable$epcc_code)),]
fwrite(dcode_epcc_post_curation_suppltable, paste0(outputfolder,"/ccu072_01_dcode_epcc_name_suppltable.csv"))