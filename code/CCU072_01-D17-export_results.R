setwd("~/collab/CCU072_01")
suppressMessages(source("00_init.R"))

# all_results <- data.frame()
# for(analysis in table_names_df$analysis){
#   df <- fread(paste0(output_folder,"/ccu072_01_detailed_adjusted_subgroup_results_",analysis,".csv"), data.table = F)
#   all_results <- rbind(all_results, df)
#   remove(df)
# }
all_results <- fread(paste0(output_folder,"/ccu072_01_detailed_adjusted_subgroup_results_all.csv"), data.table = F)

cat("Inspect results\n")
#print(head(all_results),row.names=F)
cat(length(unique(all_results$dcode)),"\n")
print(table(all_results$analysis, all_results$metric))
print(table(all_results$analysis, all_results$group))
print(table(all_results$dcode, all_results$analysis))
print(head(all_results))
cat(dim(all_results),"\n")
cat("\n")

##cat("Remove recurrence and keep only recur_strokemi\n")
fig3_data <- all_results
##fig3_data <- fig3_data[fig3_data$metric!="recurrence",]
##cat(dim(fig3_data),"\n")
##cat("\n")

cat("Collapse group and group_item in single column\n")
#fig3_data$analysis_group_group_item <- paste0(fig3_data$analysis, "#", fig3_data$group, "#", fig3_data$group_item)
#fig3_data[,c("analysis","group","group_item")] <- NULL
fig3_data$group_group_item <- paste0(fig3_data$group, "#", fig3_data$group_item)
fig3_data[,c("group","group_item")] <- NULL
cat(dim(fig3_data),"\n")
cat("\n")

# cat("Collapse beta and SE into RR and 95%CI\n")
# fig3_data$rr_ci <- get95ci(fig3_data$beta, fig3_data$se, exp=T)
# fig3_data$rr_ci[is.na(fig3_data$se)] <- "Reference"
# fig3_data$rr_ci[grepl("Inf",fig3_data$rr_ci)] <- "-"
# fig3_data$rr_ci[exp(fig3_data$beta)>1000] <- "-"
# #fig3_data[,c("beta","se")] <- NULL
# cat(dim(fig3_data),"\n")
# cat("\n")

cat("Suppress small counts and round counts to nearest 5\n")
# Remove counts <10
fig3_data$raw_numerator[fig3_data$raw_numerator<min_n] <- NA
fig3_data$raw_denominator[fig3_data$raw_denominator<min_n] <- NA
# Round to nearest 5
fig3_data$raw_numerator <- round_n*round(fig3_data$raw_numerator/round_n)
fig3_data$raw_denominator <- round_n*round(fig3_data$raw_denominator/round_n)
print(head(fig3_data),row.names=F)
cat(dim(fig3_data),"\n")
cat("\n")

cat("Remove useless columns\n")
fig3_data[,c("dispersion","intercept","intercept_plus_beta","raw_denominator")] <- NULL
print(head(fig3_data),row.names=F)
cat(dim(fig3_data),"\n")
cat("\n")

cat("Transform long to wide using only analysis\n")
fig3_data <- fig3_data %>% pivot_wider(names_from=c(analysis), values_from=c(beta,se,beta_l95ci,beta_u95ci,pvalue,exp_intercept_plus_beta,raw_numerator,raw_proportion), names_glue="{analysis}_{.value}") %>% as.data.frame()
print(head(fig3_data[,1:10]),row.names=F)
cat(dim(fig3_data),"\n")
cat("\n")

cat("Collapse all values to same column & remove unnecessary columns\n")
fig3_data$unadjusted_betaANDunadjusted_seANDunadjusted_beta_l95ciANDunadjusted_beta_u95ciANDunadjusted_pvalueANDunadjusted_exp_intercept_plus_betaANDunadjusted_raw_numeratorANDunadjusted_raw_proportionANDfullyadjusted_betaANDfullyadjusted_seANDfullyadjusted_beta_l95ciANDfullyadjusted_beta_u95ciANDfullyadjusted_pvalueANDfullyadjusted_exp_intercept_plus_betaANDfullyadjusted_raw_numeratorANDfullyadjusted_raw_proportion <-
  paste0(fig3_data$unadjusted_beta, "#",
         fig3_data$unadjusted_se, "#", 
         fig3_data$unadjusted_beta_l95ci, "#", 
         fig3_data$unadjusted_beta_u95ci, "#", 
         fig3_data$unadjusted_pvalue, "#",
         fig3_data$unadjusted_exp_intercept_plus_beta, "#", 
         fig3_data$unadjusted_raw_numerator,"#",
         fig3_data$unadjusted_raw_proportion,"#",
         fig3_data$fullyadjusted_beta, "#",
         fig3_data$fullyadjusted_se, "#", 
         fig3_data$fullyadjusted_beta_l95ci, "#", 
         fig3_data$fullyadjusted_beta_u95ci, "#", 
         fig3_data$fullyadjusted_pvalue, "#",
         fig3_data$fullyadjusted_exp_intercept_plus_beta, "#", 
         fig3_data$fullyadjusted_raw_numerator,"#",
         fig3_data$fullyadjusted_raw_proportion)
fig3_data[,c("unadjusted_beta","unadjusted_se","unadjusted_beta_l95ci","unadjusted_beta_u95ci","unadjusted_pvalue","unadjusted_exp_intercept_plus_beta","unadjusted_raw_numerator","unadjusted_raw_proportion",
             "fullyadjusted_beta","fullyadjusted_se","fullyadjusted_beta_l95ci","fullyadjusted_beta_u95ci","fullyadjusted_pvalue","fullyadjusted_exp_intercept_plus_beta","fullyadjusted_raw_numerator","fullyadjusted_raw_proportion")] <- NULL
print(head(fig3_data),row.names=F)
cat(dim(fig3_data),"\n")
cat("\n")

cat("Transform long to wide\n")
fig3_data_wide <- fig3_data %>% pivot_wider(names_from=c(metric,year), values_from=c(unadjusted_betaANDunadjusted_seANDunadjusted_beta_l95ciANDunadjusted_beta_u95ciANDunadjusted_pvalueANDunadjusted_exp_intercept_plus_betaANDunadjusted_raw_numeratorANDunadjusted_raw_proportionANDfullyadjusted_betaANDfullyadjusted_seANDfullyadjusted_beta_l95ciANDfullyadjusted_beta_u95ciANDfullyadjusted_pvalueANDfullyadjusted_exp_intercept_plus_betaANDfullyadjusted_raw_numeratorANDfullyadjusted_raw_proportion), names_glue="{metric}#{year}#{.value}") %>% as.data.frame()   #dispersion #raw_proportion,beta,se
#print(head(fig3_data_wide[,1:10]),row.names=F)
cat(dim(fig3_data_wide),"\n")
cat(names(fig3_data_wide),"\n")
cat("\n")

cat("Export results to",output_folder,"\n")
# Split "x" vector in chunks of length "n"
#chunk2 <- function(x,n) split(x, cut(seq_along(x), n, labels = FALSE)) 
chunk2 <- function(x, n){
  split(x, ceiling(seq_along(x)/n))
}
chunks <- chunk2(1:nrow(fig3_data_wide), max_n_rows-2)
i <- 1
for(chunk in chunks){
  df <- fig3_data_wide[chunk,]
  chunk_output_file <- paste0(gsub("\\.csv","", output_file),"_",sprintf("%06d", i),".csv")
  chunk_output_file <- paste0(output_folder,"/",chunk_output_file)
  cat("\t",chunk_output_file,"\n")
  cat("\t\t",head(chunk),"...",tail(chunk),"\n")
  cat("\t\t",dim(df),"\n")
  fwrite(df, chunk_output_file)
  i <- i + 1
}


# Double check results
file_names <- list.files(output_folder, pattern=paste0(gsub("\\.csv","",output_file),"_0"),full.names = T)
fig3_data_all <- do.call(rbind,lapply(file_names,fread,check.names=FALSE))
fig3_data_all <- fig3_data_all %>% as.data.frame() %>% separate(group_group_item, c("group", "group_item"), sep = "#")
fig3_data_all <- fig3_data_all %>% pivot_longer(!c(dcode, group, group_item), 
                                                names_to = c("metric","year", ".value"),
                                                names_pattern = "(.+)\\#(.+)\\#(.+)")
fig3_data_all <- fig3_data_all %>% separate(unadjusted_betaANDunadjusted_seANDunadjusted_beta_l95ciANDunadjusted_beta_u95ciANDunadjusted_pvalueANDunadjusted_exp_intercept_plus_betaANDunadjusted_raw_numeratorANDunadjusted_raw_proportionANDfullyadjusted_betaANDfullyadjusted_seANDfullyadjusted_beta_l95ciANDfullyadjusted_beta_u95ciANDfullyadjusted_pvalueANDfullyadjusted_exp_intercept_plus_betaANDfullyadjusted_raw_numeratorANDfullyadjusted_raw_proportion,
                                            c("unadjusted_beta", "unadjusted_se", "unadjusted_beta_l95ci", "unadjusted_beta_u95ci", "unadjusted_pvalue", 
                                              "unadjusted_exp_intercept_plus_beta", "unadjusted_raw_numerator", "unadjusted_raw_proportion", 
                                              "fullyadjusted_beta", "fullyadjusted_se", "fullyadjusted_beta_l95ci", "fullyadjusted_beta_u95ci", "fullyadjusted_pvalue", 
                                              "fullyadjusted_exp_intercept_plus_beta", "fullyadjusted_raw_numerator", "fullyadjusted_raw_proportion"), sep = "#")
#View(fig3_data_all[row.names(fig3_data_all)%in%c(39496, 39497, 39498, 39499, 39500, 91276, 91277, 91278, 91279, 91280, 127901, 127902, 127903, 127904, 127905, 127916, 127917, 127918, 127919, 127920),])
names(fig3_data_all) <- gsub("unadjusted_","unadjusted#",names(fig3_data_all))
names(fig3_data_all) <- gsub("fullyadjusted_","fullyadjusted#",names(fig3_data_all))
fig3_data_all_final <- fig3_data_all %>% pivot_longer(!c(dcode, group, group_item, metric, year), 
                                                names_to = c("analysis",".value"),
                                                names_pattern = "(.+)\\#(.+)")
fig3_data_all_final2 <- fig3_data_all_final %>% mutate_at((vars(-dcode, -group, -group_item, -metric, -year, -analysis)),as.numeric)
summary(fig3_data_all_final2)