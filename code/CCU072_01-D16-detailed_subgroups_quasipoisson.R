setwd("~/collab/CCU072_01")
suppressMessages(source("00_init.R"))

data <- fread(table_names_df$table_name[table_names_df$analysis=="unadjusted"], data.table = F)
max_year <- max(data$year)
groups <- sort(unique(data$group))
for(group in groups){
  #group <- groups[1]
  df <- data[data$metric=="prev" & data$group==group,]
  summarised_df <- df %>% group_by(value) %>% summarise(max_denominator=max(denominator)) %>% as.data.frame()
  summarised_df$abs_diff_from_median <- abs(summarised_df$max_denominator-median(summarised_df$max_denominator))
  cat(group,  
      "Max:",summarised_df$value[summarised_df$max_denominator==max(summarised_df$max_denominator)],
      max(summarised_df$max_denominator),
      "Median",summarised_df$value[summarised_df$abs_diff_from_median==min(summarised_df$abs_diff_from_median)],
      median(summarised_df$max_denominator),
      "\n")
  #print(summarised_df)
  remove(df, summarised_df)
}
df <- data[data$metric=="prev" & data$group=="region",]
df %>% group_by(value) %>% summarise(max_denominator=max(denominator)) %>% arrange(desc(max_denominator)) %>% as.data.frame()

remove(data)

# Set reference level for regression analyses
get_reference_level <- function(group){
  return(switch(group, 
                "age"="50_59",  # "0_19"
                "comorbidity"="3_4", #0
                "deprivation"="5_6", # 9_10_least_deprived
                "ethnicity"="British", #"White",
                "region"="Mid and South Essex",# "North West London", #"Greater Manchester",#"London",
                "sex"="Male"))
}

files <- list.files(paste0(input_folder,"/metric_dcode_files"), full.names = T)
#files <- files[grepl("prev_X420_1",files)]
#files <- files[grepl("inc_XICVT",files)]
#files <- files[grepl("fully_adjusted.csv$",files)]
#files <- files[grepl("unadjusted.csv$",files)]
#files <- files[1:2]

# Fit model function
fit_model <- function(d, formula, predictors){
  dcode <- unique(d$dcode)
  metric <- unique(d$metric)
  
  tryCatch({
    #m <- glm.nb(as.formula(formula), data=d, control=glm.control(maxit=1000))
    m <- glm(as.formula(formula), data=d, control=glm.control(maxit=1000), family = quasipoisson(link = "log"))  # family = poisson(link = "log")
    print(summary(m))
    m_results <- as.data.frame(summary(m)$coef)
    m_results$var <- row.names(m_results)
    ci_results <- as.data.frame(confint.default(m))
    ci_results$var <- row.names(ci_results)
    m_results <- merge(m_results, ci_results)
    m_results$var <- gsub("value","",m_results$var)
    print(m_results, row.names=F)
    cat("\n")
    
    fit_results <- data.frame()
    for(predictor in predictors){
      #predictors <- groups; predictor <- predictors[4]
      
      cat("\t\t\tSelect relevant results\n")
      #cat("\t\t",paste(c("(Intercept)",unique(as.character(d$value))), collapse=", "),"\n")
      predictor_m_results <- m_results
      predictor_m_results$var <- gsub(predictor,"",predictor_m_results$var)
      predictor_m_results <- predictor_m_results[predictor_m_results$var%in%c("(Intercept)",unique(as.character(d[[predictor]]))),]
      print(predictor_m_results, row.names=F)
      cat("\n")
      intercept <- predictor_m_results$Estimate[predictor_m_results$var=="(Intercept)"]  
      
      for(group_item in sort(unique(d[[predictor]]))){
        #group_item <- sort(unique(d[[predictor]]))[2]
        if(group_item%in%predictor_m_results$var){
          beta <- predictor_m_results$Estimate[predictor_m_results$var==group_item]
          se <- predictor_m_results$`Std. Error`[predictor_m_results$var==group_item]
          pvalue <- predictor_m_results$`Pr(>|t|)`[predictor_m_results$var==group_item]
          beta_l95ci <- predictor_m_results$`2.5 %`[predictor_m_results$var==group_item]
          beta_u95ci <- predictor_m_results$`97.5 %`[predictor_m_results$var==group_item]
        } else{
          beta <- 0
          se <- NA
          pvalue <- NA
          beta_l95ci <- NA
          beta_u95ci <- NA
        }
        group_item_d <- d[d[[predictor]]==group_item & d$year==0,]
        group_item_d <- group_item_d %>% summarise(numerator=sum(numerator,na.rm=T), denominator=sum(denominator,na.rm=T)) %>% as.data.frame() # dplyr::group_by(year) %>% 
        group_item_d$proportion <- group_item_d$numerator / group_item_d$denominator
        #print(group_item_d, row.names=F)
        fit_results <- rbind(fit_results,
                                      data.frame(dcode=dcode,
                                                 metric=metric,
                                                 group=predictor,
                                                 group_item=group_item,
                                                 dispersion=summary(m)$dispersion,
                                                 intercept=intercept,
                                                 beta=beta,
                                                 se=se,
                                                 beta_l95ci=beta_l95ci,
                                                 beta_u95ci=beta_u95ci,
                                                 pvalue=pvalue,
                                                 intercept_plus_beta=intercept+beta,
                                                 exp_intercept_plus_beta=exp(intercept+beta),
                                                 raw_numerator=group_item_d$numerator,
                                                 raw_denominator=group_item_d$denominator,
                                                 raw_proportion=group_item_d$proportion))
      }
    }
    
    return(fit_results)
  },
  error = function(cond) {},
  warning = function(cond) {},
  finally = {})
}


parallel_analysis <- function(file){
##all_results <- data.frame()
##for(file in files[1]){
  #file <- files[1]
  analysis <- unlist(strsplit(basename(file), "_"))
  analysis <- analysis[length(analysis)]
  analysis <- gsub("\\.csv","",analysis)
  data <- fread(file, data.table = F)
  data$log_denominator <- log(data$denominator)
  data$log_denominator[is.infinite(data$log_denominator)] <- NA
  data <- data[!is.na(data$log_denominator),]
  
  dcode <- unique(data$dcode)
  metric <- unique(data$metric)
  years <- sort(unique(data$year))
    
  cat("Metric:",metric,"\tDcode:", dcode,"\tAnalysis:", analysis, "\n")
  
  metric_dcode_results <- data.frame()
  for(year in years){
    #year <- max(years)
    cat("\tyear:",year,"\n")
    
    if(analysis=="unadjusted"){
      for(group in groups){
        #group <- groups[1]
        #cat("\t\t\t",group,"\n")
        cat("\t\tPredictor:",group,"\n")
        d <- data[data$group==group,]
        d$year <- d$year - year #Rescale so that reference 
        cat("\t\tDimensions:",dim(d),"\n")
        
        d[[group]] <- relevel(as.factor(d$value), get_reference_level(group))  # Rescale do that the reference for the current subgroup is the chosen one
        
        formula <- paste0("numerator ~ year + ",group," + offset(log_denominator)")
        cat("\t\tFormula:",formula,"\n")
        res <- fit_model(d, formula, group)
        res$analysis <- analysis
        res$year <- year
        metric_dcode_results <- rbind(metric_dcode_results, res)
        remove(d, res)
      }  
    } else if(analysis=="fullyadjusted"){
      d <- data
      d$year <- d$year - year #Rescale so that reference 
      cat("\t\tDimensions:",dim(d),"\n")
      
      for(group in groups){
        d[[group]] <- relevel(as.factor(d[[group]]), get_reference_level(group)) # Rescale so that reference is 50_59
      }
      
      formula <- paste0("numerator ~ year + ",
                        paste(groups, collapse=" + "),
                        " + offset(log_denominator)")
      cat("\t\tFormula:",formula,"\n")
      res <- fit_model(d, formula, groups)
      res$analysis <- analysis
      res$year <- year
      metric_dcode_results <- rbind(metric_dcode_results, res)
      remove(d, res)
    }
  }
  
  ##print(head(metric_dcode_results))
  ##all_results <- rbind(all_results, metric_dcode_results)
  return(metric_dcode_results)
}

cat("Performing",length(files),"analyses over",n_cores,"cores\n")
cl <- makeCluster(min(n_cores, length(files)),
                  methods=FALSE, setup_strategy = "sequential") # outfile=paste0(output_folder,"/analysis_results.log")
suppressMessages(clusterEvalQ(cl, require(data.table)))
suppressMessages(clusterEvalQ(cl, require(tidyverse)))
suppressMessages(clusterExport(cl=cl, ls(), envir=environment()))
suppressMessages(all_results <- parLapply(cl, files, parallel_analysis))
all_results <- do.call("rbind",all_results)
stopCluster(cl)
rm(cl)

cat("Inspect results\n")
#print(head(all_results),row.names=F)
cat(dim(all_results),"\n")
cat(length(unique(all_results$dcode)),"\n")
print(table(all_results$analysis, all_results$metric))
print(table(all_results$analysis, all_results$group))
print(table(all_results$dcode, all_results$analysis))
cat("\n")
print(head(all_results))

fwrite(all_results, paste0(output_folder,"/ccu072_01_detailed_adjusted_subgroup_results_all.csv"))