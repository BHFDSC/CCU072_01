suppressMessages(source("0_init.R"))
suppressMessages(library(RColorBrewer))
suppressMessages(library(cowplot))
suppressMessages(library(ggrepel))
suppressMessages(library(scales))
suppressMessages(library(pals))
suppressMessages(library(sf))
options(scipen=10000)

# Set date groups
date_group_names <- c("Pre-vaccine\nCOVID-19\nperiod",
                      "Beginning of\nvaccination\nprogramme",
                      "After\nCOVID-19\nrestrictions")
date_group_dates <- as.Date(c("2020-12-08", # https://www.bmj.com/content/372/bmj.n421
                              "2022-04-01"))   # https://www.gov.uk/government/news/prime-minister-sets-out-plan-for-living-with-covid
# It was 19/7/2021 end restrictions according to Fionna

# Set values for metrics
prevalence_value <- "prev"
incidence_value <- "inc"
fatality_value <- "fatal"
recurrence_value <- "recur"

# Set labels for metrics
prevalence_label <- "Prevalence"
incidence_label <- "Incidence"
fatality_label <- "30-day case fatality"
recurrence_label <- "Post-diagnosis 30-day to 1-year rate of MI or ischaemic stroke"

# Set units for metrics
prevalence_unit <- "Percentage"
incidence_unit <- "Events per 100,000 person-years"
fatality_unit <- prevalence_unit
recurrence_unit <- incidence_unit

# Comorbidity/region label
comorbidity_label <- "Long-term\nconditions"
region_label <- "Integrated Care Board"

# Year for subgroup analysis
group_year <- 2024

# Alpha range
alpha_range_subgroups <- c(0.01, 0.7)
alpha_range_trends <- c(0.1, 1)

# Dcode names and groups
dcode_names_groups_data <- fread(paste0(outputfolder,"/ccu072_01_dcode_name_group.csv"), data.table = F)

# Load files
file_names <- unzip(paste0(inputfolder,"/ccu072_01_fig1_notcollapsed_results.zip"), junkpaths = TRUE, exdir = tempdir())
fig1_data <- do.call(rbind,lapply(file_names,read.csv))
fig1_data <- merge(fig1_data, dcode_names_groups_data)
fig1_data <- fig1_data %>% pivot_longer(!c(group, value, dcode, dcode_name, dcode_group, date), 
                                        names_to = c(".value","metric"),
                                        names_pattern = "(.+)_(.+)") %>% as.data.frame()
fig1_data$metric <- gsub("\\.", " ", fig1_data$metric)
fig1_data$metric[fig1_data$metric=="Post event 1 year incidence"] <- recurrence_label
fig1_data$metric[fig1_data$metric=="30 day case fatality"] <- fatality_label
fig1_data$date <- as.Date(fig1_data$date)
fig1_data$month <- month(fig1_data$date)
fig1_data$year <- year(fig1_data$date)
fig1_data <- fig1_data[!is.na(fig1_data$result),]
print(fig1_data %>% dplyr::group_by(metric) %>% dplyr::summarize(min = min(date), mean = mean(date), median = median(date), max = max(date)))
remove(file_names)
# Generate mock 'data' dataset 
data <- fig1_data
data$metric <- ordered(data$metric, levels=c(incidence_label, prevalence_label, fatality_label, recurrence_label))
levels(data$metric)
# Fig 2 data
file_names <- unzip(paste0(inputfolder,"/ccu072_01_cat_trend_results.zip"), junkpaths = TRUE, exdir = tempdir())
fig2_data <- do.call(rbind,lapply(file_names,read.csv))
fig2_data$metric[fig2_data$metric=="Post-event 1-year incidence"] <- recurrence_label
fig2_data <- merge(fig2_data, dcode_names_groups_data)
remove(file_names)
# Fig 3 data
file_names <- unzip(paste0(inputfolder,"/ccu072_01_detailed_adjusted_subgroup_results.zip"), junkpaths = TRUE, exdir = tempdir())
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
names(fig3_data_all) <- gsub("unadjusted_", "unadjusted#", names(fig3_data_all))
names(fig3_data_all) <- gsub("fullyadjusted_", "fullyadjusted#", names(fig3_data_all))
fig3_data_all <- fig3_data_all %>% pivot_longer(!(c(dcode, group, group_item, metric, year)),
                                                      names_to = c("analysis", ".value"),
                                                      names_pattern = "(.+)#(.+)")
fig3_data_all <- fig3_data_all %>% mutate_at((vars(-dcode, -group, -group_item, -metric, -year, -analysis)), as.numeric)
fig3_data_all$metric <- as.character(fig3_data_all$metric)
fig3_data_all$metric[fig3_data_all$metric==incidence_value] <- incidence_label
fig3_data_all$metric[fig3_data_all$metric==prevalence_value] <- prevalence_label
fig3_data_all$metric[fig3_data_all$metric==fatality_value] <- fatality_label
fig3_data_all$metric[fig3_data_all$metric==recurrence_value] <- recurrence_label
fig3_data_all <- fig3_data_all[!is.na(fig3_data_all$metric),]
fig3_data_all <- merge(fig3_data_all, dcode_names_groups_data)
get_group_reference <- function(group){
  switch (group,
    "sex" = "Male",
    "age" = "50_59",
    "ethnicity" = "British",
    "deprivation" = "5_6",
    "comorbidity" = "3_4",
    "region" = "Mid and South Essex")
}
remove(file_names)
# Add parent group to subgroup results table
fig3_data_group_parent <- read_xlsx(paste0(inputfolder,"/ccu072_01_group_parent.xlsx"))
fig3_data_all <- merge(fig3_data_all, fig3_data_group_parent, all.x = T)
table(fig3_data_all$group_item[!is.na(fig3_data_all$group_parent)], fig3_data_all$group_parent[!is.na(fig3_data_all$group_parent)])
length(unique(fig3_data_all$group_item[fig3_data_all$group=="ethnicity" & !is.na(fig3_data_all$group_parent)]))
length(unique(fig3_data_all$group_item[fig3_data_all$group=="region" & !is.na(fig3_data_all$group_parent)]))
# Results by source
bysource_data <- fread(paste0(inputfolder,"/ccu072_01_results_bysource_weight.csv"), data.table = F)
bysource_data <- merge(bysource_data, dcode_names_groups_data)
bysource_data$metric[bysource_data$metric==incidence_value] <- incidence_label
bysource_data$metric[bysource_data$metric==prevalence_value] <- prevalence_label
bysource_data$metric[bysource_data$metric==fatality_value] <- fatality_label
bysource_data$metric[bysource_data$metric==recurrence_value] <- recurrence_label
bysource_data$source[bysource_data$source=="deaths"] <- "ONS"
bysource_data$source[bysource_data$source=="hes_apc"] <- "HES-APC"
bysource_data$source <- toupper(bysource_data$source)
for(col in c("numerator","denominator","result")){
  bysource_data[[col]][bysource_data[[col]]=="null"] <- NA
  bysource_data[[col]] <- as.numeric(bysource_data[[col]])
}
table(bysource_data$source)
# Results for Table 1 (cohort characteristics)
tab1_data <- fread(paste0(inputfolder,"/ccu072_01_tab1_results_000001.csv"), data.table = F)

# Get maps file
# Source: ONS (https://geoportal.statistics.gov.uk/datasets/ons::integrated-care-boards-july-2022-en-bgc-4/about)
# BGC stands for Boundary, Generalised Clipped, which is a type of boundary used by the Office for National Statistics (ONS)
icb_url <- "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/ICB_JUL_2022_EN_BGC_V3/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
icb_file_path <- paste0(inputfolder,"/icb_boundaries_2022.RData")
if(!file.exists(icb_file_path)) {
  #download.file(icb_url, icb_file_path)
  icb_original <- sf::st_read(icb_url)
  saveRDS(icb_original, icb_file_path)
} else{
  icb_original <- readRDS(icb_file_path)
}

# For labels
axis_metric_label <- function(x) {
  initial_x <- x
  x <- as.numeric(pretty(x)[1])
  if(length(na.omit(x))==0 | x==0){
    x <- initial_x
  }
  return(x)
}
axis_metric_label(100000)
axis_metric_transformation <- "log2" # "log10"
axis_metric_transformation_base <- 2
log_base <- 2

# Selected diseases
fixed_dcodes <- c("X411_2",    # MI
                  "X433_0",    # Ischemic stroke
                  "X428_2",    # HF NOS
                  "X443_9",    # Peripheral vascular disease 
                  "X427_2",    # Atrial fibrillation or flutter
                  "XPULMEMB",  # PE
                  "X420_1",    # Myocarditis 
                  "XICVT")     # Intracranial venous thrombosis: added in Nov 2024 when phenotypes were refreshed
common_fixed_dcodes <- c("X411_2",   # MI
                  "X433_0",    # Ischemic stroke
                  "X428_2",    # HF NOS
                  "X443_9",    # Peripheral vascular disease 
                  "X427_2")    # Atrial fibrillation or flutter
                  #"XPULMEMB", # PE
                  #"X420_1",   # Myocarditis 
                  #"XICVT")    # Intracranial venous thrombosis: added in Nov 2024 when phenotypes were refreshed
get_dcodes_by_pc <- function(metric, date=min(data$date)){
  #metric <- "Prevalence"; date <- max(data$date)
  selected_dcodes_data <- data[data$metric==metric & data$date==date & data$group=="All",]
  selected_dcodes_data <- selected_dcodes_data[!is.na(selected_dcodes_data$result),]
  max_dcode <- selected_dcodes_data$dcode[selected_dcodes_data$result==max(selected_dcodes_data$result)]
  return(unique(c(fixed_dcodes, max_dcode)))
}

# Define colours for all selected diseases
all_selected_dcodes <- vector()
for(metric in unique(data$metric)){
  all_selected_dcodes <- unique(c(all_selected_dcodes, get_dcodes_by_pc(metric, max(data$date[data$metric==metric]))))
}
# Add selected dcodes for simple trend lines
selected_dcode_names_data <- data[data$metric=="Prevalence" & data$year==2020 & data$month==1 & data$group=="All",]
selected_dcode_names_data <- selected_dcode_names_data %>% arrange(desc(result)) %>% mutate(quantile = ntile(result, 20))
remove(selected_dcode_names_data)
simplelines_selected_dcodes_data <- read.xlsx(paste0(inputfolder,"/simplelines_conditions_will.xlsx"))
simplelines_selected_dcodes <- simplelines_selected_dcodes_data$dcode[simplelines_selected_dcodes_data$selected=="yes" & !is.na(simplelines_selected_dcodes_data$selected)]
all_selected_dcodes <- unique(c(all_selected_dcodes, simplelines_selected_dcodes))
colour_palette <- rev(polychrome())[1:length(all_selected_dcodes)]
names(all_selected_dcodes) <- colour_palette


###################################################
#     TABLE 1 DATA - COHORT CHARACTERISTICS       #
###################################################

df <- tab1_data %>% dplyr::group_by(year, group) %>% mutate(pc=round(n/sum(n)*100))
df$n_pc <- paste0(trimws(format(df$n, nsmall=1, big.mark=",")), " (",df$pc,")")
df[,c("n","pc")] <- NULL
df <- df %>% pivot_wider(names_from = year, values_from = n_pc)
# Recode group items
df$group_item <- df$value
df$group_item[df$group_item=="3_4" & df$group=="comorbidity"] <- "3-4"
df$group_item[df$group_item=="1_2_most_deprived"] <- "1 most_deprived"
df$group_item[df$group_item=="3_4"] <- "2"
df$group_item[df$group_item=="5_6"] <- "3"
df$group_item[df$group_item=="7_8"] <- "4"
df$group_item[df$group_item=="9_10_least_deprived"] <- "5 least deprived"
df$group_item[df$group_item=="5_over"] <- "5+"
df$group_item[df$group_item=="90_over"] <- "90+"
df$group_item <- gsub("_","-",df$group_item)
df <- merge(df, fig3_data_group_parent, all.x = T)
ordered_groups <- c("all","age","sex","ethnicity","deprivation","comorbidity","region")
ordered_group_items <- vector()
for(group in ordered_groups){
  #group <- df$group[1]
  d <- df[df$group==group,]
  if(group%in%c("ethnicity","region")){
    items <- fig3_data_group_parent$group_item[fig3_data_group_parent$group==group]
  } else{
    items <- sort(d$group_item)
  }
  ordered_group_items <- c(ordered_group_items, paste0(group,"-",items))
  remove(items)
}
df$group_group_item <- paste0(df$group,"-",df$group_item)
df$group_group_item <- ordered(df$group_group_item, levels=ordered_group_items)
df <- df[with(df, order(df$group_group_item)),]
df$group_item[!is.na(df$group_parent)] <- paste0(df$group_parent[!is.na(df$group_parent)],": ",df$group_item[!is.na(df$group_parent)])
df$group <- str_to_title(as.character(df$group))
df$group[df$group=="Comorbidity"] <- gsub("\n"," ",comorbidity_label)
df$group[df$group=="Region"] <- region_label
df[,c("value","group_parent","group_group_item")] <- NULL
new_df <- data.frame()
for(group in unique(df$group)){
  #group <- df$group[1]
  d <- df[df$group==group,]
  if(nrow(d)>1){
    d$group[2:nrow(d)] <-"" 
  }
  new_df <- rbind(new_df, d)
}
df <- new_df
writexl::write_xlsx(df, paste0(outputfolder,"/ccu072_01_tab1.xlsx"))


#############################################################################
#     SUBGROUP HEATMAPS FOR SUBGROUPS - RELATIVE (FIG 4 & SUPPL FIGS)       #
#############################################################################

cat("Subgroup plots - all groups\n")
for(group in sort(unique(fig3_data_all$group))){
  #group <- unique(fig3_data_all$group)[2]
  cat(group,"\n")
  fig3_data <- fig3_data_all[fig3_data_all$group==group & fig3_data_all$year==group_year,]
  table(fig3_data$group)
  table(fig3_data$dcode_group)
  fig3_data$dcode_group[fig3_data$dcode_group == "Aneurysm and dissection of specified arteries"] <- "Aneurysm and\ndissection of\nspecified\narteries"
  fig3_data$dcode_group[fig3_data$dcode_group == "Arterial embolism and thrombosis"] <- "Arterial embolism\nand thrombosis"
  fig3_data$dcode_group[fig3_data$dcode_group == "Atherosclerosis"] <- "Athero-\nsclerosis"
  fig3_data$dcode_group[fig3_data$dcode_group == "Cardiac conduction disorders"] <- "Cardiac\nconduction\ndisorders"
  fig3_data$dcode_group[fig3_data$dcode_group == "Cardiac dysrhythmias"] <- "Cardiac\ndysrhythmias"
  fig3_data$dcode_group[fig3_data$dcode_group == "Cardiac infection, inflammation and other cardiovascular conditions"] <- "Cardiac infection,\ninflammation\nand other\ncardiovascular\nconditions"
  fig3_data$dcode_group[fig3_data$dcode_group == "Cardiac valve diseases"] <- "Cardiac\nvalve\ndiseases"
  fig3_data$dcode_group[fig3_data$dcode_group == "Heart failure and cardiomyopathy"] <- "Heart\nfailure\nand\ncardio-\nmyop."
  fig3_data$dcode_group[fig3_data$dcode_group == "Hypertension"] <- "Hyper-\ntension"
  fig3_data$dcode_group[fig3_data$dcode_group == "Ischaemic heart disease"] <- "Ischaemic\nheart\ndisease"
  fig3_data$dcode_group[fig3_data$dcode_group == "Peripheral vascular disease and other vasculitis"] <- "Peripheral\nvascular\ndisease and\nother\nvasculitis"
  fig3_data$dcode_group[fig3_data$dcode_group == "Pulmonary vessel disorders"] <- "Pulmonary\nvessel\ndisorders"
  fig3_data$dcode_group[fig3_data$dcode_group == "Stroke, transient ischaemic attack and vascular dementia"] <- "Stroke, transient ischaemic attack\nand vascular dementia"
  fig3_data$dcode_group[fig3_data$dcode_group == "Vein disorders"] <- "Vein\ndisorders"
  
  for(analysis in sort(unique(fig3_data$analysis))){
    #analysis <- sort(unique(fig3_data$analysis))[1]
    cat("\t",analysis,"\n")
    
    if(group=="ethnicity"){
      missing_vector <- c("withmissing","withoutmissing")
    } else{
      missing_vector <- "missing removal not applicable"
    }
    
    for(missing in missing_vector){
      #missing <- "withoutmissing"
      cat("\t\t",missing,"\n")
      
      i <- 1
      output.plots <- list()
      for(metric in levels(data$metric)){
        #metric <- levels(data$metric)[1]
        cat("\t\t\t",as.character(metric),"\n")
        df <- fig3_data[fig3_data$analysis==analysis & fig3_data$metric==metric,]
        
        # Recode group items
        df$original_group_item <- df$group_item
        df$group_item[df$group_item=="3_4" & df$group=="comorbidity"] <- "3-4"
        df$group_item[df$group_item=="1_2_most_deprived"] <- "1 most_deprived"
        df$group_item[df$group_item=="3_4"] <- "2"
        df$group_item[df$group_item=="5_6"] <- "3"
        df$group_item[df$group_item=="7_8"] <- "4"
        df$group_item[df$group_item=="9_10_least_deprived"] <- "5 least deprived"
        df$group_item[df$group_item=="5_over"] <- "5+"
        df$group_item[df$group_item=="90_over"] <- "90+"
        df$group_item <- gsub("_","-",df$group_item)
        #df$group_item[df$group_item=="3-4" & df$group=="Comorbidity"] <- "3-4\u0358" # Add invisible character so that this gets ordered correctly
        df$group[df$group=="Comorbidity"] <- gsub("\n"," ",comorbidity_label)
        
        # Get reference level
        reference_level <- unique(df$group_item[df$original_group_item==get_group_reference(group)])
        # Remove row for reference level
        df <- df[df$group_item!=reference_level,]
        if(group=="ethnicity"){
          reference_level <- "White British"
        }
        
        if(missing=="withoutmissing"){
          df <- df[df$group_item!="Missing",]
        }
        
        # Ratios
        df$result <- exp(df$beta) # exp_intercept_plus_beta #exp(df$intercept_plus_beta)
        summary(df$result)
        df$result[is.na(df$result)] <- 1
        summary(df$result)
        df$result[df$result==0] <- min(df$result[df$result!=0])
        summary(df$result)
        
        # P-values
        df$pvalue[df$pvalue==0] <- min(df$pvalue[df$pvalue!=0])
        df$log10_pvalue <- -log10(df$pvalue)
        df$log10_pvalue[df$pvalue>=0.05/(nrow(df))] <- NA
        summary(df$log10_pvalue)
        
        # Fix exp(betas) so that scale is not off the charts
        df$result[df$result<0.01 & is.na(df$log10_pvalue)] <- 1#0.01
        df$result[df$result>30 & is.na(df$log10_pvalue)] <- 1#30
        summary(df$result)
        
        # Fixed for all groups
        if(group=="region"){
          if(metric==incidence_label){
            metric_breaks <- c(10,50,150)
          } else if (metric==prevalence_label){
            metric_breaks <- c(50,150,300)
          } else if (metric==fatality_label){
            metric_breaks <- c(5,20,60)
          } else if(metric==recurrence_label){
            metric_breaks <- c(5,20,50)
          }
        } else if(group=="deprivation"){
          if(metric==incidence_label){
            metric_breaks <- c(10,50,100)
          } else if (metric==prevalence_label){
            metric_breaks <- c(50,150,300)
          } else if (metric==fatality_label){
            metric_breaks <- c(10,40,80)
          } else if(metric==recurrence_label){
            metric_breaks <- c(5,15,30)
          }
        } else{
          if(metric==incidence_label){
            metric_breaks <- c(10,100,200) #pretty(ntile(df$log10_pvalue[!is.na(df$log10_pvalue)], 3), n=3) #c(10, 50, 100)
          } else if (metric==prevalence_label){
            metric_breaks <- c(50,150,300) #pretty(df$log10_pvalue, n=3) #c(10, 50, 100)
          } else if (metric==fatality_label){
            metric_breaks <- c(10,40,80) #pretty(df$log10_pvalue, n=3) #c(10, 50, 100)
          } else if(metric==recurrence_label){
            metric_breaks <- c(5,10,15) # pretty(df$log10_pvalue, n=3) #c(10, 50, 100)
          }
        }
        label_accuracy <- 0.1
        # if(group=="ethnicity"){
        #   if(metric%in%c(incidence_label,fatality_label,recurrence_label))
        #   label_accuracy <- 0.01
        # }
        metric_ratio <- "Ratio"
        summary(df$result)
        # Selected diseases
        df$original_dcode_name <- df$dcode_name
        if(group=="ethnicity"){
          df$dcode_name <- str_trunc(df$dcode_name, 50)
        } else if(group=="region"){
          df$dcode_name <- str_trunc(df$dcode_name, 25)
        }
        selected_dcodes <- get_dcodes_by_pc(metric)
        selected_dcode_names <- unique(df$dcode_name[df$dcode%in%selected_dcodes])
        
        # Order labels
        df$dcode_name <- trimws(df$dcode_name)
        df$dcode_name <- ordered(df$dcode_name, levels=sort(unique(df$dcode_name)))
        if(group%in%c("ethnicity","region")){
          df$group_item <- ordered(df$group_item, levels=rev(fig3_data_group_parent$group_item[fig3_data_group_parent$group==group]))
        }
        df <- df[with(df, order(df$dcode, df$group, df$group_item)),]
        
        # Rename labels
        df$group <- str_to_title(as.character(df$group))
        df$group <- ordered(df$group, levels=unique(df$group))
        df$group_parent <- str_wrap(as.character(df$group_parent), 10)
        df$group_parent <- ordered(df$group_parent, levels=sort(unique(df$group_parent)))
        
        # Refine plot for each subgroup
        point_size_range <- c(0.5,3)
        if(group=="region"){
          axis_text_size <- 7
          legend_text_size <- 7
          title_hjust=0.1
          point_size_range <- c(0.05, 1.5)
        } else if(group=="ehtnicity"){
          axis_text_size <- 8.5
          legend_text_size <- 8
          title_hjust=-5
        } else{
          axis_text_size <- 8.5
          legend_text_size <- 8
          title_hjust=0.1
        }
        
        summary(df$result)
        g <- ggplot(df, aes(y=group_item, x=dcode_name, group=dcode_name, fill=result, size=log10_pvalue)) + # size=alpha , label=dcode_name
          geom_tile() +
          ggtitle(paste0(LETTERS[i],". ",metric)) +
          labs(x="", y="", 
               fill=str_wrap(paste0("Ratio relative to ",reference_level), 15),
               size=expression("-Log"[10]~"(P-value)")) + 
          scale_fill_gradient2(low = RColorBrewer::brewer.pal(length(unique(data$metric)), "Set1")[2], # Blue: RR < 1
                               mid = "#ffffff",
                               high = RColorBrewer::brewer.pal(length(unique(data$metric)), "Set1")[1], # Red: RR > 1
                               midpoint = 1, 
                               transform=axis_metric_transformation, breaks=breaks_log(), # axis_metric_transformation
                               labels=label_number(accuracy = label_accuracy) ) +
          scale_x_discrete(position = "top") +
          theme_minimal() + 
          geom_point() +
          scale_size(breaks = metric_breaks, range = point_size_range) +
          guides(size = guide_legend(override.aes = list(fill = "white"), order=2)) +
          theme(#axis.text.x.bottom = element_text(angle = 45, vjust = 0, hjust=1, size=9.5), 
            strip.placement = "outside",
            strip.text = element_text(size=axis_text_size),
            axis.text.x = element_text(angle = 45, vjust = 0.5, hjust=0, size=axis_text_size),
            axis.text.y = element_text(size=axis_text_size), 
            legend.title = element_text(size=legend_text_size),
            plot.title = element_text(face="bold", size = 14, hjust = title_hjust),
            plot.title.position = "plot")
        if(group%in%c("ethnicity","region")){
          g <- g + facet_grid(group_parent~dcode_group, scales = "free", space = "free", switch ="both")
        }
        g
        
        output.plots[[length(output.plots)+1]] <- as_grob(g)
        
        i <- i + 1
      }
      if(length(missing_vector)==1){
        file_name <- paste0(outputfolder,"/ccu072_01_subgroup_heatmap_",group,"_",analysis,".pdf")
      } else{
        file_name <- paste0(outputfolder,"/ccu072_01_subgroup_heatmap_",group,"_",missing,"_",analysis,".pdf")
      }
      ggsave(file_name, width=210*2, height=297*2, limitsize=F, unit="mm", # 210
             device = cairo_pdf, onefile = TRUE,
             marrangeGrob(output.plots, 
                          layout_matrix = matrix(1:4, byrow = T, ncol = 1), top="", bottom="")) 
    }
  }
}


###################################################################
#     COMPACT HEATMAPS FOR SUBGROUPS - RELATIVE (SUPPL FIG)       #
###################################################################

cat("Subgroup plots - all groups\n")
for(group in sort(unique(fig3_data_all$group))[4]){
  #group <- unique(fig3_data_all$group)[4]
  cat(group,"\n")
  fig3_data <- fig3_data_all[fig3_data_all$group==group & fig3_data_all$year==group_year & fig3_data_all$dcode%in%common_fixed_dcodes,]
  table(fig3_data$group)

  for(analysis in sort(unique(fig3_data$analysis))){
    #analysis <- sort(unique(fig3_data$analysis))[1]
    cat("\t",analysis,"\n")
    
    if(group=="ethnicity"){
      missing_vector <- c("withmissing","withoutmissing")
    } else{
      missing_vector <- "missing removal not applicable"
    }
    
    for(missing in missing_vector){
      #missing <- "withoutmissing"
      cat("\t\t",missing,"\n")
      
      i <- 1
      output.plots <- list()
      for(metric in levels(data$metric)){
        #metric <- levels(data$metric)[1]
        cat("\t\t\t",as.character(metric),"\n")
        df <- fig3_data[fig3_data$analysis==analysis & fig3_data$metric==metric,]
        
        # Recode group items
        df$original_group_item <- df$group_item
        df$group_item[df$group_item=="3_4" & df$group=="comorbidity"] <- "3-4"
        df$group_item[df$group_item=="1_2_most_deprived"] <- "1 most_deprived"
        df$group_item[df$group_item=="3_4"] <- "2"
        df$group_item[df$group_item=="5_6"] <- "3"
        df$group_item[df$group_item=="7_8"] <- "4"
        df$group_item[df$group_item=="9_10_least_deprived"] <- "5 least deprived"
        df$group_item[df$group_item=="5_over"] <- "5+"
        df$group_item[df$group_item=="90_over"] <- "90+"
        df$group_item <- gsub("_","-",df$group_item)
        #df$group_item[df$group_item=="3-4" & df$group=="Comorbidity"] <- "3-4\u0358" # Add invisible character so that this gets ordered correctly
        df$group[df$group=="Comorbidity"] <- gsub("\n"," ",comorbidity_label)
        
        # Get reference level
        reference_level <- unique(df$group_item[df$original_group_item==get_group_reference(group)])
        # Remove row for reference level
        df <- df[df$group_item!=reference_level,]
        if(group=="ethnicity"){
          reference_level <- "White British"
        }
        
        if(missing=="withoutmissing"){
          df <- df[df$group_item!="Missing",]
        }
        
        # Ratios
        df$result <- exp(df$beta) # exp_intercept_plus_beta #exp(df$intercept_plus_beta)
        summary(df$result)
        df$result[is.na(df$result)] <- 1
        summary(df$result)
        df$result[df$result==0] <- min(df$result[df$result!=0])
        summary(df$result)
        
        # P-values
        df$pvalue[df$pvalue==0] <- min(df$pvalue[df$pvalue!=0])
        df$log10_pvalue <- -log10(df$pvalue)
        df$log10_pvalue[df$pvalue>=0.05/(nrow(df))] <- NA
        summary(df$log10_pvalue)
        
        # Fix exp(betas) so that scale is not off the charts
        df$result[df$result<0.01 & is.na(df$log10_pvalue)] <- 1#0.01
        df$result[df$result>30 & is.na(df$log10_pvalue)] <- 1#30
        summary(df$result)
        
        # Fixed for all groups
        if(group=="region"){
          if(metric==incidence_label){
            p_metric_breaks <- c(10,50,150)
          } else if (metric==prevalence_label){
            p_metric_breaks <- c(50,150,300)
          } else if (metric==fatality_label){
            p_metric_breaks <- c(5,20,60)
          } else if(metric==recurrence_label){
            p_metric_breaks <- c(5,20,50)
          }
        } else if(group=="deprivation"){
          if(metric==incidence_label){
            p_metric_breaks <- c(10,50,100)
          } else if (metric==prevalence_label){
            p_metric_breaks <- c(50,150,300)
          } else if (metric==fatality_label){
            p_metric_breaks <- c(10,40,80)
          } else if(metric==recurrence_label){
            p_metric_breaks <- c(5,15,30)
          }
        } else{
          if(metric==incidence_label){
            p_metric_breaks <- c(10,100,200) #pretty(ntile(df$log10_pvalue[!is.na(df$log10_pvalue)], 3), n=3) #c(10, 50, 100)
            beta_metric_breaks <- c(3,1,0.3,0.1)
          } else if (metric==prevalence_label){
            p_metric_breaks <- c(50,150,300) #pretty(df$log10_pvalue, n=3) #c(10, 50, 100)
            beta_metric_breaks <- c(3,1,0.3,0.1)
          } else if (metric==fatality_label){
            p_metric_breaks <- c(10,40,80) #pretty(df$log10_pvalue, n=3) #c(10, 50, 100)
            beta_metric_breaks <- c(30,10,3,1,0.3)
          } else if(metric==recurrence_label){
            p_metric_breaks <- c(5,10,15) # pretty(df$log10_pvalue, n=3) #c(10, 50, 100)
            beta_metric_breaks <- c(10,3,1,0.3)
          }
        }
        beta_metric_breaks <- range(beta_metric_breaks)
        #beta_metric_breaks <- c(max(max(df$result),beta_metric_breaks),
        #                        min(min(df$result, beta_metric_breaks)))
        
        label_accuracy <- 0.1
        # if(group=="ethnicity"){
        #   if(metric%in%c(incidence_label,fatality_label,recurrence_label))
        #   label_accuracy <- 0.01
        # }
        metric_ratio <- "Ratio"
        summary(df$result)
        # Selected diseases
        df$original_dcode_name <- df$dcode_name
        if(group=="ethnicity"){
          df$dcode_name <- str_trunc(df$dcode_name, 50)
        } else if(group=="region"){
          df$dcode_name <- str_trunc(df$dcode_name, 25)
        }
        selected_dcodes <- get_dcodes_by_pc(metric)
        selected_dcode_names <- unique(df$dcode_name[df$dcode%in%selected_dcodes])
        
        # Order labels
        df$dcode_name <- trimws(df$dcode_name)
        df$dcode_name <- ordered(df$dcode_name, levels=sort(unique(df$dcode_name)))
        df <- df[with(df, order(df$dcode, df$group, df$group_item)),]
        
        # Rename labels
        df$group <- str_to_title(as.character(df$group))
        df$group <- ordered(df$group, levels=unique(df$group))
        df$group_parent <- str_wrap(as.character(df$group_parent), 10)
        df$group_parent <- ordered(df$group_parent, levels=sort(unique(df$group_parent)))
        
        # Refine plot for each subgroup
        point_size_range <- c(0.5,3)
        if(group=="region"){
          axis_text_size <- 7
          legend_text_size <- 7
          title_hjust=0.1
          point_size_range <- c(0.05, 1.5)
        } else if(group=="ehtnicity"){
          axis_text_size <- 8.5
          legend_text_size <- 8
          title_hjust=-5
        } else{
          axis_text_size <- 8.5
          legend_text_size <- 8
          title_hjust=0.1
        }
        
        summary(df$result)
        g <- ggplot(df, aes(y=group_item, x=dcode_name, group=dcode_name, fill=result, size=log10_pvalue)) + # size=alpha , label=dcode_name
          geom_tile() +
          ggtitle(paste0(LETTERS[i],". ",metric)) +
          labs(x="", y="", 
               fill=str_wrap(paste0("Ratio relative to ",reference_level), 15),
               size=expression("-Log"[10]~"(P-value)")) + 
          scale_fill_gradient2(low = RColorBrewer::brewer.pal(length(unique(data$metric)), "Set1")[2], # Blue: RR < 1
                               mid = "#ffffff",
                               high = RColorBrewer::brewer.pal(length(unique(data$metric)), "Set1")[1], # Red: RR > 1
                               midpoint = 1, 
                               limits=beta_metric_breaks,
                               oob = scales::squish,  # <- This prevents gray by clamping
                               transform=axis_metric_transformation, breaks=breaks_log(), # axis_metric_transformation
                               labels=label_number(accuracy = label_accuracy) ) +
          scale_x_discrete(position = "top") +
          theme_minimal() + 
          geom_point() +
          scale_size(breaks = p_metric_breaks, range = point_size_range) +
          guides(size = guide_legend(override.aes = list(fill = "white"), order=2)) +
          theme(#axis.text.x.bottom = element_text(angle = 45, vjust = 0, hjust=1, size=9.5), 
            strip.placement = "outside",
            strip.text = element_text(size=axis_text_size),
            axis.text.x = element_text(angle = 45, vjust = 0.5, hjust=0, size=axis_text_size),
            axis.text.y = element_text(size=axis_text_size), 
            legend.title = element_text(size=legend_text_size),
            plot.title = element_text(face="bold", size = 14, hjust = title_hjust),
            plot.title.position = "plot")
        if(group%in%c("ethnicity","region")){
          g <- g + facet_grid(group_parent~1, scales = "free", space = "free", switch ="both", labeller = labeller(.cols = function(x) ""))
        }
        g
        
        output.plots[[length(output.plots)+1]] <- as_grob(g)
        
        i <- i + 1
      }
      if(length(missing_vector)==1){
        file_name <- paste0(outputfolder,"/ccu072_01_compact_subgroup_heatmap_",group,"_",analysis,".pdf")
      } else{
        file_name <- paste0(outputfolder,"/ccu072_01_compact_subgroup_heatmap_",group,"_",missing,"_",analysis,".pdf")
      }
      ggsave(file_name, width=297*2, height=210, limitsize=F, unit="mm", # 210
             device = cairo_pdf, onefile = TRUE,
             marrangeGrob(output.plots, 
                          layout_matrix = matrix(1:4, byrow = T, ncol = 4), top="", bottom="")) 
    }
  }
}


##########################################
#     SUBGROUP PLOTS: MAPS (FIG 5)       #
##########################################

cat("Subgroup maps\n")
fig3_data <- fig3_data_all[fig3_data_all$group=="region" & fig3_data_all$year==group_year,]
point_size_range <- c(0.05, 1.5)

for(analysis in sort(unique(fig3_data$analysis))){
  i <- 1
  output.plots <- list()
  for(metric in levels(data$metric)){
    selected_dcodes <- get_dcodes_by_pc(metric)
    df <- fig3_data[fig3_data$analysis==analysis & fig3_data$metric==metric & fig3_data$dcode%in%common_fixed_dcodes,]# selected_dcodes
    df$ICB22NM <- paste0("NHS ",df$group_item," Integrated Care Board")
    ### Start ratios
    df$result <- exp(df$beta)
    df$result[df$result=="Reference"] <- NA
    df$result[df$result=="-"] <- "1"
    df$result <- as.numeric(df$result)
    summary(df$result)
    summary(df$result)
    df$result[df$result==0] <- min(df$result[df$result!=0])
    # P-values
    df$pvalue[df$pvalue==0] <- min(df$pvalue[df$pvalue!=0])
    df$log10_pvalue <- -log10(df$pvalue)
    df$log10_pvalue[df$pvalue>=0.05/(nrow(fig3_data[fig3_data$analysis==analysis & fig3_data$metric==metric,]))] <- NA
    summary(df$log10_pvalue)
    # Fix exp(betas) so that scale is not off the charts
    df$result[df$result<0.01 & is.na(df$log10_pvalue)] <- 1 #0.01
    df$result[df$result>30 & is.na(df$log10_pvalue)] <- 1 #30
    summary(df$result)
    ### End ratios
    reference_level <- get_group_reference("region") #reference_level <- unique(df$group_item[df$rr_ci=="Reference"])
    df$result[df$group_item==reference_level] <- NA
    summary(df$result)
    df$dcode_name <- str_wrap(df$dcode_name, 30)
    df$dcode_name <- ordered(df$dcode_name, levels=unique(sort(df$dcode_name)))
    df <- merge(icb_original, df[,c("ICB22NM","result","dcode_name","log10_pvalue")], all.x=T)
    df <- df[with(df, order(df$dcode_name)),]

    if(metric==incidence_label){
      unit <- incidence_unit
      if(analysis=="unadjusted"){
        metric_breaks <- c(5,10,15)  
      } else if(analysis=="fullyadjusted"){
        metric_breaks <- c(5,15,30)  
      }
    } else if (metric==prevalence_label){
      ##df$result <- df$result*100
      unit <- prevalence_unit
      if(analysis=="unadjusted"){
        metric_breaks <- c(10,50,100)
      } else if(analysis=="fullyadjusted"){
        metric_breaks <- c(50,150,300)
      }
    } else if (metric==fatality_label){
      unit <- fatality_unit
      if(analysis=="unadjusted"){
        metric_breaks <- c(6,10,14)
      } else if(analysis=="fullyadjusted"){
        metric_breaks <- c(5,10,15)
      }#breaks <- c(0,2,3,4,6,8,10,12,14,20)
    } else if(metric==recurrence_label){
      unit <- recurrence_unit
      if(analysis=="unadjusted"){
        metric_breaks <- c(5,10,20)
      } else if(analysis=="fullyadjusted"){
        metric_breaks <- c(5,20,50)
      }
    }
    label_accuracy <- 0.1
    
    map <- ggplot() + ggtitle(paste0(LETTERS[i],". ",metric)) +
      geom_sf(data=df, 
              #aes(fill=result_cat), 
              aes(fill=result), 
              lwd = 0.1, colour="black") +
      geom_point(data = df, 
        aes(size = log10_pvalue, geometry = geometry),
        stat = "sf_coordinates") +
      scale_size(breaks = metric_breaks, range = point_size_range) +
      labs(x="", y="", fill=str_wrap(paste0("Ratio relative to ",reference_level), 15),
           size=expression("-Log"[10]~"(P-value)")) + 
      scale_fill_gradient2(high = RColorBrewer::brewer.pal(length(unique(fig3_data$metric)), "Set1")[1], # Red: RR > 1
                           mid = "#ffffff",
                           low = RColorBrewer::brewer.pal(length(unique(fig3_data$metric)), "Set1")[2], # Blue: RR < 1
                           midpoint = 1,
                           labels=label_number(accuracy = label_accuracy) , na.value = "black") +
      theme_minimal() +
      facet_wrap(vars(dcode_name), nrow = 1) +
      guides(size = guide_legend(override.aes = list(fill = "white"), order=2)) +
      theme(strip.text.x = element_text(size = 10),
            legend.title=element_text(size=10),
            plot.title = element_text(size = 14, face = "bold")) + 
      coord_sf(datum = NA)  # Remove lines of lat / long
    map
    
    output.plots[[length(output.plots)+1]] <- as_grob(map)
    
    i <- i + 1
  }
  ggsave(paste0(outputfolder,"/ccu072_01_f5_maps_",analysis,".pdf"), width=210*1.5, height=297*1.5, limitsize=F, unit="mm", # 210
         device = cairo_pdf, onefile = TRUE,
         marrangeGrob(output.plots, 
                      layout_matrix = matrix(1:4, byrow = T, ncol = 1), top="", bottom=""))
}


##########################################################################################
#     SUBGROUP PLOTS: ABSOLUTE MAPS (SUPPLEMENT AS REQUESTED BY ANGELA ON 14.2.2025)     #
##########################################################################################

cat("Subgroup maps\n")
fig3_data <- fig3_data_all[fig3_data_all$group=="region" & fig3_data_all$year==group_year,]

for(analysis in sort(unique(fig3_data$analysis))){
  i <- 1
  output.plots <- list()
  for(metric in levels(data$metric)){

    selected_dcodes <- get_dcodes_by_pc(metric)
    df <- fig3_data[fig3_data$analysis==analysis & fig3_data$metric==metric & fig3_data$dcode%in%common_fixed_dcodes,]
    df$ICB22NM <- paste0("NHS ",df$group_item," Integrated Care Board")
    df$result <- as.numeric(df$exp_intercept_plus_beta)
    summary(df$result)
    df$result[df$result==0] <- min(df$result[df$result!=0])
    summary(df$result)
    df$dcode_name <- str_wrap(df$dcode_name, 30)
    df$dcode_name <- ordered(df$dcode_name, levels=unique(sort(df$dcode_name)))
    df <- merge(icb_original, df[,c("ICB22NM","result","dcode_name")], all.x=T)
    df <- df[with(df, order(df$dcode_name)),]
    
    if(metric==incidence_label){
      unit <- incidence_unit
      breaks <- c(0,5,50,100,200,300,400,500,1000)
      breaks_colours <- c("#ffffff",RColorBrewer::brewer.pal(length(unique(data$metric)), "Set1")[3],"#333300") # Green
    } else if (metric==prevalence_label){
      df$result <- df$result*100
      unit <- prevalence_unit
      breaks <- c(0,0.01,0.02,0.04,0.06,0.08,0.1)
      #breaks_labels <- c("0.0-0.9","1.0-2.9","3.0-4.9","5.0-6.9","7.0-8.9","9.0+")
      breaks_colours <- c("#ffffff",RColorBrewer::brewer.pal(length(unique(data$metric)), "Set1")[2],"blue") # Blue
    } else if (metric==fatality_label){
      df$result <- df$result*100
      unit <- fatality_unit
      breaks <- c(0,2,3,4,6,8,10,12,14,20)
      breaks_colours <- c("#ffffff",RColorBrewer::brewer.pal(length(unique(data$metric)), "Set1")[1],"red") # Red
    } else if(metric==recurrence_label){
      unit <- recurrence_unit
      breaks <- c(0,1000,2500,5000,10000,20000,30000,40000,80000)
      breaks_colours <- c("#ffffff",RColorBrewer::brewer.pal(length(unique(data$metric)), "Set1")[4],"purple") # Violet
    }
    label_accuracy <- 1
    
    map <- ggplot() + ggtitle(paste0(LETTERS[i],". ",metric)) +
      geom_sf(data=df, 
              aes(fill=result), 
              lwd = 0.1, colour="black") +
      scale_fill_gradient2(name=str_wrap(unit, 15),
                           high = RColorBrewer::brewer.pal(length(unique(fig3_data$metric)), "Set1")[1], # Red: RR > 1
                           mid = "#ffffff", 
                           low = RColorBrewer::brewer.pal(length(unique(fig3_data$metric)), "Set1")[2], # Blue: RR < 1
                           midpoint = mean(df$result),
                           labels=label_number(accuracy = label_accuracy)) +
      theme_minimal() +
      facet_wrap(vars(dcode_name), nrow = 1) +
      theme(strip.text.x = element_text(size = 10),
            plot.title = element_text(size = 14, face = "bold")) + 
      coord_sf(datum = NA)  # Remove lines of lat / long
    map
    
    output.plots[[length(output.plots)+1]] <- as_grob(map)
    
    i <- i + 1
  }
  ggsave(paste0(outputfolder,"/ccu072_01_f5_maps_absolute_",analysis,".pdf"), width=210*1.5, height=297*1.5, limitsize=F, unit="mm", # 210
         device = cairo_pdf, onefile = TRUE,
         marrangeGrob(output.plots, 
                      layout_matrix = matrix(1:4, byrow = T, ncol = 1), top="", bottom="")) 
}



#################################################
#     SIMPLIFIED LINE PLOTS (SUPPL FIG 2)       #
#################################################

output.plots <- list()
output.data <- data.frame()
i <- 1
for(metric in levels(data$metric)){
  #metric <- levels(data$metric)[4]
  cat(metric,"\n")
  df <- fig2_data[fig2_data$date_group==1 & fig2_data$metric==metric,]
  df$date <- stringr::str_extract(string = df$var_name, pattern = "(?<=\\().*(?=\\))") %>% 
    str_split(pattern = " to ") %>% 
    map(\(x) mean(as.Date(paste0(x,"-01")))) %>%
    unlist %>% as.Date()
  # date_groups <- sort(unique(df$date))
  # names(date_groups) <- date_group_names

  # Specify plot-specific groups
  date_groups <- list(c(min(df$date), date_group_dates[1]-1),
                      c(date_group_dates[1], date_group_dates[2]-1),
                      c(date_group_dates[2], max(df$date)))
  names(date_groups) <- date_group_names

  # Selected dcode_names
  selected_dcodes <- simplelines_selected_dcodes
  selected_dcode_names <- unique(df$dcode_name[df$dcode%in%selected_dcodes])
  df$polished_dcode_name <- NA
  df$polished_dcode_name[df$dcode_name%in%selected_dcode_names & df$date==max(df$date)] <- str_wrap(df$dcode_name[df$dcode_name%in%selected_dcode_names & df$date==max(df$date)], 17)
  table(df$polished_dcode_name, df$date_group)
  
  # Set alpha based on numerator - one value per disease
  df_alpha <- data[data$metric==metric,] %>% group_by(dcode, dcode_name) %>% summarise(alpha=sum(numerator, na.rm=T)) %>% as_data_frame()
  df_alpha$alpha[is.infinite(df_alpha$alpha)] <- 0
  df_alpha$alpha = scales::rescale(df_alpha$alpha, alpha_range_trends)
  df_alpha$alpha[df_alpha$dcode_name%in%selected_dcode_names] <- 1
  df <- merge(df, df_alpha)
  remove(df_alpha)

  # Colours
  df$colour <- "darkgrey"
  for(dcode in selected_dcodes){
    df$colour[df$dcode==dcode] <- names(all_selected_dcodes[all_selected_dcodes==dcode])
  }
  colours <- as.character(df$colour)
  names(colours) <- as.character(df$dcode_name)

  df$result <- df$exp_intercept_plus_beta
  
  if(metric==incidence_label){
    label_accuracy <- 1
    unit <- incidence_unit
  } else if (metric==prevalence_label){
    df$result <- df$result * 100
    label_accuracy <- 0.01
    unit <- prevalence_unit
  } else if (metric==fatality_label){
    df$result <- df$result * 100
    label_accuracy <- 1#0.0001
    unit <- fatality_unit
  } else if(metric==recurrence_label){
    label_accuracy <- 1#0.1 # 0.00001
    unit <- recurrence_unit
  }
  
  # Identify the last data point for each group
  df_last <- df %>%
    group_by(dcode_name) %>%
    filter(date == max(date)) # Get the last date point per group
  
  
  g <- ggplot(df, aes(x=date, y=result, group=dcode_name, colour=dcode_name, alpha=alpha, label=polished_dcode_name)) + 
    geom_line() + geom_point() +
    labs(x="", y=unit, title=paste0(LETTERS[i],". ",metric)) +
    geom_text_repel(x = max(df$date), # Set the position of the text to always be at '14.25'
                    hjust = 0,
                    na.rm = TRUE,
                    segment.linetype = "dotted", segment.size = 0.5,
                    xlim = c(max(df$date)+4*365.25/12, max(df$date)+6*365.25),
                    size = 3, direction = "y", #max.date=1000, max.iter=100000, #nudge_y = -0.25,
                    box.padding=0.7, # force=10,
                    max.overlaps=20) +
    #scale_color_brewer(palette="Set1") +
    geom_vline(xintercept = date_groups[[2]][1], linetype="dashed", colour="grey") + 
    geom_vline(xintercept = date_groups[[3]][1], linetype="dashed", colour="grey") + 
    annotate("text", label = names(date_groups)[1], x=mean(date_groups[[1]])-90, y=2*max(df$result,na.rm=T), size = 4, colour = "black") +
    annotate("text", label = names(date_groups)[2], x=mean(date_groups[[2]]), y=2*max(df$result,na.rm=T), size = 4, colour = "black") +
    annotate("text", label = names(date_groups)[3], x=mean(date_groups[[3]])+45, y=2*max(df$result,na.rm=T), size = 4, colour = "black") +
    annotate("text", label = names(date_groups)[1], x=mean(date_groups[[1]]), y=-1, size = 4, colour = "darkgrey") +
    scale_colour_manual(values = colours) +
    coord_cartesian(xlim = c(min(df$date)-120, as.Date("2023-10-31")), # This focuses the x-axis on the range of interest
                    #ylim = c(min(df$result), 1.2*max(df$result)),
                    clip = 'off') +   # This keeps the labels from disappearing
    #guides(colour = guide_legend(nrow = 1)) +
    scale_y_continuous(transform=axis_metric_transformation, breaks=breaks_log(), labels = label_number(accuracy = label_accuracy)) + # labels=axis_metric_label
    theme_classic() + 
    theme(plot.title = element_text(size=15, hjust = 0.5, face = "bold"),
          axis.ticks.x = element_blank(), axis.text.x = element_blank(),
          legend.position = "none", plot.margin = unit(c(0.1, 2.5, 0.1, 0), "cm")) # unit(c(top, right, bottom, left)
  g
  
  output.plots[[length(output.plots)+1]] <- g
  
  #df$pcchange_p <- df$p
  df$date_group <- gsub(" \\(.*","",df$var_name)
  groups <- vector()
  for(date in sort(unique(df$date))){
    groups <- c(groups, unique(df$date_group[df$date==date]))
  }
  df[df$date==min(df$date),c("pcchange","pcchange_l95ci","pcchange_u95ci","pcchange_p")] <- NA
  export.df <- df
  export.df$date_group <- ordered(export.df$date_group, levels=groups)
  
  # Add ranks
  export.df.rank <- data.frame()
  for(date_group in unique(export.df$date_group)){
    #date_group <- unique(export.df$date_group)[1]
    d <- export.df[export.df$date_group==date_group,]
    d$rank <- rank(-1*d$result)
    export.df.rank <- rbind(export.df.rank, d)
    remove(d)
  }
  export.df <- export.df.rank
  remove(export.df.rank)
  
  export.df$selected_diagnosis[export.df$dcode_name%in%selected_dcode_names] <- "Yes"
  export.df$selected_diagnosis[!export.df$dcode_name%in%selected_dcode_names] <- "No"
  names(export.df)[names(export.df) == "dcode_name"] <- "diagnosis"
  export.df <- export.df[,c("metric","diagnosis","date_group","result","rank","pcchange","pcchange_l95ci","pcchange_u95ci","pcchange_p","selected_diagnosis")]
  output.data <- rbind(output.data, export.df)
  remove(export.df)
  
  i <- i + 1
}
ggsave(paste0(outputfolder,"/ccu072_01_f2_simplelines.pdf"), 
       width=210*1.5*2, height=297*1.5/2, limitsize=F, unit="mm", # 210
       device = cairo_pdf, onefile = TRUE,
       marrangeGrob(output.plots, layout_matrix = matrix(1:4, byrow = T, ncol = 4), top="", bottom=""))
# Store data in a table
output.data$metric <- ordered(output.data$metric, levels=levels(data$metric))
output.data <- output.data[with(output.data, order(output.data$metric, output.data$diagnosis, output.data$date_group)),]
writexl::write_xlsx(output.data, paste0(outputfolder,"/ccu072_01_f2_simplelines.xlsx"))


##########################################
#     PRELOCKDOWN COMPARISON TABLE       #
##########################################

output.data <- data.frame()
i <- 1
for(metric in levels(data$metric)){
  cat(metric,"\n")
  df <- fig2_data[fig2_data$date_group==2 & fig2_data$metric==metric,]
  df$date <- stringr::str_extract(string = df$var_name, pattern = "(?<=\\().*(?=\\))") %>% 
    str_split(pattern = " to ") %>% 
    map(\(x) mean(as.Date(paste0(x,"-01")))) %>%
    unlist %>% as.Date()

  df$result <- df$exp_intercept_plus_beta
  if(metric %in% c(prevalence_label,fatality_label)){
    df$result <- df$result * 100
  }
  
  df$date_group <- gsub(" \\(.*","",df$var_name)
  df$date_group_new <- df$date_group
  df$date_group[df$date_group=="Before first COVID-19 wave"] <- "Before the pandemic"
  df$date_group[df$date_group=="In the last year"] <- "After the pandemic - in the last year"
  groups <- vector()
  for(date in sort(unique(df$date))){
    groups <- c(groups, unique(df$date_group[df$date==date]))
  }
  df[df$date==min(df$date),c("pcchange","pcchange_l95ci","pcchange_u95ci","pcchange_p")] <- NA
  export.df <- df
  export.df$date_group <- ordered(export.df$date_group, levels=groups)
  
  # Add ranks
  export.df.rank <- data.frame()
  for(date_group in unique(export.df$date_group)){
    #date_group <- unique(export.df$date_group)[1]
    d <- export.df[export.df$date_group==date_group,]
    d$rank <- rank(-1*d$result)
    export.df.rank <- rbind(export.df.rank, d)
    remove(d)
  }
  export.df <- export.df.rank
  remove(export.df.rank)
  export.df$selected_diagnosis[export.df$dcode%in%get_dcodes_by_pc(metric)] <- "Yes"
  export.df$selected_diagnosis[!export.df$dcode%in%get_dcodes_by_pc(metric)] <- "No"
  names(export.df)[names(export.df) == "dcode_name"] <- "diagnosis"
  export.df <- export.df[,c("metric","diagnosis","date_group","result","rank","pcchange","pcchange_l95ci","pcchange_u95ci","pcchange_p","selected_diagnosis")]
  output.data <- rbind(output.data, export.df)
  remove(export.df)
  
  i <- i + 1
}
# Store data in a table
output.data$metric <- ordered(output.data$metric, levels=levels(data$metric))
output.data <- output.data[with(output.data, order(output.data$metric, output.data$diagnosis, output.data$date_group)),]
writexl::write_xlsx(output.data, paste0(outputfolder,"/ccu072_01_prelockdown_table.xlsx"))


########################################################
#     SUBGROUP PLOTS: ALL (SUPPLEMENTARY TABLES)       #
########################################################

cat("Subgroup suppl tables - all groups\n")
fig3_data <- fig3_data_all[fig3_data_all$year==group_year,]
output.data <- data.frame()
for(analysis in sort(unique(fig3_data$analysis))){
  cat(analysis,"\n")
  for(metric in levels(data$metric)){
    cat("\t",as.character(metric),"\n")
    df <- fig3_data[fig3_data$analysis==analysis & fig3_data$metric==metric,]
    
    # Selected diseases
    selected_dcodes <- get_dcodes_by_pc(metric)
    selected_dcode_names <- unique(df$dcode_name[df$dcode%in%selected_dcodes])
    
    # Rename labels
    df$group <- str_to_title(df$group)
    df$group_item[df$group_item=="3_4" & df$group=="Comorbidity"] <- "3-4\u0358" # Add invisible character so that this gets ordered correctly
    df$group_item[df$group_item=="1_2_most_deprived"] <- "1 most_deprived"
    df$group_item[df$group_item=="3_4"] <- "2"
    df$group_item[df$group_item=="5_6"] <- "3"
    df$group_item[df$group_item=="7_8"] <- "4"
    df$group_item[df$group_item=="9_10_least_deprived"] <- "5 least deprived"
    df$group_item[df$group_item=="5_over"] <- "5+"
    df$group_item[df$group_item=="90_over"] <- "90+"
    df$group_item <- gsub("_","-",df$group_item)
    df$group[df$group=="Comorbidity"] <- gsub("\n"," ",comorbidity_label)
    df$group[df$group=="Region"] <- gsub("\n"," ",region_label)
    
    df$diagnosis <- df$dcode_name
    df$analysis <- analysis
    df$result <- df$exp_intercept_plus_beta
    df$rr <- exp(df$beta) #as.numeric(df$rr)
    df$rr_l95ci <- exp(df$beta_l95ci)
    df$rr_u95ci <- exp(df$beta_u95ci)
    df$rr_p <- df$pvalue
    export.df <- df[,c("analysis","metric","diagnosis","group","group_item","result","rr","rr_l95ci","rr_u95ci","rr_p")]
    export.df$metric <- metric
    export.df$group_group_item <- paste0(export.df$group,"_",export.df$group_item)
    # Add ranks
    export.df.rank <- data.frame()
    for(group_group_item in unique(export.df$group_group_item)){
      d <- export.df[export.df$group_group_item==group_group_item,]
      d$rank_within_group_item <- rank(-1*d$result)
      export.df.rank <- rbind(export.df.rank, d)
      remove(d)
    }
    export.df <- export.df.rank
    export.df$group_group_item <- NULL
    export.df$selected_diagnosis[export.df$diagnosis%in%selected_dcode_names] <- "Yes"
    export.df$selected_diagnosis[!export.df$diagnosis%in%selected_dcode_names] <- "No"
    remove(export.df.rank)
    export.df <- export.df[,c("analysis","metric","diagnosis","group","group_item","result","rank_within_group_item","rr","rr_l95ci","rr_u95ci","rr_p","selected_diagnosis")]
    
    output.data <- rbind(output.data, export.df)
    
  }
}
output.data$analysis[output.data$analysis=="unadjusted"] <- "Unadjusted"
output.data$analysis[output.data$analysis=="fullyadjusted"] <- "Adjusted"
output.data$analysis <- ordered(output.data$analysis, levels=c("Adjusted","Unadjusted"))
output.data$metric <- ordered(output.data$metric, levels=levels(data$metric))
output.data <- output.data[with(output.data, order(output.data$analysis, output.data$metric, output.data$diagnosis, output.data$group, output.data$group_item)),]
writexl::write_xlsx(output.data, paste0(outputfolder,"/ccu072_01_subgroup_results.xlsx"))
remove(output.data)


###############################################################################
#     SUBGROUP PLOTS EXCEPT FOR ETHNICITY AND GEOGRAPHIC REGION (FIG 3)       #
###############################################################################

fig3_data <- fig3_data_all[fig3_data_all$group%in%c("age","comorbidity","deprivation","sex") & fig3_data_all$year==group_year,]
table(fig3_data$group)
cat("Subgroup plots - all except for ethnicity and geography\n")
for(analysis in sort(unique(fig3_data$analysis))){
  #analysis <- sort(unique(fig3_data$analysis))[1]
  cat(analysis,"\n")
  i <- 1
  output.plots <- list()
  for(metric in levels(data$metric)){
    #metric <- levels(data$metric)[1]
    cat("\t",as.character(metric),"\n")
    df <- fig3_data[fig3_data$analysis==analysis & fig3_data$metric==metric,]
    
    # Exponentiate and rescale outcomes
    df$result <- df$exp_intercept_plus_beta #exp(df$intercept_plus_beta)
    if(metric==incidence_label){
      label_accuracy <- 1 # 0.1
      unit <- incidence_unit
    } else if (metric==prevalence_label){
      df$result <- df$result * 100
      df$result[df$result<0.001] <- NA
      label_accuracy <- 0.01
      unit <- prevalence_unit
    } else if (metric==fatality_label){
      df$result <- df$result * 100
      df$result[df$result<0.1] <- NA
      label_accuracy <- 1 # 0.1
      unit <- fatality_unit
    } else if(metric==recurrence_label){
      label_accuracy <- 1 # 0.1 #0.00001
      df$result[df$result<100] <- NA
      df$result[df$result<1] <- NA
      unit <- recurrence_unit
    }

    # Selected diseases
    df$original_dcode_name <- df$dcode_name
    df$dcode_name <- str_trunc(df$dcode_name, 23)
    selected_dcodes <- get_dcodes_by_pc(metric)
    selected_dcode_names <- unique(df$dcode_name[df$dcode%in%selected_dcodes])
    
    # Rename labels
    df$group <- str_to_title(df$group)
    df$group_item[df$group_item=="3_4" & df$group=="Comorbidity"] <- "3-4\u0358" # Add invisible character so that this gets ordered correctly
    df$group_item[df$group_item=="1_2_most_deprived"] <- "1 most_deprived"
    df$group_item[df$group_item=="3_4"] <- "2"
    df$group_item[df$group_item=="5_6"] <- "3"
    df$group_item[df$group_item=="7_8"] <- "4"
    df$group_item[df$group_item=="9_10_least_deprived"] <- "5 least deprived"
    df$group_item[df$group_item=="5_over"] <- "5+"
    df$group_item[df$group_item=="90_over"] <- "90+"
    df$group_item <- gsub("_","-",df$group_item)
    df$group[df$group=="Comorbidity"] <- comorbidity_label
    
    # Order labels
    all_labels <- vector()
    for(group in unique(df$group)){
      # group <- unique(df$group)[3]
      group_labels <- sort(unique(df$group_item[df$group==group]))
      if(group%in%c("Region","Deprivation", "Ethnicity")){
        group_labels <- rev(group_labels)
      }
      # if(group=="Ethnicity"){
      #   group_labels <- rev(c("Other","Mixed","Black or Black British","Asian or Asian British","White"))
      # }
      all_labels <- unique(c(all_labels, group_labels))
      remove(group_labels)
    }
    df$group <- ordered(df$group, levels=c("Age","Sex","Deprivation","Long-term\nconditions"))
    df$group_item <- ordered(df$group_item, levels=all_labels)
    df <- df[with(df, order(df$dcode, df$group, df$group_item)),]
    
    # Set alpha based on numerator - one value per disease
    df_alpha <- df[df$group==df$group[1],c("dcode","raw_numerator")]
    df_alpha <- df_alpha[!duplicated(df_alpha),]
    df_alpha <- df_alpha %>% group_by(dcode) %>% summarise(alpha=sum(raw_numerator, na.rm=T)) %>% as.data.frame()
    df_alpha$alpha[is.infinite(df_alpha$alpha)] <- 0
    df_alpha$alpha = scales::rescale(df_alpha$alpha, alpha_range_subgroups)
    df_alpha$alpha[df_alpha$dcode%in%selected_dcodes] <- 1
    df <- merge(df, df_alpha)
    remove(df_alpha)

    # Colours
    df$colour <- "grey"
    for(dcode in selected_dcodes){
      df$colour[df$dcode==dcode] <- names(all_selected_dcodes[all_selected_dcodes==dcode])
    }
    colours <- as.character(df$colour)
    names(colours) <- as.character(df$dcode_name)
    # Line colours
    line_groups <- c("Age",comorbidity_label,"Deprivation")
    
    g <- ggplot(df, aes(x=group_item, y=result, group=dcode_name, colour=dcode_name, alpha=alpha)) + # size=alpha , label=dcode_name
      facet_grid(. ~ group, scales="free", space = "free") + # switch = "x", 
      geom_point() + geom_path(data=df[df$group%in%line_groups,]) +
      labs(y=unit, x="", title=paste0(LETTERS[i],". ",metric)) +
      scale_colour_manual(name="", values = colours, breaks = rev(selected_dcode_names)) +
      #scale_x_continuous(transform=axis_metric_transformation, breaks=breaks_log(), labels = label_number(accuracy = label_accuracy)) + # labels=axis_metric_label
      scale_y_continuous(transform=axis_metric_transformation, breaks=breaks_log()) + # labels=axis_metric_label
      #scale_x_discrete(guide = guide_axis(n.dodge=2)) +
      theme_classic() + 
      guides(alpha = "none", colour=guide_legend(nrow=4,byrow=TRUE)) +
      theme(plot.title = element_text(size=15, hjust = 0.5, face = "bold"),
            panel.grid.major = element_blank(),
            panel.grid.minor = element_blank(),
            axis.text.y = element_text(size=11),
            axis.text.x = element_text(size=11, angle = 45, vjust = 1, hjust=1), 
            legend.text = element_text(size=11),
            panel.grid = element_blank(), 
            strip.background = element_blank(), 
            strip.text = element_text(size=11, face = "bold"),
            #strip.placement = "outside",
            legend.position="bottom",
            legend.box.margin=margin(0,0,0,0)) # unit(c(top, right, bottom, left)
    g

    output.plots[[length(output.plots)+1]] <- g

    i <- i + 1
  }
  ggsave(paste0(outputfolder,"/ccu072_01_f3_subgroups_noethnicitynoregion_",analysis,"_rotated.pdf"), 
         height=297*2, width=180*2, limitsize=F, unit="mm", # 210
         device = cairo_pdf, onefile = TRUE,
         marrangeGrob(output.plots, 
                      layout_matrix = matrix(1:4, byrow = T, ncol = 2), top="", bottom=""))
  ggsave(paste0(outputfolder,"/ccu072_01_f3_subgroups_noethnicitynoregion_",analysis,"_rotated_landscape.pdf"), 
         height=180*2, width=297*2, limitsize=F, unit="mm", # 210
         device = cairo_pdf, onefile = TRUE,
         marrangeGrob(output.plots, 
                      layout_matrix = matrix(1:4, byrow = T, ncol = 2), top="", bottom=""))
}


########################################################
#     SUBGROUP PLOTS FOR ETHNICITY (FIG 3 STYLE)       #
########################################################

fig3_data <- fig3_data_all[fig3_data_all$group==c("ethnicity"),]
table(fig3_data$group)
cat("Subgroup plots - ethnicity only\n")
for(analysis in sort(unique(fig3_data$analysis))){
  cat(analysis,"\n")
  i <- 1
  output.plots <- list()
  for(metric in levels(data$metric)){
    cat("\t",as.character(metric),"\n")
    df <- fig3_data[fig3_data$analysis==analysis & fig3_data$metric==metric,]
    
    # Exponentiate and rescale outcomes
    df$result <- df$exp_intercept_plus_beta #exp(df$intercept_plus_beta)
    if(metric==incidence_label){
      label_accuracy <- 1 # 0.1
      unit <- incidence_unit
    } else if (metric==prevalence_label){
      df$result <- df$result * 100
      df$result[df$result<0.001] <- NA
      label_accuracy <- 0.01
      unit <- prevalence_unit
    } else if (metric==fatality_label){
      df$result <- df$result * 100
      df$result[df$result<1] <- NA
      label_accuracy <- 1 # 0.1
      unit <- fatality_unit
    } else if(metric==recurrence_label){
      label_accuracy <- 1 # 0.1 #0.00001
      df$result[df$result<100] <- NA
      df$result[df$result<1] <- NA
      unit <- recurrence_unit
    }
    df$result[df$result<10^-6] <- NA
    df$result[df$result>10^6] <- NA

    # Selected diseases
    df$original_dcode_name <- df$dcode_name
    df$dcode_name <- str_trunc(df$dcode_name, 35)
    selected_dcodes <- get_dcodes_by_pc(metric)
    selected_dcode_names <- unique(df$dcode_name[df$dcode%in%selected_dcodes])
    
    # Rename labels
    df$group <- str_to_title(df$group)

    # Order labels
    all_labels <- vector()
    for(group in unique(df$group)){
      # group <- unique(df$group)[2]
      group_labels <- sort(unique(df$group_item[df$group==group]))
      if(group%in%c("Region","Deprivation", "Ethnicity")){
        group_labels <- rev(group_labels)
      }
      # if(group=="Ethnicity"){
      #   group_labels <- rev(c("Other","Mixed","Black or Black British","Asian or Asian British","White"))
      # }
      all_labels <- unique(c(all_labels, group_labels))
      remove(group_labels)
    }
    df$group_item <- ordered(df$group_item, levels=all_labels)
    df <- df[with(df, order(df$dcode, df$group, df$group_item)),]
    
    # Set alpha based on numerator - one value per disease
    df_alpha <- df[df$group==df$group[1],c("dcode","raw_numerator")]
    df_alpha <- df_alpha[!duplicated(df_alpha),]
    df_alpha <- df_alpha %>% group_by(dcode) %>% summarise(alpha=sum(raw_numerator, na.rm=T)) %>% as.data.frame()
    df_alpha$alpha[is.infinite(df_alpha$alpha)] <- 0
    df_alpha$alpha = scales::rescale(df_alpha$alpha, alpha_range_subgroups)
    df_alpha$alpha[df_alpha$dcode%in%selected_dcodes] <- 1
    df <- merge(df, df_alpha)
    remove(df_alpha)

    # Colours
    df$colour <- "grey"
    for(dcode in selected_dcodes){
      df$colour[df$dcode==dcode] <- names(all_selected_dcodes[all_selected_dcodes==dcode])
    }
    colours <- as.character(df$colour)
    names(colours) <- as.character(df$dcode_name)
    # Line colours
    line_groups <- c("Age",comorbidity_label,"Deprivation")
    
    g <- ggplot(df, aes(y=group_item, x=result, group=dcode_name, colour=dcode_name, alpha=alpha)) + # size=alpha , label=dcode_name
      facet_grid(group ~ ., scales="free", switch = "y", space = "free") + # scales="free", switch = "y", independent = "x"
      geom_point() + geom_path(data=df[df$group%in%line_groups,]) +
      labs(x=unit, y="", title=paste0(LETTERS[i],". ",metric)) +
      scale_colour_manual(name="", values = colours, breaks = rev(selected_dcode_names)) +
      #scale_x_continuous(transform=axis_metric_transformation, breaks=breaks_log(), labels = label_number(accuracy = label_accuracy)) + # labels=axis_metric_label
      scale_x_continuous(transform=axis_metric_transformation, breaks=breaks_log()) + # labels=axis_metric_label
      theme_classic() + 
      guides(alpha = "none", colour=guide_legend(nrow=3,byrow=TRUE)) +
      theme(plot.title = element_text(size=15, hjust = 0.5, face = "bold"),
            panel.grid.major = element_blank(),
            panel.grid.minor = element_blank(),
            panel.grid = element_blank(), strip.background = element_blank(), strip.text = element_text(size=11, face = "bold"),
            strip.placement = "outside",
            legend.position="bottom",
            legend.box.margin=margin(0,0,0,-130)) # unit(c(top, right, bottom, left)
    g
    
    output.plots[[length(output.plots)+1]] <- g
    
    i <- i + 1
  }
  ggsave(paste0(outputfolder,"/ccu072_01_f4_subgroups_ethnicity_",analysis,".pdf"), width=297*2, height=210*2, limitsize=F, unit="mm", # 210
         device = cairo_pdf, onefile = TRUE,
         marrangeGrob(output.plots, 
                      layout_matrix = matrix(1:4, byrow = T, ncol = 2), top="", bottom=""))
}


###########################
#     TABLE BY SOURCE     #
###########################

bysource_data_output <- bysource_data
bysource_data_output$diagnosis <- bysource_data_output$dcode_name
bysource_data_output$metric <- ordered(bysource_data_output$metric, levels=levels(data$metric))
bysource_data_output$date <- as.Date(paste0(bysource_data_output$year,"-",sprintf("%02d",bysource_data_output$month),"-01"))
bysource_data_output$events <- bysource_data_output$numerator
bysource_data_output$events[is.na(bysource_data_output$events)] <- "-"
bysource_data_output$result[is.na(bysource_data_output$result)] <- "-"
bysource_data_output$selected_diagnosis[bysource_data_output$dcode%in%fixed_dcodes] <- "Yes"
bysource_data_output$selected_diagnosis[!bysource_data_output$dcode%in%fixed_dcodes] <- "No"
bysource_data_output <- bysource_data_output[with(bysource_data_output, order(bysource_data_output$metric, bysource_data_output$diagnosis, bysource_data_output$source, bysource_data_output$date)),]
bysource_data_output <- bysource_data_output[,c("metric","diagnosis","source","date","events","result","selected_diagnosis")]
writexl::write_xlsx(bysource_data_output, paste0(outputfolder,"/ccu072_01_bysource.xlsx"))
head(bysource_data_output)
dim(bysource_data_output)
cat("\n")


############################
#     TRENDS BY SOURCE     #
############################

df <- bysource_data_output[bysource_data_output$diagnosis%in%unique(data$dcode_name[data$dcode%in%common_fixed_dcodes]),c("metric","diagnosis","source","date","result")]
df$diagnosis <- str_wrap(df$diagnosis, 30)
df$result <- as.numeric(df$result)
df$result[df$result==0] <- NA
df <- df[df$date<as.Date("2023-04-01"),]
summary(df$date)
head(df)
# Set labels
bysource_label <- c(paste0(incidence_label,"\n(",incidence_unit,")"),
                    paste0(prevalence_label,"\n(",prevalence_unit,")"), 
                    paste0(fatality_label,"\n(",fatality_unit,")"), 
                    paste0(recurrence_label,"\n(",recurrence_unit,")"),
                    sort(unique(df$diagnosis)))
names(bysource_label) <- c(incidence_label, prevalence_label, fatality_label, recurrence_label, sort(unique(df$diagnosis)))

ggplot(df, aes(x=date, y=result,  colour=source)) + geom_line() + 
  xlab("Years") + ylab("") +
  facet_grid(rows = vars(metric), cols = vars(diagnosis), scales = "free", switch = "y",
             labeller = as_labeller(bysource_label)) + 
  #scale_colour_manual(values=c("#E69F00", "#56B4E9")) + 
  scale_y_continuous(transform=axis_metric_transformation, breaks=breaks_log()) + # labels = label_number(accuracy = label_accuracy)
  theme_classic() + 
  scale_colour_brewer(palette = "Set1") +
  guides(color = guide_legend(nrow = 1)) +
  theme(strip.background = element_blank(),
        strip.text = element_text(face="bold"),
        strip.placement = "outside",
        legend.position = "bottom", legend.title = element_blank())
ggsave(paste0(outputfolder,"/ccu072_01_bysource.pdf"), width=210*1.5, height=297*1.5, unit="mm",
       device = cairo_pdf)


#################################
#     RESULTS DESCRIPTION       #
#################################

cat("Range of events and results for key phenotypes\n")
for(metric in levels(data$metric)){
  #metric <- levels(data$metric)[1]
  cat("\t",metric,"\n")
  selected_dcodes <- get_dcodes_by_pc(metric)
  for(dcode in selected_dcodes){
    #dcode <- selected_dcodes[1]
    cat("\t\t",unique(data$dcode_name[data$dcode%in%dcode]),"\n")
    df <- fig1_data[fig1_data$group=="All" & fig1_data$metric==metric & fig1_data$dcode==dcode,]
    cat("\t\t\tNumerator:",paste(range(df$numerator),collapse = "-"),"\n")
    cat("\t\t\tResult:",paste(range(format.numbers(df$result, digits = 1)),collapse = "-"),"\n")
  }
}


#####################################################################################################
#     COMPARISONS OF BURDEN OF STROKE/MI/HYPERTENSION/AF/HF/PVD VS BURDEN OF EVERYTHING ELSE)       #
#####################################################################################################
# By burden we mean average N of counts (numerator) across all time points, for each metric, using the data for Fig 1

ohid_main_dcodes <- c("X411_2", # MI
                          "X433_0", # Ischemic stroke
                          "X401_1", # hypertension
                          "X427_2", # atrial fibrillation
                          "X428_2", # HF, NOS
                          "XHFREF", # HF, REF
                          "X443_9") # PVD

for(metric in levels(data$metric)){
  #metric <- levels(data$metric)[3]
  cat(metric, "\n")
  df <- fig1_data[fig1_data$metric==metric,]
  
  # For each disease, get median numerator across all time points
  new_df <- data.frame()
  for(dcode in unique(df$dcode)){
    #dcode <- unique(df$dcode)[1]
    d <- df[df$dcode==dcode,]
    new_df <- rbind(new_df, data.frame(metric=metric,
                                       dcode=dcode,
                                       numerator=median(d$numerator, na.rm=T),stringsAsFactors = F))
  }
  new_df$main <- ifelse(new_df$dcode%in%ohid_main_dcodes, "Yes", "No")
  new_df <- new_df %>% group_by(main)%>% summarise(sum_counts=sum(numerator, na.rm = T)) %>% as.data.frame()
  new_df$pc_counts <- round(new_df$sum_counts/sum(new_df$sum_counts)*100)
  print(new_df)
}


######################################
#     TREND LINE PLOTS (FIG 1)       #
######################################

group <- "All"
output.plots <- list()
output.data <- data.frame()
i <- 1
for(metric in levels(data$metric)){
  #metric <- levels(data$metric)[3]
  cat(metric,"\n")
  df <- fig1_data[fig1_data$group==group & fig1_data$metric==metric,]
  df <- df[!is.na(df$result),]
  df <- df[df$result>0,]
  
  # Selected diseases
  selected_dcodes <- unique(get_dcodes_by_pc(metric)) # , max(data$date[data$metric==metric])
  
  # Interpolate last estimate, if non-existing, to ensure label is shown
  df$interpolation <- 0
  for(dcode in selected_dcodes){
    if(max(df$date[df$dcode==dcode])<max(df$date)){
      d <- df[df$dcode==dcode,]
      min_value <- min(d$result)
      min_numerator <- min(d$numerator[d$result==min_value])
      d <- d[d$date==max(d$date),]
      d$result <- min_value
      d$numerator <- min_numerator
      d$date <- max(df$date)
      d$month <- month(d$date)
      d$year <- year(d$date)
      d$interpolation <- 1
      df <- rbind(df, d)
      remove(d)
    }
  }
  
  # Add polished dcode names to last date
  selected_dcode_names <- unique(df$dcode_name[df$dcode%in%selected_dcodes])
  df$polished_dcode_name <- NA
  df$polished_dcode_name[df$dcode%in%selected_dcodes & df$date==max(df$date, na.rm = T)] <- str_wrap(df$dcode_name[df$dcode%in%selected_dcodes & df$date==max(df$date, na.rm = T)], 15)
  table(df$polished_dcode_name)
  
  # Set alpha based on numerator - one value per disease
  df_alpha <- df %>% group_by(dcode, dcode_name) %>% summarise(alpha=sum(numerator, na.rm=T)) %>% as_data_frame()
  df_alpha$alpha[is.infinite(df_alpha$alpha)] <- 0
  df_alpha$alpha = scales::rescale(df_alpha$alpha, alpha_range_trends)
  df_alpha$alpha[df_alpha$dcode_name%in%selected_dcode_names] <- 1
  df <- merge(df, df_alpha)
  remove(df_alpha)

  # Set colours
  df$colour <- "darkgrey"
  for(dcode in selected_dcodes){
    #dcode <- "XICVT"
    df$colour[df$dcode==dcode] <- names(all_selected_dcodes[all_selected_dcodes==dcode])
  }
  colours <- as.character(df$colour)
  names(colours) <- as.character(df$dcode_name)
  
  # Specify plot-specific groups
  date_groups <- list(c(min(df$date), date_group_dates[1]-1),
                      c(date_group_dates[1], date_group_dates[2]-1), 
                      c(date_group_dates[2], max(df$date)))
  names(date_groups) <- date_group_names


  #df$result[df$result==0] <- min(df$result[df$result!=0])/5
  if(metric==incidence_label){
    label_accuracy <- 0.1
    unit <- incidence_unit
  } else if (metric==prevalence_label){
    label_accuracy <- 0.01
    unit <- prevalence_unit
  } else if (metric==fatality_label){
    #df$result <- df$result * 100
    label_accuracy <- 1
    unit <- fatality_unit
  } else if(metric==recurrence_label){
    label_accuracy <- 1 # 0.00001
    unit <- recurrence_unit
  }
  
  g <- ggplot(df, aes(x=date, y=result, group=dcode_name, colour=dcode_name, alpha=alpha, label=polished_dcode_name)) + geom_line() + 
    labs(x="Years", y=unit, title=paste0(LETTERS[i],". ",metric)) +
    geom_vline(xintercept = date_groups[[2]][1], linetype="dashed", colour="grey") + 
    geom_vline(xintercept = date_groups[[3]][1], linetype="dashed", colour="grey") + 
    annotate("text", label = names(date_groups)[1], x=mean(date_groups[[1]]), y=1.7*max(df$result,na.rm=T), size = 4, colour = "black") +
    annotate("text", label = names(date_groups)[2], x=mean(date_groups[[2]]), y=1.7*max(df$result,na.rm=T), size = 4, colour = "black") +
    annotate("text", label = names(date_groups)[3], x=mean(date_groups[[3]]), y=1.7*max(df$result,na.rm=T), size = 4, colour = "black") +
    scale_colour_manual(values = colours) +
    coord_cartesian(xlim = c(min(df$date), max(df$date)), # This focuses the x-axis on the range of interest # as.Date("2023-8-31")
                    #ylim = c(min(df$result), 1.2*max(df$result)),
                    clip = 'off') +   # This keeps the labels from disappearing
    scale_y_continuous(transform=axis_metric_transformation, breaks=breaks_log(), labels = label_number(accuracy = label_accuracy)) + # labels=axis_metric_label # # scales::pseudo_log_trans(base=axis_metric_transformation_base)
    geom_text_repel(x = max(df$date),
                    hjust = 0,
                    na.rm = FALSE,
                    xlim = c(max(df$date)+365.25/12, max(df$date)+1.3*365.25),
                    segment.linetype = "dotted", linewidth = 0.1,
                    size = 4, direction = "y", force=20, nudge_y = -0.1, max.date=10, max.iter=100000, box.padding=0.7) +
    theme_classic() + 
    theme(plot.title = element_text(size=15, hjust = 0.5, face = "bold"),
          legend.position = "none", plot.margin = unit(c(0.1, 2.5, 0.1, 0), "cm")) # unit(c(top, right, bottom, left)
  g
  
  output.plots[[length(output.plots)+1]] <- g
  
  df$events <- df$numerator
  df$events[is.na(df$events)] <- "-"
  export.df <- df[df$interpolation==0,]
  export.df <- export.df[,c("metric","dcode_name","date","events","result")]
  export.df$metric <- metric
  export.df$date <- as.character(export.df$date)
  reference_date <- "2020-01-01" # reference_date <- "2020-02-01"
  change_dates <- sort(unique(export.df$date[export.df$date!=reference_date]))
  export.df.pc <- data.frame()
  for(dcode_name in unique(export.df$dcode_name)){
    d <- export.df[export.df$dcode_name==dcode_name,]
    d$pc_change <- NA
    for(change_date in change_dates){
      if(nrow(d[d$date==reference_date | d$date==change_date,])==2){
        d$pc_change[d$date==change_date] <- round((d$result[d$date==change_date]-d$result[d$date==reference_date])/d$result[d$date==reference_date]*100)
      }
    }
    export.df.pc <- rbind(export.df.pc, d)
    remove(d)
  }
  export.df.pc$pc_change[export.df.pc$date==reference_date] <- "Reference"
  export.df.pc$selected_diagnosis[export.df.pc$dcode_name%in%selected_dcode_names] <- "Yes"
  export.df.pc$selected_diagnosis[!export.df.pc$dcode_name%in%selected_dcode_names] <- "No"
  names(export.df.pc)[names(export.df.pc) == "dcode_name"] <- "diagnosis"
  output.data <- rbind(output.data, export.df.pc)
  remove(export.df, export.df.pc)
  
  i <- i + 1
}
ggsave(paste0(outputfolder,"/ccu072_01_f1_trendlines_landscape.pdf"), width=297*1.5, height=210*1.5, limitsize=F, unit="mm", # 210
       device = cairo_pdf, onefile = TRUE,
       marrangeGrob(output.plots,  layout_matrix = matrix(1:4, byrow = T, ncol = 2), top="", bottom=""))
ggsave(paste0(outputfolder,"/ccu072_01_f1_trendlines.pdf"), width=210*1.5, height=297*1.5, limitsize=F, unit="mm", # 210
       device = cairo_pdf, onefile = TRUE,
       marrangeGrob(output.plots,  layout_matrix = matrix(1:4, byrow = T, ncol = 2), top="", bottom=""))
output.data$metric <- ordered(output.data$metric, levels=levels(data$metric))
output.data <- output.data[!is.na(output.data$metric),]
output.data$date <- as.Date(output.data$date)
output.data$pc_change[output.data$pc_change==Inf] <- NA
output.data$pc_change[is.na(output.data$pc_change)] <- "-"
output.data <- output.data[with(output.data, order(output.data$metric, output.data$diagnosis, output.data$date)),]
writexl::write_xlsx(output.data, paste0(outputfolder,"/ccu072_01_f1_trendlines.xlsx"))


cat("\n\n\nDONE.\n")