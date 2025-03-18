setwd("~/collab/CCU072_01")
suppressMessages(source("00_init.R"))

for(analysis in table_names_df$analysis){
  input_file <- table_names_df$table_name[table_names_df$analysis==analysis]
  cat("Creating split files for the",analysis,"analysis. Loading from CSV file",input_file,"\n")
  data <- fread(input_file, data.table = F)
  print(head(data))
  cat("\tGetting vectors of unique metrics and dcodes\n")
  metrics <- sort(unique(data$metric))
  dcodes <- sort(unique(data$dcode))
  for(metric in metrics){
    for(dcode in dcodes){
      cat("\tSelecting",metric,"-",dcode,"\n")
      d <- data[data$metric==metric & data$dcode==dcode,] 
      cat("\t\tDimensions:",dim(d),"\n")
      d_file <- paste0(input_folder,"/metric_dcode_files/",metric,"_",dcode,"_",analysis,".csv")
      cat("\t\tWriting to",d_file,"\n")
      fwrite(d, d_file)
      remove(d, d_file)
    }  
  }
}