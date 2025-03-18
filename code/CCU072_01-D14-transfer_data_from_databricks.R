library(DBI)
library(data.table)
con <- dbConnect(
odbc::odbc(),
dsn = 'databricks',
HTTPPath = 'sql/protocolv1/o/847064027862604/0622-162121-dts9kxvy',
PWD = rstudioapi::askForPassword('Please enter Databricks PAT')
)

setwd("/db-mnt/databricks/rstudio_collab/CCU072_01/Data")

df <- NULL
for (i in 1:10) {
 print(paste0("Transferring chunk ",i," of ",10,"."))
 tmp <- DBI::dbGetQuery(con, paste0("SELECT * FROM dsa_391419_j3w9t_collab.ccu072_01_results_allcovadj_year_weight_age1_ethnicregion"
                                    ," WHERE CHUNK=",i
                                    ))
df <- rbind(df,tmp)
}

fwrite(df,"Results_year_allcovadj_weight_age1_ethnicregion.csv")