# Use reticulate for now. R package requires ODBC installation separately
library(reticulate)
library(data.table)
library(dplyr)
source_python(paste0('./src/1. Data Functions.py'))


# connect to snowflake
OKTA_USER <- Sys.getenv("OKTA_USER")
OKTA_PW <- Sys.getenv("OKTA_PW")

snowflake <- get_connections_fn(
  OKTA_USER=OKTA_USER,
  OKTA_PASSWORD=OKTA_PW
)
Sys.sleep(2)
conn <- snowflake("TRANSIENT")


# Do data stuff
# Here we just get current timestamp
now <- format(Sys.time(), "%b-%y %H:%M:%S")
df <- data.frame(now)
colnames(df) <- c("DESCRIPTION")


# Push file back to snowflake
# table_name = "TESTING_R_TABLE"
# tmp = conn
snake_case <- function(camelCase){
  gsub("([a-z])([A-Z])", "\\1_\\L\\2", camelCase, perl = TRUE)
}


snowflake_create_query_r <- function(tbl_df, table_name) {
  df <- as.data.table(head(tbl_df))
  paste(
    "CREATE or REPLACE TABLE"
    ,  table_name
    , "("
    , sapply(colnames(df), function(x) {
      sx <- snake_case(x)
      ifelse(is.character(df[, get(x)]) | is.factor(df[, get(x)]), paste(sx, "VARCHAR"),
             ifelse((is.Date(df[, get(x)])) | (is.POSIXct(df[, get(x)])), paste(sx, "TIMESTAMP_NTZ"),
                    ifelse(is.logical(df[, get(x)]), paste(sx, "BOOLEAN"),
                           ifelse(is.numeric(df[, get(x)]), paste(sx, "NUMBER(36, 9)"),
                                  print(x)))))
    }) %>% paste(collapse=", \n")
    , ")")
}


upload_table_r <- function(df, tmp, table_name) {
  query = snowflake_create_query_r(as.data.frame(head(df)), table_name)
  tmp$cursor()$execute(query)
  tmp$cursor()$execute(paste("CREATE or REPLACE TEMPORARY STAGE", table_name))
  tempf.name <- tempfile(fileext = '.csv.gz')
  fwrite(df, tempf.name, compress="gzip", col.names=FALSE)
  path <- paste0("PUT 'file://", tempf.name, "' @%", table_name)
  tmp$cursor()$execute(gsub("\\\\", "//", path))
  tmp$cursor()$execute(paste("COPY INTO", table_name, "purge = true"))
  unlink(tempf.name)
}

upload_table_r(df, conn, "TESTING_R_TABLE")



#x <- rnorm(1:10)
#save(x, file = paste0("data-raw/data_", make.names(Sys.time()), ".Rda"))
#save(x, file = paste0("data_", make.names(Sys.time()), ".Rda"))
