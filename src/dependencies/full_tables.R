# loading of generic table data
full_table <- function(db_conn = NULL, limiter = NULL, table_name = NULL) {
   table_file <- file.path(getwd(), "src", "full_tables", paste0(table_name, '.R'))
   source(table_file, local = TRUE)

   return(data)
}