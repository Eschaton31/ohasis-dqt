##------------------------------------------------------------------------------
##  HARP Registry Linkage Controller
##------------------------------------------------------------------------------

# update warehouse - Form A
ohasis$warehouse(table = "form_a", path = "src/data_warehouse/upsert")

# update warehouse - HTS Form
ohasis$warehouse(table = "form_hts", path = "src/data_warehouse/upsert")

# update warehouse - OHASIS IDs
ohasis$warehouse(table = "id_registry", path = "src/data_warehouse/upsert")

# define datasets
if (!exists('nhsss'))
   nhsss <- list()

# open connections
log_info("Opening connections...")
dw_conn <- ohasis$conn("dw")
db_conn <- ohasis$conn("db")

# check if registry is to be re-loaded
nhsss$harp_dx$reload <- ohasis$input(prompt = "Reload the previous HARP Dx dataset?", c("yes", "no"), "yes")
nhsss$harp_dx$reload <- StrLeft(nhsss$harp_dx$reload, 1) %>% toupper()

# if Yes, reload registry
if (nhsss$harp_dx$reload == "Y") {
   log_info("Reloading the HARP Dx dataset from the previous reporting period.")
   df <- ohasis$get_data("harp_dx", ohasis$prev_yr, ohasis$prev_mo) %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      )

   # remove legacy columns
   legaci_ids <- c("rec_id", "central_id")
   for (id in legaci_ids) {
      if (id %in% names(df))
         df <- df %>% select(-as.symbol(!!id))
   }
   rm(id, legaci_ids)

   log_info("Updating `harp_dx`.")
   # delete existing data, full refresh always
   if (dbExistsTable(dw_conn, "harp_dx"))
      dbExecute(dw_conn, "DROP TABLE `harp_dx`;")

   # upload info
   ohasis$upsert(dw_conn, "harp_dx", df %>% select(REC_ID, PATIENT_ID, idnum), "PATIENT_ID")
   rm(df)
}

##------------------------------------------------------------------------------
##  Begin linkage of datasets
##------------------------------------------------------------------------------

# source("01_initial.R")
dbDisconnect(dw_conn)
dbDisconnect(db_conn)