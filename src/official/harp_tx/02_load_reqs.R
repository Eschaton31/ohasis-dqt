##  Generate pre-requisites and endpoints --------------------------------------

check <- input(
   prompt  = glue("Check the {green('GDrive Endpoints')}?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Checking endpoints.")
   local(envir = nhsss$harp_tx, {
      gdrive      <- list()
      gdrive$path <- gdrive_endpoint("HARP Tx", ohasis$ym)
   })
}

check <- input(
   prompt  = glue("Re-download the {green('data corrections')}?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Downloading corrections list.")
   # local(envir = nhsss$harp_tx, {
   #    .log_info("Getting corrections.")
   #    corr <- gdrive_correct2(gdrive$path, ohasis$ym)
   # })
   nhsss <- gdrive_correct2(nhsss, ohasis$ym, "harp_tx")
}

# run through all tables
check <- input(
   prompt  = glue("Update {green('data/forms')} to be used for consolidation?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Updating data lake and data warehouse.")
   local(envir = nhsss$harp_tx, invisible({
      tables           <- list()
      tables$lake      <- c("lab_wide", "disp_meds")
      tables$warehouse <- c("form_art_bc", "id_registry")

      lapply(tables$lake, function(table) ohasis$data_factory("lake", table, "upsert", TRUE))
      lapply(tables$warehouse, function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE))
   }))
}

##  Get earliest & latest visit data -------------------------------------------

# check if art starts to be re-processed
update <- input(
   prompt  = "Do you want to re-process the ART Start Dates?",
   options = c("1" = "Yes", "2" = "No"),
   default = "1"
)
# if Yes, re-process
if (update == "1") {
   lw_conn <- ohasis$conn("lw")
   db_name <- "ohasis_warehouse"

   # download the data
   .log_info("Downloading dataset.")
   data <- dbGetQuery(
      lw_conn,
      read_file(file.path(nhsss$harp_tx$wd, "art_first.sql")),
      params = as.character(ohasis$next_date)
   )

   # update lake
   .log_info("Clearing old data.")
   table_space <- Id(schema = "ohasis_warehouse", table = "art_first")
   if (dbExistsTable(lw_conn, table_space))
      dbxDelete(lw_conn, table_space, batch_size = 1000)

   .log_info("Payload = {red(formatC(nrow(data), big.mark = ','))} rows.")
   ohasis$upsert(lw_conn, "warehouse", "art_first", data, c("CENTRAL_ID", "REC_ID"))
   .log_success("Done!")
   dbDisconnect(lw_conn)
   rm(db_name, lw_conn, data, table_space)
}

# check if art starts to be re-processed
update <- input(
   prompt  = "Do you want to re-process the ART Latest Dates?",
   options = c("1" = "Yes", "2" = "No"),
   default = "1"
)
# if Yes, re-process
if (update == "1") {
   lw_conn <- ohasis$conn("lw")
   db_name <- "ohasis_warehouse"

   # download the data
   .log_info("Downloading dataset.")
   data <- dbGetQuery(
      lw_conn,
      read_file(file.path(nhsss$harp_tx$wd, "art_last.sql")),
      params = as.character(ohasis$next_date)
   )

   # update lake
   .log_info("Clearing old data.")
   table_space <- Id(schema = "ohasis_warehouse", table = "art_last")
   if (dbExistsTable(lw_conn, table_space))
      dbxDelete(lw_conn, table_space, batch_size = 1000)

   .log_info("Payload = {red(formatC(nrow(data), big.mark = ','))} rows.")
   ohasis$upsert(lw_conn, "warehouse", "art_last", data, c("CENTRAL_ID", "REC_ID"))
   .log_success("Done!")
   dbDisconnect(lw_conn)
   rm(db_name, lw_conn, data, table_space)
}
##  Download records -----------------------------------------------------------

update <- input(
   prompt  = "Do you want to download the relevant form data?",
   options = c("1" = "Yes", "2" = "No"),
   default = "1"
)
# if Yes, re-process
if (update == "1") {
   local(envir = nhsss$harp_tx, {
      lw_conn <- ohasis$conn("lw")
      forms   <- list()

      min <- as.Date(paste(sep = "-", ohasis$yr, ohasis$mo, "01"))
      max <- (min %m+% months(1)) %m-% days(1)
      min <- as.character(min)
      max <- as.character(max)

      .log_info("Downloading {green('Central IDs')}.")
      forms$id_registry <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "id_registry")) %>%
         select(PATIENT_ID, CENTRAL_ID) %>%
         collect()

      .log_info("Downloading {green('Earliest ART Visits')}.")
      forms$art_first <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "art_first")) %>%
         select(CENTRAL_ID, REC_ID) %>%
         left_join(
            y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_art_bc")),
            by = "REC_ID"
         ) %>%
         collect()

      .log_info("Downloading {green('Latest ART Visits')}.")
      forms$art_last <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "art_last")) %>%
         select(CENTRAL_ID, REC_ID) %>%
         left_join(
            y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_art_bc")),
            by = "REC_ID"
         ) %>%
         collect()

      .log_info("Downloading {green('ART Visits w/in the scope')}.")
      forms$form_art_bc <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_art_bc")) %>%
         filter(
            (VISIT_DATE >= min & VISIT_DATE <= max) |
               (as.Date(CREATED_AT) >= min & as.Date(CREATED_AT) <= max) |
               (as.Date(UPDATED_AT) >= min & as.Date(UPDATED_AT) <= max)
         ) %>%
         collect()

      dbDisconnect(lw_conn)
      rm(min, max, lw_conn)
   })
}

##  Get the previous report's HARP Registry ------------------------------------

check <- input(
   prompt  = "Reload previous dataset?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Getting previous datasets.")
   local(envir = nhsss$harp_tx, {
      official         <- list()
      official$old_reg <- ohasis$load_old_dta(
         path            = ohasis$get_data("harp_tx-reg", ohasis$prev_yr, ohasis$prev_mo),
         corr            = corr$old_reg,
         warehouse_table = "harp_tx_old",
         id_col          = c("art_id" = "integer"),
         dta_pid         = "PATIENT_ID",
         remove_cols     = "CENTRAL_ID",
         remove_rows     = corr$anti_join
      )

      official$old_outcome <- ohasis$get_data("harp_tx-outcome", ohasis$prev_yr, ohasis$prev_mo) %>%
         read_dta() %>%
         # convert Stata string missing data to NAs
         mutate_if(
            .predicate = is.character,
            ~if_else(. == '', NA_character_, .)
         )

      # clean if any for cleaning found
      if (!is.null(corr$old_outcome)) {
         .log_info("Performing cleaning on the outcome dataset.")
         official$old_outcome <- .cleaning_list(official$old_outcome, corr$old_outcome, "ART_ID", "integer")
      }
   })
}
rm(check)