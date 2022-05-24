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

##  Get the previous report's HARP Registry ------------------------------------

check <- input(
   prompt  = "Reload previous dataset?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Getting previous datasets.")
   local(envir = nhsss$harp_tx, {
      .log_info("Getting corrections.")
      corr <- gdrive_correct(gdrive$path, ohasis$ym)

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