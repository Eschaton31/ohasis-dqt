##  Generate pre-requisites and endpoints --------------------------------------

download_corrections <- function() {
   check <- input(
      prompt  = glue("Re-download the {green('data corrections')}?"),
      options = c("1" = "yes", "2" = "no"),
      default = "2"
   )
   if (check == "1") {
      log_info("Downloading corrections list.")
      nhsss <- gdrive_correct2(nhsss, ohasis$ym, "harp_dead")
   }
}

# run through all tables
update_warehouse <- function() {
   check <- input(
      prompt  = glue("Update {green('data/forms')} to be used for consolidation?"),
      options = c("1" = "yes", "2" = "no"),
      default = "2"
   )
   if (check == "1") {
      log_info("Updating data lake and data warehouse.")
      local(envir = nhsss$harp_tx, invisible({
         tables           <- list()
         tables$warehouse <- c("form_d", "id_registry")

         lapply(tables$warehouse, function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE))
      }))
   }
}

##  Download records -----------------------------------------------------------

download_tables <- function() {
   update <- input(
      prompt  = "Do you want to download the relevant form data?",
      options = c("1" = "Yes", "2" = "No"),
      default = "1"
   )
   # if Yes, re-process
   if (update == "1") {
      local(envir = nhsss$harp_dead, {
         lw_conn <- ohasis$conn("lw")
         forms   <- list()

         min <- as.Date(paste(sep = "-", ohasis$yr, ohasis$mo, "01"))
         max <- (min %m+% months(1)) %m-% days(1)
         min <- as.character(min)
         max <- as.character(max)

         log_info("Downloading {green('Central IDs')}.")
         forms$id_registry <- dbTable(
            lw_conn,
            "ohasis_warehouse",
            "id_registry",
            cols = c("CENTRAL_ID", "PATIENT_ID")
         )

         log_info("Downloading {green('ART Visits w/in the scope')}.")
         forms$form_d <- dbTable(
            lw_conn,
            "ohasis_warehouse",
            "form_d"
         )

         log_success("Done.")
         dbDisconnect(lw_conn)
         rm(min, max, lw_conn)
      })
   }
}

##  Get the previous report's HARP Registry ------------------------------------

update_dataset <- function() {
   check <- input(
      prompt  = "Reload previous dataset?",
      options = c("1" = "yes", "2" = "no"),
      default = "2"
   )
   if (check == "1") {
      log_info("Getting previous datasets.")
      local(envir = nhsss$harp_dead, {
         official     <- list()
         official$old <- ohasis$load_old_dta(
            path            = ohasis$get_data("harp_dead", ohasis$prev_yr, ohasis$prev_mo),
            corr            = corr$old_reg,
            warehouse_table = "harp_dead_old",
            id_col          = c("mort_id" = "integer"),
            dta_pid         = "PATIENT_ID",
            remove_cols     = "CENTRAL_ID",
            remove_rows     = corr$anti_join,
            id_registry     = forms$id_registry
         )
      })
   }
   rm(check)
}

define_params <- function() {
   local(envir = nhsss$harp_dead, {
      params                <- list()
      params$latest_mort_id <- max(as.integer(official$old$mort_id), na.rm = TRUE)
   })
}

.init <- function() {
   download_corrections()
   update_warehouse()
   download_tables()
   update_dataset()
   define_params()
}