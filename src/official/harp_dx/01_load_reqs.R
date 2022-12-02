##  Generate pre-requisites and endpoints --------------------------------------

download_corrections <- function() {
   check <- input(
      prompt  = glue("Re-download the {green('data corrections')}?"),
      options = c("1" = "yes", "2" = "no"),
      default = "2"
   )
   if (check == "1") {
      .log_info("Downloading corrections list.")
      nhsss <- gdrive_correct2(nhsss, ohasis$ym, "harp_dx", speed = FALSE)
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
      .log_info("Updating data lake and data warehouse.")
      local(envir = nhsss$harp_dx, invisible({
         tables           <- list()
         tables$warehouse <- c("form_a", "form_hts", "id_registry")

         lapply(tables$warehouse, function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE))
      }))
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
      .log_info("Getting previous datasets.")
      local(envir = nhsss$harp_dx, {
         official     <- list()
         official$old <- ohasis$load_old_dta(
            path            = ohasis$get_data("harp_dx", ohasis$prev_yr, ohasis$prev_mo),
            corr            = corr$old_reg,
            warehouse_table = "harp_dx_old",
            id_col          = c("idnum" = "integer"),
            dta_pid         = "PATIENT_ID",
            remove_cols     = "CENTRAL_ID",
            remove_rows     = corr$anti_join
         )
      })
   }
   rm(check)
}

.init <- function () {
   download_corrections()
   update_warehouse()
   update_dataset()
}