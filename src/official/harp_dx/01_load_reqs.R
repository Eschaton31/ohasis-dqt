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
         tables$lake      <- c(
            "px_pii",
            "px_faci_info",
            "px_ob",
            "px_hiv_testing",
            "px_consent",
            "px_occupation",
            "px_ofw",
            "px_risk",
            "px_expose_profile",
            "px_test_reason",
            "px_test_refuse",
            "px_test_previous",
            "px_med_profile",
            "px_staging",
            "px_cfbs",
            "px_reach",
            "px_linkage",
            "px_other_service"
         )
         tables$warehouse <- c("form_a", "form_hts", "id_registry")

         lapply(tables$lake, function(table) ohasis$data_factory("lake", table, "upsert", TRUE))
         lapply(tables$warehouse, function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE))
      }))
   }
}

##  get pdf results ------------------------------------------------------------

get_rhivda_pdf <- function() {
   .log_info("Loading list of rHIVda PDF Results.")
   rhivda <- dir_info(file.path(Sys.getenv("DRIVE_DROPBOX"), "File requests/rHIVda Submission/FORMS", ohasis$ym), recurse = TRUE)
   rhivda %<>%
      filter(type == "file") %>%
      mutate(
         CONFIRM_CODE = str_extract(basename(path), "[A-Z][A-Z][A-Z][0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9][0-9]"),
         .before      = 1
      ) %>%
      filter(!is.na(CONFIRM_CODE))

   .GlobalEnv$nhsss$harp_dx$pdf_rhivda$data <- rhivda
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
            path            = hs_data("harp_dx", "reg", ohasis$prev_yr, ohasis$prev_mo),
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

define_params <- function() {
   local(envir = nhsss$harp_dx, {
      params              <- list()
      params$p10y         <- (as.numeric(ohasis$yr) - 10)
      params$latest_idnum <- max(as.integer(official$old$idnum), na.rm = TRUE)
   })
}

.init <- function() {
   download_corrections()
   update_warehouse()
   update_dataset()
   define_params()
   get_rhivda_pdf()
}