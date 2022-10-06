##  Generate pre-requisites and endpoints --------------------------------------

check <- input(
   prompt  = glue("Re-download the {green('data corrections')}?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Downloading corrections list.")
   # local(envir = nhsss$harp_pmtct, {
   #    .log_info("Getting corrections.")
   #    corr <- gdrive_correct2(gdrive$path, ohasis$ym)
   # })
   nhsss <- gdrive_correct2(nhsss, ohasis$ym, "harp_pmtct")
}

# run through all tables
check <- input(
   prompt  = glue("Update {green('data/forms')} to be used for consolidation?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Updating data lake and data warehouse.")
   local(envir = nhsss$harp_pmtct, invisible({
      tables           <- list()
      tables$lake      <- c("lab_wide", "disp_meds")
      tables$warehouse <- c("form_pmtct", "form_amc_mom", "form_amc_mom_children", "form_amc_child", "id_registry")

      lapply(tables$lake, function(table) ohasis$data_factory("lake", table, "upsert", TRUE))
      lapply(tables$warehouse, function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE))
   }))
}

##  Download records -----------------------------------------------------------

update <- input(
   prompt  = "Do you want to download the relevant form data?",
   options = c("1" = "Yes", "2" = "No"),
   default = "1"
)
# if Yes, re-process
if (update == "1") {
   local(envir = nhsss$harp_pmtct, {
      lw_conn <- ohasis$conn("lw")
      forms   <- list()

      min <- as.Date(paste(sep = "-", ohasis$yr, ohasis$mo, "01"))
      max <- (min %m+% months(1)) %m-% days(1)
      min <- as.character(min)
      max <- as.character(max)

      .log_info("Downloading {green('Central IDs')}.")
      forms$id_registry <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "id_registry",
         cols = c("CENTRAL_ID", "PATIENT_ID")
      )

      .log_info("Downloading {green('A-MC (Mother)')}.")
      forms$amc_mom <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "form_amc_mom"
      )

      .log_info("Downloading {green('A-MC (Mother-Children)')}.")
      forms$amc_mom_children <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "form_amc_mom_children"
      )

      .log_info("Downloading {green('A-MC (Child)')}.")
      forms$amc_child <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "form_amc_child"
      )

      .log_info("Downloading {green('PMTCT')}.")
      forms$amc_child <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "form_pmtct"
      )

      .log_info("Downloading {green('ARVs Disepensed w/in the scope')}.")
      forms$disp_meds <- dbTable(
         lw_conn,
         "ohasis_lake",
         "disp_meds",
         where     = glue("(DATE(DISP_DATE) >= '{min}' AND DATE(DISP_DATE) <= '{max}') OR (DATE(CREATED_AT) >= '{min}' AND DATE(CREATED_AT) <= '{max}') OR (DATE(UPDATED_AT) >= '{min}' AND DATE(UPDATED_AT) <= '{max}')"),
         raw_where = TRUE
      )

      .log_info("Downloading {green('CD4 Data')}.")
      forms$lab_cd4 <- dbTable(
         lw_conn,
         "ohasis_lake",
         "lab_cd4",
         cols      = c(
            "CD4_DATE",
            "CD4_RESULT",
            "PATIENT_ID"
         ),
         where     = glue("DATE(CD4_DATE) < '{ohasis$next_date}'"),
         raw_where = TRUE,
         join      = list(
            "ohasis_warehouse.id_registry" = list(by = c("PATIENT_ID" = "PATIENT_ID"), cols = "CENTRAL_ID")
         )
      ) %>%
         mutate(
            CENTRAL_ID = if_else(
               condition = is.na(CENTRAL_ID),
               true      = PATIENT_ID,
               false     = CENTRAL_ID
            )
         ) %>%
         relocate(CENTRAL_ID, .before = 1)

      .log_success("Done.")
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
   local(envir = nhsss$harp_pmtct, {
      official         <- list()
      official$old_reg <- ohasis$load_old_dta(
         path            = ohasis$get_data("harp_pmtct-reg", ohasis$prev_yr, ohasis$prev_mo),
         corr            = corr$old_reg,
         warehouse_table = "harp_pmtct_old",
         id_col          = c("art_id" = "integer"),
         dta_pid         = "PATIENT_ID",
         remove_cols     = "CENTRAL_ID",
         remove_rows     = corr$anti_join,
         id_registry     = forms$id_registry
      )

      official$old_outcome <- ohasis$get_data("harp_pmtct-outcome", ohasis$prev_yr, ohasis$prev_mo) %>%
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