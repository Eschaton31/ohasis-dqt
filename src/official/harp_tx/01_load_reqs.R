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
   prompt  = glue("Do you want to re-process the {green('ART Start & Latest Dates')}?"),
   options = c("1" = "Yes", "2" = "No"),
   default = "1"
)
# if Yes, re-process
if (update == "1") {
   lw_conn <- ohasis$conn("lw")
   db_name <- "ohasis_warehouse"

   # download the data
   for (scope in c("art_first", "art_last")) {
      name <- case_when(
         scope == "art_first" ~ "ART Start Dates",
         scope == "art_last" ~ "ART Latest Visits",
      )
      .log_info("Processing {green(name)}.")

      # update lake
      table_space <- Id(schema = db_name, table = scope)
      if (dbExistsTable(lw_conn, table_space))
         dbxDelete(lw_conn, table_space, batch_size = 1000)

      dbExecute(
         lw_conn,
         glue(r"(INSERT INTO {db_name}.{scope}
         )", read_file(file.path(nhsss$harp_tx$wd, glue("{scope}.sql")))),
         params = as.character(ohasis$next_date)
      )
   }
   .log_success("Done!")
   dbDisconnect(lw_conn)
   rm(db_name, lw_conn, table_space, name, scope)
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
      forms$id_registry <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "id_registry",
         cols = c("CENTRAL_ID", "PATIENT_ID")
      )

      .log_info("Downloading {green('Earliest ART Visits')}.")
      forms$art_first <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "art_first",
         cols = c("CENTRAL_ID", "REC_ID"),
         join = list(
            "ohasis_warehouse.form_art_bc" = list(
               by   = c("REC_ID" = "REC_ID"),
               cols = c(
                  "REC_ID",
                  "CREATED_AT",
                  "PATIENT_ID",
                  "FORM_VERSION",
                  "CONFIRMATORY_CODE",
                  "UIC",
                  "PATIENT_CODE",
                  "PHILHEALTH_NO",
                  "PHILSYS_ID",
                  "FIRST",
                  "MIDDLE",
                  "LAST",
                  "SUFFIX",
                  "BIRTHDATE",
                  "AGE",
                  "AGE_MO",
                  "SEX",
                  "SELF_IDENT",
                  "SELF_IDENT_OTHER",
                  "CURR_PSGC_REG",
                  "CURR_PSGC_PROV",
                  "CURR_PSGC_MUNC",
                  "CURR_ADDR",
                  "FACI_ID",
                  "SUB_FACI_ID",
                  "SERVICE_FACI",
                  "SERVICE_SUB_FACI",
                  "FACI_DISP",
                  "SUB_FACI_DISP",
                  "CLIENT_TYPE",
                  "TX_STATUS",
                  "WHO_CLASS",
                  "VISIT_TYPE",
                  "VISIT_DATE",
                  "RECORD_DATE",
                  "DISP_DATE",
                  "LATEST_NEXT_DATE",
                  "MEDICINE_SUMMARY",
                  "IS_PREGNANT",
                  "NUM_OF_DRUGS",
                  "CLINIC_NOTES",
                  "COUNSEL_NOTES"
               )
            )
         )
      )

      .log_info("Downloading {green('Latest ART Visits')}.")
      forms$art_last <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "art_last",
         cols = c("CENTRAL_ID", "REC_ID"),
         join = list(
            "ohasis_warehouse.form_art_bc" = list(
               by   = c("REC_ID" = "REC_ID"),
               cols = c(
                  "REC_ID",
                  "CREATED_AT",
                  "PATIENT_ID",
                  "FORM_VERSION",
                  "CONFIRMATORY_CODE",
                  "UIC",
                  "PATIENT_CODE",
                  "PHILHEALTH_NO",
                  "PHILSYS_ID",
                  "FIRST",
                  "MIDDLE",
                  "LAST",
                  "SUFFIX",
                  "BIRTHDATE",
                  "AGE",
                  "AGE_MO",
                  "SEX",
                  "SELF_IDENT",
                  "SELF_IDENT_OTHER",
                  "CURR_PSGC_REG",
                  "CURR_PSGC_PROV",
                  "CURR_PSGC_MUNC",
                  "CURR_ADDR",
                  "FACI_ID",
                  "SUB_FACI_ID",
                  "SERVICE_FACI",
                  "SERVICE_SUB_FACI",
                  "FACI_DISP",
                  "SUB_FACI_DISP",
                  "CLIENT_TYPE",
                  "TX_STATUS",
                  "WHO_CLASS",
                  "VISIT_TYPE",
                  "VISIT_DATE",
                  "RECORD_DATE",
                  "DISP_DATE",
                  "LATEST_NEXT_DATE",
                  "MEDICINE_SUMMARY",
                  "IS_PREGNANT",
                  "NUM_OF_DRUGS",
                  "CLINIC_NOTES",
                  "COUNSEL_NOTES"
               )
            )
         )
      )

      .log_info("Downloading {green('ART Visits w/in the scope')}.")
      forms$form_art_bc <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "form_art_bc",
         cols      = c(
            "REC_ID",
            "CREATED_AT",
            "PATIENT_ID",
            "FORM_VERSION",
            "CONFIRMATORY_CODE",
            "UIC",
            "PATIENT_CODE",
            "PHILHEALTH_NO",
            "PHILSYS_ID",
            "FIRST",
            "MIDDLE",
            "LAST",
            "SUFFIX",
            "BIRTHDATE",
            "AGE",
            "AGE_MO",
            "SEX",
            "SELF_IDENT",
            "SELF_IDENT_OTHER",
            "CURR_PSGC_REG",
            "CURR_PSGC_PROV",
            "CURR_PSGC_MUNC",
            "CURR_ADDR",
            "FACI_ID",
            "SUB_FACI_ID",
            "SERVICE_FACI",
            "SERVICE_SUB_FACI",
            "FACI_DISP",
            "SUB_FACI_DISP",
            "CLIENT_TYPE",
            "TX_STATUS",
            "WHO_CLASS",
            "VISIT_TYPE",
            "VISIT_DATE",
            "RECORD_DATE",
            "DISP_DATE",
            "LATEST_NEXT_DATE",
            "MEDICINE_SUMMARY",
            "IS_PREGNANT",
            "NUM_OF_DRUGS",
            "CLINIC_NOTES",
            "COUNSEL_NOTES"
         ),
         where     = glue("(VISIT_DATE >= '{min}' AND VISIT_DATE <= '{max}') OR (DATE(CREATED_AT) >= '{min}' AND DATE(CREATED_AT) <= '{max}') OR (DATE(UPDATED_AT) >= '{min}' AND DATE(UPDATED_AT) <= '{max}')"),
         raw_where = TRUE
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
   local(envir = nhsss$harp_tx, {
      official         <- list()
      official$old_reg <- ohasis$load_old_dta(
         path            = ohasis$get_data("harp_tx-reg", ohasis$prev_yr, ohasis$prev_mo),
         corr            = corr$old_reg,
         warehouse_table = "harp_tx_old",
         id_col          = c("art_id" = "integer"),
         dta_pid         = "PATIENT_ID",
         remove_cols     = "CENTRAL_ID",
         remove_rows     = corr$anti_join,
         id_registry     = forms$id_registry
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