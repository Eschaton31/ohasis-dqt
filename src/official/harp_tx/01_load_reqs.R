##  Generate pre-requisites and endpoints --------------------------------------

download_corrections <- function() {
   check <- input(
      prompt  = glue("Re-download the {green('data corrections')}?"),
      options = c("1" = "yes", "2" = "no"),
      default = "2"
   )
   if (check == "1") {
      log_info("Downloading corrections list.")
      nhsss <- gdrive_correct2(nhsss, ohasis$ym, "harp_tx")
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
         tables$warehouse <- c("form_art_bc", "id_registry")

         lapply(tables$warehouse, function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE))
      }))
   }
}

##  Get earliest & latest visit data -------------------------------------------

# check if art starts to be re-processed
update_first_last_art <- function() {
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
      for (scope in c("art_first", "art_last", "art_lastdisp")) {
         name <- case_when(
            scope == "art_first" ~ "ART Start Dates",
            scope == "art_last" ~ "ART Latest Visits",
            scope == "art_lastdisp" ~ "ART Latest Dispense",
         )
         log_info("Processing {green(name)}.")

         # update lake
         table_space  <- Id(schema = db_name, table = scope)
         table_schema <- dbplyr::in_schema(db_name, scope)
         if (dbExistsTable(lw_conn, table_space))
            dbRemoveTable(lw_conn, table_space)

         dbExecute(
            lw_conn,
            glue(r"(CREATE TABLE {db_name}.{scope} AS )",
                 read_file(file.path(nhsss$harp_tx$wd, glue("{scope}.sql")))),
            params = as.character(ohasis$next_date)
         )
         # db_create_index(lw_conn, table_schema, "CENTRAL_ID")
      }
      log_success("Done!")
      dbDisconnect(lw_conn)
      rm(db_name, lw_conn, table_space, name, scope)
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
      local(envir = nhsss$harp_tx, {
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
            cols      = c("CENTRAL_ID", "PATIENT_ID"),
            where     = r"(
               (PATIENT_ID IN (SELECT CENTRAL_ID FROM ohasis_warehouse.art_first)) OR
                  (CENTRAL_ID IN (SELECT CENTRAL_ID FROM ohasis_warehouse.art_first))
            )",
            raw_where = TRUE
         )

         log_info("Downloading {green('ART Visits w/in the scope')}.")
         forms$form_art_bc <- dbTable(
            lw_conn,
            "ohasis_warehouse",
            "form_art_bc",
            where     = glue("
            (VISIT_DATE BETWEEN '{min}' AND '{max}') OR
               (DATE(CREATED_AT) BETWEEN '{min}' AND '{max}') OR
               (DATE(UPDATED_AT) BETWEEN '{min}' AND '{max}') OR
               (REC_ID IN (SELECT REC_ID FROM ohasis_warehouse.art_first)) OR
               (REC_ID IN (SELECT REC_ID FROM ohasis_warehouse.art_last))"),
            raw_where = TRUE
         )

         log_info("Downloading {green('Earliest ART Visits')}.")
         forms$art_first <- lw_conn %>%
            dbTable(
               "ohasis_warehouse",
               "art_first",
               cols = c("CENTRAL_ID", "REC_ID", "VISIT_DATE")
            ) %>%
            left_join(forms$form_art_bc)

         log_info("Downloading {green('Latest ART Visits')}.")
         forms$art_last <- lw_conn %>%
            dbTable(
               "ohasis_warehouse",
               "art_last",
               cols = c("CENTRAL_ID", "REC_ID", "VISIT_DATE")
            ) %>%
            left_join(forms$form_art_bc)

         log_info("Downloading {green('Latest ART Dispensing')}.")
         forms$art_lastdisp <- lw_conn %>%
            dbTable(
               "ohasis_warehouse",
               "art_lastdisp",
               cols = c("CENTRAL_ID", "REC_ID", "VISIT_DATE")
            ) %>%
            left_join(forms$form_art_bc)

         log_info("Downloading {green('ARVs Disepensed w/in the scope')}.")
         forms$disp_meds <- dbTable(
            lw_conn,
            "ohasis_lake",
            "disp_meds",
            where     = glue("(DATE(DISP_DATE) >= '{min}' AND DATE(DISP_DATE) <= '{max}') OR (DATE(CREATED_AT) >= '{min}' AND DATE(CREATED_AT) <= '{max}') OR (DATE(UPDATED_AT) >= '{min}' AND DATE(UPDATED_AT) <= '{max}')"),
            raw_where = TRUE
         )

         log_info("Downloading {green('CD4 Data')}.")
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
            log_info("Performing cleaning on the outcome dataset.")
            official$old_outcome <- .cleaning_list(official$old_outcome, corr$old_outcome, "ART_ID", "integer")
         }
      })
   }
   rm(check)
}

define_params <- function() {
   local(envir = nhsss$harp_tx, {
      params               <- list()
      params$latest_art_id <- max(as.integer(official$old_reg$art_id), na.rm = TRUE)

      # params$cutoff_mo   <- if_else(as.numeric(ohasis$next_mo) <= 3, as.numeric(ohasis$next_mo) + 9, as.numeric(ohasis$next_mo) - 3)
      # params$cutoff_mo   <- stri_pad_left(as.character(params$cutoff_mo), 2, '0')
      # params$cutoff_yr   <- if_else(as.numeric(ohasis$next_mo) <= 3, as.numeric(ohasis$next_yr) - 1, as.numeric(ohasis$next_yr))
      # params$cutoff_yr   <- as.character(params$cutoff_yr)
      # params$cutoff_date <- as.Date(paste(sep = '-', params$cutoff_yr, params$cutoff_mo, '01'))
      params$cutoff_date <- paste(sep = "-", ohasis$yr, ohasis$mo, "01") %>%
         as.Date() %>%
         ceiling_date(unit = "month") %m-%
         days(31) %>%
         as.character()
   })
}

.init <- function() {
   download_corrections()
   update_warehouse()
   update_first_last_art()
   download_tables()
   update_dataset()
   define_params()
}