##  Generate pre-requisites and endpoints --------------------------------------

check <- input(
   prompt  = glue("Re-download the {green('data corrections')}?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Downloading corrections list.")
   nhsss <- gdrive_correct2(nhsss, ohasis$ym, "prep")
}

# run through all tables
check <- input(
   prompt  = glue("Update {green('data/forms')} to be used for consolidation?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Updating data lake and data warehouse.")
   local(envir = nhsss$prep, invisible({
      tables           <- list()
      tables$lake      <- c("lab_wide", "disp_meds")
      tables$warehouse <- c("form_prep", "id_registry", "rec_link")

      lapply(tables$lake, function(table) ohasis$data_factory("lake", table, "upsert", TRUE))
      lapply(tables$warehouse, function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE))
   }))
}

##  Get earliest & latest visit data -------------------------------------------

# check if prep starts to be re-processed
update <- input(
   prompt  = glue("Do you want to re-process the {green('PrEP Start & Latest Dates')}?"),
   options = c("1" = "Yes", "2" = "No"),
   default = "1"
)
# if Yes, re-process
if (update == "1") {
   lw_conn <- ohasis$conn("lw")
   db_name <- "ohasis_warehouse"

   # download the data
   for (scope in c("prep_first", "prepdisp_first", "prep_last", "prepdisp_last")) {
      name <- case_when(
         scope == "prep_first" ~ "PrEP Earliest Visits",
         scope == "prepdisp_first" ~ "PrEP Enrollment",
         scope == "prep_last" ~ "PrEP Latest Visits",
         scope == "prepdisp_last" ~ "PrEP Latest Dispensing",
      )
      .log_info("Processing {green(name)}.")

      # update lake
      table_space <- Id(schema = db_name, table = scope)
      if (dbExistsTable(lw_conn, table_space))
         dbxDelete(lw_conn, table_space, batch_size = 1000)

      dbExecute(
         lw_conn,
         glue(r"(INSERT INTO {db_name}.{scope}
         )", read_file(file.path(nhsss$prep$wd, glue("{scope}.sql")))),
         params = as.character(ohasis$next_date)
      )
   }
   .log_success("Done!")
   dbDisconnect(lw_conn)
   rm(db_name, lw_conn, table_space, name, scope)
}

##  Update Record Links --------------------------------------------------------

update <- input(
   prompt  = "Do you want to update the rec_link table?",
   options = c("1" = "Yes", "2" = "No"),
   default = "1"
)
if (update == "1") {

   update_prep_rec_link <- function() {

      db_conn  <- ohasis$conn("db")
      rec_link <- dbTable(db_conn, "ohasis_interim", "rec_link")
      dbDisconnect(db_conn)

      lw_conn     <- ohasis$conn("lw")
      form_a      <- dbTable(lw_conn,
                             "ohasis_warehouse",
                             "form_a",
                             cols = c("REC_ID", "RECORD_DATE", "PATIENT_ID", "FORM_VERSION"))
      form_hts    <- dbTable(lw_conn,
                             "ohasis_warehouse",
                             "form_hts",
                             cols = c("REC_ID", "RECORD_DATE", "PATIENT_ID", "FORM_VERSION"))
      form_cfbs   <- dbTable(lw_conn,
                             "ohasis_warehouse",
                             "form_cfbs",
                             cols = c("REC_ID", "RECORD_DATE", "PATIENT_ID", "FORM_VERSION"))
      form_prep   <- dbTable(lw_conn, "ohasis_warehouse", "form_prep")
      id_registry <- dbTable(lw_conn, "ohasis_warehouse", "id_registry")
      dbDisconnect(lw_conn)

      prep_data <- form_prep %>%
         left_join(
            y  = id_registry %>%
               select(CENTRAL_ID, PATIENT_ID),
            by = "PATIENT_ID"
         ) %>%
         mutate(
            CENTRAL_ID = if_else(
               condition = is.na(CENTRAL_ID),
               true      = PATIENT_ID,
               false     = CENTRAL_ID
            )
         ) %>%
         select(REC_ID, CENTRAL_ID)

      df <- prep_data %>%
         anti_join(
            y = rec_link %>%
               select(REC_ID = DESTINATION_REC)
         ) %>%
         inner_join(
            y = form_prep,
         ) %>%
         inner_join(
            y  = bind_rows(form_hts, form_a, form_cfbs) %>%
               left_join(
                  y  = id_registry %>%
                     select(CENTRAL_ID, PATIENT_ID),
                  by = "PATIENT_ID"
               ) %>%
               mutate(
                  CENTRAL_ID = if_else(
                     condition = is.na(CENTRAL_ID),
                     true      = PATIENT_ID,
                     false     = CENTRAL_ID
                  )
               ) %>%
               select(
                  HTS_DATE = RECORD_DATE,
                  HTS_REC  = REC_ID,
                  CENTRAL_ID
               ),
            by = "CENTRAL_ID"
         ) %>%
         mutate(
            days_from_test = interval(HTS_DATE, RECORD_DATE) / days(1),
            sort           = case_when(
               RECORD_DATE == HTS_DATE ~ 1,
               abs(days_from_test) <= 7 ~ 2,
               TRUE ~ 9999
            )
         ) %>%
         filter(sort != 9999) %>%
         arrange(REC_ID, sort, days_from_test) %>%
         distinct(REC_ID, .keep_all = TRUE) %>%
         select(CENTRAL_ID, HTS_REC, REC_ID, RECORD_DATE, HTS_DATE, days_from_test, sort)

      df %<>%
         select(
            SOURCE_REC      = HTS_REC,
            DESTINATION_REC = REC_ID,
         ) %>%
         mutate(
            PRIME      = NA_character_,
            CREATED_BY = '1300000001',
            CREATED_AT = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
            UPDATED_BY = NA_character_,
            UPDATED_AT = NA_character_,
            DELETED_BY = NA_character_,
            DELETED_AT = NA_character_,
         )

      if (nrow(df) > 0) {
         db_conn <- ohasis$conn("db")
         dbAppendTable(
            db_conn,
            Id(schema = "ohasis_interim", table = "rec_link"),
            df
         )
         dbDisconnect(db_conn)
      }
      .log_info(r"(Total New Linkage: {green(nrow(df))} rows)")
   }

   update_prep_rec_link()
}

##  Download records -----------------------------------------------------------

update <- input(
   prompt  = "Do you want to download the relevant form data?",
   options = c("1" = "Yes", "2" = "No"),
   default = "1"
)
# if Yes, re-process
if (update == "1") {
   local(envir = nhsss$prep, {
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

      .log_info("Downloading {green('PrEP Earliest Screenings')}.")
      forms$prep_first <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "prep_first",
         cols = c("CENTRAL_ID", "REC_ID"),
         join = list(
            "ohasis_warehouse.form_prep" = list(
               by = c("REC_ID" = "REC_ID")
            )
         )
      )

      .log_info("Downloading {green('PrEP Enrollment')}.")
      forms$prepdisp_first <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "prepdisp_first",
         cols = c("CENTRAL_ID", "REC_ID"),
         join = list(
            "ohasis_warehouse.form_prep" = list(
               by = c("REC_ID" = "REC_ID")
            )
         )
      )

      .log_info("Downloading {green('Latest PrEP Visits')}.")
      forms$prep_last <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "prep_last",
         cols = c("CENTRAL_ID", "REC_ID"),
         join = list(
            "ohasis_warehouse.form_prep" = list(
               by = c("REC_ID" = "REC_ID")
            )
         )
      )

      .log_info("Downloading {green('Latest PrEP Dispensing')}.")
      forms$prepdisp_last <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "prepdisp_last",
         cols = c("CENTRAL_ID", "REC_ID"),
         join = list(
            "ohasis_warehouse.form_prep" = list(
               by = c("REC_ID" = "REC_ID")
            )
         )
      )

      .log_info("Downloading {green('PrEP Visits w/in the scope')}.")
      forms$form_prep <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "form_prep",
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


      .log_info("Downloading {green('Form A')}.")
      forms$form_a <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "form_a",
         where     = glue("((RECORD_DATE >= '{min}' AND RECORD_DATE <= '{max}') OR (DATE(DATE_CONFIRM) >= '{min}' AND DATE(DATE_CONFIRM) <= '{max}') OR (DATE(T3_DATE) >= '{min}' AND DATE(T3_DATE) <= '{max}') OR (DATE(T2_DATE) >= '{min}' AND DATE(T2_DATE) <= '{max}') OR (DATE(T1_DATE) >= '{min}' AND DATE(T1_DATE) <= '{max}') OR (DATE(T0_DATE) >= '{min}' AND DATE(T0_DATE) <= '{max}'))"),
         raw_where = TRUE
      ) %>%
         left_join(
            y  = forms$id_registry,
            by = "PATIENT_ID"
         ) %>%
         mutate(
            CENTRAL_ID = if_else(
               condition = is.na(CENTRAL_ID),
               true      = PATIENT_ID,
               false     = CENTRAL_ID
            ),
         )

      .log_info("Downloading {green('HTS Form')}.")
      forms$form_hts <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "form_hts",
         where     = glue("((RECORD_DATE >= '{min}' AND RECORD_DATE <= '{max}') OR (DATE(DATE_CONFIRM) >= '{min}' AND DATE(DATE_CONFIRM) <= '{max}') OR (DATE(T3_DATE) >= '{min}' AND DATE(T3_DATE) <= '{max}') OR (DATE(T2_DATE) >= '{min}' AND DATE(T2_DATE) <= '{max}') OR (DATE(T1_DATE) >= '{min}' AND DATE(T1_DATE) <= '{max}') OR (DATE(T0_DATE) >= '{min}' AND DATE(T0_DATE) <= '{max}'))"),
         raw_where = TRUE
      ) %>%
         left_join(
            y  = forms$id_registry,
            by = "PATIENT_ID"
         ) %>%
         mutate(
            CENTRAL_ID = if_else(
               condition = is.na(CENTRAL_ID),
               true      = PATIENT_ID,
               false     = CENTRAL_ID
            ),
         )

      .log_info("Downloading {green('CFBS Form')}.")
      forms$form_cfbs <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "form_cfbs",
         where     = glue("(RECORD_DATE >= '{min}' AND RECORD_DATE <= '{max}') OR (DATE(TEST_DATE) >= '{min}' AND DATE(TEST_DATE) <= '{max}')"),
         raw_where = TRUE
      ) %>%
         left_join(
            y  = forms$id_registry,
            by = "PATIENT_ID"
         ) %>%
         mutate(
            CENTRAL_ID = if_else(
               condition = is.na(CENTRAL_ID),
               true      = PATIENT_ID,
               false     = CENTRAL_ID
            ),
         )


      dbDisconnect(lw_conn)

      .log_info("Downloading {green('Record Links')}.")
      db_conn        <- ohasis$conn("db")
      forms$rec_link <- dbTable(
         db_conn,
         "ohasis_interim",
         "rec_link",
         raw_where = TRUE
      )
      dbDisconnect(db_conn)
      .log_success("Done.")

      rm(min, max, lw_conn, db_conn)
   })
}

##  Get the previous report's PrEP Registry ------------------------------------

check <- input(
   prompt  = "Reload previous dataset?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Getting previous datasets.")
   local(envir = nhsss$prep, {
      official         <- list()
      official$old_reg <- ohasis$load_old_dta(
         path            = ohasis$get_data("prep-reg", ohasis$prev_yr, ohasis$prev_mo),
         corr            = corr$old_reg,
         warehouse_table = "prep_old",
         id_col          = c("prep_id" = "integer"),
         dta_pid         = "PATIENT_ID",
         remove_cols     = "CENTRAL_ID",
         remove_rows     = corr$anti_join
      )

      official$old_outcome <- ohasis$get_data("prep-outcome", ohasis$prev_yr, ohasis$prev_mo) %>%
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