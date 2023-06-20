##  Generate pre-requisites and endpoints --------------------------------------

download_corrections <- function() {
   check <- input(
      prompt  = glue("Re-download the {green('data corrections')}?"),
      options = c("1" = "yes", "2" = "no"),
      default = "2"
   )
   if (check == "1") {
      .log_info("Downloading corrections list.")
      nhsss <- gdrive_correct2(nhsss, ohasis$ym, "prep")
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
      local(envir = nhsss$harp_tx, invisible({
         tables           <- list()
         tables$warehouse <- c("form_prep", "id_registry", "rec_link", "form_hts", "form_a", "form_cfbs")

         lapply(tables$warehouse, function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE))
      }))
   }
}

##  Get earliest & latest visit data -------------------------------------------

# check if prep starts to be re-processed
update_first_last_prep <- function() {
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
            dbRemoveTable(lw_conn, table_space)


         dbExecute(
            lw_conn,
            glue(r"(CREATE TABLE {db_name}.{scope} AS )",
                 read_file(file.path(nhsss$prep$wd, glue("{scope}.sql")))),
            params = as.character(ohasis$next_date)
         )
      }
      .log_success("Done!")
      dbDisconnect(lw_conn)
      rm(db_name, lw_conn, table_space, name, scope)
   }
}

##  Update Record Links --------------------------------------------------------

update_prep_rec_link <- function() {

   update <- input(
      prompt  = "Do you want to update {green('rec_link')}?",
      options = c("1" = "Yes", "2" = "No"),
      default = "1"
   )
   if (update == "1") {
      lw_conn <- ohasis$conn("lw")
      sql     <- read_file(file.path(nhsss$prep$wd, "prep_link.sql"))

      .log_info("Downloading data.")
      data <- dbGetQuery(lw_conn, sql)
      dbDisconnect(lw_conn)

      df <- data %>%
         arrange(PREP_REC, sort, days_from_test) %>%
         distinct(PREP_REC, .keep_all = TRUE)

      df %<>%
         select(
            SOURCE_REC      = HTS_REC,
            DESTINATION_REC = PREP_REC,
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
         .log_info("Uploading to OHASIS.")
         db_conn <- ohasis$conn("db")
         dbxUpsert(
            db_conn,
            Id(schema = "ohasis_interim", table = "rec_link"),
            df,
            c("SOURCE_REC", "DESTINATION_REC")
         )
         dbDisconnect(db_conn)
      }
      .log_info(r"(Total New Linkage: {green(nrow(df))} rows)")
   }
}

##  Update initiation dates ----------------------------------------------------

initiation_dates <- function() {
   update <- input(
      prompt  = "Do you want to update {green('prep_init_p12m')}?",
      options = c("1" = "Yes", "2" = "No"),
      default = "1"
   )
   if (update == "1") {
      # coverage of checking for prep reinitiation date is past 12months
      .log_info("Getting PrEP records for the past year.")
      lw_conn <- ohasis$conn("lw")
      # define params
      min     <- ohasis$next_date %m-% months(13)
      max     <- ohasis$next_date %m-% days(1)

      # read and run query
      sql  <- read_file(file.path(nhsss$prep$wd, "prep_init.sql"))
      data <- dbGetQuery(lw_conn, sql, params = list(min, max))
      dbDisconnect(lw_conn)

      .log_info("Getting time diff between visits.")
      disp_p12m <- data %>%
         arrange(VISIT_DATE, desc(LATEST_NEXT_DATE)) %>%
         distinct(CENTRAL_ID, VISIT_DATE, .keep_all = TRUE) %>%
         arrange(CENTRAL_ID, VISIT_DATE, desc(LATEST_NEXT_DATE)) %>%
         group_by(CENTRAL_ID) %>%
         mutate(
            DATE_BEFORE = lag(VISIT_DATE, order_by = VISIT_DATE),
            DATE_AFTER  = lead(VISIT_DATE, order_by = VISIT_DATE),
         ) %>%
         ungroup() %>%
         mutate(
            DIFF_BEFORE = interval(DATE_BEFORE, VISIT_DATE) %/% months(1),
            DIFF_AFTER  = interval(VISIT_DATE, DATE_AFTER) %/% months(1),
         )

      .log_info("Getting initiation dates.")
      df_latest_start <- disp_p12m %>%
         filter(DIFF_BEFORE >= 4 | is.na(DIFF_BEFORE)) %>%
         arrange(CENTRAL_ID, desc(VISIT_DATE)) %>%
         distinct(CENTRAL_ID, .keep_all = TRUE) %>%
         mutate(
            INITIATION_DATE = if_else(
               condition = is.na(DIFF_BEFORE),
               true      = VISIT_DATE,
               false     = DATE_BEFORE
            )
         ) %>%
         select(
            CENTRAL_ID,
            REC_ID,
            INITIATION_DATE
         )

      # create table
      lw_conn     <- ohasis$conn("lw")
      table_space <- Id(schema = "ohasis_warehouse", table = "prep_init_p12m")
      if (dbExistsTable(lw_conn, table_space))
         dbRemoveTable(lw_conn, table_space, batch_size = 1000)

      ohasis$upsert(lw_conn, "warehouse", "prep_init_p12m", df_latest_start, "CENTRAL_ID")
      dbDisconnect(lw_conn)
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
      local(envir = nhsss$prep, {
         lw_conn <- ohasis$conn("lw")
         forms   <- list()

         min <- as.Date(paste(sep = "-", ohasis$yr, ohasis$mo, "01"))
         max <- (min %m+% months(1)) %m-% days(1)
         min <- as.character(min)
         max <- as.character(max)

         .log_info("Downloading {green('Central IDs')}.")
         forms$id_registry <- lw_conn %>%
            dbTable(
               "ohasis_warehouse",
               "id_registry",
               cols      = c("CENTRAL_ID", "PATIENT_ID"),
               where     = r"(
               (PATIENT_ID IN (SELECT CENTRAL_ID FROM ohasis_warehouse.prep_first)) OR
                  (CENTRAL_ID IN (SELECT CENTRAL_ID FROM ohasis_warehouse.prep_first))
            )",
               raw_where = TRUE
            )

         .log_info("Downloading {green('Form A')}.")
         hts_where    <- r"(REC_ID IN (SELECT SOURCE_REC FROM ohasis_warehouse.rec_link))"
         forms$form_a <- lw_conn %>%
            dbTable(
               "ohasis_warehouse",
               "form_a",
               where     = hts_where,
               raw_where = TRUE
            )

         .log_info("Downloading {green('HTS Form')}.")
         forms$form_hts <- lw_conn %>%
            dbTable(
               "ohasis_warehouse",
               "form_hts",
               where     = hts_where,
               raw_where = TRUE
            )

         .log_info("Downloading {green('CFBS Form')}.")
         forms$form_cfbs <- lw_conn %>%
            dbTable(
               "ohasis_warehouse",
               "form_cfbs",
               where     = hts_where,
               raw_where = TRUE
            )

         .log_info("Processing {green('HTS Data')}.")
         forms$hts_data <- process_hts(forms$form_hts, forms$form_a, forms$form_cfbs)

         .log_info("Downloading {green('Record Links')}.")
         forms$rec_link <- lw_conn %>%
            dbTable(
               "ohasis_warehouse",
               "rec_link",
               raw_where = TRUE,
               where     = r"(
               (DESTINATION_REC IN (SELECT REC_ID FROM ohasis_warehouse.prep_first)) OR
                  (DESTINATION_REC IN (SELECT REC_ID FROM ohasis_warehouse.prepdisp_first)) OR
                  (DESTINATION_REC IN (SELECT REC_ID FROM ohasis_warehouse.prep_last)) OR
                  (DESTINATION_REC IN (SELECT REC_ID FROM ohasis_warehouse.prepdisp_last)) OR
                  (DESTINATION_REC IN (SELECT REC_ID FROM ohasis_warehouse.prep_init_p12m))
               )"
            )

         .log_info("Downloading {green('PrEP Visits w/in the scope')}.")
         forms$form_prep <- lw_conn %>%
            dbTable(
               "ohasis_warehouse",
               "form_prep",
               where     = glue("
            (REC_ID IN (SELECT REC_ID FROM ohasis_warehouse.prep_first)) OR
               (REC_ID IN (SELECT REC_ID FROM ohasis_warehouse.prepdisp_first)) OR
               (REC_ID IN (SELECT REC_ID FROM ohasis_warehouse.prepdisp_last)) OR
               (REC_ID IN (SELECT REC_ID FROM ohasis_warehouse.prep_last))"),
               raw_where = TRUE
            ) %>%
            process_prep(forms$hts_data, forms$rec_link)

         .log_info("Downloading {green('PrEP Earliest Screenings')}.")
         forms$prep_first <- lw_conn %>%
            dbTable(
               "ohasis_warehouse",
               "prep_first",
               cols = c("CENTRAL_ID", "REC_ID"),
            ) %>%
            left_join(forms$form_prep)

         .log_info("Downloading {green('PrEP Enrollment')}.")
         forms$prepdisp_first <- lw_conn %>%
            dbTable(

               "ohasis_warehouse",
               "prepdisp_first",
               cols = c("CENTRAL_ID", "REC_ID")
            ) %>%
            left_join(forms$form_prep)

         .log_info("Downloading {green('Latest PrEP Visits')}.")
         forms$prep_last <- lw_conn %>%
            dbTable(
               "ohasis_warehouse",
               "prep_last",
               cols = c("CENTRAL_ID", "REC_ID")
            ) %>%
            left_join(forms$form_prep)

         .log_info("Downloading {green('Latest PrEP Dispensing')}.")
         forms$prepdisp_last <- lw_conn %>%
            dbTable(
               "ohasis_warehouse",
               "prepdisp_last",
               cols = c("CENTRAL_ID", "REC_ID")
            ) %>%
            left_join(forms$form_prep)

         .log_info("Downloading {green('Latest PrEP Initiation')}.")
         forms$prep_init_p12m <- lw_conn %>%
            dbTable(
               "ohasis_warehouse",
               "prep_init_p12m",
               cols = c("CENTRAL_ID", "REC_ID")
            ) %>%
            left_join(forms$form_prep)

         .log_info("Downloading {green('ARVs Disepensed w/in the scope')}.")
         forms$disp_meds <- lw_conn %>%
            dbTable(
               "ohasis_lake",
               "disp_meds",
               where     = glue("(DATE(DISP_DATE) >= '{min}' AND DATE(DISP_DATE) <= '{max}') OR (DATE(CREATED_AT) >= '{min}' AND DATE(CREATED_AT) <= '{max}') OR (DATE(UPDATED_AT) >= '{min}' AND DATE(UPDATED_AT) <= '{max}')"),
               raw_where = TRUE
            )

         dbDisconnect(lw_conn)
         .log_success("Done.")

         rm(min, max, lw_conn, hts_where)
      })
   }
}

##  Get the previous report's PrEP Registry ------------------------------------

update_dataset <- function() {
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
}

define_params <- function() {
   local(envir = nhsss$prep, {
      params                <- list()
      params$latest_prep_id <- max(as.integer(official$old_reg$prep_id), na.rm = TRUE)

      params$cutoff_mo   <- if_else(as.numeric(ohasis$next_mo) <= 3, as.numeric(ohasis$next_mo) + 9, as.numeric(ohasis$next_mo) - 3)
      params$cutoff_mo   <- stri_pad_left(as.character(params$cutoff_mo), 2, '0')
      params$cutoff_yr   <- if_else(as.numeric(ohasis$next_mo) <= 3, as.numeric(ohasis$next_yr) - 1, as.numeric(ohasis$next_yr))
      params$cutoff_yr   <- as.character(params$cutoff_yr)
      params$cutoff_date <- as.Date(paste(sep = '-', params$cutoff_yr, params$cutoff_mo, '01'))
   })
}

.init <- function() {
   download_corrections()
   update_warehouse()
   update_first_last_prep()
   update_prep_rec_link()
   initiation_dates()
   download_tables()
   update_dataset()
   define_params()
}
