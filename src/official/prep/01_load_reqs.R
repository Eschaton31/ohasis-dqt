##  Set coverage ---------------------------------------------------------------

set_coverage <- function(max = end_friday(Sys.time())) {
   params   <- list()
   max_date <- as.Date(max)

   params$yr  <- year(max_date)
   params$mo  <- month(max_date)
   params$ym  <- str_c(sep = ".", params$yr, stri_pad_left(params$mo, 2, "0"))
   params$min <- max_date %m-% days(30) %>% as.character()
   params$max <- max

   params$prev_mo <- month(max_date %m-% months(1))
   params$prev_yr <- year(max_date %m-% months(1))

   return(params)
}

##  Generate pre-requisites and endpoints --------------------------------------

# run through all tables
update_warehouse <- function(update) {
   update <- ifelse(
      !is.null(update) && update %in% c("1", "2"),
      update,
      input(
         prompt  = glue("Update {green('data/forms')} to be used for consolidation?"),
         options = c("1" = "yes", "2" = "no"),
         default = "2"
      )
   )
   if (update == "1") {
      log_info("Updating data lake and data warehouse.")
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
         "px_other_service",
         "px_key_pop",
         "px_vitals",
         "px_ars_sx",
         "px_sti_sx",
         "px_prep",
         "lab_wide",
         "disp_meds"
      )
      tables$warehouse <- c("form_prep", "id_registry", "form_hts", "form_a", "form_cfbs")

      lapply(tables$lake, function(table) ohasis$data_factory("lake", table, "upsert", TRUE))
      lapply(tables$warehouse, function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE))
   }
}

##  Get earliest & latest visit data -------------------------------------------

# check if prep starts to be re-processed
update_first_last_prep <- function(update, params, path_to_sql) {
   update <- ifelse(
      !is.null(update) && update %in% c("1", "2"),
      update,
      input(
         prompt  = glue("Do you want to re-process the {green('ART Start & Latest Dates')}?"),
         options = c("1" = "Yes", "2" = "No"),
         default = "1"
      )
   )
   # if Yes, re-process
   if (update == "1") {
      lw_conn <- ohasis$conn("lw")
      db_name <- "ohasis_warehouse"

      # download the data
      for (scope in c("prep_offer", "prep_first", "prepdisp_first", "prep_last", "prepdisp_last", "prepdisc_last")) {
         name <- case_when(
            scope == "prep_offer" ~ "PrEP First Offer",
            scope == "prep_first" ~ "PrEP Earliest Visits",
            scope == "prepdisp_first" ~ "PrEP Enrollment",
            scope == "prep_last" ~ "PrEP Latest Visits",
            scope == "prepdisp_last" ~ "PrEP Latest Dispensing",
            scope == "prepdisc_last" ~ "PrEP Latest Discontinue",
         )
         log_info("Processing {green(name)}.")

         # update lake
         table_space <- Id(schema = db_name, table = scope)
         if (dbExistsTable(lw_conn, table_space))
            dbRemoveTable(lw_conn, table_space)

         dbExecute(
            lw_conn,
            glue(r"(CREATE TABLE {db_name}.{scope} AS )",
                 read_file(file.path(path_to_sql, glue("{scope}.sql")))),
            params = as.character(params$max)
         )
      }
      log_success("Done!")
      dbDisconnect(lw_conn)
   }
}

##  Update Record Links --------------------------------------------------------

update_prep_rec_link <- function(update, path_to_sql) {
   update <- ifelse(
      !is.null(update) && update %in% c("1", "2"),
      update,
      input(
         prompt  = "Do you want to update {green('rec_link')}?",
         options = c("1" = "Yes", "2" = "No"),
         default = "1"
      )
   )
   if (update == "1") {
      db_conn    <- ohasis$conn("db")
      delete_sql <- r"(
      DELETE ohasis_interim.rec_link
      FROM ohasis_interim.rec_link
               LEFT JOIN ohasis_interim.px_record
                         ON rec_link.{replace} = px_record.REC_ID
      WHERE px_record.DELETED_AT IS NOT NULL;
      )"
      dbExecute(db_conn, stri_replace_all_fixed(delete_sql, "{replace}", "DESTINATION_REC"))
      dbExecute(db_conn, stri_replace_all_fixed(delete_sql, "{replace}", "SOURCE_REC"))
      dbDisconnect(db_conn)
      # refresh once to remove deleted records from live; figure out upsert in
      # future processing
      ohasis$data_factory("warehouse", "rec_link", "refresh", TRUE)

      lw_conn     <- ohasis$conn("lw")
      nolink_hts  <- tracked_select(lw_conn, read_file(file.path(path_to_sql, "nolink_hts.sql")), "Non-linked HTS")
      nolink_prep <- tracked_select(lw_conn, read_file(file.path(path_to_sql, "nolink_prep.sql")), "Non-linked PrEP")
      dbDisconnect(lw_conn)

      df <- nolink_prep %>%
         mutate(
            HTS_PREP_BEFORE = PREP_DATE %m-% days(7),
            HTS_PREP_AFTER  = PREP_DATE %m+% days(7),
         ) %>%
         left_join(
            y  = nolink_hts,
            by = join_by(CENTRAL_ID, between(y$HTS_DATE, x$HTS_PREP_BEFORE, x$HTS_PREP_AFTER))
         ) %>%
         mutate(
            HTS_PREP_DIFF = abs(interval(HTS_DATE, PREP_DATE) / days(1))
         ) %>%
         filter(HTS_PREP_DIFF <= 7) %>%
         arrange(PREP_REC, HTS_PREP_DIFF) %>%
         distinct(PREP_REC, .keep_all = TRUE) %>%
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
         log_info("Uploading to OHASIS.")
         db_conn <- ohasis$conn("db")
         dbxUpsert(
            db_conn,
            Id(schema = "ohasis_interim", table = "rec_link"),
            df,
            c("SOURCE_REC", "DESTINATION_REC")
         )
         dbDisconnect(db_conn)

         # refresh again to get new data
         ohasis$data_factory("warehouse", "rec_link", "refresh", TRUE)
      }
      log_info(r"(Total New Linkage: {green(nrow(df))} rows.)")
   }
}

##  Update initiation dates ----------------------------------------------------

update_initiation <- function(update, params, path_to_sql) {
   update <- ifelse(
      !is.null(update) && update %in% c("1", "2"),
      update,
      input(
         prompt  = "Do you want to update {green('prep_init_p12m')}?",
         options = c("1" = "Yes", "2" = "No"),
         default = "1"
      )
   )
   if (update == "1") {
      # coverage of checking for prep reinitiation date is past 12months
      log_info("Getting PrEP records for the past year.")
      lw_conn <- ohasis$conn("lw")
      # define params
      min     <- as.Date(params$max) %m+% days(1) %m-% months(13) %>% as.character()
      max     <- params$max

      # read and run query
      sql  <- read_file(file.path(path_to_sql, "prep_init.sql"))
      data <- dbGetQuery(lw_conn, sql, params = list(min, max))
      dbDisconnect(lw_conn)

      log_info("Getting time diff between visits.")
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

      log_info("Getting initiation dates.")
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

download_tables <- function(params) {
   lw_conn <- ohasis$conn("lw")
   forms   <- list()

   min       <- params$min
   max       <- params$max
   db_name   <- "ohasis_warehouse"
   hts_where <- r"(
   REC_ID IN (SELECT SOURCE_REC FROM ohasis_warehouse.rec_link) OR
      REC_ID IN (SELECT REC_ID FROM ohasis_warehouse.prep_offer)
   )"

   log_info("Downloading {green('Central IDs')}.")
   forms$id_registry <- dbTable(lw_conn, db_name, "id_registry", cols = c("CENTRAL_ID", "PATIENT_ID"))

   log_info("Downloading {green('Form A')}.")
   forms$form_a <- dbTable(lw_conn, db_name, "form_a", where = hts_where, raw_where = TRUE)

   log_info("Downloading {green('HTS Form')}.")
   forms$form_hts <- dbTable(lw_conn, db_name, "form_hts", where = hts_where, raw_where = TRUE)

   log_info("Downloading {green('CFBS Form')}.")
   forms$form_cfbs <- dbTable(lw_conn, db_name, "form_cfbs", where = hts_where, raw_where = TRUE)

   log_info("Processing {green('HTS Data')}.")
   forms$hts_data <- process_hts(forms$form_hts, forms$form_a, forms$form_cfbs)

   log_info("Downloading {green('Record Links')}.")
   forms$rec_link <- lw_conn %>%
      dbTable(
         db_name,
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

   log_info("Downloading {green('PrEP Visits w/in the scope')}.")
   forms$form_prep <- lw_conn %>%
      dbTable(
         db_name,
         "form_prep",
         where     = glue("
            (REC_ID IN (SELECT REC_ID FROM ohasis_warehouse.prep_first)) OR
               (REC_ID IN (SELECT REC_ID FROM ohasis_warehouse.prepdisp_first)) OR
               (REC_ID IN (SELECT REC_ID FROM ohasis_warehouse.prepdisp_last)) OR
               (REC_ID IN (SELECT REC_ID FROM ohasis_warehouse.prepdisc_last)) OR
               (REC_ID IN (SELECT REC_ID FROM ohasis_warehouse.prep_last))"),
         raw_where = TRUE
      )

   log_info("Downloading {green('PrEP First Offer')}.")
   forms$prep_offer <- lw_conn %>%
      dbTable(
         db_name,
         "prep_offer",
         cols = c("CENTRAL_ID", "REC_ID", "VISIT_DATE")
      ) %>%
      left_join(forms$hts_data, join_by(REC_ID))


   log_info("Appending offer to prep.")
   offer_cols <- intersect(names(forms$form_prep), names(forms$prep_offer))
   forms$form_prep %<>%
      bind_rows(
         forms$prep_offer %>%
            filter(hts_result != "R") %>%
            select(any_of(offer_cols)) %>%
            mutate(hts_src = 1)
      ) %>%
      process_prep(forms$hts_data, forms$rec_link)


   log_info("Downloading {green('PrEP Earliest Screenings')}.")
   forms$prep_first <- lw_conn %>%
      dbTable(
         db_name,
         "prep_first",
         cols = c("CENTRAL_ID", "REC_ID", "VISIT_DATE")
      ) %>%
      left_join(forms$form_prep, join_by(REC_ID, VISIT_DATE))

   log_info("Downloading {green('PrEP Enrollment')}.")
   forms$prepdisp_first <- lw_conn %>%
      dbTable(
         db_name,
         "prepdisp_first",
         cols = c("CENTRAL_ID", "REC_ID", "VISIT_DATE")
      ) %>%
      left_join(forms$form_prep, join_by(REC_ID, VISIT_DATE))

   log_info("Downloading {green('Latest PrEP Visits')}.")
   forms$prep_last <- lw_conn %>%
      dbTable(
         db_name,
         "prep_last",
         cols = c("CENTRAL_ID", "REC_ID", "VISIT_DATE")
      ) %>%
      left_join(forms$form_prep, join_by(REC_ID, VISIT_DATE))

   log_info("Downloading {green('Latest PrEP Dispensing')}.")
   forms$prepdisp_last <- lw_conn %>%
      dbTable(
         db_name,
         "prepdisp_last",
         cols = c("CENTRAL_ID", "REC_ID", "VISIT_DATE")
      ) %>%
      left_join(forms$form_prep, join_by(REC_ID, VISIT_DATE))

   log_info("Downloading {green('Latest PrEP Initiation')}.")
   forms$prep_init_p12m <- lw_conn %>%
      dbTable(
         db_name,
         "prep_init_p12m",
         cols = c("CENTRAL_ID", "REC_ID", "INITIATION_DATE")
      )

   log_info("Downloading {green('Latest PrEP Discontinuation')}.")
   forms$prepdisc_last <- lw_conn %>%
      dbTable(
         "ohasis_warehouse",
         "prepdisc_last",
         cols = c("CENTRAL_ID", "REC_ID", "VISIT_DATE")
      ) %>%
      left_join(forms$form_prep, join_by(REC_ID, VISIT_DATE))

   dbDisconnect(lw_conn)
   log_success("Done.")
   return(forms)
}

##  Get the previous report's PrEP Registry ------------------------------------

update_dataset <- function(params, corr, forms, reprocess) {
   log_info("Getting previous datasets.")
   official         <- list()
   official$old_reg <- ohasis$load_old_dta(
      path            = hs_data("prep", "reg", params$prev_yr, params$prev_mo),
      corr            = corr$old_reg,
      warehouse_table = "prep_old",
      id_col          = c("prep_id" = "integer"),
      dta_pid         = "PATIENT_ID",
      remove_cols     = "CENTRAL_ID",
      remove_rows     = corr$anti_join,
      id_registry     = forms$id_registry,
      reload          = reprocess
   )

   official$old_outcome <- hs_data("prep", "outcome", params$prev_yr, params$prev_mo) %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      )

   # clean if any for cleaning found
   if (!is.null(corr$old_outcome)) {
      log_info("Performing cleaning on the outcome dataset.")
      official$old_outcome <- .cleaning_list(official$old_outcome, corr$old_outcome, "PREP_ID", "integer")
   }
   official$dupes <- official$old_reg %>% get_dupes(CENTRAL_ID)
   if (nrow(official$dupes) > 0)
      log_warn("Duplicate {green('Central IDs')} found.")

   return(official)
}

.init <- function(envir = parent.env(environment()), ...) {
   p    <- envir
   vars <- as.list(list(...))

   # handle logic here
   update_warehouse(vars$update_lw)
   p$params <- set_coverage(vars$end_date)
   update_first_last_prep(vars$update_visits, p$params, p$wd)
   update_initiation(vars$update_init, p$params, p$wd)
   update_prep_rec_link(vars$update_link, p$wd)

   # ! corrections
   dl <- ifelse(
      !is.null(vars$dl_corr) && vars$dl_corr %in% c("1", "2"),
      vars$dl_corr,
      input(
         prompt  = glue("GET: {green('corrections')}?"),
         options = c("1" = "yes", "2" = "no"),
         default = "2"
      )
   )
   if (dl == "1")
      p$corr <- gdrive_correct3(params$ym, "prep")

   # ! forms
   dl <- ifelse(
      !is.null(vars$dl_forms) && vars$dl_forms %in% c("1", "2"),
      vars$dl_forms,
      input(
         prompt  = "GET: {green('forms')}?",
         options = c("1" = "Yes", "2" = "No"),
         default = "1"
      )
   )
   if (dl == "1")
      p$forms <- download_tables(p$params)

   # ! old dataset
   update <- ifelse(
      !is.null(vars$update_harp) && vars$update_harp %in% c("1", "2"),
      vars$update_harp,
      input(
         prompt  = "Reload previous dataset?",
         options = c("1" = "yes", "2" = "no"),
         default = "2"
      )
   )
   if (update == "1")
      p$official <- update_dataset(p$params, p$corr, p$forms, vars$harp_reprocess)

   p$params$latest_prep_id <- max(as.integer(p$official$old_reg$prep_id), na.rm = TRUE)
}
