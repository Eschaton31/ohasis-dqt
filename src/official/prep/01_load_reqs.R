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
         prompt  = glue("Do you want to re-process the {green('PrEP Start & Latest Dates')}?"),
         options = c("1" = "Yes", "2" = "No"),
         default = "1"
      )
   )
   # if Yes, re-process
   if (update == "1") {
      lw_conn <- connect('ohasis-lw')
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
            dbExecute(lw_conn, glue(r"(TRUNCATE `{db_name}`.`{scope}`;)"))
         # dbRemoveTable(lw_conn, table_space)

         dbExecute(
            lw_conn,
            glue(r"(INSERT INTO {db_name}.{scope} )",
                 stri_replace_all_fixed(read_file(file.path(path_to_sql, glue("{scope}.sql"))), "?", glue("'{as.character(params$max)}'"))),
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
      DELETE ohasis.rec_link
      FROM ohasis.rec_link
               LEFT JOIN ohasis.px_record
                         ON rec_link.{replace} collate utf8mb4_unicode_ci = px_record.rec_id
      WHERE px_record.deleted_at IS NOT NULL;
      )"
      dbExecute(db_conn, stri_replace_all_fixed(delete_sql, "{replace}", "destination_rec"))
      dbExecute(db_conn, stri_replace_all_fixed(delete_sql, "{replace}", "source_rec"))
      dbDisconnect(db_conn)
      # refresh once to remove deleted records from live; figure out upsert in
      # future processing
      # ohasis$data_factory("warehouse", "rec_link", "refresh", TRUE)

      lw_conn     <- connect('mariadb-lw')
      nolink_hts  <- QB$new(lw_conn)$
         from('ohasis_lake.px_demographics as pii')$
         leftJoin('ohasis_lake.px_hiv_testing as test', 'pii.rec_id', '=', 'test.rec_id')$
         leftJoin('ohasis_lake.px_hiv_confirmatory as conf', 'pii.rec_id', '=', 'conf.rec_id')$
         select(rec_id, patient_id, record_date, form_id)$
         whereRaw("pii.rec_id not in (select source_rec from ohasis_lake.rec_link)")$
         whereRaw("left(coalesce(nullif(nullif(conf.confirm_result, '4_Pending'), '5_Duplicate'), test.t3_result, test.t2_result, test.t1_result, test.t0_result, ''), 1) <> '1'")$
         whereNull("pii.deleted_at")$
         whereIn('pii.form_id', c('hts2021', 'a2017', 'cfbs2020'))$
         get()
      nolink_prep <- QB$new(lw_conn)$
         from('ohasis_warehouse.form_prep')$
         select(rec_id, patient_id, record_date, form_id)$
         whereRaw("rec_id not in (select destination_rec from ohasis_lake.rec_link)")$
         whereNull("deleted_at")$
         get()
      dbDisconnect(lw_conn)

      idreg <- update_idreg()

      df <- nolink_prep %>%
         get_cid(idreg, patient_id) %>%
         rename(
            prep_rec  = rec_id,
            prep_date = record_date
         ) %>%
         mutate(
            hts_prep_before = prep_date %m-% days(7),
            hts_prep_after  = prep_date %m+% days(7),
         ) %>%
         left_join(
            y  = nolink_hts %>%
               get_cid(idreg, patient_id) %>%
               rename(
                  hts_rec  = rec_id,
                  hts_date = record_date
               ),
            by = join_by(central_id, between(y$hts_date, x$hts_prep_before, x$hts_prep_after))
         ) %>%
         mutate(
            hts_prep_diff = abs(interval(hts_date, prep_date) / days(1))
         ) %>%
         filter(hts_prep_diff <= 7) %>%
         arrange(prep_rec, hts_prep_diff) %>%
         distinct(prep_rec, .keep_all = TRUE) %>%
         select(
            source_rec      = hts_rec,
            destination_rec = prep_rec,
         ) %>%
         mutate(
            created_by = '1300000001',
            created_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
            updated_by = NA_character_,
            updated_at = NA_character_,
            deleted_by = NA_character_,
            deleted_at = NA_character_,
         )

      if (nrow(df) > 0) {
         log_info("Uploading to OHASIS.")
         db_conn <- ohasis$conn("db")
         dbxUpsert(
            db_conn,
            Id(schema = "ohasis", table = "rec_link"),
            df,
            'source_rec'
         )
         dbDisconnect(db_conn)

         # refresh again to get new data
         # ohasis$data_factory("warehouse", "rec_link", "refresh", TRUE)
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
   lw_conn <- connect('mariadb-lw')
   forms   <- list()

   min       <- params$min
   max       <- params$max
   db_name   <- "ohasis_warehouse"
   hts_where <- r"(
   pii.rec_id in (select source_rec from ohasis_lake.rec_link) or
      pii.rec_id in (select rec_id from ohasis_warehouse.prep_offer)
   )"

   log_info("Downloading {green('Central IDs')}.")
   # forms$id_registry <- dbTable(lw_conn, db_name, "id_registry", cols = c("CENTRAL_ID", "PATIENT_ID"))
   forms$id_registry <- update_idreg() %>% select(central_id, patient_id)

   log_info("Downloading {green('Form A')}.")
   forms$form_a <- QB$new(lw_conn)$
      from('ohasis_warehouse.form_a as pii')$
      whereRaw(hts_where)$
      get()

   log_info("Downloading {green('HTS Form')}.")
   forms$form_hts <- QB$new(lw_conn)$
      from('ohasis_warehouse.form_hts as pii')$
      whereRaw(hts_where)$
      get()

   log_info("Downloading {green('CFBS Form')}.")
   forms$form_cfbs <- QB$new(lw_conn)$
      from('ohasis_warehouse.form_cfbs as pii')$
      whereRaw(hts_where)$
      get()

   log_info("Downloading {green('Testing')}.")
   forms$testing <- QB$new(lw_conn)$
      from('ohasis_lake.px_demographics as pii')$
      leftJoin('ohasis_lake.px_hiv_testing as test', 'pii.rec_id', '=', 'test.rec_id')$
      leftJoin('ohasis_lake.px_hiv_confirmatory as confirmatory', 'pii.rec_id', '=', 'confirmatory.rec_id')$
      leftJoin('ohasis_lake.id_registry as id', 'pii.patient_id', '=', 'id.patient_id')$
      selectRaw("coalesce(id.central_id, pii.patient_id) as central_id")$
      select('pii.rec_id',
             'pii.patient_id',
             'pii.faci_id',
             'pii.sub_faci_id',
             'pii.record_date',
             'pii.confirmatory_code',
             'pii.uic',
             'pii.philhealth_no',
             'pii.sex',
             'pii.birthdate',
             'pii.patient_code',
             'pii.philsys_id',
             'pii.first',
             'pii.middle',
             'pii.last',
             'pii.suffix',
             'test.t0_date',
             'test.t0_result',
             'test.t1_kit',
             'test.t1_date',
             'test.t1_result',
             'test.t2_kit',
             'test.t2_date',
             'test.t2_result',
             'test.t3_kit',
             'test.t3_date',
             'test.t3_result',
             'confirmatory.confirm_faci',
             'confirmatory.confirm_sub_faci',
             'confirmatory.confirm_type',
             'confirmatory.confirm_code',
             'confirmatory.specimen_refer_type',
             'confirmatory.specimen_source',
             'confirmatory.specimen_sub_source',
             'confirmatory.date_collect',
             'confirmatory.date_receive',
             'confirmatory.confirm_result',
             'confirmatory.confirm_remarks',
             'confirmatory.signatory_1',
             'confirmatory.signatory_2',
             'confirmatory.signatory_3',
             'confirmatory.date_release',
             'confirmatory.date_confirm',
             'confirmatory.idnum',
             'confirmatory.rt_agreed',
             'confirmatory.rt_date',
             'confirmatory.rt_result',
             'confirmatory.rt_kit',
             'confirmatory.rt_vl_requested',
             'confirmatory.rt_vl_done',
             'confirmatory.rt_vl_date',
             'confirmatory.rt_vl_result',
             'confirmatory.rita_result',
             'pii.created_by',
             'pii.created_at',
             'pii.updated_by',
             'pii.updated_at',
             'pii.deleted_by',
             'pii.deleted_at')$
      whereRaw(hts_where)$
      get()

   log_info("Processing {green('HTS Data')}.")
   forms$hts_data <- process_hts(forms$form_hts, forms$form_a, forms$form_cfbs, forms$testing)

   log_info("Downloading {green('Record Links')}.")
   forms$rec_link <- QB$new(lw_conn)$from('ohasis_lake.rec_link')$get()

   log_info("Downloading {green('PrEP Visits w/in the scope')}.")
   recs <- QB$new(lw_conn)$from("ohasis_warehouse.form_prep")
   recs$where(function(query = QB$new(lw_conn)) {
      query$whereRaw("rec_id in (select rec_id from ohasis_warehouse.prep_first)", "or")
      query$whereRaw("rec_id in (select rec_id from ohasis_warehouse.prepdisp_first)", "or")
      query$whereRaw("rec_id in (select rec_id from ohasis_warehouse.prepdisp_last)", "or")
      query$whereRaw("rec_id in (select rec_id from ohasis_warehouse.prepdisc_last)", "or")
      query$whereRaw("rec_id in (select rec_id from ohasis_warehouse.prep_last)", "or")
      query$whereNested
   })
   recs$whereNull('deleted_at')

   forms$form_prep <- recs$get()

   log_info("Downloading {green('PrEP First Offer')}.")
   forms$prep_offer <- QB$new(lw_conn)$from("ohasis_warehouse.prep_offer")$select(central_id, rec_id, visit_date)$get() %>%
      left_join(forms$hts_data, join_by(rec_id))


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
   forms$prep_first <- QB$new(lw_conn)$from("ohasis_warehouse.prep_first")$select(central_id, rec_id, visit_date)$get() %>%
      left_join(forms$form_prep, join_by(rec_id, visit_date))

   log_info("Downloading {green('PrEP Enrollment')}.")
   forms$prepdisp_first <- QB$new(lw_conn)$from("ohasis_warehouse.prepdisp_first")$select(central_id, rec_id, visit_date)$get() %>%
      left_join(forms$form_prep, join_by(rec_id, visit_date))

   log_info("Downloading {green('Latest PrEP Visits')}.")
   forms$prep_last <- QB$new(lw_conn)$from("ohasis_warehouse.prep_last")$select(central_id, rec_id, visit_date)$get() %>%
      left_join(forms$form_prep, join_by(rec_id, visit_date))

   log_info("Downloading {green('Latest PrEP Dispensing')}.")
   forms$prepdisp_last <- QB$new(lw_conn)$from("ohasis_warehouse.prepdisp_last")$select(central_id, rec_id, visit_date)$get() %>%
      left_join(forms$form_prep, join_by(rec_id, visit_date))

   # log_info("Downloading {green('Latest PrEP Initiation')}.")
   # forms$prep_init_p12m <- lw_conn %>%
   #    dbTable(
   #       db_name,
   #       "prep_init_p12m",
   #       cols = c("CENTRAL_ID", "REC_ID", "INITIATION_DATE")
   #    )

   log_info("Downloading {green('Latest PrEP Discontinuation')}.")
   forms$prepdisc_last <- QB$new(lw_conn)$from("ohasis_warehouse.prepdisc_last")$select(central_id, rec_id, visit_date)$get() %>%
      left_join(forms$form_prep, join_by(rec_id, visit_date))

   dbDisconnect(lw_conn)
   log_success("Done.")
   return(forms)
}

##  Get the previous report's PrEP Registry ------------------------------------

update_dataset <- function(params, corr, forms, reprocess) {
   log_info("Getting previous datasets.")
   official         <- list()
   # official$old_reg <- ohasis$load_old_dta(
   #    path            = hs_data("prep", "reg", params$prev_yr, params$prev_mo),
   #    corr            = corr$corr_reg,
   #    warehouse_table = "prep_old",
   #    id_col          = c("prep_id" = "integer"),
   #    dta_pid         = "PATIENT_ID",
   #    remove_cols     = "CENTRAL_ID",
   #    remove_rows     = corr$corr_drop,
   #    id_registry     = forms$id_registry,
   #    reload          = reprocess
   # )

   conn         <- connect('mariadb-lw')
   official$old_reg <- QB$new(conn)$from('ohasis_warehouse.prep_old')$get()
   dbDisconnect(conn)

   official$old_outcome <- hs_data("prep", "outcome", params$prev_yr, params$prev_mo) %>%
      read_dta() %>%
      rename_all(tolower) %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      )

   # clean if any for cleaning found
   if (nrow(corr$corr_outcome) > 0) {
      log_info("Performing cleaning on the outcome dataset.")
      official$old_outcome <- apply_corrections(official$old_outcome, corr$corr_outcome %>%
         rename_all(tolower) %>%
         mutate(variable = tolower(variable)), 'prep_id')
      # .cleaning_list(official$old_outcome, corr$corr_outcome %>% rename_all(tolower), "ART_ID", "integer")
   }
   official$dupes <- official$old_reg %>% get_dupes(central_id)
   if (nrow(official$dupes) > 0)
      log_warn("Duplicate {green('Central IDs')} found.")

   return(official)
}

.init <- function(envir = parent.env(environment()), ...) {
   p    <- envir
   vars <- as.list(list(...))

   # handle logic here
   # update_warehouse(vars$update_lw)
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
   if (dl == "1") {
      p$corr <- flow_corr(params$ym, "prep")
      # p$corr <- gdrive_correct3(params$ym, "prep")
   }
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
