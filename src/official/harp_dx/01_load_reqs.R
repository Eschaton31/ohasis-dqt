##  Set coverage ---------------------------------------------------------------

set_coverage <- function(max = end_friday(Sys.time())) {
   params   <- list()
   max_date <- as.Date(max)

   params$yr   <- year(max_date)
   params$mo   <- month(max_date)
   params$ym   <- str_c(sep = ".", params$yr, stri_pad_left(params$mo, 2, "0"))
   params$p10y <- params$yr - 10
   params$min  <- max_date %m-% days(30) %>% as.character()
   params$max  <- max

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
         "px_other_service"
      )
      tables$warehouse <- c("form_a", "form_hts", "id_registry")

      lapply(tables$lake, function(table) ohasis$data_factory("lake", table, "upsert", TRUE))
      lapply(tables$warehouse, function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE))
   }
}

##  Get nen diagnoses ----------------------------------------------------------

# check if art starts to be re-processed
update_dx_new <- function(update, params, path_to_sql) {
   update <- ifelse(
      !is.null(update) && update %in% c("1", "2"),
      update,
      input(
         prompt  = glue("Do you want to re-process the {green('Newly Diagnoses Records')}?"),
         options = c("1" = "Yes", "2" = "No"),
         default = "1"
      )
   )
   # if Yes, re-process
   if (update == "1") {
      lw_conn <- ohasis$conn("lw")
      db_name <- "ohasis_warehouse"

      # download the data
      for (scope in "dx_new") {
         name <- case_when(
            scope == "dx_new" ~ "Newly Diagnosed",
         )
         log_info("Processing {green(name)}.")

         # update lake
         table_space <- Id(schema = db_name, table = scope)
         if (dbExistsTable(lw_conn, table_space))
            dbRemoveTable(lw_conn, table_space)

         dbExecute(
            lw_conn,
            glue(r"(CREATE TABLE {db_name}.{scope} AS )",
                 read_file(file.path(path_to_sql, glue("{scope}.sql"))))
         )
      }
      log_success("Done!")
      dbDisconnect(lw_conn)
   }
}

##  Filter Initial Data & Remove Already Reported ------------------------------

download_tables <- function(path_to_sql) {
   lw_conn <- ohasis$conn("lw")

   # read queries
   sql              <- list()
   sql$form_a       <- read_file(file.path(path_to_sql, "form_a.sql"))
   sql$form_hts     <- read_file(file.path(path_to_sql, "form_hts.sql"))
   sql$form_cfbs    <- read_file(file.path(path_to_sql, "form_cfbs.sql"))
   sql$px_confirmed <- read_file(file.path(path_to_sql, "px_confirmed.sql"))
   sql$cd4          <- read_file(file.path(path_to_sql, "lab_cd4.sql"))


   # read data
   data              <- list()
   data$form_a       <- tracked_select(lw_conn, sql$form_a, "New Form A")
   data$form_hts     <- tracked_select(lw_conn, sql$form_hts, "New HTS Form")
   data$form_cfbs    <- tracked_select(lw_conn, sql$form_cfbs, "New CFBS Form")
   data$px_confirmed <- tracked_select(lw_conn, sql$px_confirmed, "New Confirmed w/ no Form")
   data$cd4          <- tracked_select(lw_conn, sql$cd4, "Baseline CD4", list(as.character(params$max)))
   data$non_dupes    <- tracked_select(lw_conn, "SELECT PATIENT_ID, NON_PAIR_ID FROM ohasis_warehouse.non_dupes", "Non-dupes")

   dbDisconnect(lw_conn)

   return(data)
}

##  get pdf results ------------------------------------------------------------

get_rhivda_pdf <- function(params) {
   log_info("Loading list of rHIVda PDF Results.")
   rhivda <- dir_info(file.path(Sys.getenv("DRIVE_DROPBOX"), "File requests/rHIVda Submission/FORMS", params$yr, params$ym), recurse = TRUE)
   rhivda %<>%
      filter(type == "file") %>%
      mutate(
         CONFIRM_CODE = str_extract(basename(path), "[A-Z][A-Z][A-Z][0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9][0-9]"),
         .before      = 1
      ) %>%
      filter(!is.na(CONFIRM_CODE))

   return(rhivda)
}

##  Get the previous report's HARP Registry ------------------------------------

update_dataset <- function(params, corr, reprocess) {
   log_info("Getting previous datasets.")
   official       <- list()
   official$old   <- ohasis$load_old_dta(
      path            = hs_data("harp_dx", "reg", params$prev_yr, params$prev_mo),
      corr            = corr$old_reg,
      warehouse_table = "harp_dx_old",
      id_col          = c("idnum" = "integer"),
      dta_pid         = "PATIENT_ID",
      remove_cols     = "CENTRAL_ID",
      remove_rows     = corr$anti_join,
      reload          = reprocess
   )
   official$dupes <- official$old %>% get_dupes(CENTRAL_ID)
   if (nrow(official$dupes) > 0)
      log_warn("Duplicate {green('Central IDs')} found.")

   return(official)
}

.init <- function(envir = parent.env(environment()), ...) {
   p    <- envir
   vars <- as.list(list(...))

   update_warehouse(vars$update_lw)
   p$params <- set_coverage(vars$end_date)

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
      p$corr <- gdrive_correct3(p$params$ym, "harp_dx")

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
      p$official <- update_dataset(p$params, p$corr, vars$harp_reprocess)

   p$params$latest_idnum <- max(as.integer(p$official$old$idnum), na.rm = TRUE)

   update_dx_new(vars$update_visits, p$params, p$wd)
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
      p$forms <- download_tables(p$wd)

   p$pdf_rhivda$data <- get_rhivda_pdf(p$params)
}