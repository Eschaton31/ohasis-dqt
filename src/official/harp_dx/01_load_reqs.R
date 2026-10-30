##  Set coverage ---------------------------------------------------------------

set_coverage <- function(max = end_friday(Sys.time())) {
   params      <- coverage_params(max)
   params$p10y <- params$yr - 10

   return(params)
}

##  Generate pre-requisites and endpoints --------------------------------------

# run through all tables
update_warehouse <- function(update) {
   update <- ask_input(update, glue("Update {green('data/forms')} to be used for consolidation?"))
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
      lw_conn <- connect('ohasis-lw')
      db_name <- "ohasis_warehouse"

      # download the data
      for (scope in "dx_new") {
         name <- case_when(
            scope == "dx_new" ~ "Newly Diagnosed",
         )
         log_info("Processing {green(name)}.")

         # update lake
         table_space <- Id(schema = db_name, table = scope)
         if (dbExistsTable(lw_conn, scope))
            dbExecute(lw_conn, glue(r"(TRUNCATE `{db_name}`.`{scope}`;)"))
         # dbRemoveTable(lw_conn, table_space)

         dbExecute(
            lw_conn,
            glue(r"(INSERT INTO {db_name}.{scope} )",
                 read_file(file.path(path_to_sql, glue("{scope}.sql"))))
         )
      }
      log_success("Done!")
      dbDisconnect(lw_conn)
   }
}

##  Filter Initial Data & Remove Already Reported ------------------------------

download_tables <- function(path_to_sql, params) {
   lw_conn <- connect('mariadb-lw')
   on.exit(dbDisconnect(lw_conn), add = TRUE)

   # read queries
   sql              <- list()
   sql$form_a       <- read_file(file.path(path_to_sql, "form_a.sql"))
   sql$form_hts     <- read_file(file.path(path_to_sql, "form_hts.sql"))
   sql$form_cfbs    <- read_file(file.path(path_to_sql, "form_cfbs.sql"))
   sql$px_confirmed <- read_file(file.path(path_to_sql, "px_confirmed.sql"))
   sql$cd4          <- glue(read_file(file.path(path_to_sql, "lab_cd4.sql")), max_date = as.character(params$max))


   # read data
   forms              <- list()
   forms$form_a       <- tracked_select(lw_conn, sql$form_a, "New Form A", format = "Arrow") %>% remove_table_alias()
   forms$form_hts     <- tracked_select(lw_conn, sql$form_hts, "New HTS Form", format = "Arrow") %>% remove_table_alias()
   forms$form_cfbs    <- tracked_select(lw_conn, sql$form_cfbs, "New CFBS Form", format = "Arrow") %>% remove_table_alias()
   forms$px_confirmed <- tracked_select(lw_conn, sql$px_confirmed, "New Confirmed w/ no Form", format = "Arrow") %>% remove_table_alias()
   forms$cd4          <- tracked_select(lw_conn, sql$cd4, "Baseline CD4", format = "Arrow") %>% remove_table_alias()
   forms$non_dupes    <- tracked_select(lw_conn, "SELECT patient_id, non_pair_id FROM ohasis_warehouse.non_dupes", "Non-dupes", format = "Arrow") %>% remove_table_alias()

   log_success("Done.")
   return(forms)
}

##  get pdf results ------------------------------------------------------------

get_rhivda_pdf <- function(params) {
   log_info("Loading list of rHIVda PDF Results.")
   rhivda <- dir_info(file.path(Sys.getenv("DRIVE_DROPBOX"), "File requests/rHIVda Submission/FORMS", params$yr, params$ym), recurse = TRUE)
   rhivda %<>%
      filter(type == "file") %>%
      mutate(
         confirm_code = str_extract(basename(path), "[A-Z][A-Z][A-Z][0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9][0-9]"),
         .before      = 1
      ) %>%
      filter(!is.na(confirm_code))

   return(rhivda)
}

##  Get the previous report's HARP Registry ------------------------------------

update_dataset <- function(params, corr) {
   log_info("Getting previous datasets.")
   official <- list()
   # official$old   <- ohasis$load_old_dta(
   #    path            = hs_data("harp_dx", "reg", params$prev_yr, params$prev_mo),
   #    corr            = corr$corr_reg %>% rename_all(tolower),
   #    warehouse_table = "harp_dx_old",
   #    id_col          = c("idnum" = "integer"),
   #    dta_pid         = "patient_id",
   #    remove_cols     = "central_id",
   #    remove_rows     = corr$corr_drop,
   #    reload          = reprocess
   # )

   conn <- connect('mariadb-lw')
   on.exit(dbDisconnect(conn), add = TRUE)
   # corr_reg & corr_drop are already applied to harp_dx_old in the warehouse
   official$old_reg <- cached_table(conn, 'ohasis_warehouse.harp_dx_old')
   official$dupes   <- get_reg_dupes(official$old_reg)

   return(official)
}

.init <- function(envir = parent.env(environment()), ...) {
   p    <- envir
   vars <- as.list(list(...))

   p$params <- set_coverage(vars$end_date)
   # update_warehouse(vars$update_lw)
   # update_dx_new(vars$update_visits, p$params, p$wd)

   # ! corrections
   if (ask_input(vars$dl_corr, glue("GET: {green('corrections')}?")) == "1")
      p$corr <- flow_corr(p$params$ym, "harp_dx")

   # ! forms
   if (ask_input(vars$dl_forms, glue("GET: {green('forms')}?"), default = "1") == "1")
      p$forms <- download_tables(p$wd, p$params)

   # ! old dataset
   if (ask_input(vars$update_harp, "Reload previous dataset?") == "1")
      p$official <- update_dataset(p$params, p$corr)

   p$params$latest_idnum <- max(as.integer(p$official$old_reg$idnum), na.rm = TRUE)

   # p$pdf_rhivda$data <- get_rhivda_pdf(p$params)
}