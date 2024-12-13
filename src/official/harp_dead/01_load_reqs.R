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
      tables$warehouse <- c("form_d", "id_registry")

      lapply(tables$warehouse, function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE))
   }
}

##  Get new dead ---------------------------------------------------------------

# check if art starts to be re-processed
update_dead_new <- function(path_to_sql) {
   lw_conn <- ohasis$conn("lw")
   db_name <- "ohasis_warehouse"

   # download the data
   for (scope in "dead_new") {
      name <- case_when(
         scope == "dead_new" ~ "Newly Dead",
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
      dbExecute(lw_conn, glue("ALTER TABLE {db_name}.{scope} ADD INDEX `CENTRAL_ID` (`CENTRAL_ID`);"))

   }
   log_success("Done!")
   dbDisconnect(lw_conn)
}

##  Download records -----------------------------------------------------------

download_tables <- function(params) {
   lw_conn <- ohasis$conn("lw")
   forms   <- list()

   min     <- params$min
   max     <- params$max
   db_name <- "ohasis_warehouse"

   log_info("Downloading {green('Central IDs')}.")
   # forms$id_registry <- dbTable(lw_conn, db_name, "id_registry", cols = c("CENTRAL_ID", "PATIENT_ID"))
   forms$id_registry <- update_idreg() %>% select(CENTRAL_ID, PATIENT_ID)

   log_info("Downloading {green('ART Visits w/in the scope')}.")
   forms$form_d <- dbTable(lw_conn, db_name, "form_d")

   log_success("Done.")
   dbDisconnect(lw_conn)
   return(forms)
}

##  Get the previous report's HARP Registry ------------------------------------

update_dataset <- function(params, corr, forms, reprocess) {
   log_info("Getting previous datasets.")
   official       <- list()
   official$old   <- ohasis$load_old_dta(
      path            = hs_data("harp_dead", "reg", params$prev_yr, params$prev_mo),
      corr            = corr$corr_reg,
      warehouse_table = "harp_dead_old",
      id_col          = c("mort_id" = "integer"),
      dta_pid         = "PATIENT_ID",
      remove_cols     = "CENTRAL_ID",
      remove_rows     = corr$corr_drop,
      id_registry     = forms$id_registry,
      reload          = reprocess
   )
   official$dupes <- official$old %>% get_dupes(CENTRAL_ID)
   if (nrow(official$dupes) > 0)
      log_warn("Duplicate {green('Central IDs')} found.")

   return(official)
}

define_params <- function() {
   local(envir = nhsss$harp_dead, {
      params                <- list()
      params$latest_mort_id <- max(as.integer(official$old$mort_id), na.rm = TRUE)
   })
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
   if (dl == "1") {
      p$corr <- flow_corr(p$params$ym, "harp_dead")
      # p$corr <- gdrive_correct3(p$params$ym, "harp_dead")
   }
   dl <- ifelse(
      !is.null(vars$dl_forms) && vars$dl_forms %in% c("1", "2"),
      vars$dl_forms,
      input(
         prompt  = "GET: {green('forms')}?",
         options = c("1" = "Yes", "2" = "No"),
         default = "1"
      )
   )

   update_dead_new(p$wd)
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

   p$params$latest_mort_id <- max(as.integer(p$official$old$mort_id), na.rm = TRUE)
}