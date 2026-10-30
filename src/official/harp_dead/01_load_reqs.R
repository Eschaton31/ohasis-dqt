##  Set coverage ---------------------------------------------------------------

set_coverage <- function(max = end_friday(Sys.time())) {
   params <- coverage_params(max)

   return(params)
}

##  Generate pre-requisites and endpoints --------------------------------------

# run through all tables
update_warehouse <- function(update) {
   update <- ask_input(update, glue("Update {green('data/forms')} to be used for consolidation?"))
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
   lw_conn <- connect('ohasis-lw')
   db_name <- "ohasis_warehouse"

   # download the data
   for (scope in "dead_new") {
      name <- case_when(
         scope == "dead_new" ~ "Newly Dead",
      )
      log_info("Processing {green(name)}.")

      # update lake
      table_space <- Id(schema = db_name, table = scope)
      if (dbExistsTable(lw_conn, scope))
         dbExecute(lw_conn, glue(r"(TRUNCATE `{db_name}`.`{scope}`;)"))
      # dbRemoveTable(lw_conn, table_space)

      dbExecute(
         lw_conn,
         glue(r"(insert into {db_name}.{scope} )",
              read_file(file.path(path_to_sql, glue("{scope}.sql"))))
      )
      # dbExecute(lw_conn, glue("alter table {db_name}.{scope} add index `central_id` (`central_id`);"))

   }
   log_success("Done!")
   dbDisconnect(lw_conn)
}

##  Download records -----------------------------------------------------------

download_tables <- function(params) {
   lw_conn <- connect('mariadb-lw')
   on.exit(dbDisconnect(lw_conn), add = TRUE)
   forms <- list()

   log_info("Downloading {green('Central IDs')}.")
   forms$id_registry <- update_idreg() %>% select(central_id, patient_id)

   log_info("Downloading {green('Form D')}.")
   forms$form_d <- QB$new(lw_conn)$from("ohasis_warehouse.form_d")$whereNull('deleted_at')$get(format = 'Arrow') %>% remove_table_alias()

   # read once; used by data_mortality and dedup_dx
   dx_cols      <- c(
      "rec_id",
      "labcode",
      "labcode2",
      "idnum",
      "uic",
      "firstname",
      "middle",
      "last",
      "name_suffix",
      "name",
      "bdate",
      "sex",
      "pxcode",
      "philhealth",
      "region",
      "province",
      "muncity"
   )
   forms$dx_reg <- lw_registry("harp_dx", dx_cols, params, forms$id_registry, keep_pid = TRUE) %>%
      mutate(
         labcode2 = coalesce(labcode2, labcode)
      )

   log_success("Done.")
   return(forms)
}

##  Get the previous report's harp Registry ------------------------------------

update_dataset <- function(params, corr) {
   log_info("Getting previous datasets.")
   official <- list()
   # official$old   <- ohasis$load_old_dta(
   #    path            = hs_data("harp_dead", "reg", params$prev_yr, params$prev_mo),
   #    corr            = corr$corr_reg,
   #    warehouse_table = "harp_dead_old",
   #    id_col          = c("mort_id" = "integer"),
   #    dta_pid         = "patient_id",
   #    remove_cols     = "central_id",
   #    remove_rows     = corr$corr_drop,
   #    id_registry     = forms$id_registry,
   #    reload          = reprocess
   # )

   conn <- connect('mariadb-lw')
   on.exit(dbDisconnect(conn), add = TRUE)

   # corr_reg & corr_drop are already applied to harp_dead_old in the warehouse
   official$old_reg <- cached_table(conn, 'ohasis_warehouse.harp_dead_old')
   official$dupes   <- get_reg_dupes(official$old_reg)

   return(official)
}

.init <- function(envir = parent.env(environment()), ...) {
   p    <- envir
   vars <- as.list(list(...))

   p$params <- set_coverage(vars$end_date)
   update_warehouse(vars$update_lw)
   # update_dead_new(p$wd)

   # ! corrections
   if (ask_input(vars$dl_corr, glue("GET: {green('corrections')}?")) == "1")
      p$corr <- flow_corr(p$params$ym, "harp_dead")

   # ! forms
   if (ask_input(vars$dl_forms, glue("GET: {green('forms')}?"), default = "1") == "1")
      p$forms <- download_tables(p$params)

   # ! old dataset
   if (ask_input(vars$update_harp, "Reload previous dataset?") == "1")
      p$official <- update_dataset(p$params, p$corr)

   p$params$latest_mort_id <- max(as.integer(p$official$old_reg$mort_id), na.rm = TRUE)
}