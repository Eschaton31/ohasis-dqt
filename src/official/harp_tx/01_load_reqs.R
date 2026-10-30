##  Set coverage ---------------------------------------------------------------

set_coverage <- function(max = end_friday(Sys.time())) {
   params <- coverage_params(max)

   # special clinics
   params$clinics <- list(
      sail = c("040200", "040211", "130748", "130814"),
      tly  = c("040198", "070021", "130001", "130173", "130707", "130708", "130749", "130751", "130845")
   )

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
         "px_key_pop",
         "px_staging",
         "lab_wide",
         "px_vaccine",
         "px_tb_info",
         "px_prophylaxis",
         "px_oi",
         "px_ob",
         "disc_meds",
         "disp_meds"
      )
      tables$warehouse <- c("form_art_bc", "id_registry")

      lapply(tables$lake, function(table) ohasis$data_factory("lake", table, "upsert", TRUE))
      lapply(tables$warehouse, function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE))
   }
}

##  Get earliest & latest visit data -------------------------------------------

# check if art starts to be re-processed
update_first_last_art <- function(update, params, path_to_sql) {
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
      lw_conn <- connect('ohasis-lw')
      db_name <- "ohasis_warehouse"

      # download the data
      for (scope in c("art_first", "art_last", "art_lastdisp", "confirm_last")) {
         name <- case_when(
            scope == "art_first" ~ "ART Start Dates",
            scope == "art_last" ~ "ART Latest Visits",
            scope == "art_lastdisp" ~ "ART Latest Dispense",
            scope == "vl_last" ~ "Latest VL Data",
            scope == "confirm_last" ~ "Latest Confirmatory Data",
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

##  Download records -----------------------------------------------------------

download_tables <- function(params) {
   lw_conn <- connect('mariadb-lw')
   on.exit(dbDisconnect(lw_conn), add = TRUE)
   forms <- list()

   min      <- params$min
   max      <- params$max
   max_next <- as.character(as.Date(max) + 1)
   db_name  <- "ohasis_warehouse"

   log_info("Downloading {green('Central IDs')}.")
   # forms$id_registry <- dbTable(lw_conn, db_name, "id_registry", cols = c("CENTRAL_ID", "PATIENT_ID"))
   forms$id_registry <- update_idreg() %>% select(central_id, patient_id)

   log_info("Downloading {green('Non-Dupes')}.")
   # forms$non_dupes <- dbTable(lw_conn, db_name, "non_dupes", cols = c("patient_id", "non_pair_id"))
   forms$non_dupes <- tracked_select(lw_conn, "SELECT patient_id, non_pair_id FROM ohasis_warehouse.non_dupes", "Non-dupes", format = "Arrow") %>% remove_table_alias()

   log_info("Downloading {green('ART Visits w/in the scope')}.")
   recs <- QB$new(lw_conn)$from("ohasis_warehouse.form_art_bc")
   # bare column ranges (no date() wrapper) so the key/index can be used
   recs$where(function(query = QB$new(lw_conn)) {
      query$whereRaw(glue("visit_date >= '{min}' and visit_date < '{max_next}'"), "or")
      query$whereRaw(glue("created_at >= '{min}' and created_at < '{max_next}'"), "or")
      query$whereRaw(glue("updated_at >= '{min}' and updated_at < '{max_next}'"), "or")
      query$whereRaw(
         r"(rec_id in (
            select rec_id from ohasis_warehouse.art_first
            union all select rec_id from ohasis_warehouse.art_lastdisp
            union all select rec_id from ohasis_warehouse.art_last
         ))",
         "or"
      )
      query$whereNested
   })
   recs$whereNull('deleted_at')
   recs$select(
      "rec_id",
      "created_at",
      "patient_id",
      "confirmatory_code",
      "uic",
      "patient_code",
      "philhealth_no",
      "philsys_id",
      "first",
      "middle",
      "last",
      "suffix",
      "birthdate",
      "age",
      "age_mo",
      "sex",
      "self_ident",
      "self_ident_other",
      "client_mobile",
      "client_email",
      "curr_reg",
      "curr_prov",
      "curr_munc",
      "curr_addr",
      "faci_id",
      "sub_faci_id",
      "service_faci",
      "service_sub_faci",
      "faci_disp",
      "sub_faci_disp",
      "client_type",
      "tx_status",
      "who_class",
      "visit_type",
      "visit_date",
      "record_date",
      "disp_date",
      "latest_next_date",
      "medicine_summary",
      "lab_xray_date",
      "lab_xray_result",
      "lab_xpert_date",
      "lab_xpert_result",
      "lab_dssm_date",
      "lab_dssm_result",
      "tb_status",
      "oi_syph_present",
      "oi_hepb_present",
      "oi_hepc_present",
      "oi_pcp_present",
      "oi_cmv_present",
      "oi_orocand_present",
      "oi_herpes_present",
      "oi_other_text",
      "is_pregnant",
      "clinic_notes",
      "counsel_notes"
   )
   forms$form_art_bc <- recs$get(format = 'Arrow') %>%
      get_cid(forms$id_registry, patient_id) %>%
      mutate(medicine_summary = remove_non_arv(medicine_summary))

   # visit tables carry their own central_id
   visit_data <- forms$form_art_bc %>% select(-central_id)

   log_info("Downloading {green('Earliest ART Visits')}.")
   forms$art_first <- QB$new(lw_conn)$from("ohasis_warehouse.art_first")$select("central_id", "rec_id", "visit_date")$get(format = 'Arrow') %>%
      left_join(visit_data, join_by(rec_id, visit_date))

   log_info("Downloading {green('Latest ART Visits')}.")
   forms$art_last <- QB$new(lw_conn)$from("ohasis_warehouse.art_last")$select("central_id", "rec_id", "visit_date")$get(format = 'Arrow') %>%
      left_join(visit_data, join_by(rec_id, visit_date))

   log_info("Downloading {green('Latest ART Dispensing')}.")
   forms$art_lastdisp <- QB$new(lw_conn)$from("ohasis_warehouse.art_lastdisp")$select("central_id", "rec_id", "visit_date")$get(format = 'Arrow') %>%
      left_join(visit_data, join_by(rec_id, visit_date))

   log_info("Downloading {green('Latest VL Data')}.")
   # forms$vl_last <- lw_conn %>%
   #    dbTable(
   #       db_name,
   #       "vl_last",
   #       cols = c("CENTRAL_ID", "LAB_VIRAL_DATE", "LAB_VIRAL_RESULT")
   #    )
   vl_cols       <- c("vl_drop", "vl_error", "central_id", "vl_date", "vl_sort", "res_tag", "vl_result_clean")
   forms$vl_last <- hs_data("harp_vl", "all", params$yr, params$mo) %>%
      read_dta(col_select = matches(str_c("^(", str_c(vl_cols, collapse = "|"), ")$"), ignore.case = TRUE)) %>%
      rename_all(tolower) %>%
      filter(
         vl_drop == 0,
         vl_error == 0,
         coalesce(central_id, "") != "",
         vl_date <= params$max,
      ) %>%
      arrange(vl_sort, desc(vl_date), res_tag) %>%
      select(
         central_id,
         lab_viral_date   = vl_date,
         lab_viral_result = vl_result_clean
      ) %>%
      rename_all(tolower) %>%
      distinct(central_id, .keep_all = TRUE)

   log_info("Downloading {green('Latest Confirmatory Data')}.")
   forms$confirm_last <- QB$new(lw_conn)$from("ohasis_warehouse.confirm_last")$select("central_id", "date_confirm", "confirm_code", "confirm_result", "confirm_remarks")$get(format = 'Arrow')

   # CD4 is downloaded in 02_data_tx_new, only for the month's enrollees

   log_success("Done.")
   return(forms)
}

##  Get the previous report's HARP Registry ------------------------------------

update_dataset <- function(params, corr) {
   log_info("Getting previous datasets.")
   official <- list()
   # official$old_reg <- ohasis$load_old_dta(
   #    path            = hs_data("harp_tx", "reg", params$prev_yr, params$prev_mo),
   #    corr            = corr$corr_reg %>% rename_all(tolower),
   #    warehouse_table = "harp_tx_old",
   #    id_col          = c("art_id" = "integer"),
   #    dta_pid         = "patient_id",
   #    remove_cols     = "central_id",
   #    remove_rows     = corr$corr_drop,
   #    id_registry     = forms$id_registry,
   #    reload          = reprocess
   # )

   conn <- connect('mariadb-lw')
   on.exit(dbDisconnect(conn), add = TRUE)

   # corr_reg & corr_drop are already applied to harp_tx_old in the warehouse
   official$old_reg <- cached_table(conn, 'ohasis_warehouse.harp_tx_old') %>%
      mutate(artstart_regimen = remove_non_arv(artstart_regimen))
   official$dupes   <- get_reg_dupes(official$old_reg)

   official$old_outcome <- load_old_outcome("harp_tx", "art_id", params, corr) %>%
      mutate(across(any_of(c("latest_regimen", "previous_regimen")), remove_non_arv))

   return(official)
}

.init <- function(envir = parent.env(environment()), ...) {
   p    <- envir
   vars <- as.list(list(...))

   p$params <- set_coverage(vars$end_date)
   # update_warehouse(vars$update_lw)
   update_first_last_art(vars$update_visits, p$params, p$wd)

   # ! corrections
   if (ask_input(vars$dl_corr, glue("GET: {green('corrections')}?")) == "1")
      p$corr <- flow_corr(p$params$ym, "harp_tx")

   # ! forms
   if (ask_input(vars$dl_forms, glue("GET: {green('forms')}?"), default = "1") == "1")
      p$forms <- download_tables(p$params)

   # ! old dataset
   if (ask_input(vars$update_harp, "Reload previous dataset?") == "1")
      p$official <- update_dataset(p$params, p$corr)

   p$params$latest_art_id <- max(as.integer(p$official$old_reg$art_id), na.rm = TRUE)
}