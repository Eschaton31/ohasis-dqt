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
      lw_conn <- ohasis$conn("lw")
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

##  Download records -----------------------------------------------------------

download_tables <- function(params) {
   lw_conn <- ohasis$conn("lw")
   forms   <- list()

   min     <- params$min
   max     <- params$max
   db_name <- "ohasis_warehouse"

   log_info("Downloading {green('Central IDs')}.")
   forms$id_registry <- dbTable(lw_conn, db_name, "id_registry", cols = c("CENTRAL_ID", "PATIENT_ID"))

   log_info("Downloading {green('Non-Dupes')}.")
   forms$non_dupes <- dbTable(lw_conn, db_name, "non_dupes", cols = c("PATIENT_ID", "NON_PAIR_ID"))

   log_info("Downloading {green('ART Visits w/in the scope')}.")
   forms$form_art_bc <- lw_conn %>%
      dbTable(
         db_name,
         "form_art_bc",
         cols      = c(
            "REC_ID",
            "REC_ID_GRP",
            "CREATED_AT",
            "PATIENT_ID",
            "FORM_VERSION",
            "CONFIRMATORY_CODE",
            "UIC",
            "PATIENT_CODE",
            "PHILHEALTH_NO",
            "PHILSYS_ID",
            "FIRST",
            "MIDDLE",
            "LAST",
            "SUFFIX",
            "BIRTHDATE",
            "AGE",
            "AGE_MO",
            "SEX",
            "SELF_IDENT",
            "SELF_IDENT_OTHER",
            "CLIENT_MOBILE",
            "CLIENT_EMAIL",
            "CURR_PSGC_REG",
            "CURR_PSGC_PROV",
            "CURR_PSGC_MUNC",
            "CURR_ADDR",
            "FACI_ID",
            "SUB_FACI_ID",
            "SERVICE_FACI",
            "SERVICE_SUB_FACI",
            "FACI_DISP",
            "SUB_FACI_DISP",
            "CLIENT_TYPE",
            "TX_STATUS",
            "WHO_CLASS",
            "VISIT_TYPE",
            "VISIT_DATE",
            "RECORD_DATE",
            "DISP_DATE",
            "LATEST_NEXT_DATE",
            "MEDICINE_SUMMARY",
            "LAB_XRAY_DATE",
            "LAB_XRAY_RESULT",
            "LAB_XPERT_DATE",
            "LAB_XPERT_RESULT",
            "LAB_DSSM_DATE",
            "LAB_DSSM_RESULT",
            "TB_STATUS",
            "OI_SYPH_PRESENT",
            "OI_HEPB_PRESENT",
            "OI_HEPC_PRESENT",
            "OI_PCP_PRESENT",
            "OI_CMV_PRESENT",
            "OI_OROCAND_PRESENT",
            "OI_HERPES_PRESENT",
            "OI_OTHER_TEXT",
            "IS_PREGNANT",
            "NUM_OF_DRUGS",
            "CLINIC_NOTES",
            "COUNSEL_NOTES"
         ),
         where     = glue("
            (VISIT_DATE BETWEEN '{min}' AND '{max}') OR
               (DATE(CREATED_AT) BETWEEN '{min}' AND '{max}') OR
               (DATE(UPDATED_AT) BETWEEN '{min}' AND '{max}') OR
               (REC_ID IN (SELECT REC_ID FROM ohasis_warehouse.art_first)) OR
               (REC_ID IN (SELECT REC_ID FROM ohasis_warehouse.art_lastdisp)) OR
               (REC_ID IN (SELECT REC_ID FROM ohasis_warehouse.art_last))"),
         raw_where = TRUE
      )

   log_info("Downloading {green('Earliest ART Visits')}.")
   forms$art_first <- lw_conn %>%
      dbTable(
         db_name,
         "art_first",
         cols = c("CENTRAL_ID", "REC_ID", "VISIT_DATE")
      ) %>%
      left_join(forms$form_art_bc, join_by(REC_ID, VISIT_DATE))

   log_info("Downloading {green('Latest ART Visits')}.")
   forms$art_last <- lw_conn %>%
      dbTable(
         db_name,
         "art_last",
         cols = c("CENTRAL_ID", "REC_ID", "VISIT_DATE")
      ) %>%
      left_join(forms$form_art_bc, join_by(REC_ID, VISIT_DATE))

   log_info("Downloading {green('Latest ART Dispensing')}.")
   forms$art_lastdisp <- lw_conn %>%
      dbTable(
         db_name,
         "art_lastdisp",
         cols = c("CENTRAL_ID", "REC_ID", "VISIT_DATE")
      ) %>%
      left_join(forms$form_art_bc, join_by(REC_ID, VISIT_DATE))

   log_info("Downloading {green('Latest VL Data')}.")
   # forms$vl_last <- lw_conn %>%
   #    dbTable(
   #       db_name,
   #       "vl_last",
   #       cols = c("CENTRAL_ID", "LAB_VIRAL_DATE", "LAB_VIRAL_RESULT")
   #    )
   forms$vl_last <- hs_data("harp_vl", "all", params$yr, params$mo) %>%
      read_dta() %>%
      filter(
         VL_DROP == 0,
         VL_ERROR == 0,
         coalesce(CENTRAL_ID, "") != "",
         vl_date <= params$max,
      ) %>%
      arrange(VL_SORT, desc(vl_date), res_tag) %>%
      select(
         CENTRAL_ID,
         LAB_VIRAL_DATE   = vl_date,
         LAB_VIRAL_RESULT = vl_result_clean
      ) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE)

   log_info("Downloading {green('Latest Confirmatory Data')}.")
   forms$confirm_last <- lw_conn %>%
      dbTable(
         db_name,
         "confirm_last",
         cols = c("CENTRAL_ID", "DATE_CONFIRM", "CONFIRM_CODE", "CONFIRM_RESULT", "CONFIRM_REMARKS")
      )

   log_info("Downloading {green('CD4 Data')}.")
   forms$lab_cd4 <- lw_conn %>%
      dbTable(
         "ohasis_lake",
         "lab_cd4",
         cols      = c(
            "CD4_DATE",
            "CD4_RESULT",
            "PATIENT_ID"
         ),
         where     = glue("DATE(CD4_DATE) <= '{max}'"),
         raw_where = TRUE
      ) %>%
      get_cid(forms$id_registry, PATIENT_ID)

   log_success("Done.")
   dbDisconnect(lw_conn)
   return(forms)
}

##  Get the previous report's HARP Registry ------------------------------------

update_dataset <- function(params, corr, forms, reprocess) {
   log_info("Getting previous datasets.")
   official         <- list()
   official$old_reg <- ohasis$load_old_dta(
      path            = hs_data("harp_tx", "reg", params$prev_yr, params$prev_mo),
      corr            = corr$old_reg,
      warehouse_table = "harp_tx_old",
      id_col          = c("art_id" = "integer"),
      dta_pid         = "PATIENT_ID",
      remove_cols     = "CENTRAL_ID",
      remove_rows     = corr$anti_join,
      id_registry     = forms$id_registry,
      reload          = reprocess
   )

   official$old_outcome <- hs_data("harp_tx", "outcome", params$prev_yr, params$prev_mo) %>%
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
   update_first_last_art(vars$update_visits, p$params, p$wd)

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
      p$corr <- gdrive_correct3(params$ym, "harp_tx")

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

   p$params$latest_art_id <- max(as.integer(p$official$old_reg$art_id), na.rm = TRUE)
}