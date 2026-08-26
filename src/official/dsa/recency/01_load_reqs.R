##  Set coverage ---------------------------------------------------------------

set_coverage <- function() {
   local_gs4_quiet()
   params   <- list()
   max_date <- as.Date(ohasis$next_date) %m-% days(1)

   params$yr   <- year(max_date)
   params$mo   <- month(max_date)
   params$ym   <- str_c(sep = ".", params$yr, stri_pad_left(params$mo, 2, "0"))
   params$p10y <- params$yr - 10
   params$min  <- max_date %m-% days(30) %>% as.character()
   params$max  <- max

   params$prev_mo <- month(max_date %m-% months(1))
   params$prev_yr <- year(max_date %m-% months(1))

   params$sites <- read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw", .name_repair = "unique_quiet") %>%
      filter(site_rt_2023 == 1) %>%
      rename_all(tolower) %>%
      distinct(faci_id, .keep_all = TRUE) %>%
      filter(!is.na(rt_activation_date))

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

##  Identify relevant records --------------------------------------------------

rencecy_records <- function(faci_id, activation_date) {
   lw_conn   <- connect('mariadb-lw')
   db_name   <- "ohasis_warehouse"
   tbl_name  <- "hiv_recency"
   tbl_space <- Id(schema = db_name, table = tbl_name)

   # query for relevant testing records
   faci_sql <- glue(r"(
   select test.rec_id,
          hts.record_date,
          test.confirm_code,
          test.date_collect,
          test.date_receive,
          test.date_confirm,
          test.confirm_faci,
          test.confirm_sub_faci,
          test.specimen_source,
          test.specimen_sub_source,
          test.rt_agreed,
          test.rt_date,
          test.rt_result,
          test.rt_kit,
          test.rt_vl_requested,
          test.rt_vl_date,
          test.rt_vl_result
   from ohasis_lake.px_demographics as hts
            join ohasis_lake.px_hiv_confirmatory as test on hts.rec_id = test.rec_id
  where (coalesce(test.specimen_source, test.confirm_faci) = '{faci_id}' or ((test.confirm_faci = '130023' and test.confirm_sub_faci = '130023_001')) and test.specimen_source = '{faci_id}')
    and coalesce(test.date_collect, hts.record_date) >= '{activation_date}'
    and test.confirm_result like '%Positive%'
    and hts.deleted_at is null
   )")

   # creation of table for restarts
   if (dbExistsTable(lw_conn, tbl_space)) {
      dbExecute(lw_conn, glue("DELETE FROM {db_name}.{tbl_name} WHERE specimen_source = '{faci_id}' OR (confirm_faci = '{faci_id}' AND specimen_source is null)"))
   }

   # ref <- dbGetQuery(lw_conn, faci_sql)
   ref <- QB$new(lw_conn)$
      select('test.rec_id',
             'hts.record_date',
             'test.confirm_code',
             'test.date_collect',
             'test.date_receive',
             'test.date_confirm',
             'test.confirm_faci',
             'test.confirm_sub_faci',
             'test.specimen_source',
             'test.specimen_sub_source',
             'test.rt_agreed',
             'test.rt_date',
             'test.rt_result',
             'test.rt_kit',
             'test.rt_vl_requested',
             'test.rt_vl_date',
             'test.rt_vl_result')$
      from('ohasis_lake.px_demographics as hts')$
      join('ohasis_lake.px_hiv_confirmatory as test', 'hts.rec_id', '=', 'test.rec_id')$
      whereRaw(glue("(coalesce(test.specimen_source, test.confirm_faci) = '{faci_id}' or ((test.confirm_faci = '130023' and test.confirm_sub_faci = '130023_001')) and test.specimen_source = '{faci_id}')"))$
      whereRaw(glue("coalesce(test.date_collect, hts.record_date) >= '{activation_date}'"))$
      whereRaw(glue("test.confirm_result like '%Positive%'"))$
      whereNull('hts.deleted_at')$
      get()
   dbDisconnect(lw_conn)

   lw_conn   <- connect('mariadb-lw')
   # ohasis$upsert(lw_conn, db_name, tbl_name, ref, "rec_id")
   dbxInsert(lw_conn, Id(schema = 'ohasis_warehouse', table = 'hiv_recency'), ref)

   # dbExecute(lw_conn, glue(r"(INSERT INTO {db_name}.{tbl_name} {faci_sql})"), params = list(faci_id, activation_date))
   dbDisconnect(lw_conn)
}

##  Download forms -------------------------------------------------------------

download_tables <- function() {
   lw_conn    <- connect('mariadb-lw')
   sql_select <- "SELECT hts.* FROM ohasis_warehouse."
   sql_join   <- " AS hts JOIN ohasis_warehouse.hiv_recency AS rt ON hts.rec_id = rt.rec_id"

   # read queries
   sql           <- list()
   sql$form_a    <- str_c(sql_select, "form_a", sql_join)
   sql$form_hts  <- str_c(sql_select, "form_hts", sql_join)
   sql$form_cfbs <- str_c(sql_select, "form_cfbs", sql_join)

   # read data
   data             <- list()
   data$form_a      <- QB$new(lw_conn)$
      select(
         "hts.*",
         'confirm.confirm_faci',
         'confirm.confirm_sub_faci',
         'confirm.confirm_type',
         'confirm.confirm_code',
         'confirm.specimen_refer_type',
         'confirm.specimen_source',
         'confirm.specimen_sub_source',
         'confirm.date_collect',
         'confirm.date_receive',
         'confirm.confirm_result',
         'confirm.confirm_remarks',
         'confirm.signatory_1',
         'confirm.signatory_2',
         'confirm.signatory_3',
         'confirm.date_release',
         'confirm.date_confirm',
         'test.t1_date',
         'test.t1_result',
         'test.t1_kit',
         'test.t2_date',
         'test.t2_result',
         'test.t2_kit',
         'test.t3_date',
         'test.t3_result',
         'test.t3_kit'
      )$
      from("ohasis_warehouse.form_a as hts")$
      leftJoin("ohasis_lake.px_hiv_confirmatory as confirm", "form_hts.rec_id", "=", "confirm.rec_id")$
      leftJoin("ohasis_lake.px_hiv_testing as test", "form_hts.rec_id", "=", "test.rec_id")$
      join('ohasis_warehouse.hiv_recency as rt', 'hts.rec_id', '=', 'rt.rec_id')$
      whereNull("hts.deleted_at")$
      get()
   data$form_hts    <- QB$new(lw_conn)$
      select(
         "hts.*",
         'confirm.confirm_faci',
         'confirm.confirm_sub_faci',
         'confirm.confirm_type',
         'confirm.confirm_code',
         'confirm.specimen_refer_type',
         'confirm.specimen_source',
         'confirm.specimen_sub_source',
         'confirm.date_collect',
         'confirm.date_receive',
         'confirm.confirm_result',
         'confirm.confirm_remarks',
         'confirm.signatory_1',
         'confirm.signatory_2',
         'confirm.signatory_3',
         'confirm.date_release',
         'confirm.date_confirm',
         'test.t1_date',
         'test.t1_result',
         'test.t1_kit',
         'test.t2_date',
         'test.t2_result',
         'test.t2_kit',
         'test.t3_date',
         'test.t3_result',
         'test.t3_kit'
      )$
      from("ohasis_warehouse.form_hts as hts")$
      leftJoin("ohasis_lake.px_hiv_confirmatory as confirm", "form_hts.rec_id", "=", "confirm.rec_id")$
      leftJoin("ohasis_lake.px_hiv_testing as test", "form_hts.rec_id", "=", "test.rec_id")$
      join('ohasis_warehouse.hiv_recency as rt', 'hts.rec_id', '=', 'rt.rec_id')$
      whereNull("hts.deleted_at")$
      get()
   data$form_cfbs   <- QB$new(lw_conn)$
      select(
         "hts.*",
         'confirm.confirm_faci',
         'confirm.confirm_sub_faci',
         'confirm.confirm_type',
         'confirm.confirm_code',
         'confirm.specimen_refer_type',
         'confirm.specimen_source',
         'confirm.specimen_sub_source',
         'confirm.date_collect',
         'confirm.date_receive',
         'confirm.confirm_result',
         'confirm.confirm_remarks',
         'confirm.signatory_1',
         'confirm.signatory_2',
         'confirm.signatory_3',
         'confirm.date_release',
         'confirm.date_confirm',
         'test.t1_date',
         'test.t1_result',
         'test.t1_kit',
         'test.t2_date',
         'test.t2_result',
         'test.t2_kit',
         'test.t3_date',
         'test.t3_result',
         'test.t3_kit'
      )$
      from("ohasis_warehouse.form_cfbs as hts")$
      leftJoin("ohasis_lake.px_hiv_confirmatory as confirm", "form_hts.rec_id", "=", "confirm.rec_id")$
      leftJoin("ohasis_lake.px_hiv_testing as test", "form_hts.rec_id", "=", "test.rec_id")$
      join('ohasis_warehouse.hiv_recency as rt', 'hts.rec_id', '=', 'rt.rec_id')$
      whereNull("hts.deleted_at")$
      get()
   data$id_reg      <- update_idreg()
   data$lab_cd4     <- QB$new(lw_conn)$
      from("ohasis_lake.lab_wide as cd4")$
      join('ohasis_lake.px_demographics as pii', 'form.rec_id', '=', 'pii.rec_id')$
      select("pii.patient_id", "cd4.lab_cd4_date as cd4_date", "cd4.lab_cd4_result as cd4_result")$
      whereNotNull("lab_cd4_date")$
      whereNotNull("lab_cd4_result")$
      get()
   data$hiv_recency <- QB$new(lw_conn)$
      from("ohasis_warehouse.hiv_recency")$
      get()

   dbDisconnect(lw_conn)

   return(data)
}

##  Get HARP datasets ----------------------------------------------------------

get_harp <- function(id_reg) {
   harp    <- list()
   lw_conn <- connect('mariadb-lw')

   extension <- stri_c(ohasis$yr, stri_pad_left(ohasis$mo, 2, '0'))

   harp$dx <- QB$new(lw_conn)$
      select(idnum, labcode2, patient_id, confirm_date, year, month)$
      from(stri_c('harp_dx.reg_', extension))$
      get() %>%
      mutate(
         harp_inclusion_date = end_ym(year, month)
      ) %>%
      get_cid(id_reg, patient_id)

   harp$tx <- QB$new(lw_conn)$
      select(art_id, patient_id, artstart_date)$
      from(stri_c('harp_tx.reg_', extension))$
      get() %>%
      get_cid(id_reg, patient_id)

   extension <- '202507'
   harp$prep <- QB$new(lw_conn)$
      select('reg.prep_id', 'reg.patient_id', 'out.prepstart_date')$
      from(stri_c('prep.reg_', extension, ' as reg'))$
      leftJoin(stri_c('prep.outcome_', extension, ' as out'), 'reg.prep_id', '=', 'out.prep_id')$
      get() %>%
      get_cid(id_reg, patient_id)

   dbDisconnect(lw_conn)

   return(harp)
}

.init <- function(envir = parent.env(environment()), ...) {
   p    <- envir
   vars <- as.list(list(...))

   update_warehouse(vars$update_lw)
   p$params <- set_coverage()

   # ! corrections
   update_rt <- ifelse(
      !is.null(vars$update_rt) && vars$update_rt %in% c("1", "2"),
      vars$update_rt,
      input(
         prompt  = glue("Update RT records?"),
         options = c("1" = "yes", "2" = "no"),
         default = "2"
      )
   )
   if (update_rt == "1") {
      invisible(apply(p$params$sites, 1, function(row) {
         row <- as.list(row)

         log_info("Running {green(row$faci_name)}.")
         data <- rencecy_records(row$faci_id, row$rt_activation_date)
      }))
   }

   # download data
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
      p$forms <- download_tables()

   # get harp
   dl <- ifelse(
      !is.null(vars$get_harp) && vars$get_harp %in% c("1", "2"),
      vars$get_harp,
      input(
         prompt  = "GET: {green('HARP')}?",
         options = c("1" = "Yes", "2" = "No"),
         default = "1"
      )
   )
   if (dl == "1")
      p$harp <- get_harp(p$forms$id_reg)
}
