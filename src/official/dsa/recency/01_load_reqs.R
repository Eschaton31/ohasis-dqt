##  Set coverage ---------------------------------------------------------------

set_coverage <- function() {
  local_gs4_quiet()
  params <- list()
  max_date <- as.Date(ohasis$next_date) %m-% days(1)

  params$yr <- year(max_date)
  params$mo <- month(max_date)
  params$ym <- str_c(sep = ".", params$yr, stri_pad_left(params$mo, 2, "0"))
  params$p10y <- params$yr - 10
  params$min <- max_date %m-% days(30) %>% as.character()
  params$max <- max

  params$prev_mo <- month(max_date %m-% months(1))
  params$prev_yr <- year(max_date %m-% months(1))

  params$sites <- read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw", .name_repair = "unique_quiet") %>%
    filter(site_rt_2023 == 1) %>%
    distinct(FACI_ID, .keep_all = TRUE)

  return(params)
}

##  Generate pre-requisites and endpoints --------------------------------------

# run through all tables
update_warehouse <- function(update) {
  update <- ifelse(
    !is.null(update) && update %in% c("1", "2"),
    update,
    input(
      prompt = glue("Update {green('data/forms')} to be used for consolidation?"),
      options = c("1" = "yes", "2" = "no"),
      default = "2"
    )
  )
  if (update == "1") {
    log_info("Updating data lake and data warehouse.")
    tables <- list()
    tables$lake <- c(
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
  lw_conn <- ohasis$conn("lw")
  db_name <- "ohasis_warehouse"
  tbl_name <- "hiv_recency"
  tbl_space <- Id(schema = db_name, table = tbl_name)

  # query for relevant testing records
  faci_sql <- r"(
   SELECT test.REC_ID,
          hts.RECORD_DATE,
          test.CONFIRM_CODE,
          test.DATE_COLLECT,
          test.DATE_RECEIVE,
          test.DATE_CONFIRM,
          test.CONFIRM_FACI,
          test.CONFIRM_SUB_FACI,
          test.SPECIMEN_SOURCE,
          test.SPECIMEN_SUB_SOURCE,
          test.RT_AGREED,
          test.RT_DATE,
          test.RT_RESULT,
          test.RT_KIT,
          test.RT_VL_REQUESTED,
          test.RT_VL_DATE,
          test.RT_VL_RESULT
   FROM ohasis_lake.px_pii AS hts
            JOIN ohasis_lake.px_hiv_testing AS test ON hts.REC_ID = test.REC_ID
  WHERE (COALESCE(test.SPECIMEN_SOURCE, test.CONFIRM_FACI) = ? OR
         ((test.CONFIRM_FACI = '130023' AND test.CONFIRM_SUB_FACI = '130023_001')) AND test.SPECIMEN_SOURCE = ?)
    AND COALESCE(test.DATE_COLLECT, hts.RECORD_DATE) >= ?
    AND test.CONFIRM_RESULT REGEXP 'Positive'
   )"

  # creation of table for restarts
  if (dbExistsTable(lw_conn, tbl_space)) {
    dbExecute(lw_conn, glue("DELETE FROM {db_name}.{tbl_name} WHERE SPECIMEN_SOURCE = ? OR (CONFIRM_FACI = ? AND SPECIMEN_SOURCE IS NULL)"), params = list(faci_id, faci_id))
  }

  ref <- dbxSelect(lw_conn, faci_sql, params = list(faci_id, faci_id, activation_date))
  ohasis$upsert(lw_conn, db_name, tbl_name, ref, "REC_ID")

  # dbExecute(lw_conn, glue(r"(INSERT INTO {db_name}.{tbl_name} {faci_sql})"), params = list(faci_id, activation_date))
  dbDisconnect(lw_conn)
}

##  Download forms -------------------------------------------------------------

download_tables <- function() {
  lw_conn <- ohasis$conn("lw")
  sql_select <- "SELECT hts.* FROM ohasis_warehouse."
  sql_join <- " AS hts JOIN ohasis_warehouse.hiv_recency AS rt ON hts.REC_ID = rt.REC_ID"

  # read queries
  sql <- list()
  sql$form_a <- str_c(sql_select, "form_a", sql_join)
  sql$form_hts <- str_c(sql_select, "form_hts", sql_join)
  sql$form_cfbs <- str_c(sql_select, "form_cfbs", sql_join)

  # read data
  data <- list()
  data$form_a <- tracked_select(lw_conn, sql$form_a, "New Form A")
  data$form_hts <- tracked_select(lw_conn, sql$form_hts, "New HTS Form")
  data$form_cfbs <- tracked_select(lw_conn, sql$form_cfbs, "New CFBS Form")
  data$id_reg <- dbTable(lw_conn, "ohasis_warehouse", "id_registry", cols = c("PATIENT_ID", "CENTRAL_ID"))
  data$lab_cd4 <- dbTable(lw_conn, "ohasis_lake", "lab_cd4", cols = c("PATIENT_ID", "CD4_DATE", "CD4_RESULT"))
  data$hiv_recency <- dbTable(lw_conn, "ohasis_warehouse", "hiv_recency")

  dbDisconnect(lw_conn)

  return(data)
}

##  Get HARP datasets ----------------------------------------------------------

get_harp <- function(id_reg) {
  harp <- list()
  harp$dx <- hs_data("harp_dx", "reg", ohasis$yr, ohasis$mo) %>%
    read_dta(col_select = c(idnum, labcode2, PATIENT_ID, confirm_date, year, month)) %>%
    mutate(
      HARP_INCLUSION_DATE = end_ym(year, month)
    ) %>%
    get_cid(id_reg, PATIENT_ID)

  harp$tx <- hs_data("harp_tx", "reg", ohasis$yr, ohasis$mo) %>%
    read_dta(col_select = c(art_id, PATIENT_ID, artstart_date)) %>%
    get_cid(id_reg, PATIENT_ID)

  harp$prep <- hs_data("prep", "reg", ohasis$yr, ohasis$mo) %>%
    read_dta(col_select = c(prep_id, PATIENT_ID)) %>%
    left_join(
      y = hs_data("prep", "outcome", ohasis$yr, ohasis$mo) %>%
        read_dta(col_select = c(prep_id, prepstart_date)),
      by = join_by(prep_id)
    ) %>%
    get_cid(id_reg, PATIENT_ID)

  return(harp)
}

.init <- function(envir = parent.env(environment()), ...) {
  p <- envir
  vars <- as.list(list(...))

  update_warehouse(vars$update_lw)
  p$params <- set_coverage()

  # ! corrections
  update_rt <- ifelse(
    !is.null(vars$update_rt) && vars$update_rt %in% c("1", "2"),
    vars$update_rt,
    input(
      prompt = glue("Update RT records?"),
      options = c("1" = "yes", "2" = "no"),
      default = "2"
    )
  )
  if (update_rt == "1") {
    invisible(apply(p$params$sites, 1, function(row) {
      row <- as.list(row)

      log_info("Running {green(row$FACI_NAME)}.")
      data <- rencecy_records(row$FACI_ID, row$rt_activation_date)
    }))
  }

  # download data
  dl <- ifelse(
    !is.null(vars$dl_forms) && vars$dl_forms %in% c("1", "2"),
    vars$dl_forms,
    input(
      prompt = "GET: {green('forms')}?",
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
      prompt = "GET: {green('HARP')}?",
      options = c("1" = "Yes", "2" = "No"),
      default = "1"
    )
  )
  if (dl == "1")
    p$harp <- get_harp(p$forms$id_reg)
}
