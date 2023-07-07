##  Generate pre-requisites and endpoints --------------------------------------

download_corrections <- function() {
   check <- input(
      prompt  = glue("Re-download the {green('data corrections')}?"),
      options = c("1" = "yes", "2" = "no"),
      default = "2"
   )
   if (check == "1") {
      .log_info("Downloading corrections list.")
      nhsss <- gdrive_correct2(nhsss, ohasis$ym, "harp_dx", speed = FALSE)
   }
}

# run through all tables
update_warehouse <- function() {
   check <- input(
      prompt  = glue("Update {green('data/forms')} to be used for consolidation?"),
      options = c("1" = "yes", "2" = "no"),
      default = "2"
   )
   if (check == "1") {
      .log_info("Updating data lake and data warehouse.")
      local(envir = nhsss$harp_dx, invisible({
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
      }))
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
   data$cd4          <- tracked_select(lw_conn, sql$cd4, "Baseline CD4", list(as.character(ohasis$next_date)))

   dbDisconnect(lw_conn)

   return(data)
}

##  get pdf results ------------------------------------------------------------

get_rhivda_pdf <- function() {
   .log_info("Loading list of rHIVda PDF Results.")
   rhivda <- dir_info(file.path(Sys.getenv("DRIVE_DROPBOX"), "File requests/rHIVda Submission/FORMS", ohasis$ym), recurse = TRUE)
   rhivda %<>%
      filter(type == "file") %>%
      mutate(
         CONFIRM_CODE = str_extract(basename(path), "[A-Z][A-Z][A-Z][0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9][0-9]"),
         .before      = 1
      ) %>%
      filter(!is.na(CONFIRM_CODE))

   .GlobalEnv$nhsss$harp_dx$pdf_rhivda$data <- rhivda
}

##  Get the previous report's HARP Registry ------------------------------------

update_dataset <- function() {
   check <- input(
      prompt  = "Reload previous dataset?",
      options = c("1" = "yes", "2" = "no"),
      default = "2"
   )
   if (check == "1") {
      .log_info("Getting previous datasets.")
      local(envir = nhsss$harp_dx, {
         official     <- list()
         official$old <- ohasis$load_old_dta(
            path            = hs_data("harp_dx", "reg", ohasis$prev_yr, ohasis$prev_mo),
            corr            = corr$old_reg,
            warehouse_table = "harp_dx_old",
            id_col          = c("idnum" = "integer"),
            dta_pid         = "PATIENT_ID",
            remove_cols     = "CENTRAL_ID",
            remove_rows     = corr$anti_join
         )
         official$dupes <- official$old %>% get_dupes(CENTRAL_ID)
      })
   }
   rm(check)
}

define_params <- function() {
   local(envir = nhsss$harp_dx, {
      params              <- list()
      params$p10y         <- (as.numeric(ohasis$yr) - 10)
      params$latest_idnum <- max(as.integer(official$old$idnum), na.rm = TRUE)

      params$min <- ohasis$date
      params$max <- ohasis$next_date %m-% days(1)
      params$yr  <- year(params$max)
      params$mo  <- month(params$max)
      params$ym  <- str_c(sep = ".", params$yr, stri_pad_left(params$mo, 2, "0"))
   })
}

.init <- function(envir = parent.env(environment())) {
   download_corrections()
   update_warehouse()
   update_dataset()
   define_params()
   get_rhivda_pdf()

   p       <- envir
   p$forms <- download_tables(p$wd)
}