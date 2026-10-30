##  Helpers shared by the official surveillance pipelines ----------------------
# harp_dx, harp_tx, harp_dead & prep. Pipeline-specific helpers live in each
# pipeline's _shared.R instead.

# reporting period params common to all pipelines; each set_coverage() adds its own
coverage_params <- function(max = end_friday(Sys.time())) {
   params   <- list()
   max_date <- as.Date(max)

   params$yr  <- year(max_date)
   params$mo  <- month(max_date)
   params$ym  <- str_c(sep = ".", params$yr, stri_pad_left(params$mo, 2, "0"))
   params$min <- floor_date(max_date, "month") %>% as.character()
   params$max <- max

   params$prev_mo <- month(max_date %m-% months(1))
   params$prev_yr <- year(max_date %m-% months(1))

   return(params)
}

# use a pre-supplied answer if valid, otherwise prompt for one
ask_input <- function(value, prompt, default = "2", options = c("1" = "yes", "2" = "no")) {
   if (!is.null(value) && value %in% names(options))
      return(value)

   return(input(prompt = prompt, options = options, default = default))
}

# rows w/ duplicated central_id; same rows/columns as get_dupes(central_id),
# which is slow on a full registry
get_reg_dupes <- function(reg) {
   dup_cids <- unique(reg$central_id[duplicated(reg$central_id)])
   dupes    <- reg %>%
      filter(central_id %in% dup_cids) %>%
      add_count(central_id, name = "dupe_count") %>%
      relocate(central_id, dupe_count, .before = 1) %>%
      arrange(desc(dupe_count), central_id)

   if (nrow(dupes) > 0)
      log_warn("Duplicate {green('Central IDs')} found.")

   return(dupes)
}

# previous period's outcome dataset, w/ the period's outcome corrections applied
load_old_outcome <- function(surv_name, id_col, params, corr) {
   data <- hs_data(surv_name, "outcome", params$prev_yr, params$prev_mo) %>%
      read_dta() %>%
      rename_all(tolower) %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      )

   if (nrow(corr$corr_outcome %||% tibble()) > 0) {
      log_info("Performing cleaning on the outcome dataset.")
      data <- apply_corrections(data, corr$corr_outcome %>%
         rename_all(tolower) %>%
         mutate(variable = tolower(variable)), id_col)
   }

   return(data)
}

# convert POSIXct to Date and blank out Stata's 1900-01-01 placeholder dates
clean_dates <- function(data, tz = "UTC") {
   data %>%
      mutate_if(
         .predicate = is.POSIXct,
         ~as.Date(., tz = tz)
      ) %>%
      mutate_if(
         .predicate = is.Date,
         ~if_else(. <= -25567, NA_Date_, ., .)
      )
}

# north-to-south sort order of nhsss regions; case-insensitive, unmatched go last
region_order <- function(reg) {
   order <- c(
      "1"      = 1,
      "2"      = 2,
      "car"    = 3,
      "3"      = 4,
      "ncr"    = 5,
      "4a"     = 6,
      "4b"     = 7,
      "5"      = 8,
      "6"      = 9,
      "7"      = 10,
      "8"      = 11,
      "9"      = 12,
      "10"     = 13,
      "11"     = 14,
      "12"     = 15,
      "caraga" = 16,
      "armm"   = 17,
      "barmm"  = 17
   )

   return(coalesce(unname(order[tolower(reg)]), 9999))
}

# current period's registry of a surveillance from the lake, w/ central ids
lw_registry <- function(surv_name, cols, params, id_registry, keep_pid = FALSE) {
   log_info("Reading {green(surv_name)} registry.")
   conn <- connect('mariadb-lw')
   on.exit(dbDisconnect(conn), add = TRUE)

   # QB$select() captures its arguments, so pass the names through do.call
   qb <- QB$new(conn)
   do.call(qb$select, as.list(c("patient_id", cols)))
   data <- qb$
      from(stri_c(surv_name, ".reg_", params$yr, stri_pad_left(params$mo, 2, "0")))$
      get(format = "Arrow") %>%
      # non-nullable columns hold Stata's missing strings as ""
      mutate_if(
         .predicate = is.character,
         ~na_if(., "")
      ) %>%
      # null Date32 values arrive as -5877641-06-23 instead of NA
      clean_dates() %>%
      get_cid(id_registry, patient_id)

   if (!keep_pid)
      data %<>% select(-patient_id)

   return(data)
}

##  Dropping deferred records --------------------------------------------------
# corr_reg & corr_drop are applied to the *_old tables in the warehouse; only
# corr_defer (by rec_id) is applied by the pipelines.

# tag records listed in the period's corr_defer for reporting in a later period
tag_fordrop <- function(data, corr) {
   log_info("Tagging records for dropping.")
   if (!("corr_defer" %in% names(data)))
      data$corr_defer <- 0

   defer_recs      <- (corr$corr_defer %||% tibble(rec_id = character())) %>% rename_all(tolower)
   data$corr_defer <- if_else(data$rec_id %in% defer_recs$rec_id, 1, coalesce(data$corr_defer, 0))

   return(data)
}

# subsets of dropped records, for documentation
subset_drops <- function(data) {
   log_info("Archive those for dropping.")
   return(list(dropped_notyet = data %>% filter(corr_defer == 1)))
}

# remove records listed in the period's corr_defer from every validation, except tabstat
exclude_deferred <- function(check, corr) {
   defer_recs <- (corr$corr_defer %||% tibble(rec_id = character())) %>% rename_all(tolower)
   for (issue in setdiff(names(check), "tabstat"))
      check[[issue]] %<>% filter(!(rec_id %in% defer_recs$rec_id))

   return(check)
}

##  Validations & outputs ------------------------------------------------------

# answers for a step's validation prompts; exclusion is only asked when checks are run
ask_checks <- function(step_name, run_checks = NULL, exclude_drops = NULL) {
   answers         <- list()
   answers$run     <- ask_input(run_checks, glue("Run `{step_name}` validations?"), default = "1")
   answers$exclude <- if (answers$run == "1")
      ask_input(exclude_drops, "Exclude clients initially tagged for dropping from validations?", default = "1")
   else
      "2"

   return(answers)
}

# write the period's datasets as Stata files; `files` maps official datasets to file name stems
save_dta <- function(official, params, surv_name, files) {
   log_info("Checking output directory.")
   version <- format(Sys.time(), "%Y%m%d")
   dir     <- Sys.getenv(surv_name)
   check_dir(dir)

   log_info("Saving in Stata data format.")
   period_ext <- stri_c(params$yr, "-", stri_pad_left(params$mo, 2, "0"), ".dta")
   for (output in intersect(names(files), names(official))) {
      if (nrow(official[[output]]) > 0) {
         official[[output]] %>%
            format_stata() %>%
            select(-contains(".")) %>%
            write_dta(file.path(dir, stri_c(version, "_", files[[output]], "_", period_ext)))
      }
   }
}
