##  Generate pre-requisites and endpoints --------------------------------------

set_coverage <- function(report = NULL, yr = NULL, mo = NULL) {
   rep_type      <- ifelse(
      !is.null(report),
      switch(report, HFR = "1", QR = "2", `1` = "1", `2` = "2"),
      input(
         prompt  = "What is the type of report for consolidation?",
         options = c("1" = "Monthly", "2" = "Quarterly"),
         default = "1"
      )
   )
   coverage      <- list()
   coverage$type <- switch(rep_type, `1` = "HFR", `2` = "QR")

   coverage$curr      <- list()
   coverage$curr$mo   <- ifelse(!is.null(mo), mo, input(prompt = "What is the reporting month?", max.char = 2))
   coverage$curr$mo   <- stri_pad_left(coverage$curr$mo, width = 2, pad = "0")
   coverage$curr$yr   <- ifelse(!is.null(yr), yr, input(prompt = "What is the reporting year?", max.char = 4))
   coverage$curr$yr   <- stri_pad_left(coverage$curr$yr, width = 4, pad = "0")
   coverage$curr$ym   <- stri_c(coverage$curr$yr, ".", coverage$curr$mo)
   coverage$curr$date <- paste(sep = "-", coverage$curr$yr, coverage$curr$mo, "01")

   coverage$min <- switch(
      coverage$type,
      HFR = coverage$curr$date,
      QR  = as.character(floor_date(as.Date(coverage$curr$date), unit = "quarter"))
   )
   coverage$max <- as.character(ceiling_date(as.Date(coverage$curr$date), unit = "month") - 1)

   coverage$curr$fy <- case_when(
      coverage$curr$mo %in% c("01", "02", "03") ~ stri_c("Q2 FY", as.numeric(str_right(coverage$curr$yr, 2))),
      coverage$curr$mo %in% c("04", "05", "06") ~ stri_c("Q3 FY", as.numeric(str_right(coverage$curr$yr, 2))),
      coverage$curr$mo %in% c("07", "08", "09") ~ stri_c("Q4 FY", as.numeric(str_right(coverage$curr$yr, 2))),
      coverage$curr$mo %in% c("10", "11", "12") ~ stri_c("Q1 FY", as.numeric(str_right(coverage$curr$yr, 2)) + 1)
   )

   ref_prev           <- strsplit(as.character(as.Date(coverage$min) - 1), "-")[[1]]
   coverage$prev      <- list()
   coverage$prev$mo   <- ref_prev[2]
   coverage$prev$yr   <- ref_prev[1]
   coverage$prev$ym   <- stri_c(coverage$prev$yr, ".", coverage$prev$mo)
   coverage$prev$date <- as.character(stri_c(ref_prev, collapse = "-"))

   coverage$sites <- read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw") %>%
      distinct(FACI_ID, .keep_all = TRUE) %>%
      rename(faci_id = FACI_ID)

   return(coverage)
}

##  Download records -----------------------------------------------------------

download_forms <- function(coverage) {
   min <- coverage$min
   max <- coverage$max

   forms        <- get_hts(min, max)
   names(forms) <- c("form_hts", "form_a", "form_cfbs")

   log_info("Downloading {green('Central IDs')}.")
   forms$id_reg <- update_idreg()

   log_success("Done.")

   return(forms)
}

##  Get the previous report's harp Registry ------------------------------------

load_harp <- function(coverage, id_reg) {
   harp <- list()

   log_info("Getting harp Dx Dataset.")
   harp$dx <- hs_data("harp_dx", "reg", coverage$curr$yr, coverage$curr$mo) %>%
      read_dta(
         col_select = c(
            any_of(c('PATIENT_ID', 'patient_id')),
            idnum,
            transmit,
            sexhow,
            sex,
            year,
            month,
            sex,
            self_identity,
            self_identity_other,
            bdate,
            confirm_date,
            dxlab_standard,
            dx_region,
            dx_province,
            dx_muncity,
            age
         )
      ) %>%
      rename_all(tolower) %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == "", NA_character_, .)
      ) %>%
      get_cid(id_reg, patient_id) %>%
      mutate(
         ref_report = as.Date(stri_c(sep = "-", year, stri_pad_left(month, 2, "0"), "01"))
      ) %>%
      dxlab_to_id(
         c("harpdx_faci", "harpdx_sub_faci"),
         c("dx_region", "dx_province", "dx_muncity", "dxlab_standard"),
         ohasis$ref_faci
      )

   log_info("Getting the previous harp Tx Datasets.")
   harp$tx$old <- hs_data("harp_tx", "reg", coverage$prev$yr, coverage$prev$mo) %>%
      read_dta(
         col_select = c(
            any_of(c('PATIENT_ID', 'patient_id')),
            art_id,
            sex,
            birthdate
         )
      ) %>%
      rename_all(tolower) %>%
      left_join(
         y  = hs_data("harp_tx", "outcome", coverage$prev$yr, coverage$prev$mo) %>%
            read_dta() %>%
            select(
               -any_of(c(
                  "patient_id",
                  "central_id",
                  "central_id",
                  "sex",
                  "birthdate"
               ))
            ),
         by = join_by(art_id)
      ) %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == "", NA_character_, .)
      ) %>%
      get_cid(id_reg, patient_id)

   log_info("Getting the current harp Tx Datasets.")
   harp$tx$new <- hs_data("harp_tx", "reg", coverage$curr$yr, coverage$curr$mo) %>%
      read_dta(
         col_select = c(
            any_of(c('PATIENT_ID', 'patient_id')),
            art_id,
            sex,
            birthdate
         )
      ) %>%
      rename_all(tolower) %>%
      left_join(
         y  = hs_data("harp_tx", "outcome", coverage$curr$yr, coverage$curr$mo) %>%
            read_dta() %>%
            select(
               -any_of(c(
                  "patient_id",
                  "central_id",
                  "central_id",
                  "sex",
                  "birthdate"
               ))
            ),
         by = join_by(art_id)
      ) %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == "", NA_character_, .)
      ) %>%
      get_cid(id_reg, patient_id)

   log_info("Getting the previous PrEP Datasets.")
   harp$prep$old <- hs_data("prep", "reg", coverage$prev$yr, coverage$prev$mo) %>%
      read_dta(
         col_select = c(
            any_of(c('PATIENT_ID', 'patient_id')),
            prep_id,
            sex,
            birthdate
         )
      ) %>%
      rename_all(tolower) %>%
      left_join(
         y  = hs_data("prep", "outcome", coverage$prev$yr, coverage$prev$mo) %>%
            read_dta() %>%
            select(
               -any_of(c(
                  "patient_id",
                  "central_id",
                  "central_id",
                  "sex",
                  "birthdate"
               ))
            ) %>%
            distinct(prep_id, .keep_all = TRUE),
         by = join_by(prep_id)
      ) %>%
      select(
         -ends_with(".y")
      ) %>%
      rename_all(~stri_replace_all_fixed(., ".x", "")) %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == "", NA_character_, .)
      ) %>%
      get_cid(id_reg, patient_id)


   log_info("Getting the current PrEP Datasets.")
   harp$prep$new <- hs_data("prep", "reg", coverage$curr$yr, coverage$curr$mo) %>%
      read_dta(
         col_select = c(
            any_of(c('PATIENT_ID', 'patient_id')),
            prep_id,
            sex,
            birthdate,
            prep_first_screen
         )
      ) %>%
      rename_all(tolower) %>%
      left_join(
         y  = hs_data("prep", "outcome", coverage$curr$yr, coverage$curr$mo) %>%
            read_dta() %>%
            select(
               -any_of(c(
                  "patient_id",
                  "central_id",
                  "central_id",
                  "sex",
                  "birthdate"
               ))
            ) %>%
            distinct(prep_id, .keep_all = TRUE),
         by = join_by(prep_id)
      ) %>%
      select(
         -ends_with(".y")
      ) %>%
      rename_all(~stri_replace_all_fixed(., ".x", "")) %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == "", NA_character_, .)
      ) %>%
      get_cid(id_reg, patient_id)

   if (!("prep_risk_sexwithm" %in% names(harp$prep$new)))
      harp$prep$new %<>% mutate(prep_risk_sexwithm = "(no data)")

   if (!("hts_risk_sexwithm" %in% names(harp$prep$new)))
      harp$prep$new %<>% mutate(hts_risk_sexwithm = "(no data)")

   risk <- harp$prep$new %>%
      select(
         prep_id,
         contains("risk", ignore.case = FALSE)
      ) %>%
      select(-ends_with("screen")) %>%
      pivot_longer(
         cols = contains("risk", ignore.case = FALSE)
      ) %>%
      group_by(prep_id) %>%
      summarise(
         risks = stri_c(collapse = ", ", unique(sort(value)))
      ) %>%
      ungroup()

   harp$prep$new %<>%
      select(-contains("risks")) %>%
      left_join(
         y  = risk,
         by = join_by(prep_id)
      )

   if (!("risks" %in% names(harp$prep$new)))
      harp$prep$new %<>% mutate(risks = NA_character_)

   return(harp)
}

.init <- function(..., envir = parent.env(environment())) {
   p          <- envir
   p$coverage <- set_coverage(...)
   p$forms    <- download_forms(p$coverage)
   p$harp     <- load_harp(p$coverage, p$forms$id_reg)
}
