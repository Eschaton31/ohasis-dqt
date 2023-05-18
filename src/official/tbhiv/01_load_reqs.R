##  Generate pre-requisites and endpoints --------------------------------------

set_coverage <- function(yr = NULL, mo = NULL) {
   coverage <- list()

   coverage      <- list()
   coverage$mo   <- ifelse(!is.null(mo), mo, input(prompt = "What is the reporting month?", max.char = 2))
   coverage$mo   <- stri_pad_left(coverage$mo, width = 2, pad = "0")
   coverage$yr   <- ifelse(!is.null(yr), yr, input(prompt = "What is the reporting year?", max.char = 4))
   coverage$yr   <- stri_pad_left(coverage$yr, width = 4, pad = "0")
   coverage$ym   <- stri_c(coverage$yr, ".", coverage$mo)
   coverage$date <- paste(sep = "-", coverage$yr, coverage$mo, "01")

   date_ref        <- as.Date(coverage$date)
   coverage$min$mo <- as.character(floor_date(date_ref, unit = "month"))
   coverage$min$qr <- as.character(floor_date(date_ref, unit = "quarter"))
   coverage$min$sy <- as.character(floor_date(date_ref, unit = "halfyear"))
   coverage$min$yr <- as.character(floor_date(date_ref, unit = "year"))
   coverage$max    <- as.character(ceiling_date(date_ref, unit = "month") - 1)

   return(coverage)
}

##  Download records -----------------------------------------------------------

download_forms <- function(coverage) {
   min <- coverage$min$yr
   max <- coverage$max

   lw_conn <- ohasis$conn("lw")
   dbname  <- "ohasis_warehouse"
   forms   <- list()

   log_info("Downloading {green('Central IDs')}.")
   forms$id_reg <- lw_conn %>%
      dbTable(dbname, "id_registry", cols = c("CENTRAL_ID", "PATIENT_ID"))

   log_info("Downloading {green('ART Visits w/in the scope')}.")
   forms$form_art_bc <- lw_conn %>%
      dbTable(
         dbname,
         "form_art_bc",
         cols      = c(
            "REC_ID",
            "PATIENT_ID",
            "VISIT_DATE",
            "LAB_XRAY_DATE",
            "LAB_XRAY_RESULT",
            "LAB_XPERT_DATE",
            "LAB_XPERT_RESULT",
            "LAB_DSSM_DATE",
            "LAB_DSSM_RESULT",
            "TB_SCREEN",
            "TB_STATUS",
            "TB_ACTIVE_ALREADY",
            "TB_TX_ALREADY",
            "TB_SITE_P",
            "TB_SITE_EP",
            "TB_DRUG_RESISTANCE",
            "TB_DRUG_RESISTANCE_OTHER",
            "TB_TX_STATUS",
            "TB_TX_STATUS_OTHER",
            "TB_REGIMEN",
            "TB_TX_START_DATE",
            "TB_TX_END_DATE",
            "TB_TX_OUTCOME",
            "TB_TX_OUTCOME_OTHER",
            "TB_IPT_STATUS",
            "TB_IPT_START_DATE",
            "TB_IPT_END_DATE",
            "TB_IPT_OUTCOME",
            "TB_IPT_OUTCOME_OTHER"
         ),
         where     = glue("
      (VISIT_DATE BETWEEN '{min}' AND '{max}')
      "),
         raw_where = TRUE
      ) %>%
      get_cid(forms$id_reg, PATIENT_ID)

   dbDisconnect(lw_conn)
   log_success("Done.")

   return(forms)
}

##  Get the previous report's HARP Registry ------------------------------------

load_harp <- function(coverage, id_reg) {
   harp <- list()

   log_info("Getting the current HARP Tx Datasets.")
   harp$tx$new <- hs_data("harp_tx", "reg", coverage$yr, coverage$mo) %>%
      read_dta(
         col_select = c(
            PATIENT_ID,
            art_id,
            sex,
            birthdate
         )
      ) %>%
      left_join(
         y  = hs_data("harp_tx", "outcome", coverage$yr, coverage$mo) %>%
            read_dta() %>%
            select(
               -any_of(c(
                  "PATIENT_ID",
                  "central_id",
                  "CENTRAL_ID",
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
      get_cid(id_reg, PATIENT_ID)

   return(harp)
}

.init <- function(..., envir = parent.env(environment())) {
   p          <- envir
   p$coverage <- set_coverage(...)
   p$forms    <- download_forms(p$coverage)
   p$harp     <- load_harp(p$coverage, p$forms$id_reg)
}
