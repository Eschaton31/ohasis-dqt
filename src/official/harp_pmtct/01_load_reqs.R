##  Generate pre-requisites and endpoints --------------------------------------
local(envir = nhsss$harp_pmtct, {
   coverage    <- list()
   coverage$mo <- input(prompt = "What is the reporting month for the reports?", max.char = 2)
   coverage$mo <- stri_pad_left(coverage$mo, 2, "0")
   coverage$yr <- input(prompt = "What is the reporting year for the reports?", max.char = 4)

   coverage$ym <- paste(sep = ".", coverage$yr, coverage$mo)


   coverage$min <- paste(
      sep = "-",
      coverage$yr,
      stri_pad_left(as.numeric(coverage$mo) - 2, 2, "0"),
      "01"
   )

   coverage$max <- as.character((as.Date(paste(
      sep = "-",
      coverage$yr,
      coverage$mo,
      "01"
   )) %m+% months(1)) - 1)
})

check <- input(
   prompt  = glue("Re-download the {green('data corrections')}?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Downloading corrections list.")
   # local(envir = nhsss$harp_pmtct, {
   #    .log_info("Getting corrections.")
   #    corr <- gdrive_correct2(gdrive$path, ohasis$ym)
   # })
   nhsss <- gdrive_correct2(nhsss, nhsss$harp_pmtct$coverage$ym, "harp_pmtct")
}

# run through all tables
check <- input(
   prompt  = glue("Update {green('data/forms')} to be used for consolidation?"),
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Updating data lake and data warehouse.")
   local(envir = nhsss$harp_pmtct, invisible({
      tables           <- list()
      tables$lake      <- c("lab_wide", "disp_meds")
      tables$warehouse <- c("form_pmtct", "form_amc_mom", "form_amc_mom_children", "form_amc_child", "form_a", "form_hts", "id_registry")

      lapply(tables$lake, function(table) ohasis$data_factory("lake", table, "upsert", TRUE))
      lapply(tables$warehouse, function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE))
   }))
}

##  Download records -----------------------------------------------------------

update <- input(
   prompt  = "Do you want to download the relevant form data?",
   options = c("1" = "Yes", "2" = "No"),
   default = "1"
)
# if Yes, re-process
if (update == "1") {
   local(envir = nhsss$harp_pmtct, {
      lw_conn <- ohasis$conn("lw")
      forms   <- list()

      min <- as.Date(paste(sep = "-", ohasis$yr, ohasis$mo, "01"))
      max <- (min %m+% months(1)) %m-% days(1)
      min <- as.character(min)
      max <- as.character(max)

      .log_info("Downloading {green('Central IDs')}.")
      forms$id_registry <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "id_registry",
         cols = c("CENTRAL_ID", "PATIENT_ID")
      )

      .log_info("Downloading {green('Pregnant - ART')}.")
      forms$form_art_bc <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "form_art_bc",
         raw_where = TRUE,
         where     = glue(r"(
         IS_PREGNANT = "1_Yes" AND (VISIT_DATE >= '{coverage$min}' AND VISIT_DATE <= '{coverage$max}')
         )")
      )

      .log_info("Downloading {green('A-MC (Mother)')}.")
      forms$amc_mom <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "form_amc_mom",
         join = list(
            "ohasis_lake.px_pii" = list(
               by   = c("REC_ID" = "REC_ID"),
               cols = c(
                  "REC_ID",
                  "FACI_ID",
                  "RECORD_DATE"
               )
            )
         )
      )

      .log_info("Downloading {green('A-MC (Mother-Children)')}.")
      forms$amc_mom_children <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "form_amc_mom_children"
      )

      .log_info("Downloading {green('A-MC (Child)')}.")
      forms$amc_child <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "form_amc_child",
         join = list(
            "ohasis_lake.px_pii" = list(
               by   = c("REC_ID" = "REC_ID"),
               cols = c(
                  "REC_ID",
                  "FACI_ID",
                  "RECORD_DATE"
               )
            )
         )
      )

      .log_info("Downloading {green('PMTCT')}.")
      forms$form_pmtct <- dbTable(
         lw_conn,
         "ohasis_warehouse",
         "form_pmtct"
      )

      .log_success("Done.")
      dbDisconnect(lw_conn)
      rm(min, max, lw_conn)
   })
}

##  Get the previous report's HARP Registry ------------------------------------

check <- input(
   prompt  = "Reload previous dataset?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Getting previous datasets.")
   local(envir = nhsss$harp_pmtct, {
      official            <- list()
      official$old_mother <- ohasis$load_old_dta(
         path            = hs_data("pmtct", "mother", coverage$yr, coverage$mo),
         corr            = corr$old_reg,
         warehouse_table = "pmtct_mother_old",
         id_col          = c("pmtct_mom_id" = "integer"),
         dta_pid         = "PATIENT_ID",
         remove_cols     = "CENTRAL_ID",
         remove_rows     = corr$anti_join,
         id_registry     = forms$id_registry
      )

      official$old_child <- ohasis$load_old_dta(
         path            = hs_data("pmtct", "child", coverage$yr, coverage$mo),
         corr            = corr$old_reg,
         warehouse_table = "pmtct_child_old",
         id_col          = c("pmtct_child_id" = "integer"),
         dta_pid         = "PATIENT_ID",
         remove_cols     = "CENTRAL_ID",
         remove_rows     = corr$anti_join,
         id_registry     = forms$id_registry
      )
   })
}

##  Get the HARP Datasets ------------------------------------------------------

check <- input(
   prompt  = "Reload HARP datasets?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (check == "1") {
   .log_info("Getting previous datasets.")
   local(envir = nhsss$harp_pmtct, {
      harp    <- list()
      harp$dx <- read_dta(hs_data("harp_dx", "reg", coverage$yr, coverage$mo)) %>%
         select(-CENTRAL_ID) %>%
         # get latest central ids
         left_join(
            y  = forms$id_registry,
            by = "PATIENT_ID"
         ) %>%
         mutate(
            CENTRAL_ID = if_else(
               condition = is.na(CENTRAL_ID),
               true      = PATIENT_ID,
               false     = CENTRAL_ID
            ),
         )

      harp$tx_reg <- read_dta(hs_data("harp_tx", "reg", coverage$yr, coverage$mo)) %>%
         select(-CENTRAL_ID) %>%
         # get latest central ids
         left_join(
            y  = forms$id_registry,
            by = "PATIENT_ID"
         ) %>%
         mutate(
            CENTRAL_ID = if_else(
               condition = is.na(CENTRAL_ID),
               true      = PATIENT_ID,
               false     = CENTRAL_ID
            ),
         )

      harp$tx_out <- read_dta(hs_data("harp_tx", "outcome", coverage$yr, coverage$mo))
   })
}
rm(check)
