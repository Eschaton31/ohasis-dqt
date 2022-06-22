dr                                        <- new.env()
dr$`20220606-kuya_chard`                  <- new.env()
dr$`20220606-kuya_chard`$data             <- list()
dr$`20220606-kuya_chard`$request          <- list()
dr$`20220606-kuya_chard`$coverage$min     <- "2021-03-01"
dr$`20220606-kuya_chard`$coverage$max     <- "2022-05-31"
dr$`20220606-kuya_chard`$coverage$curr_mo <- "05"
dr$`20220606-kuya_chard`$coverage$curr_yr <- "2022"
dr$`20220606-kuya_chard`$coverage$faci_id <- "130605"

invisible(
   lapply(
      c("form_a", "form_hts", "form_cfbs", "form_prep", "id_registry"),
      function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE)
   )
)

local(envir = dr$`20220606-kuya_chard`, invisible({
   lw_conn <- ohasis$conn("lw")

   min     <- coverage$min
   max     <- coverage$max
   faci_id <- coverage$faci_id

   forms <- list()

   .log_info("Downloading {green('Central IDs')}.")
   forms$id_registry <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "id_registry")) %>%
      select(CENTRAL_ID, PATIENT_ID) %>%
      collect()

   .log_info("Downloading {green('Form A')}.")
   forms$form_a <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_a")) %>%
      filter(
         (FACI_ID == faci_id | SERVICE_FACI == faci_id) & ((RECORD_DATE >= min & RECORD_DATE <= max) |
            (as.Date(CREATED_AT) >= min & as.Date(CREATED_AT) <= max) |
            (as.Date(UPDATED_AT) >= min & as.Date(UPDATED_AT) <= max) |
            (as.Date(DATE_CONFIRM) >= min & as.Date(DATE_CONFIRM) <= max) |
            (as.Date(T3_DATE) >= min & as.Date(T3_DATE) <= max) |
            (as.Date(T2_DATE) >= min & as.Date(T2_DATE) <= max) |
            (as.Date(T1_DATE) >= min & as.Date(T1_DATE) <= max) |
            (T0_DATE >= min & T0_DATE <= max))
      ) %>%
      collect() %>%
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

   .log_info("Downloading {green('HTS Form')}.")
   forms$form_hts <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_hts")) %>%
      filter(
         (FACI_ID == faci_id | SERVICE_FACI == faci_id) & ((RECORD_DATE >= min & RECORD_DATE <= max) |
            (as.Date(CREATED_AT) >= min & as.Date(CREATED_AT) <= max) |
            (as.Date(UPDATED_AT) >= min & as.Date(UPDATED_AT) <= max) |
            (as.Date(DATE_CONFIRM) >= min & as.Date(DATE_CONFIRM) <= max) |
            (as.Date(T3_DATE) >= min & as.Date(T3_DATE) <= max) |
            (as.Date(T2_DATE) >= min & as.Date(T2_DATE) <= max) |
            (as.Date(T1_DATE) >= min & as.Date(T1_DATE) <= max) |
            (T0_DATE >= min & T0_DATE <= max))
      ) %>%
      collect() %>%
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

   .log_info("Downloading {green('CFBS Form')}.")
   forms$form_cfbs <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_cfbs")) %>%
      filter(
         (FACI_ID == faci_id | SERVICE_FACI == faci_id) & ((RECORD_DATE >= min & RECORD_DATE <= max) |
            (as.Date(CREATED_AT) >= min & as.Date(CREATED_AT) <= max) |
            (as.Date(UPDATED_AT) >= min & as.Date(UPDATED_AT) <= max) |
            (as.Date(TEST_DATE) >= min & as.Date(TEST_DATE) <= max))
      ) %>%
      collect() %>%
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

   .log_info("Downloading {green('PrEP Form')}.")
   forms$form_prep <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_prep")) %>%
      filter(
         (FACI_ID == faci_id | SERVICE_FACI == faci_id) & ((RECORD_DATE >= min & RECORD_DATE <= max) |
            (as.Date(CREATED_AT) >= min & as.Date(CREATED_AT) <= max) |
            (as.Date(UPDATED_AT) >= min & as.Date(UPDATED_AT) <= max) |
            (as.Date(DISP_DATE) >= min & as.Date(DISP_DATE) <= max))
      ) %>%
      collect() %>%
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

   dbDisconnect(lw_conn)
   rm(min, max, lw_conn)
}))
local(envir = dr$`20220606-kuya_chard`, {
   harp <- list()

   .log_info("Getting HARP Dx Dataset.")
   harp$dx <- ohasis$get_data("harp_dx", coverage$curr_yr, coverage$curr_mo) %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      ) %>%
      select(-starts_with("CENTRAL_ID")) %>%
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

   .log_info("Getting the new HARP Tx Datasets.")
   harp$tx$new_reg <- ohasis$get_data("harp_tx-reg", coverage$curr_yr, coverage$curr_mo) %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      ) %>%
      select(-starts_with("CENTRAL_ID")) %>%
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

   harp$tx$new_outcome <- ohasis$get_data("harp_tx-outcome", coverage$curr_yr, coverage$curr_mo) %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      ) %>%
      select(-starts_with("CENTRAL_ID")) %>%
      left_join(
         y  = harp$tx$new_reg %>%
            select(art_id, CENTRAL_ID),
         by = "art_id"
      )

   .log_info("Getting HARP Dead Dataset.")
   harp$dead_reg <- ohasis$get_data("harp_dead", coverage$curr_yr, coverage$curr_mo) %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      ) %>%
      select(-starts_with("CENTRAL_ID")) %>%
      left_join(
         y  = forms$id_registry,
         by = "PATIENT_ID"
      ) %>%
      mutate(
         CENTRAL_ID       = if_else(
            condition = is.na(CENTRAL_ID),
            true      = PATIENT_ID,
            false     = CENTRAL_ID
         ),
         proxy_death_date = as.Date(ceiling_date(as.Date(paste(sep = '-', year, month, '01')), unit = 'month')) - 1,
         ref_death_date   = if_else(
            condition = is.na(date_of_death),
            true      = proxy_death_date,
            false     = date_of_death
         )
      )

   .log_info("Getting the new PrEP Datasets.")
   harp$prep$new_reg <- ohasis$get_data("prep-reg", coverage$curr_yr, coverage$curr_mo) %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      ) %>%
      select(-starts_with("CENTRAL_ID")) %>%
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

   harp$prep$new_outcome <- ohasis$get_data("prep-outcome", coverage$curr_yr, coverage$curr_mo) %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      ) %>%
      select(-starts_with("CENTRAL_ID")) %>%
      left_join(
         y  = harp$prep$new_reg %>%
            select(prep_id, CENTRAL_ID),
         by = "prep_id"
      )
})

source("src/data_requests/20220606-kuya_chard/01-screening.R")
source("src/data_requests/20220606-kuya_chard/02-prep.R")
source("src/data_requests/20220606-kuya_chard/03-confirm_art.R")

write_xlsx(
   list(
      "1a" = dr$`20220606-kuya_chard`$request$`1`$a,
      "2a" = dr$`20220606-kuya_chard`$request$`2`$a,
      "2b" = dr$`20220606-kuya_chard`$request$`2`$b,
      "3a" = dr$`20220606-kuya_chard`$request$`3`$a,
      "3b" = dr$`20220606-kuya_chard`$request$`3`$b,
      "3c" = dr$`20220606-kuya_chard`$request$`3`$c
   ),
   glue(r"(H:/20220606-kuya_chard.xlsx)")
)