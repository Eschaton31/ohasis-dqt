dr                                        <- new.env()
dr$`20220606-kuya_chard`                  <- new.env()
dr$`20220606-kuya_chard`$coverage$min     <- "2021-03-01"
dr$`20220606-kuya_chard`$coverage$max     <- "2022-05-31"
dr$`20220606-kuya_chard`$coverage$faci_id <- "130605"

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

   dbDisconnect(lw_conn)
   rm(min, max, lw_conn)
}))
