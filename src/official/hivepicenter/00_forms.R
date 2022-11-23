local(envir = epictr, {
   forms   <- list()
   lw_conn <- ohasis$conn("lw")
   db_conn <- ohasis$conn("db")

   min <- "2020-01-01"
   max <- "2022-09-30"

   forms             <- list()
   .log_info("Downloading {green('Central IDs')}.")
   forms$id_registry <- dbTable(
      lw_conn,
      "ohasis_warehouse",
      "id_registry",
      cols = c("CENTRAL_ID", "PATIENT_ID")
   )

   .log_info("Downloading {green('Form A')}.")
   forms$form_a <- dbTable(
      lw_conn,
      "ohasis_warehouse",
      "form_a",
      where     = glue("((RECORD_DATE >= '{min}' AND RECORD_DATE <= '{max}') OR (DATE(DATE_CONFIRM) >= '{min}' AND DATE(DATE_CONFIRM) <= '{max}') OR (DATE(T3_DATE) >= '{min}' AND DATE(T3_DATE) <= '{max}') OR (DATE(T2_DATE) >= '{min}' AND DATE(T2_DATE) <= '{max}') OR (DATE(T1_DATE) >= '{min}' AND DATE(T1_DATE) <= '{max}') OR (DATE(T0_DATE) >= '{min}' AND DATE(T0_DATE) <= '{max}'))"),
      raw_where = TRUE
   ) %>%
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
   forms$form_hts <- dbTable(
      lw_conn,
      "ohasis_warehouse",
      "form_hts",
      where     = glue("((RECORD_DATE >= '{min}' AND RECORD_DATE <= '{max}') OR (DATE(DATE_CONFIRM) >= '{min}' AND DATE(DATE_CONFIRM) <= '{max}') OR (DATE(T3_DATE) >= '{min}' AND DATE(T3_DATE) <= '{max}') OR (DATE(T2_DATE) >= '{min}' AND DATE(T2_DATE) <= '{max}') OR (DATE(T1_DATE) >= '{min}' AND DATE(T1_DATE) <= '{max}') OR (DATE(T0_DATE) >= '{min}' AND DATE(T0_DATE) <= '{max}'))"),
      raw_where = TRUE
   ) %>%
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
   forms$form_cfbs <- dbTable(
      lw_conn,
      "ohasis_warehouse",
      "form_cfbs",
      where     = glue("(RECORD_DATE >= '{min}' AND RECORD_DATE <= '{max}') OR (DATE(TEST_DATE) >= '{min}' AND DATE(TEST_DATE) <= '{max}')"),
      raw_where = TRUE
   ) %>%
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

   dbDisconnect(db_conn)
   dbDisconnect(lw_conn)
   rm(min, max, lw_conn, db_conn)
})