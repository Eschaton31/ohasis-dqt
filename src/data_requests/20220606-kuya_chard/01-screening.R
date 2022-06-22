##  prepare dataset for HTS_TST ------------------------------------------------

.log_info("Preparing {green('Linelist')}.")
local(envir = dr$`20220606-kuya_chard`, {
   data$`1`$a <- bind_rows(forms$form_a, forms$form_hts, forms$form_cfbs) %>%
      mutate(
         use_test          = case_when(
            !is.na(T0_RESULT) ~ "hts",
            !is.na(TEST_RESULT) ~ "cfbs",
            TRUE ~ NA_character_
         ),
         FINAL_TEST_DATE   = case_when(
            use_test == "hts" & T0_DATE >= -25567 ~ as.Date(T0_DATE),
            use_test == "hts" & is.na(T0_DATE) ~ RECORD_DATE,
            use_test == "cfbs" & TEST_DATE >= -25567 ~ as.Date(TEST_DATE),
            use_test == "cfbs" & is.na(TEST_DATE) ~ RECORD_DATE,
            !is.na(use_test) ~ RECORD_DATE,
            TRUE ~ NA_Date_
         ),
         FINAL_TEST_RESULT = case_when(
            use_test == "hts" & !is.na(T0_RESULT) ~ T0_RESULT,
            use_test == "cfbs" & !is.na(TEST_RESULT) ~ TEST_RESULT,
            TRUE ~ NA_character_
         ),

         use_record_faci   = if_else(
            condition = is.na(SERVICE_FACI),
            true      = 1,
            false     = 0
         ),
         SERVICE_FACI      = if_else(
            condition = use_record_faci == 1,
            true      = FACI_ID,
            false     = SERVICE_FACI
         ),
      ) %>%
      filter(
         !is.na(FINAL_TEST_RESULT),
         SERVICE_FACI == "130605"
      )

   data$`1`$a <- ohasis$get_addr(
      data$`1`$a,
      c(
         "CBS_REG"  = "HIV_SERVICE_PSGC_REG",
         "CBS_PROV" = "HIV_SERVICE_PSGC_PROV",
         "CBS_MUNC" = "HIV_SERVICE_PSGC_MUNC"
      ),
      "name"
   )
   data$`1`$a <- ohasis$get_faci(
      data$`1`$a,
      list("SITE_NAME" = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
      "name",
      c("SITE_REG", "SITE_PROV", "SITE_MUNC")
   )

   data$`1`$a %<>%
      mutate(
         SITE_REG  = if_else(
            condition = CBS_MUNC != "Unknown",
            true      = CBS_REG,
            false     = SITE_REG,
            missing   = SITE_REG
         ),
         SITE_PROV = if_else(
            condition = CBS_MUNC != "Unknown",
            true      = CBS_PROV,
            false     = SITE_PROV,
            missing   = SITE_PROV
         ),
         SITE_MUNC = if_else(
            condition = CBS_MUNC != "Unknown",
            true      = CBS_MUNC,
            false     = SITE_MUNC,
            missing   = SITE_MUNC
         ),
      )


   data$`1`$a_nonreactive <- data$`1`$a %>%
      filter(StrLeft(FINAL_TEST_RESULT, 1) == "2") %>%
      anti_join(
         y  = data$`1`$a %>%
            filter(StrLeft(FINAL_TEST_RESULT, 1) == "1") %>%
            select(CENTRAL_ID),
         by = "CENTRAL_ID"
      )

   data$`1`$a_reactive <- data$`1`$a %>%
      filter(StrLeft(FINAL_TEST_RESULT, 1) == "1") %>%
      anti_join(
         y  = data$`1`$a %>%
            filter(StrLeft(FINAL_TEST_RESULT, 1) == "2") %>%
            select(CENTRAL_ID),
         by = "CENTRAL_ID"
      )

   data$`1`$a_seroconverted <- data$`1`$a %>%
      filter(StrLeft(FINAL_TEST_RESULT, 1) == "2") %>%
      inner_join(
         y  = data$`1`$a %>%
            filter(StrLeft(FINAL_TEST_RESULT, 1) == "1") %>%
            select(
               CENTRAL_ID,
               CONVERT_REC  = REC_ID,
               CONVERT_DATE = FINAL_TEST_DATE
            ),
         by = "CENTRAL_ID"
      ) %>%
      filter(
         FINAL_TEST_DATE < CONVERT_DATE
      )

   data$`1`$a_serodiscordant <- data$`1`$a %>%
      filter(StrLeft(FINAL_TEST_RESULT, 1) == "2") %>%
      inner_join(
         y  = data$`1`$a %>%
            filter(StrLeft(FINAL_TEST_RESULT, 1) == "1") %>%
            select(
               CENTRAL_ID,
               CONVERT_REC  = REC_ID,
               CONVERT_DATE = FINAL_TEST_DATE
            ),
         by = "CENTRAL_ID"
      ) %>%
      filter(
         FINAL_TEST_DATE >= CONVERT_DATE
      )
})

.log_info("Generating {green('Aggregates')}.")
local(envir = dr$`20220606-kuya_chard`, {
   request$`1`$a <- data$`1`$a_reactive %>%
      arrange(FINAL_TEST_DATE) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      group_by(SITE_REG) %>%
      summarise(
         Reactive = n()
      ) %>%
      full_join(
         y = data$`1`$a_nonreactive %>%
            arrange(FINAL_TEST_DATE) %>%
            distinct(CENTRAL_ID, .keep_all = TRUE) %>%
            group_by(SITE_REG) %>%
            summarise(
               Negative = n()
            )
      ) %>%
      full_join(
         y = data$`1`$a_serodiscordant %>%
            arrange(FINAL_TEST_DATE) %>%
            distinct(CENTRAL_ID, .keep_all = TRUE) %>%
            group_by(SITE_REG) %>%
            summarise(
               Serodiscordant = n()
            )
      ) %>%
      full_join(
         y = data$`1`$a_seroconverted %>%
            arrange(FINAL_TEST_DATE) %>%
            distinct(CENTRAL_ID, .keep_all = TRUE) %>%
            group_by(SITE_REG) %>%
            summarise(
               Seroconverted = n()
            )
      ) %>%
      ungroup() %>%
      adorn_totals() %>%
      # mutate_at(
      #    .vars = vars(Reactive, Negative, Seroconverted, Serodiscordant),
      #    ~if_else(
      #       condition = is.na(.),
      #       true      = "-",
      #       false     = format(., big.mark = ",")
      #    )
      # ) %>%
      arrange(SITE_REG) %>%
      rename(Region = 1)
})