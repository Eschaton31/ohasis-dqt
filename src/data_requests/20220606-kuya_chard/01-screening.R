##  prepare dataset for HTS_TST ------------------------------------------------
dr$`20220606-kuya_chard`$request <- list()
local(envir = dr$`20220606-kuya_chard`, {
   request$`1`$a <- bind_rows(forms$form_a, forms$form_hts, forms$form_cfbs) %>%
      mutate(
         use_test          = case_when(
            !is.na(T0_RESULT) ~ "hts",
            !is.na(TEST_RESULT) ~ "cfbs",
            TRUE ~ NA_character_
         ),
         FINAL_TEST_DATE   = case_when(
            use_test == "hts" & !is.na(T0_DATE) ~ as.Date(T0_DATE),
            use_test == "hts" & is.na(T0_DATE) ~ RECORD_DATE,
            use_test == "cfbs" & !is.na(TEST_DATE) ~ as.Date(TEST_DATE),
            use_test == "cfbs" & is.na(TEST_DATE) ~ RECORD_DATE,
            TRUE ~ NA_Date_
         ),
         FINAL_TEST_RESULT = case_when(
            use_test == "hts" & !is.na(T0_RESULT) ~ T0_RESULT,
            use_test == "cfbs" & !is.na(TEST_RESULT) ~ TEST_RESULT,
            TRUE ~ NA_character_
         )
      )
})