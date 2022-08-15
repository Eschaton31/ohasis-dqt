##  prepare dataset for HTS_TST ------------------------------------------------

epic$linelist$hts_tst <- bind_rows(epic$forms$form_hts, epic$forms$form_a) %>%
   filter(
      !is.na(CONFIRM_RESULT),
      CONFIRM_RESULT != "4_Pending"
   ) %>%
   mutate(
      # confirm date
      LATEST_CONFIRM_DATE = case_when(
         !is.na(DATE_CONFIRM) ~ as.Date(DATE_CONFIRM),
         !is.na(T3_DATE) ~ as.Date(T3_DATE),
         !is.na(T2_DATE) ~ as.Date(T2_DATE),
         !is.na(T1_DATE) ~ as.Date(T1_DATE),
      ),

      # get confirm msm or not
      CONFIRM_MSM         = case_when(
         FORMA_MSM == 1 ~ 1,
         HTS_MSM == 1 ~ 1,
         TRUE ~ 0
      ),
      CONFIRM_TGW         = case_when(
         FORMA_TGW == 1 ~ 1,
         HTS_TGW == 1 ~ 1,
         TRUE ~ 0
      ),

      # use source for special
      SERVICE_FACI        = case_when(
         CONFIRM_FACI == "030001" & SPECIMEN_SOURCE != SERVICE_FACI ~ SPECIMEN_SOURCE,
         is.na(SERVICE_FACI) & !is.na(SPECIMEN_SOURCE) ~ SPECIMEN_SOURCE,
         is.na(SERVICE_FACI) & !is.na(FACI_ID) ~ FACI_ID,
         TRUE ~ SERVICE_FACI
      )
   ) %>%
   filter(
      (LATEST_CONFIRM_DATE >= as.Date(epic$coverage$min) & LATEST_CONFIRM_DATE <= as.Date(epic$coverage$max))
   ) %>%
   arrange(LATEST_CONFIRM_DATE) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   select(
      CENTRAL_ID,
      LATEST_CONFIRM_RESULT = CONFIRM_RESULT,
      LATEST_CONFIRM_DATE,
      CONFIRM_REC           = REC_ID,
      CONFIRM_MSM,
      CONFIRM_TGW,
      CONFIRM_SEX           = SEX,
      CONFIRM_BIRTHDATE     = BIRTHDATE,
      CONFIRM_AGE           = AGE,
      CONFIRM_FACI          = SERVICE_FACI,
      CONFIRM_SUB_FACI      = SERVICE_SUB_FACI
   ) %>%
   full_join(
      y  = bind_rows(epic$forms$form_hts, epic$forms$form_a) %>%
         filter(
            is.na(CONFIRM_RESULT) | CONFIRM_RESULT == "4_Pending",
            StrLeft(MODALITY, 6) == "101101"
         ) %>%
         mutate(
            LATEST_TEST_DATE   = case_when(
               !is.na(T0_DATE) ~ as.Date(T0_DATE),
               !is.na(T1_DATE) ~ as.Date(T1_DATE),
               TRUE ~ RECORD_DATE
            ),
            LATEST_TEST_RESULT = case_when(
               !is.na(T0_RESULT) ~ T0_RESULT,
               !is.na(T1_RESULT) ~ T1_RESULT,
               TRUE ~ NA_character_
            ),

            # get confirm msm or not
            TEST_MSM           = case_when(
               FORMA_MSM == 1 ~ 1,
               HTS_MSM == 1 ~ 1,
               TRUE ~ 0
            ),
            TEST_TGW           = case_when(
               FORMA_TGW == 1 ~ 1,
               HTS_TGW == 1 ~ 1,
               TRUE ~ 0
            ),

            # use source for special
            SERVICE_FACI       = case_when(
               CONFIRM_FACI == "030001" & SPECIMEN_SOURCE != SERVICE_FACI ~ SPECIMEN_SOURCE,
               is.na(SERVICE_FACI) & !is.na(SPECIMEN_SOURCE) ~ SPECIMEN_SOURCE,
               is.na(SERVICE_FACI) & !is.na(FACI_ID) ~ FACI_ID,
               TRUE ~ SERVICE_FACI
            )
         ) %>%
         filter(
            (LATEST_TEST_DATE >= as.Date(epic$coverage$min) & LATEST_TEST_DATE <= as.Date(epic$coverage$max))
         ) %>%
         arrange(desc(LATEST_TEST_DATE)) %>%
         distinct(CENTRAL_ID, .keep_all = TRUE) %>%
         select(
            CENTRAL_ID,
            LATEST_TEST_DATE,
            LATEST_TEST_RESULT,
            TEST_REC       = REC_ID,
            TEST_MSM,
            TEST_TGW,
            TEST_SEX       = SEX,
            TEST_BIRTHDATE = BIRTHDATE,
            TEST_AGE       = AGE,
            TEST_FACI      = SERVICE_FACI,
            TEST_SUB_FACI  = SERVICE_SUB_FACI
         ),
      by = "CENTRAL_ID"
   ) %>%
   full_join(
      y  = bind_rows(epic$forms$form_hts, epic$forms$form_cfbs) %>%
         filter(
            (StrLeft(MODALITY, 6) %in% c("101103", "101104")) | is.na(MODALITY)
         ) %>%
         mutate(
            LATEST_CFBS_DATE   = case_when(
               !is.na(TEST_DATE) ~ as.Date(TEST_DATE),
               !is.na(T0_DATE) ~ as.Date(T0_DATE),
               !is.na(T1_DATE) ~ as.Date(T1_DATE),
               TRUE ~ RECORD_DATE
            ),
            LATEST_CFBS_RESULT = case_when(
               !is.na(TEST_RESULT) ~ TEST_RESULT,
               !is.na(T0_RESULT) ~ T0_RESULT,
               !is.na(T1_RESULT) ~ T1_RESULT,
               TRUE ~ NA_character_
            ),

            # get confirm msm or not
            CFBS_MSM           = case_when(
               CFBS_MSM == 1 ~ 1,
               HTS_MSM == 1 ~ 1,
               TRUE ~ 0
            ),
            CFBS_TGW           = case_when(
               CFBS_TGW == 1 ~ 1,
               HTS_TGW == 1 ~ 1,
               TRUE ~ 0
            ),

            # use source for special
            SERVICE_FACI       = case_when(
               CONFIRM_FACI == "030001" & SPECIMEN_SOURCE != SERVICE_FACI ~ SPECIMEN_SOURCE,
               is.na(SERVICE_FACI) & !is.na(SPECIMEN_SOURCE) ~ SPECIMEN_SOURCE,
               is.na(SERVICE_FACI) & !is.na(FACI_ID) ~ FACI_ID,
               TRUE ~ SERVICE_FACI
            )
         ) %>%
         filter(
            (LATEST_CFBS_DATE >= as.Date(epic$coverage$min) & LATEST_CFBS_DATE <= as.Date(epic$coverage$max))
         ) %>%
         arrange(desc(LATEST_CFBS_DATE)) %>%
         distinct(CENTRAL_ID, .keep_all = TRUE) %>%
         select(
            CENTRAL_ID,
            LATEST_CFBS_DATE,
            LATEST_CFBS_RESULT,
            CFBS_REC       = REC_ID,
            CFBS_MSM,
            CFBS_TGW,
            CFBS_SEX       = SEX,
            CFBS_BIRTHDATE = BIRTHDATE,
            CFBS_AGE       = AGE,
            CFBS_FACI      = SERVICE_FACI,
            CFBS_SUB_FACI  = SERVICE_SUB_FACI
         ),
      by = "CENTRAL_ID"
   ) %>%
   full_join(
      y  = epic$forms$form_hts %>%
         filter(
            StrLeft(MODALITY, 6) == "101105"
         ) %>%
         mutate(
            LATEST_ST_DATE   = case_when(
               !is.na(T0_DATE) ~ as.Date(T0_DATE),
               !is.na(T1_DATE) ~ as.Date(T1_DATE),
               TRUE ~ RECORD_DATE
            ),
            LATEST_ST_RESULT = case_when(
               !is.na(T0_RESULT) ~ T0_RESULT,
               !is.na(T1_RESULT) ~ T1_RESULT,
               TRUE ~ NA_character_
            ),

            # use source for special
            SERVICE_FACI     = case_when(
               CONFIRM_FACI == "030001" & SPECIMEN_SOURCE != SERVICE_FACI ~ SPECIMEN_SOURCE,
               is.na(SERVICE_FACI) & !is.na(SPECIMEN_SOURCE) ~ SPECIMEN_SOURCE,
               is.na(SERVICE_FACI) & !is.na(FACI_ID) ~ FACI_ID,
               TRUE ~ SERVICE_FACI
            )
         ) %>%
         filter(
            (LATEST_ST_DATE >= as.Date(epic$coverage$min) & LATEST_ST_DATE <= as.Date(epic$coverage$max))
         ) %>%
         arrange(desc(LATEST_ST_DATE)) %>%
         distinct(CENTRAL_ID, .keep_all = TRUE) %>%
         select(
            CENTRAL_ID,
            LATEST_ST_DATE,
            LATEST_ST_RESULT,
            ST_REC       = REC_ID,
            ST_MSM       = HTS_MSM,
            ST_TGW       = HTS_TGW,
            ST_SEX       = SEX,
            ST_BIRTHDATE = BIRTHDATE,
            ST_AGE       = AGE,
            ST_FACI      = SERVICE_FACI,
            ST_SUB_FACI  = SERVICE_SUB_FACI
         ),
      by = "CENTRAL_ID"
   ) %>%
   mutate_at(
      .vars = vars(LATEST_CONFIRM_RESULT, LATEST_TEST_RESULT, LATEST_CFBS_RESULT, LATEST_ST_RESULT),
      ~remove_code(.)
   ) %>%
   left_join(
      y  = epic$harp$dx %>%
         mutate(
            ref_report = as.Date(paste(sep = "-", year, stri_pad_left(month, 2, "0"), "01"))
         ) %>%
         left_join(
            y  = bind_rows(epic$forms$form_a, epic$forms$form_hts) %>%
               select(
                  REC_ID,
                  CENTRAL_FACI     = SERVICE_FACI,
                  CENTRAL_SUB_FACI = SERVICE_SUB_FACI,
               ),
            by = "REC_ID"
         ) %>%
         select(
            CENTRAL_ID,
            idnum,
            bdate,
            reg_sex = sex,
            transmit,
            sexhow,
            self_identity,
            confirm_date,
            ref_report,
            CENTRAL_FACI,
            CENTRAL_SUB_FACI
         ),
      by = "CENTRAL_ID"
   ) %>%
   mutate(
      # tag if central to be used
      use_central       = if_else(
         condition = !is.na(idnum),
         true      = 1,
         false     = 0,
         missing   = 0
      ),

      # old confirmed
      old_dx            = case_when(
         confirm_date >= as.Date(epic$coverage$min) ~ 0,
         ref_report < as.Date(epic$coverage$min) ~ 1,
         TRUE ~ 0
      ),

      # tag which test to be used
      use_test          = case_when(
         !is.na(CONFIRM_REC) ~ "confirm",
         !is.na(TEST_REC) ~ "test",
         !is.na(CFBS_REC) ~ "cfbs",
         !is.na(ST_REC) ~ "st",
         TRUE ~ NA_character_
      ),

      FINAL_TEST_DATE   = case_when(
         old_dx == 0 & !is.na(idnum) ~ confirm_date,
         use_test == "confirm" ~ LATEST_CONFIRM_DATE,
         use_test == "test" ~ LATEST_TEST_DATE,
         use_test == "cfbs" ~ LATEST_CFBS_DATE,
         use_test == "st" ~ LATEST_ST_DATE,
      ),
      FINAL_TEST_RESULT = case_when(
         old_dx == 0 & !is.na(idnum) ~ "Confirmed: Positive",
         use_test == "confirm" ~ paste0("Confirmed: ", LATEST_CONFIRM_RESULT),
         use_test == "test" ~ paste0("Tested: ", LATEST_TEST_RESULT),
         use_test == "cfbs" ~ paste0("CBS: ", LATEST_CFBS_RESULT),
         use_test == "st" ~ paste0("Self-Testing: ", LATEST_ST_RESULT),
      ),
      FINAL_FACI        = case_when(
         old_dx == 0 & !is.na(CENTRAL_FACI) ~ CENTRAL_FACI,
         use_test == "confirm" ~ CONFIRM_FACI,
         use_test == "test" ~ TEST_FACI,
         use_test == "cfbs" ~ CFBS_FACI,
         use_test == "st" ~ ST_FACI,
      ),
      FINAL_SUB_FACI    = case_when(
         old_dx == 0 & !is.na(CENTRAL_FACI) ~ CENTRAL_SUB_FACI,
         use_test == "confirm" ~ CONFIRM_SUB_FACI,
         use_test == "test" ~ TEST_SUB_FACI,
         use_test == "cfbs" ~ CFBS_SUB_FACI,
         use_test == "st" ~ ST_SUB_FACI,
      ),
      FINAL_SUB_FACI    = if_else(
         condition = !(FINAL_FACI %in% c("130001", "130605")),
         true      = NA_character_,
         false     = FINAL_SUB_FACI,
         missing   = FINAL_SUB_FACI
      ),
      SEX               = case_when(
         use_test == "confirm" ~ CONFIRM_SEX,
         use_test == "test" ~ TEST_SEX,
         use_test == "cfbs" ~ CFBS_SEX,
         use_test == "st" ~ ST_SEX,
      ),
      AGE               = case_when(
         use_test == "confirm" ~ as.numeric(CONFIRM_AGE),
         use_test == "test" ~ as.numeric(TEST_AGE),
         use_test == "cfbs" ~ as.numeric(CFBS_AGE),
         use_test == "st" ~ as.numeric(ST_AGE),
      ),
      BIRTHDATE         = case_when(
         use_test == "confirm" ~ CONFIRM_BIRTHDATE,
         use_test == "test" ~ TEST_BIRTHDATE,
         use_test == "cfbs" ~ CFBS_BIRTHDATE,
         use_test == "st" ~ ST_BIRTHDATE,
      ),
      REC_ID            = case_when(
         use_test == "confirm" ~ CONFIRM_REC,
         use_test == "test" ~ TEST_REC,
         use_test == "cfbs" ~ CFBS_REC,
         use_test == "st" ~ ST_REC,
      )
   ) %>%
   mutate(
      # sex
      Sex             = case_when(
         use_central == 1 ~ StrLeft(reg_sex, 1),
         use_test == "confirm" ~ substr(CONFIRM_SEX, 3, 3),
         use_test == "test" ~ substr(TEST_SEX, 3, 3),
         use_test == "cfbs" ~ substr(CFBS_SEX, 3, 3),
         use_test == "st" ~ substr(ST_SEX, 3, 3),
         TRUE ~ "(no data)"
      ),

      # KAP
      KP_MSM          = case_when(
         use_test == "confirm" ~ as.numeric(CONFIRM_MSM),
         use_test == "test" ~ as.numeric(TEST_MSM),
         use_test == "cfbs" ~ as.numeric(CFBS_MSM),
         use_test == "st" ~ as.numeric(ST_MSM),
         TRUE ~ 0
      ),
      KP_TGW          = case_when(
         use_test == "confirm" ~ as.numeric(CONFIRM_TGW),
         use_test == "test" ~ as.numeric(TEST_TGW),
         use_test == "cfbs" ~ as.numeric(CFBS_TGW),
         use_test == "st" ~ as.numeric(ST_TGW),
         TRUE ~ 0
      ),
      msm             = case_when(
         reg_sex == "MALE" & sexhow %in% c("BISEXUAL", "HOMOSEXUAL") ~ 1,
         reg_sex == "MALE" & sexhow == "HETEROSEXUAL" ~ 0,
         KP_MSM == 1 ~ 1,
         TRUE ~ 0
      ),
      msm             = if_else(
         condition = msm == 1,
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      tgw             = case_when(
         reg_sex == "MALE" & self_identity %in% c("FEMALE", "OTHERS") ~ 1,
         KP_TGW == 1 ~ 1,
         TRUE ~ 0
      ),
      tgw             = if_else(
         condition = tgw == 1,
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      hetero          = if_else(
         condition = sexhow == "HETEROSEXUAL",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      unknown         = case_when(
         transmit == "UNKNOWN" ~ 1,
         is.na(transmit) ~ 1,
         TRUE ~ 0
      ),
      `KP Population` = case_when(
         msm == 1 & tgw == 0 ~ "MSM",
         msm == 1 & tgw == 1 ~ "TGW",
         Sex == "F" ~ "(not included)",
         unknown == 1 ~ "(no data)",
         TRUE ~ "Non-MSM"
      ),

      # Age Band
      curr_age        = case_when(
         use_central == 1 & !is.na(bdate) ~ floor((FINAL_TEST_DATE - bdate) / 365.25),
         use_central == 0 & !is.na(BIRTHDATE) ~ floor((FINAL_TEST_DATE - BIRTHDATE) / 365.25),
         TRUE ~ AGE
      ),
      curr_age        = floor(curr_age),
      Age_Band        = case_when(
         curr_age >= 0 & curr_age < 5 ~ "01_0-4",
         curr_age >= 5 & curr_age < 10 ~ "02_5-9",
         curr_age >= 10 & curr_age < 15 ~ "03_10-14",
         curr_age >= 15 & curr_age < 20 ~ "04_15-19",
         curr_age >= 20 & curr_age < 25 ~ "05_20-24",
         curr_age >= 25 & curr_age < 30 ~ "06_25-29",
         curr_age >= 30 & curr_age < 35 ~ "07_30-34",
         curr_age >= 35 & curr_age < 40 ~ "08_35-39",
         curr_age >= 40 & curr_age < 45 ~ "09_40-44",
         curr_age >= 45 & curr_age < 50 ~ "10_45-49",
         curr_age >= 50 & curr_age < 55 ~ "11_50-54",
         curr_age >= 55 & curr_age < 60 ~ "12_55-59",
         curr_age >= 60 & curr_age < 65 ~ "13_60-64",
         curr_age >= 65 & curr_age < 1000 ~ "14_65+",
         TRUE ~ "99_(no data)"
      ),
      `DATIM Age`     = if_else(
         condition = curr_age < 15,
         true      = "<15",
         false     = ">=15",
         missing   = "(no data)"
      ),

      `DISAG 2`       = case_when(
         stri_detect_fixed(FINAL_TEST_RESULT, "CBS") ~ "CBO (CBS)",
         stri_detect_fixed(FINAL_TEST_RESULT, "Self-Testing") ~ "Self-Testing",
         TRUE ~ "Facility (walk-in)"
      ),
      `DISAG 3`       = case_when(
         old_dx == 1 & use_test == "confirm" ~ "Confirmed: Known Pos",
         old_dx == 1 & use_test == "test" ~ "Tested: Known Pos",
         old_dx == 1 & use_test == "cfbs" ~ "CBS: Known Pos",
         old_dx == 1 & use_test == "st" ~ "Self-Testing: Known Pos",
         TRUE ~ FINAL_TEST_RESULT
      ),
   ) %>%
   filter(
      !stri_detect_regex(FINAL_TEST_RESULT, ": NA$")
   ) %>%
   left_join(
      y  = epic$sites %>%
         select(
            FINAL_FACI = FACI_ID,
            site_epic_2021,
            site_epic_2022
         ) %>%
         distinct_all(),
      by = "FINAL_FACI"
   )

epic$linelist$hts_tst <- ohasis$get_faci(
   epic$linelist$hts_tst,
   list("Site/Organization" = c("FINAL_FACI", "FINAL_SUB_FACI")),
   "name",
   c("Site Region", "Site Province", "Site City")
)