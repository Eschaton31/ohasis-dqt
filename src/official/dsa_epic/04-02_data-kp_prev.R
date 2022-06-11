##  prepare dataset for HTS_TST ------------------------------------------------

epic$linelist$kp_prev <- epic$linelist$hts_tst %>%
   mutate(
      SCREEN_AGREED = "1_Yes"
   ) %>%
   select(
      CENTRAL_ID,
      REC_ID,
      SCREEN_AGREED,
      FINAL_TEST_RESULT,
      HTS_SEX       = Sex,
      BIRTHDATE,
      PREV_DATE     = FINAL_TEST_DATE,
      PREV_FACI     = FINAL_FACI,
      PREV_SUB_FACI = FINAL_SUB_FACI,
      HTS_AGE       = curr_age,
      HTS_MSM       = msm,
      HTS_TGW       = tgw,
   ) %>%
   bind_rows(
      epic$forms$form_hts %>%
         rename(
            CFBS_MSM      = HTS_MSM,
            CFBS_TGW      = HTS_TGW,
            CFBS_FACI     = SERVICE_FACI,
            CFBS_SUB_FACI = SERVICE_SUB_FACI,
            TEST_RESULT   = T0_RESULT
         ) %>%
         bind_rows(epic$forms$form_cfbs) %>%
         mutate(
            # use source for special
            SERVICE_FACI = case_when(
               CONFIRM_FACI == "030001" & SPECIMEN_SOURCE != SERVICE_FACI ~ SPECIMEN_SOURCE,
               is.na(SERVICE_FACI) & !is.na(SPECIMEN_SOURCE) ~ SPECIMEN_SOURCE,
               is.na(SERVICE_FACI) & !is.na(FACI_ID) ~ FACI_ID,
               TRUE ~ SERVICE_FACI
            )
         ) %>%
         filter(
            (RECORD_DATE >= as.Date(epic$coverage$min) & RECORD_DATE <= as.Date(epic$coverage$max))
         ) %>%
         arrange(desc(RECORD_DATE)) %>%
         distinct(CENTRAL_ID, .keep_all = TRUE) %>%
         select(
            REC_ID,
            CENTRAL_ID,
            SCREEN_AGREED,
            MODALITY,
            FINAL_TEST_RESULT = TEST_RESULT,
            PREV_DATE         = RECORD_DATE,
            CFBS_MSM,
            CFBS_TGW,
            BIRTHDATE,
            SEX,
            AGE,
            PREV_FACI         = SERVICE_FACI,
            PREV_SUB_FACI     = SERVICE_SUB_FACI,
         )
   ) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   arrange(desc(PREV_DATE)) %>%
   left_join(
      y  = epic$harp$dx %>%
         mutate(
            ref_report = as.Date(paste(sep = "-", year, stri_pad_left(month, 2, "0"), "01"))
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
            ref_report
         ),
      by = "CENTRAL_ID"
   ) %>%
   mutate(
      # tag if central to be used
      use_central     = if_else(
         condition = !is.na(idnum),
         true      = 1,
         false     = 0,
         missing   = 0
      ),

      # old confirmed
      old_dx          = if_else(
         condition = ref_report < as.Date(epic$coverage$min),
         true      = 1,
         false     = 0,
         missing   = 0
      ),

      # sex
      Sex             = case_when(
         !is.na(HTS_SEX) ~ HTS_SEX,
         use_central == 1 ~ StrLeft(reg_sex, 1),
         !is.na(SEX) ~ substr(SEX, 3, 3),
         TRUE ~ "(no data)"
      ),

      # KAP
      KP_MSM          = case_when(
         !is.na(HTS_MSM) ~ HTS_MSM,
         !is.na(CFBS_MSM) ~ as.numeric(CFBS_MSM),
         TRUE ~ 0
      ),
      KP_TGW          = case_when(
         !is.na(HTS_TGW) ~ HTS_TGW,
         !is.na(CFBS_TGW) ~ as.numeric(CFBS_TGW),
         TRUE ~ 0
      ),
      msm             = case_when(
         !is.na(HTS_MSM) ~ HTS_MSM,
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
         !is.na(HTS_TGW) ~ HTS_TGW,
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
         !is.na(HTS_AGE) ~ as.numeric(HTS_AGE),
         use_central == 1 & !is.na(bdate) ~ floor((PREV_DATE - bdate) / 365.25) %>% as.numeric(),
         use_central == 0 & !is.na(BIRTHDATE) ~ floor((PREV_DATE - BIRTHDATE) / 365.25) %>% as.numeric(),
         TRUE ~ as.numeric(AGE)
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

      `DISAG 2`       = NA_character_,
      `DISAG 3`       = case_when(
         old_dx == 1 ~ "Known Pos",
         StrLeft(MODALITY, 6) == "101304" ~ "Reach (Not offered tesing)",
         StrLeft(SCREEN_AGREED, 1) == "0" ~ "Declined Testing",
         StrLeft(SCREEN_AGREED, 1) == "1" ~ "Tested/Referred for Testing",
         is.na(SCREEN_AGREED) & !is.na(FINAL_TEST_RESULT) ~ "Tested/Referred for Testing",
         TRUE ~ "Reach (Not offered tesing)"
      ),
   ) %>%
   left_join(
      y  = epic$sites %>%
         select(
            PREV_FACI = FACI_ID,
            site_epic_2021,
            site_epic_2022
         ) %>%
         distinct_all(),
      by = "PREV_FACI"
   )

epic$linelist$kp_prev <- ohasis$get_faci(
   epic$linelist$kp_prev,
   list("Site/Organization" = c("PREV_FACI", "PREV_SUB_FACI")),
   "name",
   c("Site Region", "Site Province", "Site City")
)
