##  prepare dataset for HTS_TST ------------------------------------------------

epic$linelist$hts_tst_verify <- bind_rows(epic$forms$form_cfbs, epic$forms$form_hts) %>%
   filter(
      StrLeft(MODALITY, 6) != "101101" | is.na(MODALITY),
      StrLeft(T0_RESULT, 1) == "1" |
         StrLeft(T1_RESULT, 1) == "1" |
         StrLeft(TEST_RESULT, 1) == "1"
   ) %>%
   mutate(
      # get confirm msm or not
      CFBS_MSM     = case_when(
         CFBS_MSM == 1 ~ 1,
         HTS_MSM == 1 ~ 1,
         TRUE ~ 0
      ),
      CFBS_TGW     = case_when(
         CFBS_TGW == 1 ~ 1,
         HTS_TGW == 1 ~ 1,
         TRUE ~ 0
      ),

      # use source for special
      SERVICE_FACI = case_when(
         CONFIRM_FACI == "030001" & SPECIMEN_SOURCE != SERVICE_FACI ~ SPECIMEN_SOURCE,
         is.na(SERVICE_FACI) & !is.na(SPECIMEN_SOURCE) ~ SPECIMEN_SOURCE,
         is.na(SERVICE_FACI) & !is.na(FACI_ID) ~ FACI_ID,
         TRUE ~ SERVICE_FACI
      )
   ) %>%
   arrange(desc(RECORD_DATE)) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   inner_join(
      y  = bind_rows(epic$forms$form_hts, epic$forms$form_a) %>%
         filter(
            StrLeft(CONFIRM_RESULT, 1) == "1"
         ) %>%
         mutate(
            # confirm date
            LATEST_CONFIRM_DATE = case_when(
               !is.na(DATE_CONFIRM) ~ as.Date(DATE_CONFIRM),
               !is.na(T3_DATE) ~ as.Date(T3_DATE),
               !is.na(T2_DATE) ~ as.Date(T2_DATE),
               !is.na(T1_DATE) ~ as.Date(T1_DATE),
            ),
         ) %>%
         filter(
            (LATEST_CONFIRM_DATE >= as.Date(epic$coverage$min) & LATEST_CONFIRM_DATE <= as.Date(epic$coverage$max))
         ) %>%
         arrange(LATEST_CONFIRM_DATE) %>%
         distinct(CENTRAL_ID, .keep_all = TRUE) %>%
         select(
            CENTRAL_ID,
            LATEST_CONFIRM_DATE,
            CONFIRM_RESULT
         ),
      by = "CENTRAL_ID"
   ) %>%
   filter(LATEST_CONFIRM_DATE >= RECORD_DATE) %>%
   left_join(
      y  = epic$harp$dx %>%
         mutate(
            ref_report = as.Date(paste(sep = "-", year, stri_pad_left(month, 2, "0"), "01"))
         ) %>%
         select(
            CENTRAL_ID,
            idnum,
            reg_sex = sex,
            transmit,
            sexhow,
            bdate,
            self_identity,
            confirm_date,
            ref_report
         ),
      by = "CENTRAL_ID"
   ) %>%
   mutate(
      # sex variable (use registry if available)
      Sex             = if_else(
         condition = !is.na(transmit),
         true      = StrLeft(reg_sex, 1),
         false     = substr(SEX, 3, 3),
         missing   = substr(SEX, 3, 3)
      ),

      # KAP
      msm             = case_when(
         reg_sex == "MALE" & sexhow %in% c("BISEXUAL", "HOMOSEXUAL") ~ 1,
         reg_sex == "MALE" & sexhow == "HETEROSEXUAL" ~ 0,
         CFBS_MSM == 1 ~ 1,
         TRUE ~ 0
      ),
      tgw             = case_when(
         reg_sex == "MALE" & self_identity %in% c("FEMALE", "OTHERS") ~ 1,
         CFBS_TGW == 1 ~ 1,
         TRUE ~ 0
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
         !is.na(bdate) ~ floor((RECORD_DATE - bdate) / 365.25),
         !is.na(BIRTHDATE) ~ floor((RECORD_DATE - BIRTHDATE) / 365.25),
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

      # disaggregations
      `DISAG 2`       = case_when(
         ref_report < as.Date(epic$coverage$min) ~ "Known Pos",
         TRUE ~ "CBS Reactive"
      ),
      `DISAG 3`       = NA_character_
   ) %>%
   left_join(
      y  = epic$sites %>%
         select(
            SERVICE_FACI = FACI_ID,
            site_epic_2021,
            site_epic_2022
         ) %>%
         distinct_all(),
      by = "SERVICE_FACI"
   )

epic$linelist$hts_tst_verify <- ohasis$get_faci(
   epic$linelist$hts_tst_verify,
   list("Site/Organization" = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
   "name",
   c("Site Region", "Site Province", "Site City")
)