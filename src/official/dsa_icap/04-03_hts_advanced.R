##  prepare dataset for HTS_TST ------------------------------------------------

icap$linelist$hts_advanced <- icap$harp$dx %>%
   mutate(
      ref_report = as.Date(paste(sep = "-", year, stri_pad_left(month, 2, "0"), "01"))
   ) %>%
   left_join(
      y  = bind_rows(icap$forms$form_a, icap$forms$form_hts) %>%
         mutate(
            SERVICE_FACI = if_else(
               is.na(SERVICE_FACI),
               FACI_ID,
               SERVICE_FACI,
               SERVICE_FACI
            )
         ) %>%
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
      age,
      class,
      class2022,
      CENTRAL_FACI,
      CENTRAL_SUB_FACI
   ) %>%
   mutate(
      # sex
      Sex              = StrLeft(reg_sex, 1),

      # KAP
      msm              = case_when(
         reg_sex == "MALE" & sexhow %in% c("BISEXUAL", "HOMOSEXUAL") ~ 1,
         reg_sex == "MALE" & sexhow == "HETEROSEXUAL" ~ 0,
         # KP_MSM == 1 ~ 1,
         TRUE ~ 0
      ),
      msm              = if_else(
         condition = msm == 1,
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      tgw              = case_when(
         reg_sex == "MALE" & self_identity %in% c("FEMALE", "OTHERS") ~ 1,
         # KP_TGW == 1 ~ 1,
         TRUE ~ 0
      ),
      tgw              = if_else(
         condition = tgw == 1,
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      hetero           = if_else(
         condition = sexhow == "HETEROSEXUAL",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      unknown          = case_when(
         transmit == "UNKNOWN" ~ 1,
         is.na(transmit) ~ 1,
         TRUE ~ 0
      ),
      `KP Population`  = case_when(
         msm == 1 & tgw == 0 ~ "MSM",
         msm == 1 & tgw == 1 ~ "TGW",
         Sex == "F" ~ "(not included)",
         unknown == 1 ~ "(no data)",
         TRUE ~ "Non-MSM"
      ),

      # Age Band
      # curr_age        = case_when(
      #    use_central == 1 & !is.na(bdate) ~ floor((FINAL_TEST_DATE - bdate) / 365.25),
      #    use_central == 0 & !is.na(BIRTHDATE) ~ floor((FINAL_TEST_DATE - BIRTHDATE) / 365.25),
      #    TRUE ~ AGE
      # ),
      curr_age         = floor(age),
      Age_Band         = case_when(
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
      `DATIM Age`      = if_else(
         condition = curr_age < 15,
         true      = "<15",
         false     = ">=15",
         missing   = "(no data)"
      ),

      `DISAG 2`        = case_when(
         class == "AIDS" ~ "Clinicallly Identified Advanced HIV",
         TRUE ~ "HIV"
      ),
      `DISAG 3`        = case_when(
         class2022 == "AIDS" ~ "WHO Guidlines' Advanced HIV",
         TRUE ~ "HIV"
      ),
      CENTRAL_SUB_FACI = NA_character_
   ) %>%
   left_join(
      y  = icap$sites %>%
         select(
            CENTRAL_FACI = FACI_ID,
            site_icap_2021,
            site_icap_2022
         ) %>%
         distinct_all(),
      by = "CENTRAL_FACI"
   )

icap$linelist$hts_advanced <- ohasis$get_faci(
   icap$linelist$hts_advanced,
   list("Site/Organization" = c("CENTRAL_FACI", "CENTRAL_SUB_FACI")),
   "name",
   c("Site Region", "Site Province", "Site City")
)