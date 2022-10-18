##  prepare dataset for HTS_TST ------------------------------------------------

icap$linelist$tx_new_verify <- bind_rows(icap$forms$form_cfbs, icap$forms$form_hts) %>%
   filter(
      StrLeft(MODALITY, 6) != "101101" | is.na(MODALITY)
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
      y  = icap$harp$tx$new_reg %>%
         filter(newonart == 1) %>%
         select(
            CENTRAL_ID,
            art_id,
            first,
            middle,
            last,
            suffix,
            confirmatory_code,
            uic,
            age,
            px_code
         ),
      by = "CENTRAL_ID"
   ) %>%
   left_join(
      y  = icap$harp$tx$new_outcome %>%
         rename(
            art_sex = sex
         ) %>%
         select(-CENTRAL_ID),
      by = "art_id"
   ) %>%
   left_join(
      y  = icap$harp$dx %>%
         select(
            idnum,
            reg_sex = sex,
            transmit,
            sexhow,
            self_identity,
            confirm_date
         ),
      by = "idnum"
   ) %>%
   mutate(
      # tag based on ICAP Defintion
      onart28         = floor(difftime(as.Date(icap$coverage$max), latest_nextpickup, units = "days")),
      onart28         = if_else(
         condition = onart28 <= 28 & outcome != "dead",
         true      = 1,
         false     = 0,
         missing   = 0
      ),

      # sex variable (use registry if available)
      Sex             = if_else(
         condition = !is.na(transmit),
         true      = StrLeft(reg_sex, 1),
         false     = StrLeft(art_sex, 1),
         missing   = StrLeft(art_sex, 1)
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
      age             = floor(age),
      Age_Band        = case_when(
         age >= 0 & age < 5 ~ "01_0-4",
         age >= 5 & age < 10 ~ "02_5-9",
         age >= 10 & age < 15 ~ "03_10-14",
         age >= 15 & age < 20 ~ "04_15-19",
         age >= 20 & age < 25 ~ "05_20-24",
         age >= 25 & age < 30 ~ "06_25-29",
         age >= 30 & age < 35 ~ "07_30-34",
         age >= 35 & age < 40 ~ "08_35-39",
         age >= 40 & age < 45 ~ "09_40-44",
         age >= 45 & age < 50 ~ "10_45-49",
         age >= 50 & age < 55 ~ "11_50-54",
         age >= 55 & age < 60 ~ "12_55-59",
         age >= 60 & age < 65 ~ "13_60-64",
         age >= 65 & age < 1000 ~ "14_65+",
         TRUE ~ "99_(no data)"
      ),
      `DATIM Age`     = if_else(
         condition = age < 15,
         true      = "<15",
         false     = ">=15",
         missing   = "(no data)"
      ),

      # disaggregations
      `DISAG 2`       = NA_character_,
      `DISAG 3`       = NA_character_
   ) %>%
   left_join(
      y  = icap$sites %>%
         select(
            SERVICE_FACI = FACI_ID,
            site_icap_2021,
            site_icap_2022
         ) %>%
         distinct_all(),
      by = "SERVICE_FACI"
   )

icap$linelist$tx_new_verify <- ohasis$get_faci(
   icap$linelist$tx_new_verify,
   list("Site/Organization" = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
   "name",
   c("Site Region", "Site Province", "Site City")
)