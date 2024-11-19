##  prepare dataset for TX_CURR ------------------------------------------------

icap$linelist$prep_curr <- icap$harp$prep$new_reg %>%
   select(
      CENTRAL_ID,
      prep_id,
      first,
      middle,
      last,
      suffix,
      uic,
      px_code,
      sex
   ) %>%
   left_join(
      y  = icap$harp$prep$new_outcome %>%
         select(-CENTRAL_ID),
      by = "prep_id"
   ) %>%
   filter(
      latest_ffupdate >= as.Date(icap$coverage$min),
      !is.na(latest_regimen)
   ) %>%
   mutate(
      # sex variable (use registry if available)
      Sex             = str_left(sex, 1),

      # KAP
      msm             = case_when(
         stri_detect_fixed(prep_risk_sexwithm, "yes") ~ 1,
         stri_detect_fixed(hts_risk_sexwithm, "yes") ~ 1,
         kp_msm == 1 ~ 1,
         TRUE ~ 0
      ),
      tgw             = if_else(
         condition = sex == "MALE" & self_identity %in% c("FEMALE", "OTHERS"),
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      hetero          = if_else(
         condition = sex == self_identity & msm != 0,
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      unknown         = if_else(
         condition = risks == "(no data)",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      `KP Population` = case_when(
         msm == 1 & tgw == 0 ~ "MSM",
         msm == 1 & tgw == 1 ~ "TGW",
         Sex == "F" ~ "(not included)",
         unknown == 1 ~ "(no data)",
         TRUE ~ "Non-MSM"
      ),

      # Age Band
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
      ffup_to_pickup  = floor(difftime(latest_nextpickup, latest_ffupdate, units = "days")),
      `DISAG 2`       = NA_character_,
      `DISAG 3`       = NA_character_,
   ) %>%
   left_join(
      y  = ohasis$ref_faci_code %>%
         mutate(
            FACI_CODE     = case_when(
               stri_detect_regex(SUB_FACI_CODE, "^HASH") ~ "HASH",
               stri_detect_regex(SUB_FACI_CODE, "^SAIL") ~ "SAIL",
               stri_detect_regex(SUB_FACI_CODE, "^TLY") ~ "TLY",
               TRUE ~ FACI_CODE
            ),
            SUB_FACI_CODE = if_else(
               condition = nchar(SUB_FACI_CODE) == 3,
               true      = NA_character_,
               false     = SUB_FACI_CODE
            ),
            SUB_FACI_CODE = case_when(
               FACI_CODE == "HASH" & is.na(SUB_FACI_CODE) ~ "HASH-QC",
               FACI_CODE == "TLY" & is.na(SUB_FACI_CODE) ~ "TLY-ANGLO",
               FACI_CODE == "SHP" & is.na(SUB_FACI_CODE) ~ "SHIP-MAKATI",
               TRUE ~ SUB_FACI_CODE
            ),
         ) %>%
         select(
            FACI_ID,
            faci                = FACI_CODE,
            branch              = SUB_FACI_CODE,
            `Site/Organization` = FACI_NAME,
            `Site City`         = FACI_NAME_MUNC,
            `Site Province`     = FACI_NAME_PROV,
            `Site Region`       = FACI_NAME_REG,
         ) %>%
         distinct_all(),
      by = c("faci", "branch")
   ) %>%
   left_join(
      y  = icap$sites %>%
         select(
            FACI_ID,
            site_icap_2021,
            site_icap_2022
         ) %>%
         distinct_all(),
      by = "FACI_ID"
   )