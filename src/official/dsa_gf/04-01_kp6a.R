##  prepare dataset for TX_CURR ------------------------------------------------

gf$linelist$kp6a <- gf$harp$prep$new_reg %>%
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
      y  = gf$harp$prep$new_outcome %>%
         select(-CENTRAL_ID),
      by = "prep_id"
   ) %>%
   filter(
      prepstart_date >= as.Date(gf$coverage$min),
      !is.na(latest_regimen)
   ) %>%
   mutate(
      # sex variable (use registry if available)
      Sex              = StrLeft(sex, 1),
      Sex              = case_when(
         Sex == "M" ~ "Male",
         Sex == "F" ~ "Female",
         TRUE ~ Sex
      ),

      # Age Band
      curr_age         = floor(curr_age),
      `Age Group`      = case_when(
         curr_age %in% seq(0, 14) ~ "01_<15",
         curr_age %in% seq(15, 17) ~ "02_15-17",
         curr_age %in% seq(18, 19) ~ "03_18-19",
         curr_age %in% seq(20, 24) ~ "04_20-24",
         curr_age %in% seq(25, 29) ~ "05_25-29",
         curr_age %in% seq(30, 34) ~ "06_30-34",
         curr_age %in% seq(35, 49) ~ "07_35-49",
         curr_age %in% seq(50, 54) ~ "08_50-54",
         curr_age %in% seq(55, 59) ~ "09_55-59",
         curr_age %in% seq(60, 64) ~ "10_60-64",
         curr_age %in% seq(65, 69) ~ "11_65-69",
         curr_age %in% seq(70, 74) ~ "12_70-74",
         curr_age %in% seq(75, 79) ~ "13_75-79",
         curr_age %in% seq(80, 1000) ~ "14_80+",
         TRUE ~ "99_(no data)"
      ),

      # KAP
      msm              = case_when(
         kp_msm == 1 ~ 1,
         TRUE ~ 0
      ),
      tgw              = if_else(
         condition = sex == "MALE" & self_identity == "FEMALE",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      hetero           = if_else(
         condition = sex == self_identity & msm != 0,
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      unknown          = if_else(
         condition = kp_msm == 0 &
            kp_tgp == 0 &
            kp_sw == 0 &
            kp_pwid == 0 &
            kp_tgw == 0,
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      `Key Population` = case_when(
         msm == 1 & tgw == 0 ~ "MSM",
         msm == 1 & tgw == 1 ~ "TGW",
         kp_pwid == 1 ~ "PWID",
         hetero == 1 & Sex == "Male" ~ "Hetero Male",
         hetero == 1 & Sex == "Female" ~ "Hetero Female",
         unknown == 1 ~ "(no data)",
         TRUE ~ "Others"
      ),

      # disaggregations
      ffup_to_pickup   = floor(difftime(latest_nextpickup, latest_ffupdate, units = "days")),
      `DISAG 1`        = if_else(
         condition = prep_type != "(no data)",
         true      = toupper(prep_type),
         false     = prep_type
      ),
      `DISAG 2`        = case_when(
         ffup_to_pickup %in% seq(0, 30) ~ "1 month of PrEP",
         ffup_to_pickup %in% seq(31, 90) ~ "3 months of PrEP",
         ffup_to_pickup %in% seq(91, 10000) ~ "3+ months of PrEP",
      ),
      `DISAG 3`        = NA_character_,
      `DISAG 4`        = NA_character_,
      `DISAG 5`        = NA_character_,
      `Data Source`    = "OHASIS"
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
      y  = gf$sites %>%
         filter(!is.na(FACI_ID)) %>%
         select(
            GF_FACI    = FACI_ID,
            LS_SUBTYPE = `Clinic Type`
         ) %>%
         distinct_all() %>%
         add_row(GF_FACI = "130605", LS_SUBTYPE = "CBO") %>%
         bind_rows(
            ohasis$ref_faci %>%
               filter(
                  !(FACI_NHSSS_REG %in% c("CARAGA", "ARMM", "1", "2", "5", "8"))
               ) %>%
               inner_join(
                  y  = gf$forms$service_art %>% select(FACI_ID),
                  by = "FACI_ID"
               ) %>%
               select(GF_FACI = FACI_ID)
         ) %>%
         distinct_all() %>%
         mutate(
            site_gf_2022 = 1,
            LS_SUBTYPE   = if_else(
               condition = is.na(LS_SUBTYPE),
               true      = "Treatment Hub",
               false     = LS_SUBTYPE,
               missing   = LS_SUBTYPE
            )
         ) %>%
         rename(FACI_ID = GF_FACI),
      by = "FACI_ID"
   ) %>%
   rename(
      `Logsheet Subtype` = LS_SUBTYPE,
   )