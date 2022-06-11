##  prepare dataset for TX_CURR ------------------------------------------------

epic$linelist$tx_new <- epic$harp$tx$new_reg %>%
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
   ) %>%
   left_join(
      y  = epic$harp$tx$new_outcome %>%
         rename(
            art_sex = sex
         ) %>%
         select(-CENTRAL_ID),
      by = "art_id"
   ) %>%
   left_join(
      y  = epic$harp$dx %>%
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
      # tag based on EpiC Defintion
      onart28         = floor(difftime(as.Date(epic$coverage$max), latest_nextpickup, units = "days")),
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
         TRUE ~ 0
      ),
      tgw             = if_else(
         condition = reg_sex == "MALE" & self_identity %in% c("FEMALE", "OTHERS"),
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
      tat_confirm_art = floor(difftime(artstart_date, confirm_date, units = "days")),
      `DISAG 2`       = case_when(
         tat_confirm_art < 0 ~ "0) Tx before dx",
         tat_confirm_art == 0 ~ "1) Same day",
         tat_confirm_art >= 1 & tat_confirm_art <= 7 ~ "2) 1 - 7 days",
         tat_confirm_art >= 8 & tat_confirm_art <= 14 ~ "3) 8 - 14 days",
         tat_confirm_art >= 15 & tat_confirm_art <= 30 ~ "4) 15 - 30 days",
         tat_confirm_art >= 31 ~ "5) More than 30 days",
         is.na(confirm_date) & !is.na(transmit) ~ "6) More than 30 days",
         TRUE ~ "(no confirm date)",
      ),
      `DISAG 3`       = NA_character_
   ) %>%
   left_join(
      y  = ohasis$ref_faci_code %>%
         select(
            FACI_ID,
            realhub             = FACI_CODE,
            realhub_branch      = SUB_FACI_CODE,
            `Site/Organization` = FACI_NAME,
            `Site City`         = FACI_NAME_MUNC,
            `Site Province`     = FACI_NAME_PROV,
            `Site Region`       = FACI_NAME_REG,
         ) %>%
         distinct_all(),
      by = c("realhub", "realhub_branch")
   ) %>%
   left_join(
      y  = epic$sites %>%
         select(
            FACI_ID,
            site_epic_2021,
            site_epic_2022
         ) %>%
         distinct_all(),
      by = "FACI_ID"
   )