##  prepare dataset for TX_CURR ------------------------------------------------

epic$linelist$tx_ml <- epic$harp$tx$old_outcome %>%
   mutate(
      # tag based on EpiC Defintion
      onart28 = floor(difftime(as.Date(epic$coverage$min) - 1, latest_nextpickup, units = "days")),
      onart28 = if_else(
         condition = onart28 <= 28 & outcome != "dead",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
   ) %>%
   filter(onart28 == 1) %>%
   select(art_id) %>%
   # get updated outcomes
   left_join(
      y  = epic$harp$tx$new_reg %>%
         select(
            CENTRAL_ID,
            art_id,
            first,
            middle,
            last,
            suffix,
            confirmatory_code,
            uic,
            px_code
         ),
      by = "art_id"
   ) %>%
   inner_join(
      y  = epic$harp$tx$new_outcome %>%
         rename(
            art_sex = sex
         ) %>%
         mutate(
            # tag based on EpiC Defintion
            onart28 = floor(difftime(as.Date(epic$coverage$max), latest_nextpickup, units = "days")),
            onart28 = if_else(
               condition = onart28 <= 28 & outcome != "dead",
               true      = 1,
               false     = 0,
               missing   = 0
            ),
         ) %>%
         filter(onart28 == 0) %>%
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
      days_before_ml  = floor(difftime(latest_nextpickup, artstart_date, units = "days")),
      `DISAG 2`       = case_when(
         outcome == "dead" ~ "Died",
         days_before_ml < 90 ~ "On ART when LTFU (<3 months)",
         days_before_ml %in% seq(90, 179) ~ "On ART when LTFU (3-5 months)",
         days_before_ml >= 180 ~ "On ART when LTFU (>=6 months)",
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