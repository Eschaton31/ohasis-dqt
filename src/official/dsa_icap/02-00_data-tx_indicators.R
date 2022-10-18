##  prepare dataset for TX_CURR ------------------------------------------------

icap$data$tx <- icap$harp$tx$new_reg %>%
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
   ) %>%
   left_join(
      y  = icap$harp$tx$new_outcome %>%
         rename(
            art_sex = sex
         ) %>%
         select(-CENTRAL_ID),
      by = "art_id"
   ) %>%
   # ML & RTT data
   left_join(
      y  = icap$harp$tx$old_outcome %>%
         mutate(
            # tag based on ICAP Defintion
            onart28 = floor(difftime(as.Date(icap$coverage$min) - 1, latest_nextpickup, units = "days")),
            onart28 = if_else(
               condition = onart28 <= 28 & outcome != "dead",
               true      = 1,
               false     = 0,
               missing   = 0
            ),
         ) %>%
         select(
            art_id,
            ltfu_date    = latest_nextpickup,
            prev_onart28 = onart28
         ),
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
      days             = floor(difftime(as.Date(icap$coverage$max), latest_nextpickup, units = "days")),
      onart28          = if_else(
         condition = days <= 28 & outcome != "dead",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      vl_elig          = if_else(
         condition = icap$coverage$type == "QR" & (interval(artstart_date, icap$coverage$max) / months(1)) >= 3,
         true      = 1,
         false     = 0,
         missing   = 0
      ),

      # tag specific indicators
      tx_curr          = if_else(
         condition = onart28 == 1,
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      tx_new           = if_else(
         condition = newonart == 1,
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      tx_ml            = if_else(
         condition = prev_onart28 == 1 & onart28 == 0,
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      tx_rtt           = if_else(
         condition = prev_onart28 == 0 & onart28 == 1,
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      tx_pvls_eligible = if (icap$coverage$type == "QR") {
         if_else(
            condition = onart28 == 1 & vl_elig == 1,
            true      = 1,
            false     = 0,
            missing   = 0
         )
      } else {
         0
      },
      tx_pvls          = if (icap$coverage$type == "QR") {
         if_else(
            condition = onart28 == 1 &
               is.na(baseline_vl) &
               !is.na(vlp12m),
            true      = 1,
            false     = 0,
            missing   = 0
         )
      } else {
         0
      },

      # sex variable (use registry if available)
      Sex              = if_else(
         condition = !is.na(transmit) & !is.na(reg_sex),
         true      = StrLeft(reg_sex, 1),
         false     = StrLeft(art_sex, 1),
         missing   = StrLeft(art_sex, 1)
      ),

      # KAP
      msm              = case_when(
         reg_sex == "MALE" & sexhow %in% c("BISEXUAL", "HOMOSEXUAL") ~ 1,
         reg_sex == "MALE" & sexhow == "HETEROSEXUAL" ~ 0,
         TRUE ~ 0
      ),
      tgw              = if_else(
         condition = reg_sex == "MALE" & self_identity %in% c("FEMALE", "OTHERS"),
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
      curr_age         = floor(curr_age),
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

      ffup_to_pickup   = floor(difftime(latest_nextpickup, latest_ffupdate, units = "days")),
      tat_confirm_art  = floor(difftime(artstart_date, confirm_date, units = "days")),
      days_before_ml   = floor(difftime(latest_nextpickup, artstart_date, units = "days")),
      days_before_rtt  = floor(difftime(latest_ffupdate, ltfu_date, units = "days")),
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
      y  = icap$sites %>%
         select(
            FACI_ID,
            site_icap_2021,
            site_icap_2022
         ) %>%
         distinct_all(),
      by = "FACI_ID"
   )