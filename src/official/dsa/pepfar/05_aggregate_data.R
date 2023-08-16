##  prepare dataset for kp_prev & hts indicators -------------------------------

pepfar_disagg <- function(linelist, coverage) {
   foragg         <- list()
   foragg$TX_CURR <- linelist$tx %>%
      filter(TX_CURR == 1) %>%
      mutate(
         # disaggregations
         `DISAG 2` = case_when(
            ffup_to_pickup < 90 ~ "<3 months worth of ARVs",
            ffup_to_pickup >= 90 & ffup_to_pickup < 180 ~ "3-5 months worth of ARVs",
            ffup_to_pickup >= 180 ~ "6 or more months worth of ARVs",
            TRUE ~ "(no data)"
         ),
         `DISAG 3` = case_when(
            outcome == "dead" ~ "Dead",
            outcome == "stopped" ~ "Stopped",
            onart28 == 1 ~ "On ART",
            onart28 == 0 ~ "LTFU",
            TRUE ~ NA_character_
         )
      ) %>%
      select(
         `Site/Organization`,
         `KP Population`,
         Sex,
         Age_Band,
         starts_with("DISAG ", ignore.case = FALSE),
         starts_with("site_", ignore.case = FALSE),
         `Site City`,
         `Site Province`,
         `Site Region`,
         `DATIM Age`,
      )

   foragg$TX_NEW <- linelist$tx %>%
      filter(TX_NEW == 1) %>%
      mutate(
         # disaggregations
         `DISAG 2` = case_when(
            tat_confirm_art < 0 ~ "0) Tx before dx",
            tat_confirm_art == 0 ~ "1) Same day",
            tat_confirm_art >= 1 & tat_confirm_art <= 7 ~ "2) 1 - 7 days",
            tat_confirm_art >= 8 & tat_confirm_art <= 14 ~ "3) 8 - 14 days",
            tat_confirm_art >= 15 & tat_confirm_art <= 30 ~ "4) 15 - 30 days",
            tat_confirm_art >= 31 ~ "5) More than 30 days",
            is.na(confirm_date) & !is.na(transmit) ~ "6) More than 30 days",
            TRUE ~ "(no confirm date)",
         ),
      ) %>%
      select(
         `Site/Organization`,
         `KP Population`,
         Sex,
         Age_Band,
         starts_with("DISAG ", ignore.case = FALSE),
         starts_with("site_", ignore.case = FALSE),
         `Site City`,
         `Site Province`,
         `Site Region`,
         `DATIM Age`,
      )

   foragg$TX_ML <- linelist$tx %>%
      filter(TX_ML == 1) %>%
      mutate(
         # disaggregations
         `DISAG 2` = case_when(
            outcome == "dead" ~ "Died",
            outcome == "stopped" ~ "Stopped",
            days_before_ml < 90 ~ "On ART when LTFU (<3 months)",
            days_before_ml %in% seq(90, 179) ~ "On ART when LTFU (3-5 months)",
            days_before_ml >= 180 ~ "On ART when LTFU (>=6 months)",
         ),
      ) %>%
      select(
         `Site/Organization`,
         `KP Population`,
         Sex,
         Age_Band,
         starts_with("DISAG ", ignore.case = FALSE),
         starts_with("site_", ignore.case = FALSE),
         `Site City`,
         `Site Province`,
         `Site Region`,
         `DATIM Age`,
      )

   foragg$TX_RTT <- linelist$tx %>%
      filter(TX_RTT == 1) %>%
      mutate(
         # disaggregations
         `DISAG 2` = case_when(
            days_before_rtt < 90 ~ "IIT (ART <3 months)",
            days_before_rtt %in% seq(90, 179) ~ "IIT (ART 3-5 months)",
            days_before_rtt >= 180 ~ "IIT (ART >=6 months)",
         ),
      ) %>%
      select(
         `Site/Organization`,
         `KP Population`,
         Sex,
         Age_Band,
         starts_with("DISAG ", ignore.case = FALSE),
         starts_with("site_", ignore.case = FALSE),
         `Site City`,
         `Site Province`,
         `Site Region`,
         `DATIM Age`,
      )

   foragg$TX_PVLS_ELIGIBLE <- linelist$tx %>%
      filter(TX_PVLS_ELIGIBLE == 1) %>%
      select(
         `Site/Organization`,
         `KP Population`,
         Sex,
         Age_Band,
         starts_with("DISAG ", ignore.case = FALSE),
         starts_with("site_", ignore.case = FALSE),
         `Site City`,
         `Site Province`,
         `Site Region`,
         `DATIM Age`,
      )

   foragg$TX_PVLS <- linelist$tx %>%
      filter(TX_PVLS == 1) %>%
      mutate(
         # disaggregations
         `DISAG 2` = "Routined",
         `DISAG 3` = case_when(
            vlp12m == 1 &
               is.na(baseline_vl) &
               vl_result < 1000 ~ "<1,000 copies/ml",
            vlp12m == 0 &
               is.na(baseline_vl) &
               vl_result >= 1000 ~ ">=1,000 copies/ml",
         ),
      ) %>%
      select(
         `Site/Organization`,
         `KP Population`,
         Sex,
         Age_Band,
         starts_with("DISAG ", ignore.case = FALSE),
         starts_with("site_", ignore.case = FALSE),
         `Site City`,
         `Site Province`,
         `Site Region`,
         `DATIM Age`,
      )

   foragg$PREP_OFFER <- linelist$reach %>%
      filter(PREP_OFFER == 1) %>%
      arrange(hts_date) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      select(
         `Site/Organization`,
         `KP Population`,
         Sex,
         Age_Band,
         starts_with("DISAG ", ignore.case = FALSE),
         starts_with("site_", ignore.case = FALSE),
         `Site City`,
         `Site Province`,
         `Site Region`,
         `DATIM Age`,
      )

   foragg$PREP_SCREEN <- linelist$prep %>%
      filter(PREP_SCREEN == 1) %>%
      select(
         `Site/Organization`,
         `KP Population`,
         Sex,
         Age_Band,
         starts_with("DISAG ", ignore.case = FALSE),
         starts_with("site_", ignore.case = FALSE),
         `Site City`,
         `Site Province`,
         `Site Region`,
         `DATIM Age`,
      )

   foragg$PREP_ELIG <- linelist$prep %>%
      filter(PREP_ELIG == 1) %>%
      select(
         `Site/Organization`,
         `KP Population`,
         Sex,
         Age_Band,
         starts_with("DISAG ", ignore.case = FALSE),
         starts_with("site_", ignore.case = FALSE),
         `Site City`,
         `Site Province`,
         `Site Region`,
         `DATIM Age`,
      )

   foragg$PREP_INELIGIBLE <- linelist$prep %>%
      filter(PREP_INELIGIBLE == 1) %>%
      select(
         `Site/Organization`,
         `KP Population`,
         Sex,
         Age_Band,
         starts_with("DISAG ", ignore.case = FALSE),
         starts_with("site_", ignore.case = FALSE),
         `Site City`,
         `Site Province`,
         `Site Region`,
         `DATIM Age`,
      )

   foragg$PREP_CURR <- linelist$prep %>%
      filter(PREP_CURR == 1) %>%
      select(
         `Site/Organization`,
         `KP Population`,
         Sex,
         Age_Band,
         starts_with("DISAG ", ignore.case = FALSE),
         starts_with("site_", ignore.case = FALSE),
         `Site City`,
         `Site Province`,
         `Site Region`,
         `DATIM Age`,
      )

   foragg$PREP_CT <- linelist$prep %>%
      filter(PREP_CT == 1) %>%
      select(
         `Site/Organization`,
         `KP Population`,
         Sex,
         Age_Band,
         starts_with("DISAG ", ignore.case = FALSE),
         starts_with("site_", ignore.case = FALSE),
         `Site City`,
         `Site Province`,
         `Site Region`,
         `DATIM Age`,
      )

   foragg$PREP_NEW <- linelist$prep %>%
      filter(PREP_NEW == 1) %>%
      mutate(
         # disaggregations
         `DISAG 2` = if_else(
            condition = prep_plan != "(no data)",
            true      = toupper(prep_plan),
            false     = prep_plan
         ),
      ) %>%
      select(
         `Site/Organization`,
         `KP Population`,
         Sex,
         Age_Band,
         starts_with("DISAG ", ignore.case = FALSE),
         starts_with("site_", ignore.case = FALSE),
         `Site City`,
         `Site Province`,
         `Site Region`,
         `DATIM Age`,
      )

   foragg$KP_PREV <- linelist$reach %>%
      filter(HTS_TST == 1) %>%
      arrange(hts_date) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      mutate(
         # disaggregations
         `DISAG 3` = case_when(
            old_dx == 1 ~ "Known Pos",
            hts_modality == "REACH" ~ "Reach (Not offered tesing)",
            SCREEN_AGREED == "0" ~ "Declined Testing",
            hts_modality != "REACH" ~ "Tested/Referred for Testing",
            TRUE ~ "Reach (Not offered tesing)"
         ),
      ) %>%
      select(
         `Site/Organization`,
         `KP Population`,
         Sex,
         Age_Band,
         starts_with("DISAG ", ignore.case = FALSE),
         starts_with("site_", ignore.case = FALSE),
         `Site City`,
         `Site Province`,
         `Site Region`,
         `DATIM Age`,
      )

   foragg$HTS_TST <- linelist$reach %>%
      filter(HTS_TST == 1) %>%
      arrange(hts_priority, hts_date) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      mutate(
         # disaggregations
         `DISAG 2` = case_when(
            hts_modality == "CBS" ~ "CBO (CBS)",
            hts_modality == "ST" ~ "Self-Testing",
            TRUE ~ "Facility (walk-in)"
         ),
         `DISAG 3` = case_when(
            old_dx == 1 & !is.na(CONFIRM_RESULT) ~ "Confirmed: Known Pos",
            old_dx == 1 & hts_modality == "FBT" ~ "Tested: Known Pos",
            old_dx == 1 & hts_modality == "FBS" ~ "Tested: Known Pos",
            old_dx == 1 & hts_modality == "CBS" ~ "CBS: Known Pos",
            old_dx == 1 & hts_modality == "ST" ~ "Self-Testing: Known Pos",
            TRUE ~ FINAL_TEST_RESULT
         ),
         `DISAG 4` = if_else(
            REACH_SSNT == "1_Yes",
            "Reached via SSNT",
            "",
            ""
         ),
         `DISAG 5` = if_else(
            REACH_INDEX_TESTING == "1_Yes",
            "Index Testing",
            "",
            ""
         ),
         `DISAG 6` = if_else(
            REACH_ONLINE == "1_Yes",
            "Online Reach",
            "",
            ""
         ),
      ) %>%
      select(
         `Site/Organization`,
         `KP Population`,
         Sex,
         Age_Band,
         starts_with("DISAG ", ignore.case = FALSE),
         starts_with("site_", ignore.case = FALSE),
         `Site City`,
         `Site Province`,
         `Site Region`,
         `DATIM Age`,
      )

   foragg$HTS_TST_VERIFY <- linelist$reach %>%
      filter(HTS_TST_VERIFY == 1) %>%
      arrange(hts_date) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      mutate(
         # disaggregations
         `DISAG 2` = case_when(
            FINAL_CONFIRM_DATE >= hts_date ~ "Confirmed Positive",
            FINAL_CONFIRM_DATE < hts_date ~ "Known Pos",
         ),
      ) %>%
      select(
         `Site/Organization`,
         `KP Population`,
         Sex,
         Age_Band,
         starts_with("DISAG ", ignore.case = FALSE),
         starts_with("site_", ignore.case = FALSE),
         `Site City`,
         `Site Province`,
         `Site Region`,
         `DATIM Age`,
      )

   foragg$TX_NEW_VERIFY <- linelist$reach %>%
      filter(TX_NEW_VERIFY == 1) %>%
      arrange(hts_date) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      select(
         `Site/Organization`,
         `KP Population`,
         Sex,
         Age_Band,
         starts_with("DISAG ", ignore.case = FALSE),
         starts_with("site_", ignore.case = FALSE),
         `Site City`,
         `Site Province`,
         `Site Region`,
         `DATIM Age`,
      )

   return(foragg)
}

.init <- function(envir = parent.env(environment())) {
   p <- envir

   p$foragg <- pepfar_disagg(p$linelist, p$coverage)
}