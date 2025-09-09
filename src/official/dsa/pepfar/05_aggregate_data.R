##  prepare dataset for kp_prev & hts indicators -------------------------------

pepfar_disagg <- function(linelist, coverage) {
   foragg         <- list()
   foragg$tx_curr <- linelist$tx %>%
      filter(tx_curr == 1) %>%
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

   foragg$tx_new <- linelist$tx %>%
      filter(tx_new == 1) %>%
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

   foragg$tx_ml <- linelist$tx %>%
      filter(tx_ml == 1) %>%
      mutate(
         # disaggregations
         `DISAG 2` = case_when(
            outcome == "dead" ~ "Died",
            outcome == "stopped" ~ "Stopped",
            days_before_ml < 90 ~ "On ART when ltfu (<3 months)",
            days_before_ml %in% seq(90, 179) ~ "On ART when ltfu (3-5 months)",
            days_before_ml >= 180 ~ "On ART when ltfu (>=6 months)",
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

   foragg$tx_rtt <- linelist$tx %>%
      filter(tx_rtt == 1) %>%
      mutate(
         # disaggregations
         `DISAG 2` = case_when(
            days_before_rtt < 90 ~ "iit (ART <3 months)",
            days_before_rtt %in% seq(90, 179) ~ "iit (ART 3-5 months)",
            days_before_rtt >= 180 ~ "iit (ART >=6 months)",
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

   foragg$tx_pvls_eligible <- linelist$tx %>%
      filter(tx_pvls_eligible == 1) %>%
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

   foragg$tx_pvls <- linelist$tx %>%
      filter(tx_pvls == 1) %>%
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

   foragg$prep_offer <- linelist$reach %>%
      filter(prep_offer == 1) %>%
      arrange(hts_date) %>%
      distinct(central_id, .keep_all = TRUE) %>%
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

   foragg$prep_screen <- linelist$prep %>%
      filter(prep_screen == 1) %>%
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

   foragg$prep_elig <- linelist$prep %>%
      filter(prep_elig == 1) %>%
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

   foragg$prep_ineligible <- linelist$prep %>%
      filter(prep_ineligible == 1) %>%
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

   foragg$prep_curr <- linelist$prep %>%
      filter(prep_curr == 1) %>%
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

   foragg$prep_ct <- linelist$prep %>%
      filter(prep_ct == 1) %>%
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

   foragg$prep_new <- linelist$prep %>%
      filter(prep_new == 1) %>%
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

   foragg$kp_prev <- linelist$reach %>%
      filter(hts_tst == 1) %>%
      arrange(hts_date) %>%
      distinct(central_id, .keep_all = TRUE) %>%
      mutate(
         # disaggregations
         `DISAG 3` = case_when(
            old_dx == 1 ~ "Known Pos",
            hts_modality == "REACH" ~ "Reach (Not offered tesing)",
            screen_agreed == "0" ~ "Declined Testing",
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

   foragg$hts_tst <- linelist$reach %>%
      filter(hts_tst == 1) %>%
      arrange(hts_priority, hts_date) %>%
      distinct(central_id, .keep_all = TRUE) %>%
      mutate(
         # disaggregations
         `DISAG 2` = case_when(
            hts_modality == "CBS" ~ "cbo (cbs)",
            hts_modality == "ST" ~ "Self-Testing",
            TRUE ~ "Facility (walk-in)"
         ),
         `DISAG 3` = case_when(
            old_dx == 1 & !is.na(confirm_result) ~ "Confirmed: Known Pos",
            old_dx == 1 & hts_modality == "FBT" ~ "Tested: Known Pos",
            old_dx == 1 & hts_modality == "FBS" ~ "Tested: Known Pos",
            old_dx == 1 & hts_modality == "CBS" ~ "CBS: Known Pos",
            old_dx == 1 & hts_modality == "ST" ~ "Self-Testing: Known Pos",
            TRUE ~ final_test_result
         ),
         `DISAG 4` = if_else(
            reach_ssnt == "1_Yes",
            "Reached via SSNT",
            "",
            ""
         ),
         `DISAG 5` = if_else(
            reach_index_testing == "1_Yes",
            "Index Testing",
            "",
            ""
         ),
         `DISAG 6` = if_else(
            reach_online == "1_Yes",
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

   foragg$hts_tst_verify <- linelist$reach %>%
      filter(hts_tst_verify == 1) %>%
      arrange(hts_date) %>%
      distinct(central_id, .keep_all = TRUE) %>%
      mutate(
         # disaggregations
         `DISAG 2` = case_when(
            final_confirm_date >= hts_date ~ "Confirmed Positive",
            final_confirm_date < hts_date ~ "Known Pos",
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

   foragg$tx_new_verify <- linelist$reach %>%
      filter(tx_new_verify == 1) %>%
      arrange(hts_date) %>%
      distinct(central_id, .keep_all = TRUE) %>%
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