##  prepare dataset for TX_CURR ------------------------------------------------

epic$linelist$tx_ml <- epic$data$tx %>%
   filter(tx_ml == 1) %>%
   mutate(
      # disaggregations
      `DISAG 2` = case_when(
         outcome == "dead" ~ "Died",
         outcome == "stopped" ~ "Stopped",
         days_before_ml < 90 ~ "On ART when LTFU (<3 months)",
         days_before_ml %in% seq(90, 179) ~ "On ART when LTFU (3-5 months)",
         days_before_ml >= 180 ~ "On ART when LTFU (>=6 months)",
      ),
      `DISAG 3` = NA_character_
   )