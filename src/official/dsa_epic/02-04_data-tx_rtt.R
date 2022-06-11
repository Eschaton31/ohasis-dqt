##  prepare dataset for TX_CURR ------------------------------------------------

epic$linelist$tx_rtt <- epic$data$tx %>%
   filter(tx_rtt == 1) %>%
   mutate(
      # disaggregations
      `DISAG 2` = case_when(
         days_before_rtt < 90 ~ "IIT (ART <3 months)",
         days_before_rtt %in% seq(90, 179) ~ "IIT (ART 3-5 months)",
         days_before_rtt >= 180 ~ "IIT (ART >=6 months)",
      ),
      `DISAG 3` = NA_character_
   )