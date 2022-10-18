##  prepare dataset for TX_CURR ------------------------------------------------

icap$linelist$tx_new <- icap$data$tx %>%
   filter(tx_new == 1) %>%
   mutate(
      # disaggregations
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
   )