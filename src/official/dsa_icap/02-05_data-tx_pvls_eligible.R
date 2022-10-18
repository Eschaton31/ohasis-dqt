##  prepare dataset for TX_CURR ------------------------------------------------

icap$linelist$tx_pvls_eligible <- icap$data$tx %>%
   filter(tx_pvls_eligible == 1) %>%
   mutate(
      # disaggregations
      `DISAG 2` = NA_character_,
      `DISAG 3` = NA_character_
   )