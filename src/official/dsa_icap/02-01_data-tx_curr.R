##  prepare dataset for TX_CURR ------------------------------------------------

icap$linelist$tx_curr <- icap$data$tx %>%
   mutate(
      # disaggregations
      `DISAG 2`      = case_when(
         ffup_to_pickup < 90 ~ "<3 months worth of ARVs",
         ffup_to_pickup >= 90 & ffup_to_pickup < 180 ~ "3-5 months worth of ARVs",
         ffup_to_pickup >= 180 ~ "6 or more months worth of ARVs",
         TRUE ~ "(no data)"
      ),
      `DISAG 3`      = case_when(
         outcome == "dead" ~ "Dead",
         outcome == "stopped" ~ "Stopped",
         onart28 == 1 ~ "On ART",
         onart28 == 0 ~ "LTFU",
         TRUE ~ NA_character_
      )
   )