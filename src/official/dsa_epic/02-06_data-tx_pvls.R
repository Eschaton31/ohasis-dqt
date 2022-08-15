##  prepare dataset for TX_CURR ------------------------------------------------

epic$linelist$tx_pvls <- epic$data$tx %>%
   filter(tx_pvls == 1) %>%
   mutate(
      # disaggregations
      `DISAG 2` = "Routined",
      `DISAG 3` = case_when(
         vlp12m == 1 & is.na(baseline_vl) ~ "<1,000 copies/ml",
         vlp12m == 0 & is.na(baseline_vl) ~ ">=1,000 copies/ml",
      ),
   )