##  Primary References ---------------------------------------------------------

by_reg           <- list()
by_reg$mo        <- "07"
by_reg$yr        <- "2022"
by_reg$faci_addr <- ohasis$ref_faci_code %>%
   select(
      hub         = FACI_CODE,
      tx_region   = FACI_NHSSS_REG,
      tx_province = FACI_NHSSS_PROV,
      tx_muncity  = FACI_NHSSS_MUNC
   ) %>%
   distinct(hub, .keep_all = TRUE)
by_reg$all_dx    <- read_dta("H:/_R/library/hiv_full/data/20220913_harp_2022-07_ram_noVL.dta") %>%
   mutate(,
      age           = floor(age),
      cur_age       = floor(cur_age),
      Age_Band      = case_when(
         age >= 0 & age < 15 ~ '<15',
         age >= 15 & age < 25 ~ '15-24',
         age >= 25 & age < 35 ~ '25-34',
         age >= 35 & age < 50 ~ '35-49',
         age >= 50 & age < 1000 ~ '50+',
         TRUE ~ '(no data)'
      ),
      Curr_Age_Band = case_when(
         cur_age >= 0 & cur_age < 15 ~ '<15',
         cur_age >= 15 & cur_age < 25 ~ '15-24',
         cur_age >= 25 & cur_age < 35 ~ '25-34',
         cur_age >= 35 & cur_age < 50 ~ '35-49',
         cur_age >= 50 & cur_age < 1000 ~ '50+',
         TRUE ~ '(no data)'
      ),
   ) %>%
   left_join(
      y  = by_reg$faci_addr,
      by = "hub"
   )
by_reg$all_tx    <- read_dta(ohasis$get_data("harp_tx-outcome", by_reg$yr, by_reg$mo)) %>%
   # left_join(
   #    y  = read_dta("H:/_R/library/hiv_tx/data/20220525_reg-art_2022-04.dta",
   #                  col_select = c('art_id', 'artstart_date')),
   #    by = "hub"
   # ) %>%
   # left_join(
   #    y  = by_reg$faci_addr,
   #    by = "hub"
   # ) %>%
   mutate(,
      age           = floor(curr_age),
      Curr_Age_Band = case_when(
         age >= 0 & age < 15 ~ '<15',
         age >= 15 & age < 25 ~ '15-24',
         age >= 25 & age < 35 ~ '25-34',
         age >= 35 & age < 50 ~ '35-49',
         age >= 50 & age < 1000 ~ '50+',
         TRUE ~ '(no data)'
      ),
   ) %>%
   rename(
      tx_region   = tx_reg,
      tx_province = tx_prov,
      tx_muncity  = tx_munc,
   )

by_reg$all_xx <- read_dta(ohasis$get_data("harp_dead", by_reg$yr, by_reg$mo)) %>%
   mutate_if(
      .predicate = is.character,
      ~if_else(. == '', NA_character_, .)
   ) %>%
   mutate(,
      age      = floor(age),
      Age_Band = case_when(
         age >= 0 & age < 15 ~ '<15',
         age >= 15 & age < 25 ~ '15-24',
         age >= 25 & age < 35 ~ '25-34',
         age >= 35 & age < 50 ~ '35-49',
         age >= 50 & age < 1000 ~ '50+',
         TRUE ~ '(no data)'
      ),
   ) %>%
   left_join(
      y  = by_reg$all_dx %>%
         select(idnum, age_dx = age),
      by = "idnum"
   ) %>%
   mutate(
      final_region   = if_else(
         condition = final_muncity == "UNKNOWN" & mort_muncity != "UNKNOWN",
         true      = mort_region,
         false     = final_region,
         missing   = final_region
      ),
      final_province = if_else(
         condition = final_muncity == "UNKNOWN" & mort_muncity != "UNKNOWN",
         true      = mort_province,
         false     = final_province,
         missing   = final_province
      ),
      final_muncity  = if_else(
         condition = final_muncity == "UNKNOWN" & mort_muncity != "UNKNOWN",
         true      = mort_muncity,
         false     = final_muncity,
         missing   = final_muncity
      ),
      final_region   = if_else(
         condition = is.na(final_muncity) & mort_muncity != "UNKNOWN",
         true      = mort_region,
         false     = final_region,
         missing   = final_region
      ),
      final_province = if_else(
         condition = is.na(final_muncity) & mort_muncity != "UNKNOWN",
         true      = mort_province,
         false     = final_province,
         missing   = final_province
      ),
      final_muncity  = if_else(
         condition = is.na(final_muncity) & mort_muncity != "UNKNOWN",
         true      = mort_muncity,
         false     = final_muncity,
         missing   = final_muncity
      ),
      final_region   = if_else(
         condition = final_muncity == "UNKNOWN" & muncity != "UNKNOWN",
         true      = region,
         false     = final_region,
         missing   = final_region
      ),
      final_province = if_else(
         condition = final_muncity == "UNKNOWN" & muncity != "UNKNOWN",
         true      = province,
         false     = final_province,
         missing   = final_province
      ),
      final_muncity  = if_else(
         condition = final_muncity == "UNKNOWN" & muncity != "UNKNOWN",
         true      = muncity,
         false     = final_muncity,
         missing   = final_muncity
      ),
   )

by_reg$reg_dx <- read_dta("H:/System/HARP/PAHI/Data Sets/2022.07/regional_2022-07.dta") %>%
   mutate(
      agegrp  = case_when(
         agegrp == 1 ~ '<15',
         agegrp == 2 ~ '15-24',
         agegrp == 3 ~ '25-34',
         agegrp == 4 ~ '35-49',
         agegrp == 5 ~ '50+',
         TRUE ~ '(no data)'
      ),
      cagegrp = case_when(
         cagegrp == 1 ~ '<15',
         cagegrp == 2 ~ '15-24',
         cagegrp == 3 ~ '25-34',
         cagegrp == 4 ~ '35-49',
         cagegrp == 5 ~ '50+',
         TRUE ~ '(no data)'
      ),
   )
by_reg$reg_tx <- read_dta("H:/System/HARP/PAHI/Data Sets/2022.07/on_art_reg_2022-07.dta") %>%
   # left_join(
   #    y  = by_reg$faci_addr %>%
   #       select(hub, oh_txreg = tx_region, oh_txprov = tx_province, oh_txmunc = tx_muncity),
   #    by = "hub"
   # ) %>%
   mutate(
      cagegrp = case_when(
         cagegrp == 1 ~ '<15',
         cagegrp == 2 ~ '15-24',
         cagegrp == 3 ~ '25-34',
         cagegrp == 4 ~ '35-49',
         cagegrp == 5 ~ '50+',
         TRUE ~ '(no data)'
      ),
   ) %>%
   rename(
      tx_region   = tx_reg,
      tx_province = tx_prov,
      tx_muncity  = tx_munc,
   )
by_reg$reg_xx <- read_dta("H:/System/HARP/PAHI/Data Sets/2022.07/mort_reg_2022-07.dta")

by_reg$data    <- c("dx", "tx", "xx")
by_reg$regions <- sort(unique(by_reg$all_dx$region))

##  Compare Total Data ---------------------------------------------------------

for (data in by_reg$data) {
   nrow_all <- nrow(by_reg[[glue::glue("all_{data}")]]) %>%
      underline() %>%
      red()
   nrow_reg <- nrow(by_reg[[glue::glue("reg_{data}")]]) %>%
      underline() %>%
      red()
   if (nrow_all != nrow_reg)
      .log_info("Total not equal for {data}: HARP = {nrow_all} | Regional = {nrow_reg}")
}

##  Compare Per Region Total ---------------------------------------------------

for (reg in by_reg$regions) {
   # dx
   nrow_all <- nrow(by_reg$all_dx %>% filter(region == reg)) %>%
      underline() %>%
      red()
   nrow_reg <- nrow(by_reg$reg_dx %>% filter(region == reg)) %>%
      underline() %>%
      red()
   if (nrow_all != nrow_reg)
      .log_info("Dx => {underline(green(reg))}: HARP = {nrow_all} | Regional = {nrow_reg}")

   # tx
   nrow_all <- nrow(by_reg$all_tx %>% filter(tx_region == reg)) %>%
      underline() %>%
      red()
   nrow_reg <- nrow(by_reg$reg_tx %>% filter(tx_region == reg)) %>%
      underline() %>%
      red()
   if (nrow_all != nrow_reg)
      .log_info("Tx => {underline(green(reg))}: HARP = {nrow_all} | Regional = {nrow_reg}")

   # xx
   nrow_all <- nrow(by_reg$all_xx %>% filter(final_region == reg)) %>%
      underline() %>%
      red()
   nrow_reg <- nrow(by_reg$reg_xx %>% filter(death_reg == reg)) %>%
      underline() %>%
      red()
   if (nrow_all != nrow_reg)
      .log_info("Mort => {underline(green(reg))}: HARP = {nrow_all} | Regional = {nrow_reg}")
}

##  Compare Per Region Disag ---------------------------------------------------

disag_dx <- list(
   c("agegrp", "Age_Band"),
   c("cagegrp", "Curr_Age_Band"),
   c("sex", "sex"),
   c("province", "province"),
   c("muncity", "muncity"),
   c("dx_region", "dx_region"),
   c("dx_province", "dx_province"),
   c("dx_muncity", "dx_muncity"),
   c("class", "class")
)

disag_tx <- list(
   c("cagegrp", "Curr_Age_Band"),
   c("tx_province", "tx_province"),
   c("tx_muncity", "tx_muncity")
)

disag_xx <- list(
   c("agegrp", "Age_Band"),
   c("age_dx", "age_dx"),
   c("age_mort", "age"),
   c("age_death", "age_death"),
   c("sex", "sex"),
   c("province", "province"),
   c("muncity", "muncity"),
   c("death_reg", "final_region"),
   c("death_prov", "final_province"),
   c("death_muncity", "final_muncity")
)

for (reg in by_reg$regions) {
   # dx
   for (i in length(disag_dx)) {
      all_var <- disag_dx[[i]][2] %>% as.symbol()
      reg_var <- disag_dx[[i]][1] %>% as.symbol()

      mv_all <- disag_dx[[i]][2]
      mv_reg <- disag_dx[[i]][1]

      match_all <- by_reg$all_dx %>%
         filter(region == reg) %>%
         group_by(!!all_var) %>%
         summarise(Total = n()) %>%
         rename_at(
            .vars = vars(mv_all),
            ~mv_reg
         )
      match_reg <- by_reg$reg_dx %>%
         filter(region == reg) %>%
         group_by(!!reg_var) %>%
         summarise(Total = n())

      match <- full_join(
         x  = match_reg,
         y  = match_all,
         by = mv_reg
      )

      match$keep <- 1
      for (i in seq_len(nrow(match))) {
         if (match[i,]$Total.x == ifelse(is.na(match[i,]$Total.y), 0, match[i,]$Total.y))
            match[i,]$keep <- 0
      }
      match <- match %>% filter(keep == 1)
      if (nrow(match) > 0) {
         .log_info("Dx of {green(reg)} mismatch with {underline(red(mv_reg))}.")
         print(match)
      }
   }

   # tx
   for (i in length(disag_tx)) {
      all_var <- disag_tx[[i]][2] %>% as.symbol()
      reg_var <- disag_tx[[i]][1] %>% as.symbol()

      mv_all <- disag_tx[[i]][2]
      mv_reg <- disag_tx[[i]][1]

      match_all <- by_reg$all_tx %>%
         filter(tx_region == reg) %>%
         group_by(!!all_var) %>%
         summarise(Total = n()) %>%
         rename_at(
            .vars = vars(mv_all),
            ~mv_reg
         )
      match_reg <- by_reg$reg_tx %>%
         filter(tx_region == reg) %>%
         group_by(!!reg_var) %>%
         summarise(Total = n())

      match <- full_join(
         x  = match_reg,
         y  = match_all,
         by = mv_reg
      )

      match$keep <- 1
      for (i in seq_len(nrow(match))) {
         if (match[i,]$Total.x == ifelse(is.na(match[i,]$Total.y), 0, match[i,]$Total.y))
            match[i,]$keep <- 0
      }
      match <- match %>% filter(keep == 1)
      if (nrow(match) > 0) {
         .log_info("Tx of {green(reg)} mismatch with {underline(red(mv_reg))}.")
         print(match)
      }
   }

   # xx
   for (i in length(disag_xx)) {
      all_var <- disag_xx[[i]][2] %>% as.symbol()
      reg_var <- disag_xx[[i]][1] %>% as.symbol()

      mv_all <- disag_xx[[i]][2]
      mv_reg <- disag_xx[[i]][1]

      match_all <- by_reg$all_xx %>%
         filter(final_region == reg) %>%
         group_by(!!all_var) %>%
         summarise(Total = n()) %>%
         rename_at(
            .vars = vars(mv_all),
            ~mv_reg
         )
      match_reg <- by_reg$reg_xx %>%
         filter(death_reg == reg) %>%
         group_by(!!reg_var) %>%
         summarise(Total = n())

      match <- full_join(
         x  = match_reg,
         y  = match_all,
         by = mv_reg
      )

      match$keep <- 1
      for (i in seq_len(nrow(match))) {
         if (!is.na(match[i,]$Total.x) && match[i,]$Total.x == ifelse(is.na(match[i,]$Total.y), 0, match[i,]$Total.y))
            match[i,]$keep <- 0
      }
      match <- match %>% filter(keep == 1)
      if (nrow(match) > 0) {
         .log_info("Mort of {green(reg)} mismatch with {underline(red(mv_reg))}.")
         print(match)
      }
   }
}
