##  prepare dataset for kp_prev & hts indicators -------------------------------

pepfar_flat <- function(foragg, coverage) {
   flat <- lapply(names(foragg), function(ind, data, coverage) {
      cols <- names(data[[ind]])
      flat <- data[[ind]] %>%
         mutate(
            hash_ncr            = case_when(
               `Site/Organization` == "HASH TL - Genesis" ~ 1,
               `Site/Organization` == "HASH TL - Marlon" ~ 1,
               `Site/Organization` == "HASH TL - Ronel" ~ 1,
               `Site/Organization` == "HIV & AIDS Support House (HASH)" ~ 1,
               `Site/Organization` == "HIV & AIDS Support House (HASH) (Calamba)" ~ 1,
               `Site/Organization` == "HIV & AIDS Support House (HASH) (Caloocan)" ~ 1,
               `Site/Organization` == "HIV & AIDS Support House (HASH) (Makati)" ~ 1,
               `Site/Organization` == "HIV & AIDS Support House (HASH) (Quezon City)" ~ 1,
               TRUE ~ 0
            ),
            `Site/Organization` = case_when(
               hash_ncr == 1 ~ "HIV & AIDS Support House (HASH) (NCR)",
               `Site/Organization` == "GILEAD" ~ "HIV & AIDS Support House (HASH) (GILEAD)",
               TRUE ~ `Site/Organization`
            ),
            `Site City`         = if_else(
               condition = hash_ncr == 1,
               true      = "Quezon City",
               false     = `Site City`
            ),
            `Site Province`     = if_else(
               condition = hash_ncr == 1,
               true      = "NCR, Second District (Not a Province)",
               false     = `Site Province`
            ),
            `Site Region`       = if_else(
               condition = hash_ncr == 1,
               true      = "National Capital Region (NCR)",
               false     = `Site Region`
            )
         ) %>%
         group_by(across(all_of(cols))) %>%
         summarise(
            Value = n()
         ) %>%
         ungroup() %>%
         mutate(
            Quarter    = coverage$curr$fy,
            Date_Start = coverage$min,
            Date_End   = coverage$max,
            .before    = `Site/Organization`
         ) %>%
         mutate(
            Implementing_Partner = NA_character_,
            External_IP_Name     = NA_character_,
            `Type of Facility`   = NA_character_,
            Indicator            = ind,
            `Type of Support`    = "DSD",
            .after               = `Site/Organization`
         )

      return(flat)
   }, foragg, coverage)
   flat <- bind_rows(flat) %>%
      select(
         Quarter,
         Date_Start,
         Date_End,
         `Site/Organization`,
         Implementing_Partner,
         External_IP_Name,
         `Type of Facility`,
         Indicator,
         `Type of Support`,
         starts_with("DISAG ", ignore.case = FALSE),
         `KP Population`,
         Age_Band,
         Value,
         Sex,
         `DATIM Age`,
         `Site City`,
         `Site Province`,
         `Site Region`,
         starts_with("site_", ignore.case = FALSE),
      )

   return(flat)
}

.init <- function(envir = parent.env(environment())) {
   p <- envir

   p$flat <- pepfar_flat(p$foragg, p$coverage)
}