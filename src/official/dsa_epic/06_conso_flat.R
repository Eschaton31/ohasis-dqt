for (ind in names(epic$linelist)) {
   ind_name              <- toupper(ind)
   epic$flat[[ind_name]] <- epic$linelist[[ind]] %>%
      filter(site_epic_2022 == 1) %>%
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
            false     = `Site Province`
         )
      ) %>%
      group_by(
         `Site/Organization`,
         `KP Population`,
         Sex,
         Age_Band,
         `DISAG 2`,
         `DISAG 3`,
         `Site City`,
         `Site Province`,
         `Site Region`,
         `DATIM Age`
      ) %>%
      summarise(
         Value = n()
      ) %>%
      ungroup() %>%
      mutate(
         Quarter              = epic$coverage$fy,
         Date_Start           = epic$coverage$min,
         Date_End             = epic$coverage$max,
         Implementing_Partner = NA_character_,
         External_IP_Name     = NA_character_,
         `Type of Facility`   = NA_character_,
         Indicator            = ind_name,
         `Type of Support`    = "DSD",
      ) %>%
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
         `DISAG 2`,
         `DISAG 3`,
         `KP Population`,
         Age_Band,
         Value,
         Sex,
         `DATIM Age`,
         `Site City`,
         `Site Province`,
         `Site Region`
      )
}