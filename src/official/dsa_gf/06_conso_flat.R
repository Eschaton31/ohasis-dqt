gf$flat$hts <- gf$linelist$hts %>%
   mutate(
      `Site/Organization` = case_when(
         stri_detect_fixed(`Site/Organization`, "HASH") ~ "HIV & AIDS Support House (HASH)",
         `Site/Organization` == "GILEAD" ~ "HIV & AIDS Support House (HASH) - GILEAD",
         `Site/Organization` == "TLF Share" ~ "HIV & AIDS Support House (HASH) - TLF Share",
         TRUE ~ `Site/Organization`
      ),
      `Level of support`  = case_when(
         `Site City` == "City of Baguio" ~ "City-wide",
         `Site City` == "City of Puerto Princesa (Capital)" ~ "City-wide",
         `Site City` == "City of Zamboanga" ~ "City-wide",
         `Site City` == "City of Cagayan De Oro (Capital)" ~ "City-wide",
         `Site Province` == "South Cotabato" ~ "Province-wide",
         `Site Province` == "Cebu" ~ "Province-wide",
         `Site Region` == "National Capital Region (NCR)" ~ "Region-wide",
         `Site Region` == "Region VII (Central Visayas)" ~ "Region-wide",
         `Site Region` == "Region XI (Davao Region)" ~ "Region-wide",
         `Site Region` == "Region III (Central Luzon)" ~ "High-burden (Cat A) Cities",
         `Site Region` == "Region IV-A (CALABARZON)" ~ "High-burden (Cat A) Cities",
      ),
      drop                = case_when(
         `Site Region` == "Region IV-B (MIMAROPA)" & `Site City` != "City of Puerto Princesa (Capital)" ~ 1,
         `Site Region` == "Region VII (Central Visayas)" & `Site Province` != "Cebu" ~ 1,
         TRUE ~ 0
      ),
   ) %>%
   filter(site_gf_2022 == 1, drop == 0) %>%
   group_by(
      Date_Start,
      Date_End,
      `Data Source`,
      `Logsheet Subtype`,
      `Level of support`,
      `Site Region`,
      `Site Province`,
      `Site City`,
      `Site/Organization`,
      Sex,
      `Key Population`,
      `Age Group`,
      `DISAG 1`,
      `DISAG 2`,
      `DISAG 3`,
      `DISAG 3.1`,
      `DISAG 4`,
      `DISAG 5`
   ) %>%
   summarise(
      Value = n()
   ) %>%
   ungroup() %>%
   mutate(
      Semester  = gf$coverage$fy,
      Indicator = "HTS"
   ) %>%
   select(
      Semester,
      Date_Start,
      Date_End,
      `Data Source`,
      `Logsheet Subtype`,
      `Site Region`,
      `Site Province`,
      `Site City`,
      `Site/Organization`,
      `Level of support`,
      Indicator,
      Sex,
      `Key Population`,
      `Age Group`,
      `DISAG 1`,
      `DISAG 2`,
      `DISAG 3`,
      `DISAG 3.1`,
      `DISAG 4`,
      `DISAG 5`,
      Value
   )

gf$flat$kp6a <- gf$linelist$kp6a %>%
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
         `Site/Organization` == "GILEAD" ~ "HIV & AIDS Support House (HASH) - GILEAD",
         `Site/Organization` == "TLF Share" ~ "HIV & AIDS Support House (HASH) - TLF Share",
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
      ),
      `Level of support`  = case_when(
         `Site City` == "City of Baguio" ~ "City-wide",
         `Site City` == "City of Puerto Princesa (Capital)" ~ "City-wide",
         `Site City` == "City of Zamboanga" ~ "City-wide",
         `Site City` == "City of Cagayan De Oro (Capital)" ~ "City-wide",
         `Site Province` == "South Cotabato" ~ "Province-wide",
         `Site Province` == "Cebu" ~ "Province-wide",
         `Site Region` == "National Capital Region (NCR)" ~ "Region-wide",
         `Site Region` == "Region VII (Central Visayas)" ~ "Region-wide",
         `Site Region` == "Region XI (Davao Region)" ~ "Region-wide",
         `Site Region` == "Region III (Central Luzon)" ~ "High-burden (Cat A) Cities",
         `Site Region` == "Region IV-A (CALABARZON)" ~ "High-burden (Cat A) Cities",
      )
   ) %>%
   filter(site_gf_2022 == 1) %>%
   group_by(
      `Data Source`,
      `Logsheet Subtype`,
      `Level of support`,
      `Site Region`,
      `Site Province`,
      `Site City`,
      `Site/Organization`,
      Sex,
      `Key Population`,
      `Age Group`,
      `DISAG 1`,
      `DISAG 2`,
      `DISAG 3`,
      `DISAG 4`,
      `DISAG 5`
   ) %>%
   summarise(
      Value = n()
   ) %>%
   ungroup() %>%
   mutate(
      Semester   = gf$coverage$fy,
      Date_Start = gf$coverage$min,
      Date_End   = gf$coverage$max,
      Indicator  = "KP-6a"
   ) %>%
   select(
      Semester,
      Date_Start,
      Date_End,
      `Data Source`,
      `Logsheet Subtype`,
      `Site Region`,
      `Site Province`,
      `Site City`,
      `Site/Organization`,
      `Level of support`,
      Indicator,
      Sex,
      `Key Population`,
      `Age Group`,
      `DISAG 1`,
      `DISAG 2`,
      `DISAG 3`,
      `DISAG 4`,
      `DISAG 5`,
      Value
   )