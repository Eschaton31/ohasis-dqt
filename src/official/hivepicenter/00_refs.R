epictr$ref_addr <- ohasis$ref_addr %>%
   mutate(
      drop = case_when(
         StrLeft(PSGC_PROV, 4) == "1339" & (PSGC_MUNC != "133900000" | is.na(PSGC_MUNC)) ~ 1,
         StrLeft(PSGC_REG, 4) == "1300" & PSGC_MUNC == "" ~ 1,
         stri_detect_fixed(NAME_PROV, "City") & NHSSS_MUNC == "UNKNOWN" ~ 1,
         TRUE ~ 0
      )
   ) %>%
   filter(drop == 0)

epictr$ref_faci <- ohasis$ref_faci_code %>%
   mutate(
      FACI_CODE     = case_when(
         stri_detect_regex(SUB_FACI_CODE, "^HASH") ~ "HASH",
         stri_detect_regex(SUB_FACI_CODE, "^SAIL") ~ "SAIL",
         stri_detect_regex(SUB_FACI_CODE, "^TLY") ~ "TLY",
         TRUE ~ FACI_CODE
      ),
      SUB_FACI_CODE = if_else(
         condition = nchar(SUB_FACI_CODE) == 3,
         true      = NA_character_,
         false     = SUB_FACI_CODE
      ),
      SUB_FACI_CODE = case_when(
         FACI_CODE == "HASH" & is.na(SUB_FACI_CODE) ~ "HASH-QC",
         FACI_CODE == "TLY" & is.na(SUB_FACI_CODE) ~ "TLY-ANGLO",
         FACI_CODE == "SHP" & is.na(SUB_FACI_CODE) ~ "SHIP-MAKATI",
         TRUE ~ SUB_FACI_CODE
      ),
   ) %>%
   select(
      FACI_ID,
      final_hub    = FACI_CODE,
      final_branch = SUB_FACI_CODE,
      TX_FACI      = FACI_NAME,
      TX_PSGC_REG  = FACI_PSGC_REG,
      TX_PSGC_PROV = FACI_PSGC_PROV,
      TX_PSGC_MUNC = FACI_PSGC_MUNC,
   ) %>%
   distinct_all()
