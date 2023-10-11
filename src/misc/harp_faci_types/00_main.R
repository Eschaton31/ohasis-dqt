
##  tx faci types --------------------------------------------------------------

lw_conn  <- ohasis$conn("lw")
form_art <- dbxSelect(lw_conn, "SELECT REC_ID, MEDICINE_SUMMARY FROM ohasis_warehouse.form_art_bc WHERE REC_ID IN (?)", params = list(tx$REC_ID))
dbDisconnect(lw_conn)
tx <- read_dta(hs_data("harp_tx", "outcome", 2023, 5)) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(FACI_ID = "realhub", SUB_FACI_ID = "realhub_branch")
   ) %>%
   left_join(ohasis$ref_faci, join_by(FACI_ID, SUB_FACI_ID)) %>%
   ohasis$get_faci(
      list(TX_HUB = c("FACI_ID", "SUB_FACI_ID")),
      "name",
   )

tx <- read_dta(hs_data("harp_tx", "reg", 2023, 6)) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(FACI_ID = "artstart_hub", SUB_FACI_ID = "artstart_branch")
   ) %>%
   left_join(ohasis$ref_faci, join_by(FACI_ID, SUB_FACI_ID)) %>%
   ohasis$get_faci(
      list(TX_HUB = c("FACI_ID", "SUB_FACI_ID")),
      "name",
   ) %>%
   select(-REC_ID) %>%
   filter(year(artstart_date) == 2023) %>%
   left_join(
      y = read_dta(hs_data("harp_tx", "reg", 2023, 6), col_select = c(art_id, REC_ID)),
      join_by(art_id)
   ) %>%
   left_join(
      y = form_art %>%
         rename(artstart_regimen = MEDICINE_SUMMARY),
      join_by(REC_ID)
   )

tx %>%
   mutate(
      faci_type = case_when(
         FACI_TYPE == "NGO" ~ "NGO",
         FACI_TYPE == "CBO" ~ "CBO",
         artstart_hub == "TDF" ~ "CBO",
         artstart_hub %in% c("DLS", "SDD", "SLQ") ~ "Private hospital",
         artstart_hub == "RDJ" ~ "Private clinic",
         artstart_hub == "MBK" ~ "SHC",
         artstart_hub %in% c("CBS", "BAG", "IMU", "MEY", "OCH", "ROD", "SVE", "SRS", "TAN", "BIL") ~ "SHC",
         artstart_hub %in% c("AVL", "CSH", "LCP", "RIT", "SJD") ~ "Public hospital",
         PUBPRIV == "PUBLIC" & stri_detect_fixed(toupper(TX_HUB), "OSPITAL") ~ "Public hospital",
         PUBPRIV == "PRIVATE" & stri_detect_fixed(toupper(TX_HUB), "OSPITAL") ~ "Private hospital",
         PUBPRIV == "PUBLIC" & stri_detect_fixed(toupper(TX_HUB), "MEDICAL CENTER") ~ "Public hospital",
         PUBPRIV == "PRIVATE" & stri_detect_fixed(toupper(TX_HUB), "MEDICAL CENTER") ~ "Private hospital",
         PUBPRIV == "PUBLIC" & stri_detect_fixed(toupper(TX_HUB), "MEDICAL CITY") ~ "Public hospital",
         PUBPRIV == "PRIVATE" & stri_detect_fixed(toupper(TX_HUB), "MEDICAL CITY") ~ "Private hospital",
         str_detect(toupper(TX_HUB), "CITY HEALTH OFFICE") ~ "SHC",
         str_detect(toupper(TX_HUB), "RURAL HEALTH UNIT") ~ "SHC",
         str_detect(toupper(TX_HUB), "RHU") ~ "SHC",
         str_detect(toupper(TX_HUB), "^KLINIKA") ~ "SHC",
         str_detect(toupper(TX_HUB), "SOCIAL HYGIENE CLINIC") ~ "SHC",
         str_detect(toupper(TX_HUB), "SHC") ~ "SHC",
         str_detect(toupper(TX_HUB), "REPRODUCTIVE HEALTH AND WELLNESS") ~ "SHC",
         str_detect(toupper(TX_HUB), "RHWC") ~ "SHC",
         str_detect(toupper(TX_HUB), "LOVEYOURSELF") ~ "LY",
         str_detect(toupper(TX_HUB), "COMMUNITY CENTER") ~ "CBO",
         str_detect(toupper(TX_HUB), "LAKAN") ~ "CBO",
         str_detect(toupper(TX_HUB), "MEDICAL CLINIC") ~ "Private clinic",
         stri_detect_fixed(toupper(TX_HUB), "(SHIP)") ~ "Private clinic",
         stri_detect_fixed(toupper(TX_HUB), "(SAIL)") ~ "CBO",
         stri_detect_fixed(toupper(TX_HUB), "(FPOP)") ~ "CBO",
      )
   ) %>%
   select(
      art_id,
      artstart_regimen   = artstart_regimen.y,
      artstart_faci_type = faci_type,
   ) %>%
   write_dta("H:/20230727_reg-art_2023-06_mod2_artstart_arv.dta")

full <- read_dta(hs_data("harp_full", "reg", 2023, 6)) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(FACI_ID = "realhub", SUB_FACI_ID = "realhub_branch")
   ) %>%
   left_join(ohasis$ref_faci, join_by(FACI_ID, SUB_FACI_ID)) %>%
   ohasis$get_faci(
      list(TX_HUB = c("FACI_ID", "SUB_FACI_ID")),
      "name",
   )

full %>%
   mutate(
      faci_type                 = case_when(
         FACI_TYPE == "NGO" ~ "NGO",
         FACI_TYPE == "CBO" ~ "CBO",
         realhub == "TDF" ~ "CBO",
         realhub %in% c("DLS", "SDD", "SLQ") ~ "Private hospital",
         realhub == "RDJ" ~ "Private clinic",
         realhub == "MBK" ~ "SHC",
         realhub %in% c("CBS", "BAG", "IMU", "MEY", "OCH", "ROD", "SVE", "SRS", "TAN", "BIL") ~ "SHC",
         realhub %in% c("AVL", "CSH", "LCP", "RIT", "SJD") ~ "Public hospital",
         PUBPRIV == "PUBLIC" & stri_detect_fixed(toupper(TX_HUB), "OSPITAL") ~ "Public hospital",
         PUBPRIV == "PRIVATE" & stri_detect_fixed(toupper(TX_HUB), "OSPITAL") ~ "Private hospital",
         PUBPRIV == "PUBLIC" & stri_detect_fixed(toupper(TX_HUB), "MEDICAL CENTER") ~ "Public hospital",
         PUBPRIV == "PRIVATE" & stri_detect_fixed(toupper(TX_HUB), "MEDICAL CENTER") ~ "Private hospital",
         PUBPRIV == "PUBLIC" & stri_detect_fixed(toupper(TX_HUB), "MEDICAL CITY") ~ "Public hospital",
         PUBPRIV == "PRIVATE" & stri_detect_fixed(toupper(TX_HUB), "MEDICAL CITY") ~ "Private hospital",
         str_detect(toupper(TX_HUB), "CITY HEALTH OFFICE") ~ "SHC",
         str_detect(toupper(TX_HUB), "RURAL HEALTH UNIT") ~ "SHC",
         str_detect(toupper(TX_HUB), "RHU") ~ "SHC",
         str_detect(toupper(TX_HUB), "^KLINIKA") ~ "SHC",
         str_detect(toupper(TX_HUB), "SOCIAL HYGIENE CLINIC") ~ "SHC",
         str_detect(toupper(TX_HUB), "SHC") ~ "SHC",
         str_detect(toupper(TX_HUB), "REPRODUCTIVE HEALTH AND WELLNESS") ~ "SHC",
         str_detect(toupper(TX_HUB), "RHWC") ~ "SHC",
         str_detect(toupper(TX_HUB), "LOVEYOURSELF") ~ "LY",
         str_detect(toupper(TX_HUB), "COMMUNITY CENTER") ~ "CBO",
         str_detect(toupper(TX_HUB), "LAKAN") ~ "CBO",
         str_detect(toupper(TX_HUB), "MEDICAL CLINIC") ~ "Private clinic",
         stri_detect_fixed(toupper(TX_HUB), "(SHIP)") ~ "Private clinic",
         stri_detect_fixed(toupper(TX_HUB), "(SAIL)") ~ "CBO",
         stri_detect_fixed(toupper(TX_HUB), "(FPOP)") ~ "CBO",
      ),
      reactive_date             = coalesce(blood_extract_date, test_date, t0_date, visit_date, confirm_date) %>% as.Date(),
      tat_reactive_confirm      = as.integer(interval(reactive_date, confirm_date) / days(1)),
      tat_reactive_enroll       = as.integer(interval(reactive_date, artstart_date) / days(1)),
      tat_reactive_enroll_crcl  = if_else(!is.na(rhivda_done), tat_reactive_enroll, NA_integer_),
      tat_reactive_enroll_saccl = if_else(is.na(rhivda_done), tat_reactive_enroll, NA_integer_)
   ) %>%
   format_stata() %>%
   write_dta("H:/20230803_harp_2023-06_wVL_dr-faci_type.dta")
