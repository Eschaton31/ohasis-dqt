forms22 <- read_rds("H:/20231024_hts-forms_2022.rds")
forms23 <- read_rds("H:/20231024_hts-forms_2023.rds")
id_reg  <- read_rds("H:/20231024_id_reg.rds")

dx <- read_dta(hs_data("harp_dx", "reg", 2023, 8)) %>%
   get_cid(id_reg, PATIENT_ID)
tx <- read_dta(hs_data("harp_tx", "outcome", 2023, 8)) %>%
   get_cid(id_reg, PATIENT_ID)

prep_new  <- read_dta(hs_data("prep", "prepstart", 2023, 8)) %>%
   get_cid(id_reg, PATIENT_ID)
prep_curr <- read_dta(hs_data("prep", "outcome", 2023, 8)) %>%
   get_cid(id_reg, PATIENT_ID)

hts22 <- process_hts(forms22$hts, forms22$a, forms22$cfbs) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   get_latest_pii(
      "CENTRAL_ID",
      c(
         "BIRTHDATE",
         "SEX",
         "UIC",
         "SELF_IDENT",
         "SELF_IDENT_OTHER",
         "CIVIL_STATUS",
         "NATIONALITY",
         "EDUC_LEVEL",
         "CURR_PSGC_REG",
         "CURR_PSGC_PROV",
         "CURR_PSGC_MUNC",
         "PERM_PSGC_REG",
         "PERM_PSGC_PROV",
         "PERM_PSGC_MUNC"
      )
   ) %>%
   convert_hts("nhsss") %>%
   mutate_at(
      .vars = vars(HTS_REG, HTS_PROV, HTS_MUNC, CBS_REG, CBS_PROV, CBS_MUNC),
      ~coalesce(na_if(., "OVERSEAS"), "UNKNOWN")
   ) %>%
   mutate(
      use_addr  = case_when(
         hts_modality %in% c("ST", "CBS", "FBS", "ST") & if_any(c(CBS_REG, CBS_PROV, CBS_MUNC), ~. != "UNKNOWN") ~ "cbs",
         hts_modality %in% c("ST", "CBS", "FBS", "ST") & if_all(c(CBS_REG, CBS_PROV, CBS_MUNC), ~. == "UNKNOWN") ~ "hts",
         if_any(c(HTS_REG, HTS_PROV, HTS_MUNC), ~. != "UNKNOWN") ~ "hts",
         if_any(c(HTS_REG, HTS_PROV, HTS_MUNC), ~. != "UNKNOWN") ~ "hts",
         CBS_PROV != "UNKNOWN" & CBS_MUNC == "UNKNOWN" ~ "cbs",
         HTS_PROV != "UNKNOWN" & HTS_MUNC == "UNKNOWN" ~ "hts",
         CBS_REG != "UNKNOWN" & CBS_PROV == "UNKNOWN" ~ "cbs",
         HTS_REG != "UNKNOWN" & HTS_PROV == "UNKNOWN" ~ "hts",
         if_all(c(HTS_REG, HTS_PROV, HTS_MUNC), ~. == "UNKNOWN") ~ "res"
      ),
      TEST_REG  = case_when(
         use_addr == "cbs" ~ CBS_REG,
         use_addr == "hts" ~ HTS_REG,
      ),
      TEST_PROV = case_when(
         use_addr == "cbs" ~ CBS_PROV,
         use_addr == "hts" ~ HTS_PROV,
      ),
      TEST_MUNC = case_when(
         use_addr == "cbs" ~ CBS_MUNC,
         use_addr == "hts" ~ HTS_MUNC,
      )
   )

hts23 <- process_hts(forms23$hts, forms23$a, forms23$cfbs) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   get_latest_pii(
      "CENTRAL_ID",
      c(
         "BIRTHDATE",
         "SEX",
         "UIC",
         "SELF_IDENT",
         "SELF_IDENT_OTHER",
         "CIVIL_STATUS",
         "NATIONALITY",
         "EDUC_LEVEL",
         "CURR_PSGC_REG",
         "CURR_PSGC_PROV",
         "CURR_PSGC_MUNC",
         "PERM_PSGC_REG",
         "PERM_PSGC_PROV",
         "PERM_PSGC_MUNC"
      )
   ) %>%
   convert_hts("nhsss") %>%
   mutate_at(
      .vars = vars(HTS_REG, HTS_PROV, HTS_MUNC, CBS_REG, CBS_PROV, CBS_MUNC),
      ~coalesce(na_if(., "OVERSEAS"), "UNKNOWN")
   ) %>%
   mutate(
      use_addr  = case_when(
         hts_modality %in% c("ST", "CBS", "FBS", "ST") & if_any(c(CBS_REG, CBS_PROV, CBS_MUNC), ~. != "UNKNOWN") ~ "cbs",
         hts_modality %in% c("ST", "CBS", "FBS", "ST") & if_all(c(CBS_REG, CBS_PROV, CBS_MUNC), ~. == "UNKNOWN") ~ "hts",
         if_any(c(HTS_REG, HTS_PROV, HTS_MUNC), ~. != "UNKNOWN") ~ "hts",
         CBS_PROV != "UNKNOWN" & CBS_MUNC == "UNKNOWN" ~ "cbs",
         HTS_PROV != "UNKNOWN" & HTS_MUNC == "UNKNOWN" ~ "hts",
         CBS_REG != "UNKNOWN" & CBS_PROV == "UNKNOWN" ~ "cbs",
         HTS_REG != "UNKNOWN" & HTS_PROV == "UNKNOWN" ~ "hts",
         if_all(c(HTS_REG, HTS_PROV, HTS_MUNC), ~. == "UNKNOWN") ~ "res",
      ),
      TEST_REG  = case_when(
         use_addr == "cbs" ~ CBS_REG,
         use_addr == "hts" ~ HTS_REG,
      ),
      TEST_PROV = case_when(
         use_addr == "cbs" ~ CBS_PROV,
         use_addr == "hts" ~ HTS_PROV,
      ),
      TEST_MUNC = case_when(
         use_addr == "cbs" ~ CBS_MUNC,
         use_addr == "hts" ~ HTS_MUNC,
      ),
   )

diagnosed <- dx %>%
   distinct(CENTRAL_ID) %>%
   mutate(
      harp_included = 1
   ) %>%
   bind_rows(
      hts22 %>%
         filter(CONFIRM_RESULT == 1) %>%
         distinct(CENTRAL_ID),
      hts23 %>%
         filter(CONFIRM_RESULT == 1) %>%
         distinct(CENTRAL_ID),
   ) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   mutate(
      hts_tst_pos = 1
   )

hts22 %<>%
   select(-any_of(c("final_modality", "HARPDX_REC", "hts_tst_pos", "harp_included"))) %>%
   left_join(
      y  = diagnosed,
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate(
      final_modality = if_else(hts_result == "(no data)", "REACH", hts_modality, "REACH"),
   ) %>%
   mutate_at(
      .vars = vars(starts_with("REACH_")),
      ~as.integer(keep_code(.))
   )

hts23 %<>%
   select(-any_of(c("final_modality", "HARPDX_REC", "hts_tst_pos", "harp_included"))) %>%
   left_join(
      y  = diagnosed,
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate(
      final_modality = if_else(hts_result == "(no data)", "REACH", hts_modality, "REACH"),
   ) %>%
   mutate_at(
      .vars = vars(starts_with("REACH_")),
      ~as.integer(keep_code(.))
   ) %>%
   group_by(CENTRAL_ID) %>%
   mutate(
      CID_REACH_CLINICAL      = max(REACH_CLINICAL, na.rm = TRUE),
      CID_REACH_ONLINE        = max(REACH_ONLINE, na.rm = TRUE),
      CID_REACH_INDEX_TESTING = max(REACH_INDEX_TESTING, na.rm = TRUE),
      CID_REACH_SSNT          = max(REACH_SSNT, na.rm = TRUE),
      CID_REACH_VENUE         = max(REACH_VENUE, na.rm = TRUE),
   ) %>%
   ungroup()

reach_types <- c('REACH_CLINICAL', 'REACH_ONLINE', 'REACH_INDEX_TESTING', 'REACH_SSNT', 'REACH_VENUE')
for (reach in reach_types) {
   rec_var  <- as.name(reach)
   cid_name <- stri_c("CID_", reach)
   cid_var  <- as.name(cid_name)
   reach    <- hts23 %>%
      filter({{rec_var}} == 1) %>%
      distinct(CENTRAL_ID) %>%
      mutate(
         {{cid_var}} := 1
      )

   hts23 %<>%
      select(-matches(cid_name)) %>%
      left_join(reach, join_by(CENTRAL_ID))
}

hts22 %<>%
   select(-matches("SEXUAL_RISK"), -starts_with("KAP_")) %>%
   mutate(
      SEXUAL_RISK = case_when(
         str_detect(risk_sexwithm, "yes") & str_detect(risk_sexwithf, "yes") ~ "M+F",
         str_detect(risk_sexwithm, "yes") & !str_detect(risk_sexwithf, "yes") ~ "M",
         !str_detect(risk_sexwithm, "yes") & str_detect(risk_sexwithf, "yes") ~ "F",
      ),
      KAP_UNKNOWN = if_else(coalesce(risks, "(no data)") == "(no data)", "1_Yes", NA_character_),
      KAP_MSM     = if_else(SEX == "1_Male" & SEXUAL_RISK %in% c("M", "M+F"), "1_Yes", NA_character_),
      KAP_HETEROM = if_else(SEX == "1_Male" & SEXUAL_RISK == "F", "1_Yes", NA_character_),
      KAP_HETEROF = if_else(SEX == "2_Female" & !is.na(SEXUAL_RISK), "1_Yes", NA_character_),
      KAP_PWID    = if_else(str_detect(risk_injectdrug, "yes"), "1_Yes", NA_character_),
      KAP_PIP     = if_else(str_detect(risk_paymentforsex, "yes"), "1_Yes", NA_character_),
      KAP_PDL     = case_when(
         StrLeft(CLIENT_TYPE, 1) == "7" ~ "1_Yes",
         str_detect(HTS_FACI, "JAIL") ~ "1_Yes",
         str_detect(HTS_FACI, "BJMP") ~ "1_Yes",
         str_detect(HTS_FACI, "PRISON") ~ "1_Yes",
         str_detect(toupper(HIV_SERVICE_ADDR), "JAIL") ~ "1_Yes",
         str_detect(toupper(HIV_SERVICE_ADDR), "BJMP") ~ "1_Yes",
         str_detect(toupper(HIV_SERVICE_ADDR), "PRISON") ~ "1_Yes",
         str_detect(toupper(CLINIC_NOTES), "JAIL") ~ "1_Yes",
         str_detect(toupper(CLINIC_NOTES), "BJMP") ~ "1_Yes",
         str_detect(toupper(CLINIC_NOTES), "PRISON") ~ "1_Yes",
         str_detect(toupper(COUNSEL_NOTES), "JAIL") ~ "1_Yes",
         str_detect(toupper(COUNSEL_NOTES), "BJMP") ~ "1_Yes",
         str_detect(toupper(COUNSEL_NOTES), "PRISON") ~ "1_Yes",
         TRUE ~ NA_character_
      ),
   ) %>%
   mutate_at(
      .vars = vars(starts_with("KAP_")),
      ~as.integer(keep_code(.))
   )

hts23 %<>%
   select(-matches("SEXUAL_RISK"), -starts_with("KAP_")) %>%
   mutate(
      SEXUAL_RISK = case_when(
         str_detect(risk_sexwithm, "yes") & str_detect(risk_sexwithf, "yes") ~ "M+F",
         str_detect(risk_sexwithm, "yes") & !str_detect(risk_sexwithf, "yes") ~ "M",
         !str_detect(risk_sexwithm, "yes") & str_detect(risk_sexwithf, "yes") ~ "F",
      ),
      KAP_UNKNOWN = if_else(coalesce(risks, "(no data)") == "(no data)", "1_Yes", NA_character_),
      KAP_MSM     = if_else(SEX == "1_Male" & SEXUAL_RISK %in% c("M", "M+F"), "1_Yes", NA_character_),
      KAP_HETEROM = if_else(SEX == "1_Male" & SEXUAL_RISK == "F", "1_Yes", NA_character_),
      KAP_HETEROF = if_else(SEX == "2_Female" & !is.na(SEXUAL_RISK), "1_Yes", NA_character_),
      KAP_PWID    = if_else(str_detect(risk_injectdrug, "yes"), "1_Yes", NA_character_),
      KAP_PIP     = if_else(str_detect(risk_paymentforsex, "yes"), "1_Yes", NA_character_),
      KAP_PDL     = case_when(
         StrLeft(CLIENT_TYPE, 1) == "7" ~ "1_Yes",
         str_detect(HTS_FACI, "JAIL") ~ "1_Yes",
         str_detect(HTS_FACI, "BJMP") ~ "1_Yes",
         str_detect(HTS_FACI, "PRISON") ~ "1_Yes",
         str_detect(toupper(HIV_SERVICE_ADDR), "JAIL") ~ "1_Yes",
         str_detect(toupper(HIV_SERVICE_ADDR), "BJMP") ~ "1_Yes",
         str_detect(toupper(HIV_SERVICE_ADDR), "PRISON") ~ "1_Yes",
         str_detect(toupper(CLINIC_NOTES), "JAIL") ~ "1_Yes",
         str_detect(toupper(CLINIC_NOTES), "BJMP") ~ "1_Yes",
         str_detect(toupper(CLINIC_NOTES), "PRISON") ~ "1_Yes",
         str_detect(toupper(COUNSEL_NOTES), "JAIL") ~ "1_Yes",
         str_detect(toupper(COUNSEL_NOTES), "BJMP") ~ "1_Yes",
         str_detect(toupper(COUNSEL_NOTES), "PRISON") ~ "1_Yes",
         TRUE ~ NA_character_
      ),
   ) %>%
   mutate_at(
      .vars = vars(starts_with("KAP_")),
      ~as.integer(keep_code(.))
   )


kap_types <- c('KAP_MSM', 'KAP_HETEROM', 'KAP_HETEROF', 'KAP_PIP', 'KAP_PDL', 'KAP_PWID', 'KAP_UNKNOWN')
for (kap in kap_types) {
   rec_var  <- as.name(kap)
   cid_name <- stri_c("CID_", kap)
   cid_var  <- as.name(cid_name)
   kap      <- hts22 %>%
      filter({{rec_var}} == 1) %>%
      distinct(CENTRAL_ID) %>%
      mutate(
         {{cid_var}} := 1
      )

   hts22 %<>%
      select(-matches(cid_name)) %>%
      left_join(kap, join_by(CENTRAL_ID))
}

hts23 %>%
   left_join(
      y  = diagnosed,
      by = join_by(CENTRAL_ID)
   ) %>%
   filter(hts_result == "R") %>%
   tab(hts_tst_pos)

hts22 %<>%
   select(-matches("TAT_RECORD")) %>%
   mutate(
      TAT_RECORD = floor(interval(RECORD_DATE, CREATED_AT) / days(1)),
      TAT_RECORD = if_else(TAT_RECORD < 0, 0, TAT_RECORD, TAT_RECORD),
   ) %>%
   mutate_at(
      .vars = vars(TAT_RECORD),
      ~case_when(
         . == 0 ~ "1) Same day",
         . == 1 ~ "2) Day after",
         . >= 2 & . <= 7 ~ "2) Within 7 days",
         . >= 8 & . <= 14 ~ "3) Within 14 days",
         . >= 15 & . <= 30 ~ "4) Within 30 days",
         . >= 31 & . <= 90 ~ "5) Within 90 days",
         . >= 11 ~ "6) More than 90 days",
         TRUE ~ "(no data)",
      )
   )
hts23 %<>%
   select(-matches("TAT_RECORD")) %>%
   mutate(
      TAT_RECORD = floor(interval(RECORD_DATE, CREATED_AT) / days(1)),
      TAT_RECORD = if_else(TAT_RECORD < 0, 0, TAT_RECORD, TAT_RECORD),
   ) %>%
   mutate_at(
      .vars = vars(TAT_RECORD),
      ~case_when(
         . == 0 ~ "1) Same day",
         . == 1 ~ "2) Day after",
         . >= 2 & . <= 7 ~ "2) Within 7 days",
         . >= 8 & . <= 14 ~ "3) Within 14 days",
         . >= 15 & . <= 30 ~ "4) Within 30 days",
         . >= 31 & . <= 90 ~ "5) Within 90 days",
         . >= 11 ~ "6) More than 90 days",
         TRUE ~ "(no data)",
      )
   )
##  hts data
hts22 <- read_dta("H:/20231024_hts-data_2022.dta")
hts23 <- read_dta("H:/20231024_hts-data_2023.dta")


hts22 %<>%
   mutate(
      SELF_IDENT = case_when(
         StrLeft(SELF_IDENT, 1) == "1" ~ "1_Man",
         StrLeft(SELF_IDENT, 1) == "2" ~ "2_Woman",
         StrLeft(SELF_IDENT, 1) == "3" ~ "3_Other",
         TRUE ~ SELF_IDENT
      )
   ) %>%
   generate_gender_identity(SEX, SELF_IDENT, SELF_IDENT_OTHER, GENDER_ID)


hts23 %<>%
   mutate(
      SELF_IDENT = case_when(
         StrLeft(SELF_IDENT, 1) == "1" ~ "1_Man",
         StrLeft(SELF_IDENT, 1) == "2" ~ "2_Woman",
         StrLeft(SELF_IDENT, 1) == "3" ~ "3_Other",
         TRUE ~ SELF_IDENT
      )
   ) %>%
   generate_gender_identity(SEX, SELF_IDENT, SELF_IDENT_OTHER, GENDER_ID)

hts22 %<>%
   mutate(
      use_addr  = case_when(
         hts_modality %in% c("ST", "CBS", "FBS", "ST") & if_any(c(CBS_REG, CBS_PROV, CBS_MUNC), ~. != "UNKNOWN") ~ "cbs",
         hts_modality %in% c("ST", "CBS", "FBS", "ST") & if_all(c(CBS_REG, CBS_PROV, CBS_MUNC), ~. == "UNKNOWN") ~ "hts",
         CBS_PROV != "UNKNOWN" & CBS_MUNC == "UNKNOWN" ~ "cbs",
         CBS_REG != "UNKNOWN" & CBS_PROV == "UNKNOWN" ~ "cbs",
         HTS_PROV != "UNKNOWN" & HTS_MUNC == "UNKNOWN" ~ "hts",
         HTS_REG != "UNKNOWN" & HTS_PROV == "UNKNOWN" ~ "hts",
         if_any(c(HTS_REG, HTS_PROV, HTS_MUNC), ~. != "UNKNOWN") ~ "hts",
         # if_all(c(HTS_REG, HTS_PROV, HTS_MUNC), ~. == "UNKNOWN") ~ "res"
         TRUE ~ "cbs"
      ),
      TEST_REG  = case_when(
         use_addr == "cbs" ~ CBS_REG,
         use_addr == "hts" ~ HTS_REG,
      ),
      TEST_PROV = case_when(
         use_addr == "cbs" ~ CBS_PROV,
         use_addr == "hts" ~ HTS_PROV,
      ),
      TEST_MUNC = case_when(
         use_addr == "cbs" ~ CBS_MUNC,
         use_addr == "hts" ~ HTS_MUNC,
      )
   )
hts23 %<>%
   mutate(
      use_addr  = case_when(
         hts_modality %in% c("ST", "CBS", "FBS", "ST") & if_any(c(CBS_REG, CBS_PROV, CBS_MUNC), ~. != "UNKNOWN") ~ "cbs",
         hts_modality %in% c("ST", "CBS", "FBS", "ST") & if_all(c(CBS_REG, CBS_PROV, CBS_MUNC), ~. == "UNKNOWN") ~ "hts",
         CBS_PROV != "UNKNOWN" & CBS_MUNC == "UNKNOWN" ~ "cbs",
         CBS_REG != "UNKNOWN" & CBS_PROV == "UNKNOWN" ~ "cbs",
         HTS_PROV != "UNKNOWN" & HTS_MUNC == "UNKNOWN" ~ "hts",
         HTS_REG != "UNKNOWN" & HTS_PROV == "UNKNOWN" ~ "hts",
         if_any(c(HTS_REG, HTS_PROV, HTS_MUNC), ~. != "UNKNOWN") ~ "hts",
         # if_all(c(HTS_REG, HTS_PROV, HTS_MUNC), ~. == "UNKNOWN") ~ "res"
         TRUE ~ "cbs"
      ),
      TEST_REG  = case_when(
         use_addr == "cbs" ~ CBS_REG,
         use_addr == "hts" ~ HTS_REG,
      ),
      TEST_PROV = case_when(
         use_addr == "cbs" ~ CBS_PROV,
         use_addr == "hts" ~ HTS_PROV,
      ),
      TEST_MUNC = case_when(
         use_addr == "cbs" ~ CBS_MUNC,
         use_addr == "hts" ~ HTS_MUNC,
      )
   )

tab(final_modality)
hts22 %>%
   tab(TAT_RECORD)

