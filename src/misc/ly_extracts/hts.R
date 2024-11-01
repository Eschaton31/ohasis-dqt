source("src/misc/ly_extracts/site_list.R")

con <- connect("ohasis-lw")

forms <- QB$new(con)
forms$where(function(query = QB$new(con)) {
   query$whereBetween('RECORD_DATE', c(min, max), "or")
   query$whereBetween('DATE_CONFIRM', c(min, max), "or")
   query$whereBetween('T0_DATE', c(min, max), "or")
   query$whereBetween('T1_DATE', c(min, max), "or")
   query$whereBetween('T2_DATE', c(min, max), "or")
   query$whereBetween('T3_DATE', c(min, max), "or")
   query$whereNested
})
forms$where(function(query = QB$new(con)) {
   query$whereIn('FACI_ID', sites$FACI_ID, boolean = "or")
   query$whereIn('SERVICE_FACI', sites$FACI_ID, boolean = "or")
   query$whereIn('CREATED_BY', staff$STAFF_ID, boolean = "or")
   query$whereIn('SERVICE_BY', staff$STAFF_ID, boolean = "or")
   query$whereNested
})

forms$from("ohasis_warehouse.form_hts")
hts <- forms$get()

forms$from("ohasis_warehouse.form_a")
a <- forms$get()

cfbs <- QB$new(con)$
   from("ohasis_warehouse.form_cfbs")$
   limit(0)$
   get()

id_reg <- QB$new(con)$
   from("ohasis_warehouse.id_registry")$
   select(PATIENT_ID, CENTRAL_ID)$
   get()

dbDisconnect(con)

testing <- process_hts(hts, a, cfbs) %>%
   get_cid(id_reg, PATIENT_ID)

unique_cols <- names(testing)
unique_cols <- unique_cols[!(unique_cols %in% c(
   "REC_ID",
   "PATIENT_ID",
   "PRIME",
   "SNAPSHOT",
   "CREATED_BY",
   "CREATED_AT",
   "UPDATED_BY",
   "UPDATED_AT",
   "DELETED_BY",
   "DELETED_AT",
   "FACI_ID",
   "SUB_FACI_ID",
   "CURR_ADDR",
   "PERM_ADDR",
   "BIRTH_ADDR",
   'CONFIRM_FACI',
   'CONFIRM_SUB_FACI',
   'CONFIRM_TYPE',
   'CONFIRM_CODE',
   'SPECIMEN_REFER_TYPE',
   'SPECIMEN_SOURCE',
   'SPECIMEN_SUB_SOURCE',
   'CONFIRM_RESULT',
   'SIGNATORY_1',
   'SIGNATORY_2',
   'SIGNATORY_3',
   'DATE_RELEASE',
   'DATE_CONFIRM',
   'IDNUM',
   'VERBAL_CONSENT',
   'SIGNATURE_ESIG',
   'SIGNATURE_NAME',
   'T1_DATE',
   'T1_KIT',
   'T1_RESULT',
   'T2_DATE',
   'T2_KIT',
   'T2_RESULT',
   'T3_DATE',
   'T3_KIT',
   'T3_RESULT',
   'DATE_COLLECT',
   'DATE_RECEIVE'
))]
unique_cols <- unique_cols[!(unique_cols %in% c(
   "UIC",
   "uic",
   "FIRST",
   "first",
   "firstname",
   "MIDDLE",
   "middle",
   "LAST",
   "last",
   "SUFFIX",
   "suffix",
   "name_suffix",
   "PATIENT_CODE",
   "px_code",
   "PHILHEALTH_NO",
   "philhealth_no",
   "PHILHEALTH",
   "philhealth",
   "PHILSYS_ID",
   "philsys_id",
   "PERM_ADDR",
   "CURR_ADDR",
   "BIRTH_ADDR",
   "CLIENT_MOBILE",
   "client_mobile",
   "mobile",
   "CLIENT_EMAIL",
   "client_email",
   "email",
   "name",
   "STANDARD_FIRST"
))]

ly_test <- testing %>%
   mutate_at(
      .vars = vars(FIRST, MIDDLE, LAST, SUFFIX),
      ~coalesce(clean_pii(.), "")
   ) %>%
   mutate_at(
      .vars = vars(PATIENT_CODE, UIC, PHILHEALTH_NO, PHILSYS_ID, CLIENT_MOBILE, CLIENT_EMAIL),
      ~clean_pii(.)
   ) %>%
   get_latest_pii(
      "CENTRAL_ID",
      c(
         "BIRTHDATE",
         "SEX",
         "UIC",
         "CURR_PSGC_REG",
         "CURR_PSGC_PROV",
         "CURR_PSGC_MUNC",
         "PERM_PSGC_REG",
         "PERM_PSGC_PROV",
         "PERM_PSGC_MUNC"
      )
   ) %>%
   distinct(across(all_of(unique_cols)), .keep_all = TRUE) %>%
   convert_hts("name") %>%
   mutate(
      site_sort    = case_when(
         HTS_FACI %like% "LoveYourself" ~ 1,
         TRUE ~ 9999
      ),
      hts_priority = case_when(
         CONFIRM_RESULT %in% c(1, 2, 3) ~ 1,
         hts_result != "(no data)" & FORM_VERSION %in% c("Form A (v2017)", "HTS Form (v2021)") ~ 2,
         hts_result != "(no data)" & hts_modality == "FBT" ~ 3,
         hts_result != "(no data)" & hts_modality == "CBS" ~ 4,
         hts_result != "(no data)" & hts_modality == "FBS" ~ 5,
         hts_result != "(no data)" & hts_modality == "ST" ~ 6,
         TRUE ~ 9999
      ),

      AGE          = coalesce(AGE, calc_age(BIRTHDATE, RECORD_DATE)),
      AGE_BAND     = case_when(
         AGE %between% c(0, 14) ~ "1) <15",
         AGE %between% c(15, 17) ~ "2) 15-17",
         AGE %between% c(18, 24) ~ "3) 18-24",
         AGE %between% c(25, 34) ~ "4) 24-34",
         AGE %between% c(35, 49) ~ "5) 35-49",
         AGE %between% c(50, 1000) ~ "6) 50+",
         TRUE ~ "(no data)"
      ),

      drop         = if_all(c(FIRST, LAST, BIRTHDATE, SEX), ~is.na(.)),

      KP_MSM       = if_else(SEX == "1_Male" & str_detect(risk_sexwithm, "yes"), "MSM", NA_character_),
      KP_TG        = case_when(
         SEX == "1_Male" & StrLeft(SELF_IDENT, 1) %in% c("2", "3") ~ "TGW",
         SEX == "2_Female" & StrLeft(SELF_IDENT, 1) %in% c("1", "3") ~ "TGM",
         TRUE ~ NA_character_
      ),
      KP_PDL       = case_when(
         StrLeft(CLIENT_TYPE, 1) == "7" ~ "PDL",
         TRUE ~ NA_character_
      ),
      KP_SW        = case_when(
         str_detect(risk_paymentforsex, "yes") ~ "SW",
         str_detect(COUNSEL_NOTES, "SEX WORKER") ~ "SW",
         TRUE ~ NA_character_
      ),
      KP_PWUD      = case_when(
         str_detect(risk_illicitdrug, "yes") ~ "PWUD",
         str_detect(risk_injectdrug, "yes") ~ "PWUD",
         TRUE ~ NA_character_
      ),

      hts_result   = coalesce(hts_result, "NR")
   ) %>%
   unite(
      col   = "KAP_TYPE",
      sep   = "-",
      starts_with("KP_", ignore.case = FALSE),
      na.rm = TRUE
   ) %>%
   mutate(
      KAP_TYPE = coalesce(na_if(KAP_TYPE, ""), "No apparent risk")
   ) %>%
   arrange(site_sort, hts_priority) %>%
   mutate_at(
      .vars = vars(CURR_REG, CURR_PROV, CURR_MUNC, PERM_REG, PERM_PROV, PERM_MUNC),
      ~coalesce(., "Unknown")
   ) %>%
   distinct(CENTRAL_ID, hts_date, hts_modality, .keep_all = TRUE)

ly_test %>%
   filter(!drop) %>%
   group_by(KAP_TYPE) %>%
   summarise(
      Tested         = n(),
      `Non-reactive` = sum(if_else(hts_result == "NR", 1, 0, 0)),
      Reactive       = sum(if_else(hts_result == "R", 1, 0, 0)),
   ) %>%
   ungroup() %>%
   adorn_totals() %>%
   mutate(
      RR = Reactive / Tested
   ) %>%
   write_clip()


ly_test %>%
   filter(!drop) %>%
   tab(SEX)
ly_test %>%
   left_join(
      y = tx_out %>%
         select(CENTRAL_ID, outcome, latest_ffupdate, latest_nextpickup, latest_regimen),
      by = join_by(CENTRAL_ID)
   ) %>%
   format_stata() %>%
   write_dta("H:/20240907_hts-ly_ever.dta")
