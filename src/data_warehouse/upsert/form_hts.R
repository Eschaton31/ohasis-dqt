##  HTS Form -------------------------------------------------------------------

continue <- 0
id_col   <- "REC_ID"
# px identifiers (demographics, address, etc.)
object   <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
   # keep only hts form
   filter(
      DISEASE == "HIV",
      FORM_VERSION == "HTS Form (v2021)",
      SNAPSHOT >= snapshot_old,
      SNAPSHOT <= snapshot_new,
      is.na(DELETED_BY)
   )

for_delete <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
   filter(
      DISEASE == "HIV",
      FORM_VERSION == "HTS Form (v2021)",
      SNAPSHOT >= snapshot_old,
      SNAPSHOT <= snapshot_new,
      !is.na(DELETED_BY)
   ) %>%
   select(REC_ID) %>%
   collect()

# get number of affected rows
if ((object %>% count() %>% collect())$n > 0) {
   continue <- 1
   object   <- object %>%
      rename_at(
         .vars = vars(starts_with('SERVICE_')),
         ~paste0('HIV_', .)
      ) %>%
      select(
         -starts_with("DEATH_")
      ) %>%
      # facility data in form
      left_join(
         y  = tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_faci_info")) %>%
            filter(
               substr(MODALITY, 1, 6) != "101102"
            ) %>%
            select(
               REC_ID,
               MODALITY,
               SERVICE_FACI,
               SERVICE_SUB_FACI,
               REFER_TYPE,
               SERVICE_BY,
               PROVIDER_TYPE,
               PROVIDER_TYPE_OTHER,
               CLINIC_NOTES,
               COUNSEL_NOTES
            ),
         by = "REC_ID"
      ) %>%
      collect() %>%
      # ob section
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_form")) %>%
                  filter(FORM == "HTS Form"),
               by = "REC_ID"
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_ob")),
               by = "REC_ID"
            ) %>%
            mutate(
               IS_PREGNANT = case_when(
                  IS_PREGNANT == "0" ~ "0_No",
                  IS_PREGNANT == "1" ~ "1_Yes",
                  TRUE ~ as.character(IS_PREGNANT)
               )
            ) %>%
            select(
               REC_ID,
               IS_PREGNANT
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # confirmatory data
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_form")) %>%
                  filter(FORM == "HTS Form"),
               by = "REC_ID"
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_confirm")),
               by = "REC_ID"
            ) %>%
            mutate(
               CONFIRM_TYPE = case_when(
                  CONFIRM_TYPE == "1" ~ "1_Central NRL",
                  CONFIRM_TYPE == "2" ~ "2_CrCL",
                  TRUE ~ as.character(CONFIRM_TYPE)
               ),
               FINAL_RESULT = case_when(
                  FINAL_RESULT %like% "%Positive%" ~ "1_Positive",
                  FINAL_RESULT %like% "%Negative%" ~ "2_Negative",
                  FINAL_RESULT %like% "%Inconclusive%" ~ "3_Indeterminate",
                  FINAL_RESULT %like% "%Indeterminate%" ~ "3_Indeterminate",
                  FINAL_RESULT %like% "%PENDING%" ~ "4_Pending",
                  TRUE ~ FINAL_RESULT
               ),
               CLIENT_TYPE  = case_when(
                  CLIENT_TYPE == "1" ~ "1_Inpatient",
                  CLIENT_TYPE == "2" ~ "2_Walk-in / Outpatient",
                  CLIENT_TYPE == "3" ~ "3_Mobile HTS Client",
                  CLIENT_TYPE == "5" ~ "5_Satellite Client",
                  CLIENT_TYPE == "4" ~ "4_Referral",
                  CLIENT_TYPE == "6" ~ "6_Transient",
                  TRUE ~ as.character(CLIENT_TYPE)
               )
            ) %>%
            select(
               REC_ID,
               CONFIRM_FACI        = FACI_ID,
               CONFIRM_SUB_FACI    = SUB_FACI_ID,
               CONFIRM_TYPE,
               CONFIRM_CODE,
               SPECIMEN_REFER_TYPE = CLIENT_TYPE,
               SPECIMEN_SOURCE     = SOURCE,
               SPECIMEN_SUB_SOURCE = SUB_SOURCE,
               CONFIRM_RESULT      = FINAL_RESULT,
               SIGNATORY_1,
               SIGNATORY_2,
               SIGNATORY_3,
               DATE_RELEASE,
               DATE_CONFIRM,
               IDNUM
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # contact info
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_form")) %>%
                  filter(FORM == "HTS Form"),
               by = "REC_ID"
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_contact")),
               by = "REC_ID"
            ) %>%
            mutate(
               CONTACT_TYPE = case_when(
                  CONTACT_TYPE == "1" ~ "MOBILE",
                  CONTACT_TYPE == "2" ~ "EMAIL",
                  TRUE ~ CONTACT_TYPE
               )
            ) %>%
            pivot_wider(
               id_cols     = c(REC_ID, CONTACT_TYPE),
               names_from  = CONTACT_TYPE,
               values_from = CONTACT,
               names_glue  = 'CLIENT_{CONTACT_TYPE}_{.value}'
            ) %>%
            rename_at(
               .vars = vars(ends_with('_CONTACT')),
               ~stri_replace_all_fixed(., '_CONTACT', '')
            ) %>%
            select(
               REC_ID,
               starts_with("CLIENT_") & !matches("\\d")
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # consent section
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_form")) %>%
                  filter(FORM == "HTS Form"),
               by = "REC_ID"
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_consent")),
               by = "REC_ID"
            ) %>%
            mutate(
               SIGNATURE_NAME = if_else(!is.na(SIGNATURE), '1_Yes', NA_character_),
               SIGNATURE_ESIG = if_else(!is.na(ESIG), '1_Yes', NA_character_),
               VERBAL_CONSENT = if_else(VERBAL_CONSENT == 1, '1_Yes', NA_character_)
            ) %>%
            select(
               REC_ID,
               VERBAL_CONSENT,
               SIGNATURE_ESIG,
               SIGNATURE_NAME
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # occupation section
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_form")) %>%
                  filter(FORM == "HTS Form"),
               by = "REC_ID"
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_occupation")),
               by = "REC_ID"
            ) %>%
            mutate(
               WORK        = toupper(WORK),
               IS_STUDENT  = case_when(
                  IS_STUDENT == "0" ~ "0_No",
                  IS_STUDENT == "1" ~ "1_Yes",
                  TRUE ~ as.character(IS_STUDENT)
               ),
               IS_EMPLOYED = case_when(
                  IS_EMPLOYED == "0" ~ "0_No",
                  IS_EMPLOYED == "1" ~ "1_Yes",
                  TRUE ~ as.character(IS_EMPLOYED)
               ),
               IS_OFW      = case_when(
                  IS_OFW == "0" ~ "0_No",
                  IS_OFW == "1" ~ "1_Yes",
                  TRUE ~ as.character(IS_OFW)
               )
            ) %>%
            select(
               REC_ID,
               WORK = WORK_TEXT,
               IS_EMPLOYED,
               IS_STUDENT,
               IS_OFW
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # ofw data
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_form")) %>%
                  filter(FORM == "HTS Form"),
               by = "REC_ID"
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_ofw")),
               by = "REC_ID"
            ) %>%
            mutate(
               OFW_STATION = case_when(
                  OFW_STATION == "1" ~ "1_On a ship",
                  OFW_STATION == "2" ~ "2_Land",
                  TRUE ~ as.character(OFW_STATION)
               ),
            ) %>%
            select(
               REC_ID,
               OFW_YR_RET,
               OFW_STATION,
               OFW_COUNTRY
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # exposure history
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_form")) %>%
                  filter(FORM == "HTS Form"),
               by = "REC_ID"
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_expose_hist")),
               by = "REC_ID"
            ) %>%
            mutate(
               IS_EXPOSED = case_when(
                  TYPE_LAST_EXPOSE == "4" & IS_EXPOSED == "1" ~ "1",
                  TYPE_LAST_EXPOSE == "0" & IS_EXPOSED == "1" ~ "2",
                  is.na(TYPE_LAST_EXPOSE) & IS_EXPOSED == "1" ~ "2",
                  IS_EXPOSED == "0" ~ "0",
                  TRUE ~ IS_EXPOSED
               ),
               IS_EXPOSED = case_when(
                  IS_EXPOSED %in% c('1', '2', '3', '4') ~ '1_Yes',
                  IS_EXPOSED == '0' ~ '0_No',
                  IS_EXPOSED == '99999' ~ NA_character_,
                  TRUE ~ IS_EXPOSED
               ),
               EXPOSURE   = case_when(
                  EXPOSURE == '120000' ~ 'HIV_MOTHER',
                  EXPOSURE == '217000' ~ 'SEX_M',
                  EXPOSURE == '216000' ~ 'SEX_M_AV',
                  EXPOSURE == '216200' ~ 'SEX_M_AV_NOCONDOM',
                  EXPOSURE == '227000' ~ 'SEX_F',
                  EXPOSURE == '226000' ~ 'SEX_F_AV',
                  EXPOSURE == '226200' ~ 'SEX_F_AV_NOCONDOM',
                  EXPOSURE == '200010' ~ 'SEX_PAYING',
                  EXPOSURE == '200020' ~ 'SEX_PAYMENT',
                  EXPOSURE == '200300' ~ 'SEX_DRUGS',
                  EXPOSURE == '301010' ~ 'DRUG_INJECT',
                  EXPOSURE == '530000' ~ 'BLOOD_TRANSFUSE',
                  EXPOSURE == '510000' ~ 'OCCUPATION',
                  EXPOSURE == '520000' ~ 'TATTOO',
                  EXPOSURE == '400000' ~ 'STI',
                  TRUE ~ EXPOSURE
               )
            ) %>%
            rename(DATE = DATE_LAST_EXPOSE) %>%
            pivot_wider(
               id_cols     = c(REC_ID, EXPOSURE),
               names_from  = EXPOSURE,
               values_from = c(IS_EXPOSED, DATE),
               names_glue  = 'EXPOSE_{EXPOSURE}_{.value}'
            ) %>%
            rename_at(
               .vars = vars(ends_with('_IS_EXPOSED')),
               ~stri_replace_all_fixed(., '_IS_EXPOSED', '')
            ) %>%
            select(
               REC_ID,
               starts_with("EXPOSE_") & !matches("\\d")
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # medical history
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_form")) %>%
                  filter(FORM == "HTS Form"),
               by = "REC_ID"
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_med_profile")),
               by = "REC_ID"
            ) %>%
            mutate(
               IS_PROFILE = case_when(
                  IS_PROFILE == "0" ~ "0_No",
                  IS_PROFILE == "1" ~ "1_Yes",
                  TRUE ~ IS_PROFILE
               ),
               PROFILE    = case_when(
                  PROFILE == "1" ~ "TB_PX",
                  PROFILE == "3" ~ "HEP_B",
                  PROFILE == "4" ~ "HEP_C",
                  PROFILE == "6" ~ "PREP_PX",
                  PROFILE == "7" ~ "PEP_PX",
                  PROFILE == "8" ~ "STI",
                  TRUE ~ PROFILE
               )
            ) %>%
            pivot_wider(
               id_cols     = c(REC_ID, PROFILE),
               names_from  = PROFILE,
               values_from = IS_PROFILE,
               names_glue  = 'MED_{PROFILE}_{.value}'
            ) %>%
            rename_at(
               .vars = vars(ends_with('_IS_PROFILE')),
               ~stri_replace_all_fixed(., '_IS_PROFILE', '')
            ) %>%
            select(
               REC_ID,
               starts_with("MED_") & !matches("\\d")
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # reasons for testing
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_form")) %>%
                  filter(FORM == "HTS Form"),
               by = "REC_ID"
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_test_reason")),
               by = "REC_ID"
            ) %>%
            mutate(
               IS_REASON == case_when(
                  IS_REASON == "0" ~ "0_No",
                  IS_REASON == "1" ~ "1_Yes",
                  TRUE ~ as.character(IS_REASON)
               ),
               REASON = case_when(
                  REASON == "1" ~ "HIV_EXPOSE",
                  REASON == "2" ~ "PHYSICIAN",
                  REASON == "3" ~ "EMPLOY_OFW",
                  REASON == "4" ~ "EMPLOY_LOCAL",
                  REASON == "5" ~ "INSURANCE",
                  REASON == "6" ~ "NO_REASON",
                  REASON == "7" ~ "RETEST",
                  REASON == "8" ~ "PEER_ED",
                  REASON == "9" ~ "TEXT_EMAIL",
                  REASON == "8888" ~ "OTHER",
                  TRUE ~ REASON
               )
            ) %>%
            rename(
               TEXT = REASON_OTHER
            ) %>%
            pivot_wider(
               id_cols     = c(REC_ID, REASON),
               names_from  = REASON,
               values_from = c(IS_REASON, TEXT),
               names_glue  = 'TEST_REASON_{REASON}_{.value}'
            ) %>%
            rename_at(
               .vars = vars(ends_with('_IS_REASON')),
               ~stri_replace_all_fixed(., '_IS_REASON', '')
            ) %>%
            select(
               -(ends_with("TEXT") & !contains("OTHER"))
            ) %>%
            select(
               REC_ID,
               starts_with("TEST_REASON_") & !matches("\\d")
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # previous test data
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_form")) %>%
                  filter(FORM == "HTS Form"),
               by = "REC_ID"
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_prev_test")),
               by = "REC_ID"
            ) %>%
            mutate(
               PREV_TESTED      = case_when(
                  PREV_TESTED == "0" ~ "0_No",
                  PREV_TESTED == "1" ~ "1_Yes",
                  TRUE ~ as.character(PREV_TESTED)
               ),
               PREV_TEST_DATE   = if_else(PREV_TEST_DATE == "0000-00-00", as.Date(NA), PREV_TEST_DATE),
               PREV_TEST_FACI   = toupper(PREV_TEST_FACI),
               PREV_TEST_RESULT = case_when(
                  PREV_TEST_RESULT == "1" ~ "1_Positive",
                  PREV_TEST_RESULT == "2" ~ "2_Negative",
                  PREV_TEST_RESULT == "3" ~ "3_Indeterminate",
                  PREV_TEST_RESULT == "4" ~ "4_Was not able to get result",
                  TRUE ~ as.character(PREV_TEST_RESULT)
               )
            ) %>%
            select(
               REC_ID,
               PREV_TESTED,
               PREV_TEST_DATE,
               PREV_TEST_FACI,
               PREV_TEST_RESULT
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # staging section
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_form")) %>%
                  filter(FORM == "HTS Form"),
               by = "REC_ID"
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_staging")),
               by = "REC_ID"
            ) %>%
            mutate(
               SYMPTOMS     = toupper(SYMPTOMS),
               CLINICAL_PIC = case_when(
                  CLINICAL_PIC == "1" ~ "1_Asymptomatic",
                  CLINICAL_PIC == "2" ~ "2_Symptomatic",
                  TRUE ~ as.character(CLINICAL_PIC)
               ),
               WHO_CLASS    = case_when(
                  WHO_CLASS == "1" ~ "1_I",
                  WHO_CLASS == "2" ~ "2_II",
                  WHO_CLASS == "3" ~ "3_III",
                  WHO_CLASS == "4" ~ "4_IV",
                  TRUE ~ as.character(WHO_CLASS)
               )
            ) %>%
            select(
               REC_ID,
               CLINICAL_PIC,
               SYMPTOMS,
               WHO_CLASS
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # agreed section
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_form")) %>%
                  filter(FORM == "HTS Form"),
               by = "REC_ID"
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_cfbs")),
               by = "REC_ID"
            ) %>%
            mutate(
               SCREEN_AGREED = case_when(
                  SCREEN_AGREED == "1" ~ "1_Yes",
                  SCREEN_AGREED == "0" ~ "0_No",
                  TRUE ~ as.character(SCREEN_AGREED)
               ),
            ) %>%
            select(
               REC_ID,
               SCREEN_AGREED
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # reach
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_form")) %>%
                  filter(FORM == "HTS Form"),
               by = "REC_ID"
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_reach")),
               by = "REC_ID"
            ) %>%
            mutate(
               IS_REACH = case_when(
                  IS_REACH == 1 ~ '1_Yes',
                  IS_REACH == 0 ~ '0_No',
                  TRUE ~ NA_character_
               ),
               REACH    = case_when(
                  REACH == 1 ~ 'CLINICAL',
                  REACH == 2 ~ 'ONLINE',
                  REACH == 3 ~ 'INDEX_TESTING',
                  REACH == 4 ~ 'SSNT',
                  REACH == 5 ~ 'VENUE',
                  TRUE ~ NA_character_
               )
            ) %>%
            pivot_wider(
               id_cols     = c(REC_ID, REACH),
               names_from  = REACH,
               values_from = IS_REACH,
               names_glue  = 'REACH_{REACH}_{.value}'
            ) %>%
            rename_at(
               .vars = vars(ends_with('_IS_REACH')),
               ~stri_replace_all_fixed(., '_IS_REACH', '')
            ) %>%
            select(
               REC_ID,
               starts_with("REACH_") & !matches("\\d")
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # linkage
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_form")) %>%
                  filter(FORM == "HTS Form"),
               by = "REC_ID"
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_linkage")),
               by = "REC_ID"
            ) %>%
            mutate_at(
               .vars = vars(starts_with('REFER')),
               ~case_when(
                  . == 1 ~ '1_Yes',
                  . == 0 ~ '0_No',
                  TRUE ~ as.character(.)
               )
            ) %>%
            select(
               REC_ID,
               REFER_ART,
               REFER_CONFIRM,
               REFER_RETEST,
               RETEST_MOS,
               RETEST_WKS,
               RETEST_DATE
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # services
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_form")) %>%
                  filter(FORM == "HTS Form"),
               by = "REC_ID"
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_other_service")),
               by = "REC_ID"
            ) %>%
            mutate(
               GIVEN   = case_when(
                  GIVEN == '1' ~ '1_Yes',
                  GIVEN == '0' ~ '0_No',
                  TRUE ~ as.character(GIVEN)
               ),
               SERVICE = case_when(
                  SERVICE == "1013" ~ "HIV_101",
                  SERVICE == "1004" ~ "IEC_MATS",
                  SERVICE == "1002" ~ "RISK_COUNSEL",
                  SERVICE == "5001" ~ "PREP_REFER",
                  SERVICE == "5002" ~ "SSNT_OFFER",
                  SERVICE == "5003" ~ "SSNT_ACCEPT",
                  SERVICE == "2001" ~ "GIVEN_CONDOMS",
                  SERVICE == "2002" ~ "GIVEN_LUBES",
                  TRUE ~ SERVICE
               )
            ) %>%
            pivot_wider(
               id_cols     = c(REC_ID, SERVICE),
               names_from  = SERVICE,
               values_from = c(GIVEN, OTHER_SERVICE),
               names_glue  = 'SERVICE_{SERVICE}_{.value}'
            ) %>%
            rename_at(
               .vars = vars(ends_with('_GIVEN')),
               ~stri_replace_all_fixed(., '_GIVEN', '')
            ) %>%
            select(
               -(ends_with("OTHER_SERVICE") & (!contains("LUBES") | !contains("CONDOMS")))
            ) %>%
            rename_at(
               .vars = vars(ends_with('_OTHER_SERVICE')),
               ~stri_replace_all_fixed(., '_OTHER_SERVICE', '')
            ) %>%
            select(
               REC_ID,
               matches("SERVICE_HIV_101"),
               starts_with("SERVICE_") & !matches("\\d"),
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # linkage
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_form")) %>%
                  filter(FORM == "HTS Form"),
               by = "REC_ID"
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_test_refuse")),
               by = "REC_ID"
            ) %>%
            filter(
               REASON == "8888"
            ) %>%
            select(
               REC_ID,
               TEST_REFUSE_OTHER_TEXT = REASON_OTHER
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # t0 data
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_form")) %>%
                  filter(FORM == "HTS Form"),
               by = "REC_ID"
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_test")),
               by = "REC_ID"
            ) %>%
            filter(TEST_TYPE == 10, RESULT != 0) %>%
            group_by(REC_ID) %>%
            mutate(
               FIRST_VALID = first(TEST_NUM)
            ) %>%
            ungroup() %>%
            filter(TEST_NUM == FIRST_VALID) %>%
            mutate(
               RESULT = case_when(
                  RESULT == "1" ~ "1_Reactive",
                  RESULT == "2" ~ "2_Non-reactive",
                  TRUE ~ RESULT
               )
            ) %>%
            select(
               REC_ID,
               T0_DATE   = DATE_PERFORM,
               T0_RESULT = RESULT
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # rhivda data
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_test_hiv")) %>%
                  select(
                     REC_ID,
                     TEST_TYPE,
                     TEST_NUM,
                     FINAL_RESULT,
                     KIT_NAME
                  ),
               by = "REC_ID"
            ) %>%
            left_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_test")),
               by = c("REC_ID", "TEST_TYPE", "TEST_NUM")
            ) %>%
            filter(FINAL_RESULT != 0) %>%
            group_by(REC_ID, TEST_TYPE) %>%
            mutate(
               FIRST_VALID = first(TEST_NUM)
            ) %>%
            ungroup() %>%
            filter(TEST_TYPE %in% c(31, 32, 33), TEST_NUM == FIRST_VALID) %>%
            select(
               REC_ID,
               TEST_TYPE,
               DATE        = DATE_PERFORM,
               BASE_RESULT = RESULT,
               RESULT      = FINAL_RESULT,
               KIT         = KIT_NAME
            ) %>%
            mutate(
               TEST_TYPE = case_when(
                  TEST_TYPE == "31" ~ "T1",
                  TEST_TYPE == "32" ~ "T2",
                  TEST_TYPE == "33" ~ "T3",
                  TRUE ~ TEST_TYPE
               ),
               RESULT    = case_when(
                  RESULT %like% "1%" ~ "Positive / Reactive",
                  RESULT %like% "2%" ~ "Negative / Non-reactive",
                  RESULT %like% "3%" ~ "Indeterminate / Inconclusive",
                  RESULT %like% "0%" ~ "Invalid",
                  TRUE ~ as.character(RESULT)
               ),
               RESULT    = case_when(
                  RESULT == "Positive / Reactive" ~ "1",
                  RESULT == "Negative / Non-reactive" ~ "2",
                  RESULT == "Indeterminate / Inconclusive" ~ "3",
                  RESULT == "Invalid" ~ "0",
                  TRUE ~ RESULT
               ),
               RESULT    = case_when(
                  RESULT == "1" ~ "1_Positive / Reactive",
                  RESULT == "2" ~ "2_Negative / Non-reactive",
                  RESULT == "3" ~ "3_Indeterminate / Inconclusive",
                  RESULT == "0" ~ "0_Invalid",
                  TRUE ~ RESULT
               )
            ) %>%
            pivot_wider(
               id_cols     = c(REC_ID, TEST_TYPE),
               names_from  = TEST_TYPE,
               values_from = c(DATE, RESULT, KIT),
               names_glue  = "{TEST_TYPE}_{.value}"
            ) %>%
            select(
               REC_ID,
               any_of(
                  c("T1_DATE",
                    "T1_KIT",
                    "T1_RESULT",
                    "T2_DATE",
                    "T2_KIT",
                    "T2_RESULT",
                    "T3_DATE",
                    "T3_KIT",
                    "T3_RESULT")
               ),
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # date collect receive
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_test_hiv")) %>%
                  filter(TEST_TYPE == 31, TEST_NUM == 1) %>%
                  select(
                     REC_ID,
                     DATE_COLLECT,
                     DATE_RECEIVE
                  ),
               by = "REC_ID"
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # ohasis KP tagging
      mutate(
         SELF_IDENT = case_when(
            StrLeft(SELF_IDENT, 1) == '1' ~ '1_Man',
            StrLeft(SELF_IDENT, 1) == '2' ~ '2_Woman',
            StrLeft(SELF_IDENT, 1) == '3' ~ '3_Other',
            TRUE ~ SELF_IDENT
         ),
         HTS_MSM    = case_when(
            StrLeft(SEX, 1) == 1 & StrLeft(EXPOSE_SEX_M, 1) == 1 ~ 1,
            StrLeft(SEX, 1) == 1 & !is.na(EXPOSE_SEX_M_AV_DATE) ~ 1,
            StrLeft(SEX, 1) == 1 & !is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ 1,
            TRUE ~ 0
         ),
         HTS_TGW    = case_when(
            StrLeft(SEX, 1) == 1 & StrLeft(SELF_IDENT, 1) == 2 ~ 1,
            StrLeft(SEX, 1) == 1 & StrLeft(SELF_IDENT, 1) == 3 ~ 1,
            TRUE ~ 0
         ),
         HTS_PWID   = case_when(
            StrLeft(EXPOSE_DRUG_INJECT, 1) == 1 ~ 1,
            StrLeft(EXPOSE_DRUG_INJECT, 1) == 2 ~ 1,
            TRUE ~ 0
         ),
         HTS_FSW    = case_when(
            StrLeft(SEX, 1) == 2 & StrLeft(EXPOSE_SEX_PAYMENT, 1) == 1 ~ 1,
            StrLeft(SEX, 1) == 2 & StrLeft(EXPOSE_SEX_PAYMENT, 1) == 2 ~ 1,
            TRUE ~ 0
         ),
         HTS_GENPOP = HTS_MSM + HTS_TGW + HTS_PWID + HTS_FSW,
         HTS_GENPOP = if_else(HTS_GENPOP > 0, 0, 1)
      )
}