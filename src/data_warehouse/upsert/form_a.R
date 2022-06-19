##  Form A ---------------------------------------------------------------------

continue <- 0
id_col   <- "REC_ID"
# px identifiers (demographics, address, etc.)
object   <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
   filter(
      DISEASE == "HIV",
      SERVICE_TYPE %in% c("HIV FBT", NA_character_),
      substr(MODULE, 1, 1) == "2",
      SNAPSHOT >= snapshot_old,
      SNAPSHOT <= snapshot_new,
      is.na(DELETED_BY)
   )

for_delete_1 <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
   filter(
      DISEASE == "HIV",
      SERVICE_TYPE %in% c("HIV FBT", NA_character_),
      substr(MODULE, 1, 1) == "2",
      SNAPSHOT >= snapshot_old,
      SNAPSHOT <= snapshot_new,
      !is.na(DELETED_BY)
   ) %>%
   select(REC_ID) %>%
   collect()

for_delete_2 <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
   inner_join(
      y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_a")) %>%
         select(REC_ID),
      by = "REC_ID"
   ) %>%
   filter(FORM_VERSION != "Form A (v2017)") %>%
   select(REC_ID) %>%
   collect()

for_delete_3 <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
   filter(
      !is.na(DELETED_BY)
   ) %>%
   inner_join(
      y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_a")) %>%
         select(REC_ID),
      by = "REC_ID"
   ) %>%
   select(REC_ID) %>%
   collect()

for_delete <- bind_rows(for_delete_1, for_delete_2, for_delete_3)

# get number of affected rows
if ((object %>% count() %>% collect())$n > 0) {
   continue <- 1
   object   <- object %>%
      # keep only form a
      mutate(
         FORMA = case_when(
            FORM_VERSION == "Form A (v2017)" ~ 1,
            FORM_VERSION == "HTS Form (v2021)" ~ 0,
            FORM_VERSION == "CFBS Form (v2020)" ~ 0,
            FORM_VERSION == "Form BC (v2017)" ~ 0,
            is.na(FORM_VERSION) ~ 1,
            TRUE ~ 1
         )
      ) %>%
      filter(
         FORMA == 1
      ) %>%
      select(
         -starts_with("SERVICE_"),
         -starts_with("DEATH_"),
         -FORMA
      ) %>%
      # facility data in form
      left_join(
         y  = tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_faci_info")) %>%
            filter(
               substr(MODALITY, 1, 6) == "101101"
            ) %>%
            select(
               REC_ID,
               MODALITY,
               SERVICE_FACI,
               SERVICE_SUB_FACI,
               REFER_TYPE,
               SERVICE_BY,
               CLIENT_TYPE,
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
      # occupation section
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
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
                  IS_EXPOSED == "1" ~ "1_Yes, within the past 12 months",
                  IS_EXPOSED == "2" ~ "2_Yes",
                  IS_EXPOSED == "3" ~ "3_Yes, within the past 6 months",
                  IS_EXPOSED == "4" ~ "4_Yes, within the past 30 days",
                  IS_EXPOSED == "0" ~ "0_No",
                  IS_EXPOSED == "99999" ~ NA_character_,
                  TRUE ~ IS_EXPOSED
               ),
               EXPOSURE   = case_when(
                  EXPOSURE == "120000" ~ "HIV_MOTHER",
                  EXPOSURE == "220200" ~ "SEX_F_NOCONDOM",
                  EXPOSURE == "210200" ~ "SEX_M_NOCONDOM",
                  EXPOSURE == "230003" ~ "SEX_HIV",
                  EXPOSURE == "200010" ~ "SEX_PAYING",
                  EXPOSURE == "200020" ~ "SEX_PAYMENT",
                  EXPOSURE == "301200" ~ "DRUG_INJECT",
                  EXPOSURE == "530000" ~ "BLOOD_TRANSFUSE",
                  EXPOSURE == "510000" ~ "OCCUPATION",
                  EXPOSURE == "520000" ~ "TATTOO",
                  EXPOSURE == "400000" ~ "STI",
                  TRUE ~ EXPOSURE
               )
            ) %>%
            pivot_wider(
               id_cols     = c(REC_ID, EXPOSURE),
               names_from  = EXPOSURE,
               values_from = IS_EXPOSED,
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
      # exposure profile
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_expose_profile")),
               by = "REC_ID"
            ) %>%
            select(
               REC_ID,
               AGE_FIRST_SEX,
               NUM_F_PARTNER,
               YR_LAST_F,
               NUM_M_PARTNER,
               YR_LAST_M,
               AGE_FIRST_INJECT
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
                  PROFILE == "2" ~ "IS_PREGNANT",
                  PROFILE == "3" ~ "HEP_B",
                  PROFILE == "4" ~ "HEP_C",
                  PROFILE == "5" ~ "CBS_REACTIVE",
                  PROFILE == "6" ~ "PREP_PX",
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
      # t0 data
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
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
         FORMA_MSM    = if ("EXPOSE_SEX_M_NOCONDOM" %in% colnames(.)) {
            case_when(
               StrLeft(SEX, 1) == 1 & StrLeft(EXPOSE_SEX_M_NOCONDOM, 1) == 1 ~ 1,
               StrLeft(SEX, 1) == 1 & StrLeft(EXPOSE_SEX_M_NOCONDOM, 1) == 2 ~ 1,
               StrLeft(SEX, 1) == 1 &
                  !is.na(NUM_M_PARTNER) &
                  NUM_M_PARTNER > 0 ~ 1,
               StrLeft(SEX, 1) == 1 & !is.na(YR_LAST_M) ~ 1,
               TRUE ~ 0
            )
         } else {
            case_when(
               StrLeft(SEX, 1) == 1 &
                  !is.na(NUM_M_PARTNER) &
                  NUM_M_PARTNER > 0 ~ 1,
               StrLeft(SEX, 1) == 1 & !is.na(YR_LAST_M) ~ 1,
               TRUE ~ 0
            )
         },
         FORMA_TGW    = case_when(
            StrLeft(SEX, 1) == 1 & StrLeft(SELF_IDENT, 1) == 2 ~ 1,
            StrLeft(SEX, 1) == 1 & StrLeft(SELF_IDENT, 1) == 3 ~ 1,
            TRUE ~ 0
         ),
         FORMA_PWID   = if ("EXPOSE_DRUG_INJECT" %in% colnames(.)) {
            case_when(
               StrLeft(EXPOSE_DRUG_INJECT, 1) == 1 ~ 1,
               StrLeft(EXPOSE_DRUG_INJECT, 1) == 2 ~ 1,
               TRUE ~ 0
            )
         } else {
            0
         },
         FORMA_FSW    = if ("EXPOSE_SEX_PAYMENT" %in% colnames(.)) {
            case_when(
               StrLeft(SEX, 1) == 2 & StrLeft(EXPOSE_SEX_PAYMENT, 1) == 1 ~ 1,
               StrLeft(SEX, 1) == 2 & StrLeft(EXPOSE_SEX_PAYMENT, 1) == 2 ~ 1,
               TRUE ~ 0
            )
         } else {
            0
         },
         FORMA_GENPOP = FORMA_MSM + FORMA_TGW + FORMA_PWID + FORMA_FSW,
         FORMA_GENPOP = if_else(FORMA_GENPOP > 0, 0, 1)
      )
}