##------------------------------------------------------------------------------
##  Form A
##------------------------------------------------------------------------------

id_col <- "REC_ID"
object <- tbl(dl_conn, "px_pii") %>%
   filter(
      DISEASE == "HIV",
      SERVICE_TYPE %in% c("HIV FBT", NA_character_),
      substr(MODULE, 1, 1) == "2",
      SNAPSHOT >= snapshot_old,
      SNAPSHOT <= snapshot_new,
      is.na(DELETED_BY)
   ) %>%
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
   left_join(
      y  = tbl(dl_conn, "px_faci_info") %>%
         filter(
            substr(MODALITY, 1, 6) == "101101",
            SNAPSHOT >= snapshot_old,
            SNAPSHOT <= snapshot_new,
         ) %>%
         select(
            REC_ID,
            MODALITY,
            SERVICE_FACI,
            SERVICE_SUB_FACI,
            REFER_TYPE,
            SERVICE_BY,
            CLINIC_NOTES,
            COUNSEL_NOTES
         ),
      by = "REC_ID"
   ) %>%
   collect() %>%
   left_join(
      y  = tbl(db_conn, "px_record") %>%
         filter(
            (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
               (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
               (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
         ) %>%
         select(REC_ID) %>%
         inner_join(
            y  = tbl(db_conn, "px_ob"),
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
   left_join(
      y  = tbl(db_conn, "px_record") %>%
         filter(
            (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
               (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
               (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
         ) %>%
         select(REC_ID) %>%
         inner_join(
            y  = tbl(db_conn, "px_confirm"),
            by = "REC_ID"
         ) %>%
         mutate(
            CONFIRM_TYPE   = case_when(
               CONFIRM_TYPE == "1" ~ "1_Central NRL",
               CONFIRM_TYPE == "2" ~ "2_CrCL",
               TRUE ~ as.character(CONFIRM_TYPE)
            ),
            CONFIRM_RESULT = case_when(
               stri_detect_fixed(CONFIRM_RESULT, "Positive") ~ "1_Positive",
               stri_detect_fixed(CONFIRM_RESULT, "Negative") ~ "2_Negative",
               stri_detect_fixed(CONFIRM_RESULT, "Inconclusive") ~ "3_Indeterminate",
               stri_detect_fixed(CONFIRM_RESULT, "Indeterminate") ~ "3_Indeterminate",
               stri_detect_fixed(CONFIRM_RESULT, "PENDING") ~ "4_Pending",
               TRUE ~ CONFIRM_RESULT
            ),
            CLIENT_TYPE    = case_when(
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
   left_join(
      y  = tbl(db_conn, "px_record") %>%
         filter(
            (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
               (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
               (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
         ) %>%
         select(REC_ID) %>%
         inner_join(
            y  = tbl(db_conn, "px_occupation"),
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
   left_join(
      y  = tbl(db_conn, "px_record") %>%
         filter(
            (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
               (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
               (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
         ) %>%
         select(REC_ID) %>%
         inner_join(
            y  = tbl(db_conn, "px_ofw"),
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
   left_join(
      y  = tbl(db_conn, "px_record") %>%
         filter(
            (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
               (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
               (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
         ) %>%
         select(REC_ID) %>%
         inner_join(
            y  = tbl(db_conn, "px_expose_hist"),
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
            )
         ) %>%
         pivot_wider(
            id_cols     = c("REC_ID", "EXPOSURE"),
            names_from  = EXPOSURE,
            values_from = c("IS_EXPOSED", "TYPE_LAST_EXPOSE")
         ) %>%
         select(
            REC_ID,
            EXPOSE_HIV_MOTHER      = IS_EXPOSED_120000,
            EXPOSE_SEX_F_NOCONDOM  = IS_EXPOSED_220200,
            EXPOSE_SEX_M_NOCONDOM  = IS_EXPOSED_210200,
            EXPOSE_SEX_HIV         = IS_EXPOSED_230003,
            EXPOSE_SEX_PAYING      = IS_EXPOSED_200010,
            EXPOSE_SEX_PAYMENT     = IS_EXPOSED_200020,
            EXPOSE_DRUG_INJECT     = IS_EXPOSED_301200,
            EXPOSE_BLOOD_TRANSFUSE = IS_EXPOSED_530000,
            EXPOSE_OCCUPATION      = IS_EXPOSED_510000,
            EXPOSE_TATTOO          = IS_EXPOSED_520000,
            EXPOSE_STI             = IS_EXPOSED_400000
         ) %>%
         collect(),
      by = "REC_ID"
   ) %>%
   left_join(
      y  = tbl(db_conn, "px_record") %>%
         filter(
            (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
               (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
               (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
         ) %>%
         select(REC_ID) %>%
         inner_join(
            y  = tbl(db_conn, "px_expose_profile"),
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
   left_join(
      y  = tbl(db_conn, "px_record") %>%
         filter(
            (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
               (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
               (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
         ) %>%
         select(REC_ID) %>%
         inner_join(
            y  = tbl(db_conn, "px_med_profile"),
            by = "REC_ID"
         ) %>%
         mutate(
            IS_PROFILE = case_when(
               IS_PROFILE == "0" ~ "0_No",
               IS_PROFILE == "1" ~ "1_Yes",
               TRUE ~ IS_PROFILE
            )
         ) %>%
         pivot_wider(
            id_cols      = c("REC_ID", "PROFILE"),
            names_from   = PROFILE,
            values_from  = IS_PROFILE,
            names_prefix = "IS_PROFILE_",
         ) %>%
         select(
            REC_ID,
            MED_TB_PX        = IS_PROFILE_1,
            MED_IS_PREGNANT  = IS_PROFILE_2,
            MED_HEP_B        = IS_PROFILE_3,
            MED_HEP_C        = IS_PROFILE_4,
            MED_CBS_REACTIVE = IS_PROFILE_5,
            MED_PREP_PX      = IS_PROFILE_6
         ) %>%
         collect(),
      by = "REC_ID"
   ) %>%
   left_join(
      y  = tbl(db_conn, "px_record") %>%
         filter(
            (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
               (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
               (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
         ) %>%
         select(REC_ID) %>%
         inner_join(
            y  = tbl(db_conn, "px_test_reason"),
            by = "REC_ID"
         ) %>%
         mutate(
            IS_REASON = case_when(
               IS_REASON == "0" ~ "0_No",
               IS_REASON == "1" ~ "1_Yes",
               TRUE ~ as.character(IS_REASON)
            )
         ) %>%
         pivot_wider(
            id_cols     = c("REC_ID", "REASON"),
            names_from  = REASON,
            values_from = c("IS_REASON", "REASON_OTHER"),
         ) %>%
         select(
            REC_ID,
            TEST_REASON_HIV_EXPOSE   = IS_REASON_1,
            TEST_REASON_PHYSICIAN    = IS_REASON_2,
            TEST_REASON_EMPLOY_OFW   = IS_REASON_3,
            TEST_REASON_EMPLOY_LOCAL = IS_REASON_4,
            TEST_REASON_INSURANCE    = IS_REASON_5,
            TEST_REASON_NO_REASON    = IS_REASON_6,
            TEST_REASON_RETEST       = IS_REASON_7,
            TEST_REASON_OTHER        = IS_REASON_8888,
            TEST_REASON_OTHER_TEXT   = REASON_OTHER_8888,
         ) %>%
         collect(),
      by = "REC_ID"
   ) %>%
   left_join(
      y  = tbl(db_conn, "px_record") %>%
         filter(
            (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
               (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
               (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
         ) %>%
         select(REC_ID) %>%
         inner_join(
            y  = tbl(db_conn, "px_prev_test"),
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
   left_join(
      y  = tbl(db_conn, "px_record") %>%
         filter(
            (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
               (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
               (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
         ) %>%
         select(REC_ID) %>%
         inner_join(
            y  = tbl(db_conn, "px_staging"),
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
   left_join(
      y  = tbl(db_conn, "px_record") %>%
         filter(
            (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
               (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
               (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
         ) %>%
         select(REC_ID) %>%
         inner_join(
            y  = tbl(db_conn, "px_test"),
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
   left_join(
      y  = tbl(db_conn, "px_record") %>%
         filter(
            (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
               (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
               (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
         ) %>%
         select(REC_ID) %>%
         inner_join(
            y  = tbl(db_conn, "px_test"),
            by = "REC_ID"
         ) %>%
         inner_join(
            y  = tbl(db_conn, "px_test_hiv") %>%
               select(
                  REC_ID,
                  TEST_TYPE,
                  TEST_NUM,
                  DATE_COLLECT,
                  DATE_RECEIVE,
                  FINAL_RESULT,
                  KIT_NAME
               ),
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
            DATE = DATE_PERFORM,
            DATE_COLLECT,
            DATE_RECEIVE,
            KIT_NAME,
            FINAL_RESULT
         ) %>%
         mutate(
            TEST_TYPE    = case_when(
               TEST_TYPE == "31" ~ "T1",
               TEST_TYPE == "32" ~ "T2",
               TEST_TYPE == "33" ~ "T3",
               TRUE ~ TEST_TYPE
            ),
            FINAL_RESULT = case_when(
               FINAL_RESULT %like% "1%" ~ "Positive / Reactive",
               FINAL_RESULT %like% "2%" ~ "Negative / Non-reactive",
               FINAL_RESULT %like% "3%" ~ "Indeterminate / Inconclusive",
               FINAL_RESULT %like% "0%" ~ "Invalid",
               TRUE ~ as.character(FINAL_RESULT)
            ),
            FINAL_RESULT = case_when(
               FINAL_RESULT == "Positive / Reactive" ~ "1",
               FINAL_RESULT == "Negative / Non-reactive" ~ "2",
               FINAL_RESULT == "Indeterminate / Inconclusive" ~ "3",
               FINAL_RESULT == "Invalid" ~ "0",
               TRUE ~ FINAL_RESULT
            ),
            FINAL_RESULT = case_when(
               FINAL_RESULT == "1" ~ "1_Positive / Reactive",
               FINAL_RESULT == "2" ~ "2_Negative / Non-reactive",
               FINAL_RESULT == "3" ~ "3_Indeterminate / Inconclusive",
               FINAL_RESULT == "0" ~ "0_Invalid",
               TRUE ~ FINAL_RESULT
            )
         ) %>%
         pivot_wider(
            id_cols     = c("REC_ID", "TEST_TYPE"),
            names_from  = TEST_TYPE,
            values_from = c("DATE", "DATE_COLLECT", "DATE_RECEIVE", "FINAL_RESULT", "KIT_NAME"),
            names_glue  = "{TEST_TYPE}_{.value}"
         ) %>%
         select(
            REC_ID,
            DATE_COLLECT = T1_DATE_COLLECT,
            DATE_RECEIVE = T1_DATE_RECEIVE,
            T1_DATE,
            T1_KIT       = T1_KIT_NAME,
            T1_RESULT    = T1_FINAL_RESULT,
            T2_DATE,
            T2_KIT       = T2_KIT_NAME,
            T2_RESULT    = T2_FINAL_RESULT,
            T3_DATE,
            T3_KIT       = T3_KIT_NAME,
            T3_RESULT    = T3_FINAL_RESULT
         ) %>%
         collect(),
      by = "REC_ID"
   ) %>%
   mutate(
      FORMA_MSM    = case_when(
         StrLeft(SEX, 1) == 1 & StrLeft(EXPOSE_SEX_M_NOCONDOM, 1) == 1 ~ 1,
         StrLeft(SEX, 1) == 1 & StrLeft(EXPOSE_SEX_M_NOCONDOM, 1) == 2 ~ 1,
         StrLeft(SEX, 1) == 1 &
            !is.na(NUM_M_PARTNER) &
            NUM_M_PARTNER > 0 ~ 1,
         StrLeft(SEX, 1) == 1 & !is.na(YR_LAST_M) ~ 1,
         TRUE ~ 0
      ),
      FORMA_TGW    = case_when(
         StrLeft(SEX, 1) == 1 & StrLeft(SELF_IDENT, 1) == 2 ~ 1,
         StrLeft(SEX, 1) == 1 & StrLeft(SELF_IDENT, 1) == 3 ~ 1,
         TRUE ~ 0
      ),
      FORMA_PWID   = case_when(
         StrLeft(EXPOSE_DRUG_INJECT, 1) == 1 ~ 1,
         StrLeft(EXPOSE_DRUG_INJECT, 1) == 2 ~ 1,
         TRUE ~ 0
      ),
      FORMA_FSW    = case_when(
         StrLeft(SEX, 1) == 2 & StrLeft(EXPOSE_SEX_PAYMENT, 1) == 1 ~ 1,
         StrLeft(SEX, 1) == 2 & StrLeft(EXPOSE_SEX_PAYMENT, 1) == 2 ~ 1,
         TRUE ~ 0
      ),
      FORMA_GENPOP = FORMA_MSM + FORMA_TGW + FORMA_PWID + FORMA_FSW,
      FORMA_GENPOP = if_else(FORMA_GENPOP > 0, 0, 1)
   )
