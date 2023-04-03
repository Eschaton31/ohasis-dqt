##  CFBS Form ------------------------------------------------------------------

continue <- 0
id_col   <- "REC_ID"
# px identifiers (demographics, address, etc.)
object   <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
   # keep only hts form
   filter(
      DISEASE == "HIV",
      substr(MODULE, 1, 1) == "2",
      FORM_VERSION == "CFBS Form (v2020)",
      SNAPSHOT >= snapshot_old,
      SNAPSHOT <= snapshot_new,
      is.na(DELETED_BY)
   )

records <- tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
   filter(
      DISEASE == "101000",
      MODULE == 2,
      (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
         (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new),
      is.na(DELETED_BY)
   ) %>%
   select(REC_ID)

for_delete <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
   filter(
      DISEASE == "HIV",
      FORM_VERSION == "CFBS Form (v2020)",
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
      select(-any_of(c("CLIENT_MOBILE", "CLIENT_EMAIL"))) %>%
      rename_at(
         .vars = vars(starts_with("SERVICE_")),
         ~paste0("HIV_", .)
      ) %>%
      select(
         -starts_with("DEATH_"),
         -starts_with("BIRTH_"),
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
               SERVICE_BY,
               CLINIC_NOTES,
               COUNSEL_NOTES
            ),
         by = "REC_ID"
      ) %>%
      collect() %>%
      # agree data
      left_join(
         y  = records %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_cfbs")),
               by = "REC_ID"
            ) %>%
            mutate(
               SCREEN_AGREED = case_when(
                  SCREEN_AGREED == "0" ~ "0_No",
                  SCREEN_AGREED == "1" ~ "1_Yes"
               ),
               SCREEN_REFER  = case_when(
                  SCREEN_REFER == "0" ~ "0_Client opted to return some other time",
                  SCREEN_REFER == "1" ~ "1_Client was accompanied to receiving facility"
               )
            ) %>%
            select(
               REC_ID,
               SCREEN_AGREED,
               SCREEN_REFER,
               PARTNER_REFERRAL_FACI = PARTNER_FACI
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # contact info
      left_join(
         y  = records %>%
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
               id_cols     = REC_ID,
               names_from  = CONTACT_TYPE,
               values_from = CONTACT,
               names_glue  = "CLIENT_{CONTACT_TYPE}_{.value}"
            ) %>%
            rename_at(
               .vars = vars(ends_with("_CONTACT")),
               ~stri_replace_all_fixed(., "_CONTACT", "")
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
         y  = full_table(db_conn, records, "px_consent") %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # exposure history
      left_join(
         y  = records %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_expose_hist")),
               by = "REC_ID"
            ) %>%
            mutate(
               IS_EXPOSED = case_when(
                  TYPE_LAST_EXPOSE == "4" & IS_EXPOSED == "1" ~ "1",
                  TYPE_LAST_EXPOSE == "3" & IS_EXPOSED == "1" ~ "3",
                  TYPE_LAST_EXPOSE == "1" & IS_EXPOSED == "1" ~ "4",
                  TYPE_LAST_EXPOSE == "0" & IS_EXPOSED == "1" ~ "2",
                  is.na(TYPE_LAST_EXPOSE) & IS_EXPOSED == "1" ~ "2",
                  IS_EXPOSED == "0" ~ "0",
                  TRUE ~ NA_character_
               ),
               IS_EXPOSED = case_when(
                  IS_EXPOSED == "1" ~ "1_Yes, within the past 12 months",
                  IS_EXPOSED == "2" ~ "2_Yes",
                  IS_EXPOSED == "3" ~ "3_Yes, within the past 6 months",
                  IS_EXPOSED == "4" ~ "4_Yes, within the past 30 days",
                  IS_EXPOSED == "0" ~ "0_No",
                  TRUE ~ NA_character_
               ),
            ) %>%
            pivot_wider(
               id_cols     = REC_ID,
               names_from  = EXPOSURE,
               values_from = c(IS_EXPOSED, DATE_LAST_EXPOSE)
            ) %>%
            rename_all(
               ~case_when(
                  . == "IS_EXPOSED_200000" ~ "RISK_SEX_EVER",
                  . == "IS_EXPOSED_210000" ~ "RISK_M_SEX_ORAL_ANAL",
                  . == "IS_EXPOSED_200020" ~ "RISK_SEX_PAYMENT",
                  . == "DATE_LAST_EXPOSE_200020" ~ "RISK_SEX_PAYMENT_DATE",
                  . == "IS_EXPOSED_312000" ~ "RISK_DRUG_INJECT",
                  . == "IS_EXPOSED_201200" ~ "RISK_CONDOMLESS_ANAL",
                  . == "DATE_LAST_EXPOSE_201200" ~ "RISK_CONDOMLESS_ANAL_DATE",
                  . == "IS_EXPOSED_202200" ~ "RISK_CONDOMLESS_VAGINAL",
                  . == "DATE_LAST_EXPOSE_202200" ~ "RISK_CONDOMLESS_VAGINAL_DATE",
                  . == "IS_EXPOSED_301010" ~ "RISK_NEEDLE_SHARE",
                  . == "DATE_LAST_EXPOSE_301010" ~ "RISK_NEEDLE_SHARE_DATE",
                  . == "IS_EXPOSED_310000" ~ "RISK_ILLICIT_DRUGS",
                  . == "DATE_LAST_EXPOSE_310000" ~ "RISK_ILLICIT_DRUGS_DATE",
                  . == "IS_EXPOSED_200001" ~ "RISK_SEX_HIV",
                  . == "DATE_LAST_EXPOSE_200001" ~ "RISK_SEX_HIV_DATE",
                  TRUE ~ .
               )
            ) %>%
            select(
               REC_ID,
               starts_with("RISK")
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # expose profile
      left_join(
         y  = records %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_expose_profile")),
               by = "REC_ID"
            ) %>%
            select(
               REC_ID,
               NUM_F_PARTNER,
               NUM_M_PARTNER
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # previous test data
      left_join(
         y  = records %>%
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
      # t0 data
      left_join(
         y  = records %>%
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
                  RESULT == "1" ~ "1_Positive",
                  RESULT == "2" ~ "2_Negative",
                  TRUE ~ RESULT
               )
            ) %>%
            select(
               REC_ID,
               TEST_DATE   = DATE_PERFORM,
               TEST_RESULT = RESULT
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # services
      left_join(
         y  = records %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_other_service")),
               by = "REC_ID"
            ) %>%
            mutate(
               GIVEN = case_when(
                  GIVEN == "1" ~ "1_Yes",
                  GIVEN == "0" ~ "0_No",
                  TRUE ~ as.character(GIVEN)
               ),
            ) %>%
            pivot_wider(
               id_cols     = REC_ID,
               names_from  = SERVICE,
               values_from = c(GIVEN, OTHER_SERVICE)
            ) %>%
            rename_all(
               ~case_when(
                  . == "GIVEN_1013" ~ "SERVICE_HIV_101",
                  . == "GIVEN_1004" ~ "SERVICE_IEC_MATS",
                  . == "GIVEN_1002" ~ "SERVICE_RISK_COUNSEL",
                  . == "GIVEN_5001" ~ "SERVICE_PREP_REFER",
                  . == "OTHER_SERVICE_2001" ~ "SERVICE_GIVEN_CONDOMS",
                  . == "OTHER_SERVICE_2002" ~ "SERVICE_GIVEN_LUBES",
                  TRUE ~ .
               )
            ) %>%
            select(
               REC_ID,
               starts_with("SERVICE_"),
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # refusal
      left_join(
         y  = records %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_test_refuse")),
               by = "REC_ID"
            ) %>%
            mutate(
               IS_REASON = case_when(
                  IS_REASON == "0" ~ "0_No",
                  IS_REASON == "1" ~ "1_Yes"
               )
            ) %>%
            pivot_wider(
               id_cols     = REC_ID,
               names_from  = REASON,
               values_from = c(IS_REASON, REASON_OTHER),
            ) %>%
            rename_all(
               ~case_when(
                  . == "IS_REASON_2002" ~ "TEST_REFUSE_FEAR_MSM",
                  . == "IS_REASON_3002" ~ "TEST_REFUSE_NO_TIME",
                  . == "IS_REASON_2003" ~ "TEST_REFUSE_FEAR_RESULT",
                  . == "IS_REASON_2004" ~ "TEST_REFUSE_FEAR_DISCLOSE",
                  . == "IS_REASON_3003" ~ "TEST_REFUSE_NO_CURE",
                  . == "IS_REASON_8888" ~ "TEST_REFUSE_OTHER",
                  . == "REASON_OTHER_8888" ~ "TEST_REFUSE_OTHER_TEXT",
                  TRUE ~ .
               )
            ) %>%
            select(
               REC_ID,
               starts_with("TEST_REFUSE")
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # ohasis KP tagging
      mutate(
         CFBS_MSM    = case_when(
            StrLeft(SEX, 1) == 1 & StrLeft(RISK_M_SEX_ORAL_ANAL, 1) %in% c("1", "2") ~ 1,
            StrLeft(SEX, 1) == 1 & StrLeft(RISK_CONDOMLESS_ANAL, 1) %in% c("1", "2") ~ 1,
            StrLeft(SEX, 1) == 1 & !is.na(RISK_CONDOMLESS_ANAL_DATE) ~ 1,
            StrLeft(SEX, 1) == 1 &
               !is.na(NUM_M_PARTNER) &
               NUM_M_PARTNER > 0 ~ 1,
            TRUE ~ 0
         ),
         CFBS_TGW    = case_when(
            StrLeft(SEX, 1) == 1 & StrLeft(SELF_IDENT, 1) == 2 ~ 1,
            StrLeft(SEX, 1) == 1 & StrLeft(SELF_IDENT, 1) == 3 ~ 1,
            TRUE ~ 0
         ),
         CFBS_PWID   = case_when(
            StrLeft(RISK_NEEDLE_SHARE, 1) == 1 ~ 1,
            !is.na(RISK_NEEDLE_SHARE_DATE) ~ 1,
            TRUE ~ 0
         ),
         CFBS_FSW    = case_when(
            StrLeft(SEX, 1) == 2 & StrLeft(RISK_SEX_PAYMENT, 1) %in% c("1", "2") ~ 1,
            StrLeft(SEX, 1) == 2 & !is.na(RISK_SEX_PAYMENT_DATE) ~ 1,
            TRUE ~ 0
         ),
         CFBS_GENPOP = CFBS_MSM + CFBS_TGW + CFBS_PWID + CFBS_FSW,
         CFBS_GENPOP = if_else(CFBS_GENPOP > 0, 0, 1)
      )
}