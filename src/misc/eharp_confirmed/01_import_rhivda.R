invisible(lapply(list.files(eharp$wd, full.names = TRUE), function(file) {
   tbl <- tools::file_path_sans_ext(basename(file))

   df <- read_delim(file, "\\t")
   if (nrow(df) == 0)
      df <- read_delim(file, "\t")

   .GlobalEnv$eharp$exports[[tbl]] <- df
}))

df <- eharp$exports$px_rhivda %>%
   filter(!is.na(rhivda_code)) %>%
   left_join(
      y  = ohasis$get_data("harp_dx", "2022", "06") %>%
         read_dta(col_select = c("labcode2", "PATIENT_ID", "REC_ID")) %>%
         select(
            rhivda_code = labcode2,
            REG_PID     = PATIENT_ID,
            REC_ID,
         ),
      by = "rhivda_code"
   ) %>%
   left_join(
      y  = eharp$px_confirm %>%
         select(
            REC_ID,
            CREATED_BY,
            CONF_CREATE = CREATED_AT
         ),
      by = "REC_ID"
   ) %>%
   mutate(
      KEEP = case_when(
         is.na(CREATED_BY) ~ 1,
         CREATED_BY == "1300000000" ~ 1,
         TRUE ~ 0
      )
   ) %>%
   filter(KEEP == 1) %>%
   select(
      -GUID,
      -ENDDA,
      -TIMESTAMPCREATED,
      -TIMESTAMPUPDATED,
      -COUNTER_LASTUPDATE,
      -INVALID,
   ) %>%
   left_join(
      y  = eharp$exports$px_info %>%
         select(
            PATIENT_ID,
            BEGDA,
            UIC,
            PHILHEALTH_NO,
            BIRTHDATE,
            SEX,
            PATIENT_SACCL,
            RHIVDA
         ),
      by = c("PATIENT_ID", "BEGDA")
   ) %>%
   left_join(
      y  = eharp$exports$px_name %>%
         select(
            PATIENT_ID,
            BEGDA,
            FIRSTNAME,
            MIDNAME,
            LASTNAME
         ),
      by = c("PATIENT_ID", "BEGDA")
   ) %>%
   left_join(
      y  = eharp$exports$px_profile_extra %>%
         select(
            PATIENT_ID,
            BEGDA,
            name_suffix,
            self_identity,
            gender_other_text,
            with_children,
            age,
            age_months
         ),
      by = c("PATIENT_ID", "BEGDA")
   ) %>%
   left_join(
      y  = eharp$exports$px_test %>%
         select(
            PATIENT_ID,
            BEGDA
         ) %>%
         mutate(px_test = 1),
      by = c("PATIENT_ID", "BEGDA")
   ) %>%
   left_join(
      y  = eharp$exports$px_test_2 %>%
         select(
            PATIENT_ID,
            BEGDA
         ) %>%
         mutate(px_test_2 = 1),
      by = c("PATIENT_ID", "BEGDA")
   ) %>%
   left_join(
      y  = eharp$oh_ids %>%
         select(
            PATIENT_ID = OLD_PATIENT_ID,
            OH_ID      = OHASIS_PATIENT_ID
         ),
      by = "PATIENT_ID"
   ) %>%
   mutate(
      OH_ID = case_when(
         !is.na(OH_ID) ~ OH_ID,
         !is.na(REG_PID) ~ REG_PID,
         TRUE ~ OH_ID
      )
   ) %>%
   relocate(REC_ID, OH_ID, .before = 1)

df1 <- df %>%
   filter(
      px_test == 1,
      is.na(px_test_2)
   ) %>%
   left_join(
      y  = eharp$exports$px_test %>%
         select(
            -GUID,
            -ENDDA,
            -TIMESTAMPCREATED,
            -TIMESTAMPUPDATED,
            -COUNTER_LASTUPDATE,
            -INVALID
         ),
      by = c("PATIENT_ID", "BEGDA")
   ) %>%
   mutate(rhivda_test = as.character(rhivda_test))

df2 <- df %>%
   filter(
      px_test_2 == 1
   ) %>%
   left_join(
      y  = eharp$exports$px_test_2 %>%
         select(
            -GUID,
            -ENDDA,
            -TIMESTAMPCREATED,
            -TIMESTAMPUPDATED,
            -COUNTER_LASTUPDATE,
            -INVALID
         ),
      by = c("PATIENT_ID", "BEGDA")
   )

eharp$data$records <- bind_rows(df1, df2) %>%
   rename(
      CONFIRMID = HUBID.x,
      HUBID     = HUBID.y
   ) %>%
   left_join(
      y  = eharp$exports$lab_hdr_2 %>%
         select(
            source_faci = LABID,
            SOURCE_FACI = NAME
         ),
      by = "source_faci"
   ) %>%
   relocate(SOURCE_FACI, .after = source_faci) %>%
   select(-source_faci) %>%
   mutate(
      nchar_hubid = nchar(HUBID)
   ) %>%
   left_join(
      y  = eharp$exports$faci_hdr_2 %>%
         select(
            CONFIRMID    = HUBID,
            CONFIRM_FACI = FACILITY_NAME
         ),
      by = "CONFIRMID"
   ) %>%
   left_join(
      y  = eharp$exports$faci_hdr_2 %>%
         select(
            HUBID,
            TEST_FACI_1 = FACILITY_NAME
         ),
      by = "HUBID"
   ) %>%
   left_join(
      y  = eharp$exports$lab_hdr_2 %>%
         select(
            HUBID       = LABID,
            TEST_FACI_2 = NAME
         ),
      by = "HUBID"
   ) %>%
   left_join(
      y  = eharp$exports$lab_hdr_2 %>%
         select(
            PREV_TEST_HUBID = LABID,
            PREV_TEST_FACI  = NAME
         ),
      by = "PREV_TEST_HUBID"
   ) %>%
   mutate(
      TEST_FACI = case_when(
         nchar_hubid == 3 ~ TEST_FACI_1,
         nchar_hubid == 6 ~ TEST_FACI_2,
         sample_source == "W" ~ CONFIRM_FACI,
         TRUE ~ SOURCE_FACI
      ),
      TEST_FACI = case_when(
         is.na(TEST_FACI) & !is.na(SOURCE_FACI) ~ SOURCE_FACI,
         is.na(TEST_FACI) & is.na(SOURCE_FACI) ~ CONFIRM_FACI,
         TRUE ~ TEST_FACI
      ),
      .after    = HUBID
   ) %>%
   select(
      -TEST_FACI_1,
      -TEST_FACI_2,
      -HUBID,
      -CONFIRMID,
      -rhivda_testing_faci,
      -nchar_hubid
   ) %>%
   left_join(
      y  = eharp$corr$STAFF %>%
         select(
            analyzed_by = PHYS_ID,
            SIGNATORY_1 = USER_ID
         ),
      by = "analyzed_by"
   ) %>%
   left_join(
      y  = eharp$corr$STAFF %>%
         select(
            reviewed_by = PHYS_ID,
            SIGNATORY_2 = USER_ID
         ),
      by = "reviewed_by"
   ) %>%
   left_join(
      y  = eharp$corr$STAFF %>%
         select(
            noted_by    = PHYS_ID,
            SIGNATORY_3 = USER_ID
         ),
      by = "noted_by"
   ) %>%
   arrange(rhivda_code, VISIT_DATE) %>%
   select(-OH_ID) %>%
   distinct(rhivda_code, .keep_all = TRUE) %>%
   left_join(
      y  = eharp$corr$PATIENT_ID %>%
         select(
            PATIENT_ID = EH_ID,
            OH_ID
         ),
      by = "PATIENT_ID"
   ) %>%
   left_join(
      y  = eharp$corr$REC_ID %>%
         mutate(RECORD_DATE = as.Date(RECORD_DATE)) %>%
         select(
            PATIENT_ID = EH_ID,
            OH_REC     = REC_ID,
            VISIT_DATE = RECORD_DATE,
         ),
      by = c("PATIENT_ID", "VISIT_DATE")
   ) %>%
   mutate(
      REC_ID = if_else(
         condition = !is.na(REC_ID),
         true      = REC_ID,
         false     = OH_REC,
         missing   = OH_REC
      )
   ) %>%
   left_join(
      y  = eharp$corr$TEST_FACI %>%
         select(
            CONFIRM_FACI = TEST_FACI,
            FACI_ID
         ),
      by = "CONFIRM_FACI"
   ) %>%
   left_join(
      y  = eharp$corr$TEST_FACI %>%
         select(
            SOURCE_FACI = TEST_FACI,
            SOURCE      = FACI_ID
         ),
      by = "SOURCE_FACI"
   ) %>%
   left_join(
      y  = eharp$corr$TEST_FACI %>%
         select(
            TEST_FACI,
            DX_FACI = FACI_ID
         ),
      by = "TEST_FACI"
   ) %>%
   rename(
      EH_ID                    = PATIENT_ID,
      PATIENT_ID               = OH_ID,
      RECORD_DATE              = VISIT_DATE,
      FIRST                    = FIRSTNAME,
      MIDDLE                   = MIDNAME,
      LAST                     = LASTNAME,
      SUFFIX                   = name_suffix,
      PATIENT_CODE             = patient_code,
      SELF_IDENT               = self_identity,
      SELF_IDENT_OTHER         = gender_other_text,
      CONFIRM_CODE             = rhivda_code,
      SPECIMEN_REFER_TYPE      = sample_source,
      DATE_COLLECT             = blood_extract_date,
      DATE_RELEASE             = release_date,
      FINAL_RESULT             = final_interpretation,
      REMARKS                  = remarks,
      CHILDREN                 = CHILD_COUNT,
      AGE                      = age,
      AGE_MO                   = age_months,
      NATIONALITY              = COUNTRY,
      CIVIL_STATUS             = CIVIL_STAT,
      LIVING_WITH_PARTNER      = WITH_PARTNER,
      OFW_YR_RET               = OFW_END_DATE,
      OFW_STATION              = OFW_LOCATION,
      EXPOSE_HIV_MOTHER        = IS_MOM_HIV,
      EXPOSE_DRUG_INJECT       = IS_INJ_DRUG,
      EXPOSE_SEX_WITH_HIV      = is_sex_known,
      EXPOSE_SEX_M             = IS_SEX_HE,
      EXPOSE_SEX_F             = IS_SEX_SHE,
      EXPOSE_SEX_PAYING        = IS_SEX_WPROST,
      EXPOSE_SEX_PAYMENT       = IS_PROST,
      EXPOSE_BLOOD_TRANSFUSION = IS_BLOOD_TRANS,
      EXPOSE_OCCUPATION        = IS_ACC_PRICK2,
      EXPOSE_TATTOO            = is_tattoo,
      EXPOSE_STI               = IS_STI,
      NUM_M_PARTNER            = HE_COUNT,
      YR_LAST_M                = HE_YEAR,
      NUM_F_PARTNER            = SHE_COUNT,
      YR_LAST_F                = SHE_YEAR,
      AGE_FIRST_SEX            = age_first_sex,
      AGE_FIRST_INJECT         = age_injecting,
      IS_STUDENT               = in_school,
      MED_TB_PX                = IS_TB_PAT,
      # MED_PREGNANT             = preg,
      MED_HEP_B                = is_history_hepb,
      MED_HEP_C                = is_history_hepc,
      MED_CBS_REACTIVE         = is_history_cbs,
      MED_PREP_PX              = is_history_prep,
      TEST_REASON_HIV_EXPOSE   = is_reason_exposure,
      TEST_REASON_PHYSICIAN    = IS_REC_PHYS,
      TEST_RETEST              = IS_CONFIRM,
      TEST_EMPLOY_OFW          = IS_EMP_OFW,
      TEST_EMPLOY_LOCAL        = IS_EMP_PH,
      TEST_INSURANCE           = IS_REQ_INSU,
      TEST_NO_REASON           = IS_NOREAS,
      TEST_OTHER               = IS_OTHERS,
      TEST_OTHER_TEXT          = IS_OTH_TXT,
      PREV_TESTED              = previously_tested,
      PREV_TEST_DATE           = last_test_date,
      CLINICAL_PIC             = PATIENT_CLINIC_PIC,
      SYMPTOMS                 = PATIENT_CLINIC_PIC_TEXT,
      CLIENT_TYPE              = patient_type,
      REFER_TYPE               = referred_by,
      WHO_CLASS                = WHO_STAGING,
      IS_PREGNANT              = IS_PREGGY,
      DATE_CONFIRM             = RHIVDA_DATE
   ) %>%
   select(
      -starts_with("M_"),
      -starts_with("F_"),
   ) %>%
   # group_by(RECORD_DATE, SIGNATORY_1) %>%
   # mutate(
   #    .after       = REC_ID,
   #    CREATED_TIME = stri_pad_left(row_number(), 6, "0"),
   # ) %>%
   # ungroup() %>%
   distinct(CONFIRM_CODE, .keep_all = TRUE) %>%
   select(-starts_with("CREATED_AT")) %>%
   mutate(
      .after              = REC_ID,
      CREATED_DATE        = RECORD_DATE,
      CREATED_TIME        = substr(REC_ID, 9, 14),
      CREATED_TIME        = paste(
         sep = ":",
         substr(CREATED_TIME, 1, 2),
         substr(CREATED_TIME, 3, 4),
         substr(CREATED_TIME, 5, 6)
      ),
      CREATED_AT          = if_else(
         condition = is.na(CONF_CREATE),
         true      = "2022-07-25 08:30:00",
         false     = as.character(CONF_CREATE),
         missing   = as.character(CONF_CREATE)
      ),
      CREATED_BY          = "1300000000",
      UPDATED_BY          = "1300000000",
      UPDATED_AT          = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      # REC_ID              = if_else(
      #    condition = is.na(REC_ID),
      #    true      = paste(
      #       sep = "_",
      #       gsub("[^[:digit:]]", "", CREATED_AT),
      #       SIGNATORY_1
      #    ),
      #    false     = REC_ID,
      #    missing   = REC_ID
      # ),
      DISEASE             = "101000",
      SERVICE_TYPE        = "101101",
      SPECIMEN_REFER_TYPE = case_when(
         SPECIMEN_REFER_TYPE == "W" ~ 2,
         SPECIMEN_REFER_TYPE == "R" ~ 4,
      ),
      FORM                = "Form A",
      VERSION             = "2017",
      MODULE              = "2",
      CONFIRMATORY_CODE   = CONFIRM_CODE,
      CONFIRM_TYPE        = "2",
      SEX                 = case_when(
         SEX == "M" ~ "1",
         SEX == "F" ~ "2",
      ),
      SELF_IDENT          = case_when(
         SELF_IDENT == "M" ~ "1",
         SELF_IDENT == "F" ~ "2",
         SELF_IDENT == "O" ~ "3",
      ),
      CLINICAL_PIC        = CLINICAL_PIC + 1,
      WORK_TEXT           = case_when(
         !is.na(WORK) ~ WORK,
         !is.na(PREV_WORK) ~ PREV_WORK,
      ),
      FINAL_RESULT = if_else(
         condition = FINAL_RESULT == "Nonreactive",
         true      = "Negative",
         false     = FINAL_RESULT,
         missing   = FINAL_RESULT
      ),
   )

