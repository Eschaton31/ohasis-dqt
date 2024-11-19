encoded <- get_encoded(ohasis$ym, "Registry")

encoded$STAFF <- read_sheet("1BRohoSaBE73zwRMXQNcWeRf5rC2OcePS64A67ODfxXI")

encoded$data$enrollees <- encoded$FORMS %>%
   filter(
      !is.na(CREATED_TIME),
      nchar(PATIENT_ID) != 18 | is.na(PATIENT_ID)
   ) %>%
   left_join(
      y  = encoded$ref_faci %>%
         select(-encoder) %>%
         distinct_all(),
      by = c("TX_FACI" = "FACI_CODE")
   )

encoded$data$enrollees$PATIENT_ID <- NA_character_
db_conn                           <- ohasis$conn("db")
for (i in seq_len(nrow(encoded$data$enrollees)))
   encoded$data$enrollees[i, "PATIENT_ID"] <- oh_px_id(db_conn, as.character(encoded$data$enrollees[i, "FACI_ID"]))

write_clip(
   encoded$data$enrollees %>%
      select(PAGE_ID, PATIENT_ID, encoder)
)


##  Tables ---------------------------------------------------------------------

encoded$data$records <- encoded$HTS_FINAL %>%
   rename_all(
      ~case_when(
         . == "PREV_TESTED_FACI" ~ "PREVTEST_FACI",
         . == "SEX_M_AV_NOCONDOM_DATE" ~ "EXPOSE_SEX_M_AV_NOCONDOM_DATE",
         . == "SEX_M_AV_DATE" ~ "EXPOSE_SEX_M_AV_DATE",
         . == "EXPOSE_SEX_M" ~ "EXPOSE_SEX_M",
         . == "EXPOSE_HIV_MOTHER" ~ "EXPOSE_HIV_MOTHER",
         . == "EXPOSE_SEX_F" ~ "EXPOSE_SEX_F",
         . == "SEX_F_AV_DATE" ~ "EXPOSE_SEX_F_AV_DATE",
         . == "SEX_F_AV_NOCONDOM_DATE" ~ "EXPOSE_SEX_F_AV_NOCONDOM_DATE",
         . == "EXPOSE_SEX_PAYING" ~ "EXPOSE_SEX_PAYING",
         . == "SEX_PAYING_DATE" ~ "EXPOSE_SEX_PAYING_DATE",
         . == "EXPOSE_SEX_PAYMENT" ~ "EXPOSE_SEX_PAYMENT",
         . == "SEX_PAYMENT_DATE" ~ "EXPOSE_SEX_PAYMENT_DATE",
         . == "EXPOSE_SEX_DRUGS" ~ "EXPOSE_SEX_DRUGS",
         . == "SEX_DRUGS_DATE" ~ "EXPOSE_SEX_DRUGS_DATE",
         . == "EXPOSE_DRUG_INJECT" ~ "EXPOSE_DRUG_INJECT",
         . == "DRUG_INJECT_DATE" ~ "EXPOSE_DRUG_INJECT_DATE",
         . == "EXPOSE_BLOOD_TRANSFUSION" ~ "EXPOSE_BLOOD_TRANSFUSION",
         . == "BLOOD_TRANSFUSION_DATE" ~ "EXPOSE_BLOOD_TRANSFUSION_DATE",
         . == "EXPOSE_OCCUPATION" ~ "EXPOSE_OCCUPATION",
         . == "OCCUPATION_DATE" ~ "EXPOSE_OCCUPATION_DATE",
         . == "SEX_F_NOCONDOM" ~ "EXPOSE_SEX_F_NOCONDOM",
         . == "SEX_M_NOCONDOM" ~ "EXPOSE_SEX_M_NOCONDOM",
         . == "SEX_WITH_HIV" ~ "EXPOSE_SEX_WITH_HIV",
         . == "SEX_PAYING" ~ "EXPOSE_SEX_PAYING",
         . == "SEX_PAYMENT" ~ "EXPOSE_SEX_PAYMENT",
         . == "DRUG_INJECT" ~ "EXPOSE_DRUG_INJECT",
         . == "BLOOD_TRANSFUSION" ~ "EXPOSE_BLOOD_TRANSFUSION",
         . == "TATTOO" ~ "EXPOSE_TATTOO",
         . == "STI_STD" ~ "EXPOSE_STI",
         . == "MED_PrEP" ~ "MED_PEP_PX",
         . == "PREV_TESTED_YEAR" ~ "PREVTEST_YEAR",
         . == "PREV_TESTED_MONTH" ~ "PREVTEST_MONTH",
         . == "PREV_TESTED_RESULT" ~ "PREVTEST_RESULT",
         TRUE ~ .
      )
   ) %>%
   mutate(
      FORM    = "HTS Form",
      VERSION = "2021"
   ) %>%
   bind_rows(
      encoded$FORMA_FINAL %>%
         rename_all(
            ~case_when(
               . == "PREV_TESTED_FACI" ~ "PREVTEST_FACI",
               . == "SEX_M_AV_NOCONDOM_DATE" ~ "EXPOSE_SEX_M_AV_NOCONDOM_DATE",
               . == "SEX_M_AV_DATE" ~ "EXPOSE_SEX_M_AV_DATE",
               . == "EXPOSE_SEX_M" ~ "EXPOSE_SEX_M",
               . == "EXPOSE_HIV_MOTHER" ~ "EXPOSE_HIV_MOTHER",
               . == "EXPOSE_SEX_F" ~ "EXPOSE_SEX_F",
               . == "SEX_F_AV_DATE" ~ "EXPOSE_SEX_F_AV_DATE",
               . == "SEX_F_AV_NOCONDOM_DATE" ~ "EXPOSE_SEX_F_AV_NOCONDOM_DATE",
               . == "EXPOSE_SEX_PAYING" ~ "EXPOSE_SEX_PAYING",
               . == "SEX_PAYING_DATE" ~ "EXPOSE_SEX_PAYING_DATE",
               . == "EXPOSE_SEX_PAYMENT" ~ "EXPOSE_SEX_PAYMENT",
               . == "SEX_PAYMENT_DATE" ~ "EXPOSE_SEX_PAYMENT_DATE",
               . == "EXPOSE_SEX_DRUGS" ~ "EXPOSE_SEX_DRUGS",
               . == "SEX_DRUGS_DATE" ~ "EXPOSE_SEX_DRUGS_DATE",
               . == "EXPOSE_DRUG_INJECT" ~ "EXPOSE_DRUG_INJECT",
               . == "DRUG_INJECT_DATE" ~ "EXPOSE_DRUG_INJECT_DATE",
               . == "EXPOSE_BLOOD_TRANSFUSION" ~ "EXPOSE_BLOOD_TRANSFUSION",
               . == "BLOOD_TRANSFUSION_DATE" ~ "EXPOSE_BLOOD_TRANSFUSION_DATE",
               . == "EXPOSE_OCCUPATION" ~ "EXPOSE_OCCUPATION",
               . == "OCCUPATION_DATE" ~ "EXPOSE_OCCUPATION_DATE",
               . == "SEX_F_NOCONDOM" ~ "EXPOSE_SEX_F_NOCONDOM",
               . == "SEX_M_NOCONDOM" ~ "EXPOSE_SEX_M_NOCONDOM",
               . == "SEX_WITH_HIV" ~ "EXPOSE_SEX_WITH_HIV",
               . == "SEX_PAYING" ~ "EXPOSE_SEX_PAYING",
               . == "SEX_PAYMENT" ~ "EXPOSE_SEX_PAYMENT",
               . == "DRUG_INJECT" ~ "EXPOSE_DRUG_INJECT",
               . == "BLOOD_TRANSFUSION" ~ "EXPOSE_BLOOD_TRANSFUSION",
               . == "TATTOO" ~ "EXPOSE_TATTOO",
               . == "STI_STD" ~ "EXPOSE_STI",
               . == "MED_PrEP" ~ "MED_PEP_PX",
               . == "PREV_TESTED_YEAR" ~ "PREVTEST_YEAR",
               . == "PREV_TESTED_MONTH" ~ "PREVTEST_MONTH",
               . == "PREV_TESTED_RESULT" ~ "PREVTEST_RESULT",
               TRUE ~ .
            )
         ) %>%
         mutate(
            FORM    = "Form A",
            VERSION = "2017"
         )
   ) %>%
   filter(
      !is.na(CREATED_TIME),
      nchar(PATIENT_ID) == 18
   ) %>%
   mutate_at(
      .vars = vars(starts_with("EXPOSE") & !contains("DATE")),
      ~case_when(
         FORM == "Form A" & toupper(.) == "WITHIN PAST12MONTHS" ~ "1_Yes, within the past 12 months",
         FORM == "Form A" & toupper(.) == "MORE THAN 12MONTHS" ~ "2_Yes",
         FORM == "Form A" & toupper(.) == "NO" ~ "0_No",
         FORM == "HTS Form" & toupper(.) == "1_YES" ~ "2_Yes",
         is.na(.) ~ "99999_NA",
         TRUE ~ .
      )
   ) %>%
   mutate_at(
      .vars = vars(starts_with("EXPOSE") & contains("DATE")),
      ~case_when(
         nchar(.) == 4 ~ paste(sep = "-", ., "01", "01"),
         nchar(.) == 10 & stri_detect_fixed(., "-") ~ .,
         TRUE ~ NA_character_
      ) %>% as.Date()
   ) %>%
   mutate_at(
      .vars = vars(contains("RESULT")),
      ~case_when(
         . == "Reactive" ~ 1,
         . == "Non-reactive" ~ 2,
         . == "Indeterminate" ~ 3,
         . == "Unable to get result" ~ 4,
      )
   ) %>%
   mutate(
      encoder   = stri_replace_all_fixed(encoder, glue("{ohasis$ym}(Form A/HTS)_"), ""),
      FACI_ID   = "130000",
      TEST_FACI = if_else(
         condition = StrIsNumeric(str_left(TEST_FACI, 6)),
         true      = str_left(TEST_FACI, 6),
         false     = NA_character_,
         missing   = NA_character_
      ),
   ) %>%
   left_join(
      y  = ohasis$ref_country %>%
         select(
            NATIONALITY = COUNTRY_NAME,
            NATION_CODE = COUNTRY_CODE
         ),
      by = "NATIONALITY"
   ) %>%
   left_join(
      y  = ohasis$ref_country %>%
         select(
            OFW_COUNTRY = COUNTRY_NAME,
            OFW_CODE    = COUNTRY_CODE
         ),
      by = "OFW_COUNTRY"
   ) %>%
   left_join(
      y  = ohasis$ref_staff %>%
         mutate(EMAIL = str_squish(EMAIL)) %>%
         select(
            encoder    = EMAIL,
            CREATED_BY = STAFF_ID
         ),
      by = "encoder"
   ) %>%
   left_join(
      y  = encoded$STAFF %>%
         select(
            PROVIDER_ID = USER_ID,
            TEST_FACI   = FACI_ID,
            COUNSEL_BY  = PROVIDER_NAME
         ) %>%
         distinct_all(),
      by = c("TEST_FACI", "COUNSEL_BY")
   ) %>%
   mutate(
      .after       = REC_ID,
      CREATED_DATE = case_when(
         stri_detect_fixed(CREATED_DATE, "-") ~ as.Date(CREATED_DATE, format = "%Y-%m-%d"),
         stri_detect_fixed(CREATED_DATE, "/") ~ as.Date(CREATED_DATE, format = "%m/%d/%Y"),
      ),
      CREATED_TIME = format(strptime(CREATED_TIME, "%I:%M:%S %p"), "%H:%M:%S"),
      CREATED_AT   = paste(
         sep = " ",
         CREATED_DATE,
         CREATED_TIME
      ),
   ) %>%
   mutate(
      UPDATED_BY     = "1300000000",
      UPDATED_AT     = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      REC_ID         = paste(
         sep = "_",
         gsub("[^[:digit:]]", "", CREATED_AT),
         CREATED_BY
      ),
      MODULE         = "2",
      DISEASE        = "101000",
      SERVICE_TYPE   = case_when(
         MODALITY == "FBT" ~ "101101",
         MODALITY == "Community-based" ~ "101103",
         TRUE ~ "101101"
      ),
      CLIENT_TYPE    = case_when(
         CLIENT_TYPE == "Inpatient" ~ 1,
         CLIENT_TYPE == "Outpatient" ~ 2,
         CLIENT_TYPE == "Outpatient/Walk-in" ~ 2,
         CLIENT_TYPE == "Mobile HTS" ~ 3,
      ),
      CLINICAL_PIC   = case_when(
         CLINICAL_PIC == "Asymptomatic" ~ "1_Asymptomatic",
         CLINICAL_PIC == "Symptomatic" ~ "2_Symptomatic",
         TRUE ~ CLINICAL_PIC
      ),

      PREVTEST_MONTH = if_else(
         condition = StrIsNumeric(PREVTEST_MONTH),
         true      = month.name[PREVTEST_MONTH],
         false     = PREVTEST_MONTH,
         missing   = PREVTEST_MONTH
      ),
      PREV_TEST_DATE = case_when(
         !is.na(PREVTEST_YEAR) & !is.na(PREVTEST_MONTH) ~ as.Date(paste0(PREVTEST_MONTH, " 1, ", PREVTEST_YEAR), format = "%B %d, %Y"),
         nchar(PREVTEST_DATE) == 7 & as.numeric(stri_locate_first_fixed(PREVTEST_DATE, "-")[, 1]) == 5 ~ as.Date(paste0(PREVTEST_DATE, "-01"), format = "%Y-%m-%d"),
         nchar(PREVTEST_DATE) == 7 & as.numeric(stri_locate_first_fixed(PREVTEST_DATE, "-")[, 1]) == 3 ~ as.Date(paste0("01-", PREVTEST_DATE), format = "%d-%m-%Y"),
         nchar(PREVTEST_DATE) == 4 ~ as.Date(paste(sep = "-", PREVTEST_DATE, "01", "01"), format = "%Y-%m-%d"),
         nchar(PREVTEST_DATE) == 6 ~ as.Date(paste0("01-0", PREVTEST_DATE), format = "%d-%m-%Y"),
         TRUE ~ NA_Date_
      ),
      PROVIDER_TYPE  = case_when(
         PROVIDER_TYPE == "Medical Technologist" ~ "1",
         PROVIDER_TYPE == "HIV Counselor" ~ "2",
         PROVIDER_TYPE == "CBS Motivator" ~ "3",
         PROVIDER_TYPE == "Others" ~ "8888",
         TRUE ~ PROVIDER_TYPE
      ),
      DATE_PERFORMED = case_when(
         stri_detect_fixed(DATE_PERFORMED, "-") ~ as.Date(DATE_PERFORMED, format = "%m-%d-%Y"),
         stri_detect_fixed(DATE_PERFORMED, "/") ~ as.Date(DATE_PERFORMED, format = "%m/%d/%Y"),
      ),
      TEST_TYPE      = "10",
      TEST_NUM       = "1"
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(.)
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~case_when(
         . == "" ~ NA_character_,
         . %in% c("NULL", "TO FOLLOW", "PENDING") ~ NA_character_,
         . == "TRUE" ~ "1",
         . == "FALSE" ~ "0",
         TRUE ~ .
      )
   ) %>%
   mutate_at(
      .vars = vars(contains("DATE")),
      ~stri_replace_all_regex(., "^00", "20")
   ) %>%
   mutate_at(
      .vars = vars(
         SEX,
         SELF_IDENT,
         CIVIL_STATUS,
         LIVING_WITH_PARTNER,
         EDUC_LEVEL,
         EXPOSE_HIV_MOTHER,
         OFW_STATION,
         CLINICAL_PIC,
         starts_with("EXPOSE") & !contains("DATE")
      ),
      ~keep_code(.)
   ) %>%
   select(-NATIONALITY, -OFW_COUNTRY) %>%
   rename(
      AGE_FIRST_SEX     = AGE_SEX,
      NUM_F_PARTNER     = NO_F_SEX_PARTNERS,
      YR_LAST_F         = YEAR_F_SEX,
      NUM_M_PARTNER     = NO_M_SEX_PARTNERS,
      YR_LAST_M         = YEAR_M_SEX,
      AGE_FIRST_INJECT  = AGE_DRUG_INJECT,
      PREV_TEST_FACI    = PREVTEST_FACI,
      PREV_TEST_RESULT  = PREVTEST_RESULT,
      CONFIRMATORY_CODE = CONFIRM_CODE,
      RESULT            = SCREENING_RESULT,
      DATE_PERFORM      = DATE_PERFORMED,
      NATIONALITY       = NATION_CODE,
      OFW_COUNTRY       = OFW_CODE,
      SCREEN_AGREED     = TEST_AGREED,
      WORK_TEXT         = WORK
   )

##  Wide Tables ----------------------------------------------------------------

encoded$sql$wide <- list(
   "px_record"         = c("REC_ID", "PATIENT_ID"),
   "px_info"           = c("REC_ID", "PATIENT_ID"),
   "px_name"           = c("REC_ID", "PATIENT_ID"),
   "px_faci"           = c("REC_ID", "SERVICE_TYPE"),
   "px_profile"        = "REC_ID",
   "px_staging"        = "REC_ID",
   "px_ob"             = "REC_ID",
   "px_occupation"     = "REC_ID",
   "px_ofw"            = "REC_ID",
   "px_expose_profile" = "REC_ID",
   "px_prev_test"      = "REC_ID",
   "px_test"           = "REC_ID",
   "px_cfbs"           = "REC_ID",
   "px_linkage"        = "REC_ID",
   "px_form"           = c("REC_ID", "FORM"),
   "px_test"           = c("REC_ID", "TEST_TYPE", "TEST_NUM")
)

lapply(names(encoded$sql$wide), function(table) {
   db_conn <- ohasis$conn("db")
   cols    <- colnames(tbl(db_conn, dbplyr::in_schema("ohasis_interim", table)))

   col_select <- intersect(cols, names(.GlobalEnv$encoded$data$records))

   if (table != "px_record")
      .GlobalEnv$encoded$tables[[table]] <- .GlobalEnv$encoded$data$records %>%
         select(-FACI_ID) %>%
         rename(FACI_ID = TEST_FACI) %>%
         select(
            col_select
         )
   else
      .GlobalEnv$encoded$tables[[table]] <- .GlobalEnv$encoded$data$records %>%
         select(
            col_select
         )

   dbDisconnect(db_conn)
})

##  Long tables ----------------------------------------------------------------

encoded$sql$long <- list(
   "px_addr"          = c("REC_ID", "ADDR_TYPE"),
   "px_contact"       = c("REC_ID", "CONTACT_TYPE"),
   "px_expose_hist"   = c("REC_ID", "EXPOSURE"),
   "px_med_profile"   = c("REC_ID", "PROFILE"),
   "px_test_reason"   = c("REC_ID", "REASON"),
   "px_test_refuse"   = c("REC_ID", "REASON"),
   "px_reach"         = c("REC_ID", "REACH"),
   "px_other_service" = c("REC_ID", "SERVICE"),
   "px_remarks"       = c("REC_ID", "REMARK_TYPE")
)

# addr
encoded$tables$px_addr <- encoded$data$records %>%
   select(
      REC_ID,
      CREATED_AT,
      CREATED_BY,
      ends_with("_REG"),
      ends_with("_PROV"),
      ends_with("_MUNC"),
      ends_with("_ADDR")
   ) %>%
   pivot_longer(
      cols      = c(
         ends_with("_REG"),
         ends_with("_PROV"),
         ends_with("_MUNC"),
         ends_with("_ADDR")
      ),
      names_to  = "ADDR_DATA",
      values_to = "ADDR_VALUE"
   ) %>%
   separate(
      col  = "ADDR_DATA",
      into = c("ADDR_TYPE", "PIECE")
   ) %>%
   mutate(
      ADDR_TYPE = case_when(
         ADDR_TYPE == "CURR" ~ "1",
         ADDR_TYPE == "PERMANENT" ~ "2",
         ADDR_TYPE == "BIRTH" ~ "3",
         ADDR_TYPE == "DEATH" ~ "4",
         ADDR_TYPE == "HIVSERVICE" ~ "5",
         TRUE ~ ADDR_TYPE
      ),
      PIECE     = case_when(
         PIECE == "ADDR" ~ "TEXT",
         TRUE ~ PIECE
      )
   ) %>%
   pivot_wider(
      id_cols      = c(REC_ID, CREATED_AT, CREATED_BY, ADDR_TYPE),
      names_from   = PIECE,
      values_from  = ADDR_VALUE,
      names_prefix = "NAME_"
   ) %>%
   filter(!is.na(NAME_REG)) %>%
   left_join(
      y  = encoded$ref_addr %>%
         select(
            NAME_REG,
            NAME_PROV,
            NAME_MUNC,
            ADDR_REG  = PSGC_REG,
            ADDR_PROV = PSGC_PROV,
            ADDR_MUNC = PSGC_MUNC,
         ) %>%
         distinct_all(),
      by = c("NAME_REG", "NAME_PROV", "NAME_MUNC")
   ) %>%
   select(
      REC_ID,
      starts_with("ADDR_"),
      ADDR_TEXT = NAME_TEXT,
      CREATED_BY,
      CREATED_AT,
   )

# contacts
encoded$tables$px_contact <- encoded$data$records %>%
   select(
      REC_ID,
      CREATED_AT,
      CREATED_BY,
      MOBILE = MOBILE_NO,
      EMAIL
   ) %>%
   pivot_longer(
      cols      = c(MOBILE, EMAIL),
      names_to  = "CONTACT_TYPE",
      values_to = "CONTACT"
   ) %>%
   mutate(
      CONTACT_TYPE = case_when(
         CONTACT_TYPE == "MOBILE" ~ "1",
         CONTACT_TYPE == "EMAIL" ~ "2",
         TRUE ~ CONTACT_TYPE
      )
   ) %>%
   filter(!is.na(CONTACT))

# exposure
encoded$tables$px_expose_hist <- encoded$data$records %>%
   select(
      REC_ID,
      CREATED_AT,
      CREATED_BY,
      starts_with("EXPOSE")
   ) %>%
   pivot_longer(
      cols      = starts_with("EXPOSE"),
      names_to  = "EXPOSE_DATA",
      values_to = "EXPOSE_VALUE"
   ) %>%
   mutate(
      EXPOSE_DATA = if_else(
         condition = !stri_detect_regex(EXPOSE_DATA, "DATE$"),
         true      = paste0(EXPOSE_DATA, "_EXPOSED"),
         false     = EXPOSE_DATA,
         missing   = EXPOSE_DATA
      ),
      EXPOSURE    = substr(EXPOSE_DATA, 8, stri_locate_last_fixed(EXPOSE_DATA, "_") - 1),
      PIECE       = substr(EXPOSE_DATA, stri_locate_last_fixed(EXPOSE_DATA, "_") + 1, 1000),
   ) %>%
   mutate(
      EXPOSURE = case_when(
         EXPOSURE == "HIV_MOTHER" ~ "120000",
         EXPOSURE == "DRUG_INJECT" ~ "301010",
         EXPOSURE == "BLOOD_TRANSFUSION" ~ "530000",
         EXPOSURE == "OCCUPATION" ~ "510000",
         EXPOSURE == "SEX_DRUGS" ~ "200300",
         EXPOSURE == "SEX_M" ~ "217000",
         EXPOSURE == "SEX_M_AV" ~ "216000",
         EXPOSURE == "SEX_M_AV_NOCONDOM" ~ "216200",
         EXPOSURE == "SEX_F" ~ "227000",
         EXPOSURE == "SEX_F_AV" ~ "226000",
         EXPOSURE == "SEX_F_AV_NOCONDOM" ~ "226200",
         EXPOSURE == "SEX_PAYING" ~ "200010",
         EXPOSURE == "SEX_PAYMENT" ~ "200020",
         EXPOSURE == "SEX_M_NOCONDOM" ~ "210200",
         EXPOSURE == "SEX_F_NOCONDOM" ~ "220200",
         EXPOSURE == "STI" ~ "400000",
         EXPOSURE == "SEX_WITH_HIV" ~ "230003",
         EXPOSURE == "TATTOO" ~ "520000",
         TRUE ~ EXPOSURE
      )
   ) %>%
   pivot_wider(
      id_cols     = c(REC_ID, CREATED_AT, CREATED_BY, EXPOSURE),
      names_from  = PIECE,
      values_from = EXPOSE_VALUE
   ) %>%
   mutate(
      EXPOSED          = if_else(
         condition = !is.na(DATE),
         true      = "1",
         false     = EXPOSED,
         missing   = "0"
      ),
      TYPE_LAST_EXPOSE = if_else(
         condition = EXPOSED == 2,
         true      = 4,
         false     = 0,
         missing   = 0
      ),
      EXPOSED          = if_else(
         condition = EXPOSED == 2,
         true      = "1",
         false     = EXPOSED,
         missing   = EXPOSED
      ),
   ) %>%
   filter(!is.na(EXPOSED)) %>%
   rename(
      IS_EXPOSED       = EXPOSED,
      DATE_LAST_EXPOSE = DATE
   )

# med_profile
encoded$tables$px_med_profile <- encoded$data$records %>%
   select(
      REC_ID,
      CREATED_AT,
      CREATED_BY,
      starts_with("MED")
   ) %>%
   pivot_longer(
      cols      = starts_with("MED"),
      names_to  = "PROFILE",
      values_to = "IS_PROFILE"
   ) %>%
   mutate(
      PROFILE = stri_replace_all_regex(PROFILE, "^MED_", ""),
      PROFILE = case_when(
         PROFILE == "TB_PX" ~ "1",
         PROFILE == "PREGNANT" ~ "2",
         PROFILE == "HEP_B" ~ "3",
         PROFILE == "HEP_C" ~ "4",
         PROFILE == "CBS_REACTIVE" ~ "5",
         PROFILE == "PREP_PX" ~ "6",
         PROFILE == "PEP_PX" ~ "7",
         PROFILE == "STI" ~ "8",
         TRUE ~ PROFILE
      )
   ) %>%
   filter(IS_PROFILE == 1)

# test_reason
encoded$tables$px_test_reason <- encoded$data$records %>%
   select(
      REC_ID,
      CREATED_AT,
      CREATED_BY,
      starts_with("TEST_REASON")
   ) %>%
   pivot_longer(
      cols      = starts_with("TEST_REASON"),
      names_to  = "REASON",
      values_to = "IS_REASON"
   ) %>%
   mutate(
      REASON = stri_replace_all_regex(REASON, "^TEST_REASON_", ""),
      REASON = case_when(
         REASON == "HIV_EXPOSE" ~ "1",
         REASON == "PHYSICIAN" ~ "2",
         REASON == "HIV_PHYSICIAN" ~ "2",
         REASON == "EMPLOY_OFW" ~ "3",
         REASON == "EMPLOY_LOCAL" ~ "4",
         REASON == "INSURANCE" ~ "5",
         REASON == "NO_REASON" ~ "6",
         REASON == "RETEST" ~ "7",
         REASON == "OTHER" ~ "8888",
         REASON == "PEER_ED" ~ "8",
         REASON == "TEXT_EMAIL" ~ "9",
         TRUE ~ REASON
      )
   ) %>%
   filter(IS_REASON == 1) %>%
   left_join(
      y  = encoded$data$records %>%
         select(
            REC_ID,
            REASON_OTHER = TEST_REASON_OTHER_TEXT
         ) %>%
         mutate(REASON = "8888"),
      by = c("REC_ID", "REASON")
   )

# test_reason
encoded$tables$px_test_refuse <- encoded$data$records %>%
   select(
      REC_ID,
      CREATED_AT,
      CREATED_BY,
      REASON_OTHER = REFUSE_OTHER_TEXT
   ) %>%
   mutate(
      REASON    = "8888",
      IS_REASON = 1
   )

# test_reason
encoded$tables$px_reach <- encoded$data$records %>%
   select(
      REC_ID,
      CREATED_AT,
      CREATED_BY,
      starts_with("REACH")
   ) %>%
   pivot_longer(
      cols      = starts_with("REACH"),
      names_to  = "REACH",
      values_to = "IS_REACH"
   ) %>%
   mutate(
      REACH = stri_replace_all_regex(REACH, "^REACH_", ""),
      REACH = case_when(
         REACH == "CLINICAL" ~ "1",
         REACH == "ONLINE" ~ "2",
         REACH == "INDEX_TESTING" ~ "3",
         REACH == "SSNT" ~ "4",
         REACH == "VENUE" ~ "5",
         TRUE ~ REACH
      )
   ) %>%
   filter(IS_REACH == 1)

# other_service
encoded$tables$px_other_service <- encoded$data$records %>%
   select(
      REC_ID,
      CREATED_AT,
      CREATED_BY,
      starts_with("SERVICE_")
   ) %>%
   select(-SERVICE_TYPE) %>%
   pivot_longer(
      cols      = starts_with("SERVICE_"),
      names_to  = "SERVICE",
      values_to = "GIVEN"
   ) %>%
   mutate(
      SERVICE = stri_replace_all_regex(SERVICE, "^SERVICE_", ""),
      SERVICE = case_when(
         SERVICE == "HIV_101" ~ "1013",
         SERVICE == "IEC_MATS" ~ "1004",
         SERVICE == "RISK_COUNSEL" ~ "1002",
         SERVICE == "PREP_REFER" ~ "5001",
         SERVICE == "SSNT_OFFER" ~ "5002",
         SERVICE == "SSNT_ACCEPT" ~ "5003",
         SERVICE == "GIVEN_CONDOMS" ~ "2001",
         SERVICE == "GIVEN_LUBES" ~ "2002",
         TRUE ~ SERVICE
      )
   ) %>%
   filter(GIVEN == 1) %>%
   bind_rows(
      encoded$data$records %>%
         select(
            REC_ID,
            CREATED_AT,
            CREATED_BY,
            OTHER_SERVICE = CONDOMS
         ) %>%
         filter(!is.na(OTHER_SERVICE)) %>%
         mutate(
            SERVICE = "2001",
            GIVEN   = "1"
         ),
      encoded$data$records %>%
         select(
            REC_ID,
            CREATED_AT,
            CREATED_BY,
            OTHER_SERVICE = LUBES
         ) %>%
         filter(!is.na(OTHER_SERVICE)) %>%
         mutate(
            SERVICE = "2002",
            GIVEN   = "1"
         )
   )

encoded$tables$px_remarks <- encoded$data$records %>%
   select(
      REC_ID,
      CREATED_AT,
      CREATED_BY,
      ends_with("_NOTES"),
   ) %>%
   pivot_longer(
      cols      = ends_with("_NOTES"),
      names_to  = "REMARK_TYPE",
      values_to = "REMARKS"
   ) %>%
   mutate(
      REMARK_TYPE = case_when(
         REMARK_TYPE == "CLINIC_NOTES" ~ "1",
         REMARK_TYPE == "COUNSELING_NOTES" ~ "2",
         REMARK_TYPE == "COUNSELNOTES" ~ "2",
         REMARK_TYPE == "COUNSEL_NOTES" ~ "2",
         TRUE ~ REMARK_TYPE
      )
   ) %>%
   filter(!is.na(REMARKS))

##  Upsert data ----------------------------------------------------------------

invisible(lapply(names(encoded$sql$wide), function(table) {
   id_cols <- encoded$sql$wide[[table]]
   data    <- encoded$tables[[table]]

   db_conn     <- ohasis$conn("db")
   table_space <- Id(schema = "ohasis_interim", table = table)
   dbxUpsert(
      db_conn,
      table_space,
      data,
      id_cols
   )
   dbDisconnect(db_conn)
}))

invisible(lapply(names(encoded$sql$long), function(table) {
   id_cols <- encoded$sql$long[[table]]
   data    <- encoded$tables[[table]]

   db_conn     <- ohasis$conn("db")
   table_space <- Id(schema = "ohasis_interim", table = table)
   dbxUpsert(
      db_conn,
      table_space,
      data,
      id_cols
   )
   dbDisconnect(db_conn)
}))