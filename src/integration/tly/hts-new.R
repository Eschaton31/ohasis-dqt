##  inputs ---------------------------------------------------------------------

tly <- list(
   convert = list(
      addr  = read_sheet("1r8CVfX16oDSStwLfIQdExyA-X21QuGnKwZp1AGDNXdc", "addr", range = "A:F", col_types = "c"),
      staff = read_sheet("1r8CVfX16oDSStwLfIQdExyA-X21QuGnKwZp1AGDNXdc", "staff", range = "A:C", col_types = "c"),
      sites = read_sheet("1r8CVfX16oDSStwLfIQdExyA-X21QuGnKwZp1AGDNXdc", "sites", range = "A:C", col_types = "c")
   ),
   data    = list(
      raw = read_rds("H:/20240504_ly-hts_corr-addr.rds")
   )
)

#  uploaded --------------------------------------------------------------------

TIMESTAMP <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
min       <- as.character(min(tly$data$raw$visit_date_value, na.rm = TRUE))
max       <- as.character(max(tly$data$raw$visit_date_value, na.rm = TRUE))
sites     <- ohasis$ref_faci %>% filter(str_detect(FACI_NAME, "LoveYour"))
sites     <- sites$FACI_ID

# min             <- "2024-01-01"
# max             <- "2024-03-31"
db              <- "ohasis_warehouse"
lw_conn         <- ohasis$conn("lw")
tly$prev_upload <- QB$new(lw_conn)$
   from("ohasis_warehouse.form_hts")$
   whereBetween("RECORD_DATE", c(min, max))$
   whereIn("FACI_ID", sites)$
   get()
tly$prev_upload %<>%
   mutate_if(is.POSIXct, ~as.Date(.)) %>%
   mutate_all(~as.character(.))
dbDisconnect(lw_conn)

##  check convert --------------------------------------------------------------

tly$data$convert <- tly$data$raw %>%
   filter(visit_date_value >= "2024-01-01") %>%
   select(
      data_id,
      RECORD_DATE                   = visit_date_value,
      UIC                           = uic,
      LAST                          = last_name,
      FIRST                         = first_name,
      MIDDLE                        = middle_name,
      SUFFIX                        = suffix_name,
      BIRTHDATE                     = birth_date_value,
      AGE                           = age,
      SEX                           = sex,
      SELF_IDENT                    = self_identity,
      SELF_IDENT_OTHER              = self_identity_other,
      CURR_ADDR                     = current_municipality,
      CURR_REG                      = curr_reg,
      CURR_PROV                     = curr_prov,
      CURR_MUNC                     = curr_munc,
      PERM_ADDR                     = permanent_municipality,
      PERM_REG                      = perm_reg,
      PERM_PROV                     = perm_prov,
      PERM_MUNC                     = perm_munc,
      IS_PREGNANT                   = is_pregnant,
      CLIENT_MOBILE                 = contact_mobile,
      CLIENT_EMAIL                  = contact_email,
      PROVIDER                      = counselor,
      NUM_M_PARTNER                 = num_m_partner,
      EXPOSE_SEX_M                  = risk_sex_m,
      EXPOSE_SEX_M_AV               = risk_sex_m_av,
      EXPOSE_SEX_M_AV_DATE          = risk_sex_m_av_date_value,
      EXPOSE_SEX_M_AV_NOCONDOM      = risk_sex_m_av_nocondom,
      EXPOSE_SEX_M_AV_NOCONDOM_DATE = risk_sex_m_av_nocondom_date_value,
      NUM_F_PARTNER                 = num_f_partner,
      EXPOSE_SEX_F                  = risk_sex_f,
      EXPOSE_SEX_F_AV               = risk_sex_f_av,
      EXPOSE_SEX_F_AV_DATE          = risk_sex_f_av_date_value,
      EXPOSE_SEX_F_AV_NOCONDOM      = risk_sex_f_av_nocondom,
      EXPOSE_SEX_F_AV_NOCONDOM_DATE = risk_sex_f_av_nocondom_date_value,
      EXPOSE_SEX_PAYING             = risk_sex_paying,
      EXPOSE_SEX_PAYING_DATE        = risk_sex_paying_date_value,
      EXPOSE_SEX_PAYMENT            = risk_sex_payment,
      EXPOSE_SEX_PAYMENT_DATE       = risk_sex_payment_date_value,
      EXPOSE_SEX_DRUGS              = risk_sex_drugs,
      EXPOSE_SEX_DRUGS_DATE         = risk_sex_drugs_date_value,
      EXPOSE_DRUG_INJECT            = risk_drug_inject,
      EXPOSE_DRUG_INJECT_DATE       = risk_drug_inject_date_value,
      EXPOSE_BLOOD_TRANSFUSE        = risk_blood_transfuse,
      EXPOSE_BLOOD_TRANSFUSE_DATE   = risk_blood_transfuse_date_value,
      EXPOSE_OCCUPATION             = risk_occupation,
      EXPOSE_OCCUPATION_DATE        = risk_occupation_date_value,
      EXPOSE_SEX_ORAL               = risk_sex_oral,
      EXPOSE_SEX_ANAL_INSERT        = risk_sex_anal_insert,
      EXPOSE_SEX_ANAL_RECEIVE       = risk_sex_anal_receive,
      EXPOSE_SEX_VAGINAL            = risk_sex_vaginal,
      EXPOSE_CONDOM_USE             = risk_condom_use,
      PREV_TESTED                   = prev_tested,
      PREV_TEST_DATE                = prev_test_date_value,
      PREV_TEST_RESULT              = prev_test_result,
      TEST_REASON_HIV_EXPOSE        = test_reason_hiv_expose,
      TEST_REASON_OTHER_TEXT        = test_reasons,
      T0_RESULT                     = test_result,
      PATIENT_CODE                  = client_code,
      COUNSEL_NOTES                 = key_population,
      CLINIC_NOTES                  = remarks,
      BRANCH                        = branch_name,
   ) %>%
   mutate_at(
      .vars = vars(FIRST, MIDDLE, LAST, SUFFIX, PATIENT_CODE, UIC, CLIENT_MOBILE, CLIENT_EMAIL),
      ~clean_pii(.)
   ) %>%
   left_join(
      y  = tly$convert$sites,
      by = join_by(BRANCH)
   ) %>%
   left_join(
      y  = tly$convert$staff %>%
         filter(USER_ID != "0000000000") %>%
         rename(SERVICE_BY = USER_ID),
      by = join_by(PROVIDER, BRANCH)
   ) %>%
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         CURR_PSGC_REG  = "CURR_REG",
         CURR_PSGC_PROV = "CURR_PROV",
         CURR_PSGC_MUNC = "CURR_MUNC"
      ),
      aem_sub_ntl = FALSE
   ) %>%
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         PERM_PSGC_REG  = "PERM_REG",
         PERM_PSGC_PROV = "PERM_PROV",
         PERM_PSGC_MUNC = "PERM_MUNC"
      ),
      aem_sub_ntl = FALSE
   ) %>%
   mutate(
      SERVICE_FACI      = FACI_ID,
      SERVICE_SUB_FACI  = SUB_FACI_ID,

      UIC               = str_left(str_squish(UIC), 14),

      SEX               = case_when(
         SEX == "Male" ~ "1_Male",
         SEX == "MALE" ~ "1_Male",
         SEX == "FEMALE" ~ "2_Female",
         TRUE ~ SELF_IDENT
      ),

      SELF_IDENT        = case_when(
         SELF_IDENT == "MALE" ~ "1_Man",
         SELF_IDENT == "MAN" ~ "1_Man",
         SELF_IDENT == "FEMALE" ~ "2_Woman",
         SELF_IDENT == "WOMAN" ~ "2_Woman",
         SELF_IDENT == "OTHER" ~ "3_Other",
         !is.na(SELF_IDENT) ~ "3_Other",
         TRUE ~ SELF_IDENT
      ),

      T0_RESULT         = case_when(
         T0_RESULT == "REACTIVE" ~ "1_Reactive",
         T0_RESULT == "R" ~ "1_Reactive",
         T0_RESULT == "NON REACTIVE" ~ "2_Non-reactive",
         T0_RESULT == "NON-REACTIVE" ~ "2_Non-reactive",
         T0_RESULT == "NR" ~ "2_Non-reactive",
         T0_RESULT == "NOT APPLICABLE" ~ NA_character_,
         TRUE ~ T0_RESULT
      ),
      PREV_TEST_RESULT  = case_when(
         PREV_TEST_RESULT == "POSITIVE" ~ "1_Positive",
         PREV_TEST_RESULT == "R" ~ "1_Positive",
         PREV_TEST_RESULT == "NEGATIVE" ~ "2_Negative",
         PREV_TEST_RESULT == "NR" ~ "2_Negative",
         PREV_TEST_RESULT == "IND" ~ "3_Indeterminate",
         PREV_TEST_RESULT == "DON'T KNOW" ~ "4_Was not able to get result",
         PREV_TEST_RESULT == "DK" ~ "4_Was not able to get result",
         PREV_TEST_RESULT == "DID NOT GET RESULT" ~ "4_Was not able to get result",
         TRUE ~ PREV_TEST_RESULT
      ),

      MODALITY          = "101101_Facility-based Testing (FBT)",
      EXPOSE_HIV_MOTHER = "0_No"
   ) %>%
   # fix risk
   mutate_at(
      .vars = vars(
         starts_with("EXPOSE") & !contains("DATE"),
         TEST_REASON_HIV_EXPOSE,
         PREV_TESTED,
      ),
      ~case_when(
         . == "0" ~ "0_No",
         . == "No" ~ "0_No",
         . == "NO" ~ "0_No",
         . == "no" ~ "0_No",
         . == "1" ~ "1_Yes",
         . == "Yes" ~ "1_Yes",
         . == "YES" ~ "1_Yes",
         . == "yes" ~ "1_Yes",
         . == "Y" ~ "1_Yes",
         . == "N" ~ "0_No",
         TRUE ~ .
      )
   ) %>%
   select(-any_of(c("PATIENT_ID", "REC_ID"))) %>%
   # get records id if existing
   left_join(
      y  = tly$prev_upload %>%
         mutate(
            BIRTHDATE   = as.Date(BIRTHDATE),
            RECORD_DATE = as.Date(RECORD_DATE),
         ) %>%
         select(
            REC_ID,
            PATIENT_ID,
            RECORD_DATE,
            UIC,
            PATIENT_CODE,
            FIRST,
            MIDDLE,
            LAST,
            SUFFIX,
            SEX,
            BIRTHDATE,
         ) %>%
         mutate_at(
            .vars = vars(FIRST, MIDDLE, LAST, SUFFIX, PATIENT_CODE, UIC),
            ~clean_pii(.)
         ) %>%
         distinct(
            RECORD_DATE,
            UIC,
            PATIENT_CODE,
            FIRST,
            MIDDLE,
            LAST,
            SUFFIX,
            SEX,
            BIRTHDATE,
            .keep_all = TRUE
         ),
      by = join_by(
         RECORD_DATE,
         UIC,
         PATIENT_CODE,
         FIRST,
         MIDDLE,
         LAST,
         SUFFIX,
         SEX,
         BIRTHDATE,
      )
   ) %>%
   left_join(
      y  = tly$prev_upload %>%
         mutate(
            RECORD_DATE = if_else(RECORD_DATE < -25567, T0_DATE, RECORD_DATE, RECORD_DATE),
            RECORD_DATE = as.Date(RECORD_DATE),
            CREATED_AT  = as.Date(CREATED_AT),
         ) %>%
         select(
            REC_ID,
            CREATED_BY,
            CREATED_AT,
            UPDATED_BY,
            UPDATED_AT,
            PRIME,
            SNAPSHOT
         ),
      by = join_by(REC_ID)
   ) %>%
   mutate(
      CREATED_BY = coalesce(CREATED_BY, SERVICE_BY, "1300000048"),
      CREATED_AT = coalesce(CREATED_AT, RECORD_DATE),
      # CREATED_AT   = format(CREATED_AT, "%Y-%m-%d 00:00:00"),
   ) %>%
   add_missing_columns(tly$prev_upload)


tly$import <- tly$data$convert %>%
   mutate(
      MODULE       = "2_Testing",
      DISEASE      = "HIV",
      old_rec      = if_else(!is.na(REC_ID), 1, 0, 0),
      # UPDATED_BY   = if_else(old_rec == 1, "1300000048", NA_character_),
      # UPDATED_AT   = if_else(old_rec == 1, TIMESTAMP, NA_character_),
      SERVICE_TYPE = MODALITY,
      TEST_TYPE    = "10",
      TEST_NUM     = 1,
      RESULT       = T0_RESULT,
      DATE_PERFORM = T0_DATE,
      PERFORM_BY   = CREATED_BY,
      FORM         = "HTS Form",
      VERSION      = "2021",
      FORM_VERSION = "HTS Form (v2021)"
   )

tly$import %<>%
   filter(!is.na(PATIENT_ID)) %>%
   bind_rows(
      batch_px_ids(tly$import %>% filter(is.na(PATIENT_ID)), PATIENT_ID, FACI_ID, "data_id")
   )

tly$import %<>%
   filter(!is.na(REC_ID)) %>%
   bind_rows(
      batch_rec_ids(tly$import %>% filter(is.na(REC_ID)), REC_ID, CREATED_BY, "data_id")
   )

match_vars <- intersect(names(tly$import), names(tly$prev_upload))
# tly$import %<>%
#    anti_join(tly$prev_upload, join_by(PATIENT_ID, RECORD_DATE))
tly$import %<>%
   # select(-HTS_FACI) %>%
   mutate_if(
      .predicate = is.Date,
      ~as.character(.)
   ) %>%
   anti_join(
      y  = tly$prev_upload %>%
         mutate(
            AGE = as.integer(AGE)
         ) %>%
         left_join(
            y  = ohasis$ref_country %>%
               select(
                  NATIONALITY = COUNTRY_NAME,
                  COUNTRY_CODE
               ),
            by = join_by(NATIONALITY)
         ) %>%
         select(-NATIONALITY) %>%
         rename(NATIONALITY = COUNTRY_CODE) %>%
         left_join(
            y  = ohasis$ref_country %>%
               select(
                  OFW_COUNTRY = COUNTRY_NAME,
                  COUNTRY_CODE
               ),
            by = join_by(OFW_COUNTRY)
         ) %>%
         select(-OFW_COUNTRY) %>%
         rename(OFW_COUNTRY = COUNTRY_CODE) %>%
         # fix risk
         mutate_at(
            .vars = vars(
               starts_with("REFER_"),
               starts_with("TEST_REASON") & !contains("OTHER"),
               starts_with("SERVICE") &
                  !contains("TYPE") &
                  !contains("LUBE") &
                  !contains("CONDOM"),
               PREV_TESTED,
               LIVING_WITH_PARTNER,
               REFER_RETEST,
               REFER_CONFIRM,
               REFER_ART,
               IS_EMPLOYED,
               IS_STUDENT,
               IS_PREGNANT,
            ),
            ~na_if(., "0_No")
         ),
      by = match_vars
   )

tly$prev_upload %>%
   left_join(
      y  = ohasis$ref_country %>%
         select(
            NATIONALITY = COUNTRY_NAME,
            COUNTRY_CODE
         ),
      by = join_by(NATIONALITY)
   ) %>%
   select(-NATIONALITY) %>%
   rename(NATIONALITY = COUNTRY_CODE) %>%
   left_join(
      y  = ohasis$ref_country %>%
         select(
            OFW_COUNTRY = COUNTRY_NAME,
            COUNTRY_CODE
         ),
      by = join_by(OFW_COUNTRY)
   ) %>%
   select(-OFW_COUNTRY) %>%
   rename(OFW_COUNTRY = COUNTRY_CODE) %>%
   # fix risk
   mutate_at(
      .vars = vars(
         starts_with("REFER_"),
         starts_with("TEST_REASON") & !contains("OTHER"),
         starts_with("SERVICE") &
            !contains("TYPE") &
            !contains("LUBE") &
            !contains("CONDOM"),
         PREV_TESTED,
         LIVING_WITH_PARTNER,
         REFER_RETEST,
         REFER_CONFIRM,
         REFER_ART,
         IS_EMPLOYED,
         IS_STUDENT,
         IS_PREGNANT,
      ),
      ~na_if(., "0_No")
   ) %>%
   select(any_of(match_vars)) %>%
   filter(PATIENT_ID == "20231201990005003E") %>%
   write_clip()

tly$import %>%
   select(any_of(match_vars)) %>%
   filter(PATIENT_ID == "20231201990005003E") %>%
   write_clip()

tly$import %<>%
   mutate_at(
      .vars = vars(
         MODULE,
         SEX,
         SELF_IDENT,
         CIVIL_STATUS,
         EDUC_LEVEL,
         LIVING_WITH_PARTNER,
         CLIENT_TYPE,
         PROVIDER_TYPE,
         MODALITY,
         T0_RESULT,
         PREV_TEST_RESULT,
         IS_PREGNANT,
         IS_STUDENT,
         IS_EMPLOYED,
         IS_OFW,
         SCREEN_AGREED,
         CLINICAL_PIC,
         WHO_CLASS,
         REFER_ART,
         REFER_CONFIRM,
         REFER_RETEST,
         OFW_STATION,
         PREV_TESTED
      ),
      ~keep_code(.)
   ) %>%
   mutate(
      CREATED_AT   = if_else(str_length(CREATED_AT) == 10, stri_c(CREATED_AT, " 00:00:00"), CREATED_AT, CREATED_AT),
      UPDATED_BY   = "1300000048",
      UPDATED_AT   = TIMESTAMP,
      SERVICE_TYPE = keep_code(SERVICE_TYPE),
      PRIME        = 0
   )

tly$import %<>%
   distinct(REC_ID, PATIENT_ID, .keep_all = TRUE)

tly$tables <- deconstruct_hts(
   tly$import %>%
      select(
         -overseas_addr,
         -CURR_REG,
         -CURR_PROV,
         -CURR_MUNC,
         -PERM_REG,
         -PERM_PROV,
         -PERM_MUNC,
      )
)
wide       <- c("px_test_refuse", "px_other_service", "px_reach", "px_med_profile", "px_test_reason")
delete     <- tly$tables$px_record$data %>% select(REC_ID)

db_conn <- ohasis$conn("db")
lapply(wide, function(table) dbxDelete(db_conn, Id(schema = "ohasis_interim", table = table), delete))
# lapply(wide, function(table) dbExecute(conn = db_conn, statement = glue("DELETE FROM ohasis_interim.{table} WHERE REC_ID IN (?)"), params = list(tly$import$REC_ID)))

lapply(tly$tables, function(ref, db_conn) {
   log_info("Uploading {green(ref$name)}.")
   table_space <- Id(schema = "ohasis_interim", table = ref$name)
   dbxUpsert(db_conn, table_space, ref$data, ref$pk)
   # dbExecute(db_conn, glue("DELETE FROM ohasis_interim.{ref$name} WHERE REC_ID IN (?)"), params = list(unique(ref$data$REC_ID)))
}, db_conn)
dbDisconnect(db_conn)
