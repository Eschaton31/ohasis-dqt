##  inputs ---------------------------------------------------------------------

qr <- 4
yr <- 2023

aiha <- list(
   path    = "O:/My Drive/Data Sharing/AIHA",
   convert = list(
      addr  = read_sheet("1_o861XcjkgIm0HuV71Zy5xvQI5TmwYhHNK8mmMWNsTM", "addr", range = "A:F", col_types = "c"),
      staff = read_sheet("1_o861XcjkgIm0HuV71Zy5xvQI5TmwYhHNK8mmMWNsTM", "staff", range = "A:B", col_types = "c")
   )
)

##  processing -----------------------------------------------------------------

min      <- yq(stri_c(yr, ".", qr))
start_ym <- format(min, "%Y.%m")

max    <- ceiling_date(min, "quarter") %m-% days(1)
end_ym <- format(max, "%Y.%m")

report_path  <- file.path(aiha$path, stri_c(start_ym, "-", end_ym))
report_files <- list.files(report_path, ".xls*", full.names = TRUE)

regions    <- basename(report_files)
regions    <- case_when(
   str_detect(toupper(regions), "REGION 6") ~ "6",
   str_detect(toupper(regions), "REG 6") ~ "6",
   str_detect(toupper(regions), "R6") ~ "6",
   str_detect(toupper(regions), "REGION 7") ~ "7",
   str_detect(toupper(regions), "REG 7") ~ "7",
   str_detect(toupper(regions), "R7") ~ "7",
)
raw        <- lapply(report_files, read_excel)
names(raw) <- regions

##  new data -------------------------------------------------------------------

aiha$hts <- raw %>%
   lapply(function(data) {
      data %<>%
         mutate_all(~as.character(.)) %>%
         rename_all(
            ~stri_replace_all_fixed(., " ", " ") %>%
               stri_replace_all_fixed("/", " ") %>%
               stri_replace_all_fixed(".", " ") %>%
               str_replace_all("[^[:alnum:]]", " ") %>%
               str_squish() %>%
               str_replace_all(" ", "_")
         )
      return(data)
   }) %>%
   bind_rows(.id = "region") %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(.) %>%
         na_if("N/A") %>%
         na_if("n/a") %>%
         na_if("-")
   ) %>%
   mutate_at(
      .vars = vars(RECORD_FACI, TESTING_FACILITY),
      ~case_when(
         . == "1_Malay Social Hygiene Clinic" ~ "100017",
         . == "AIHA" ~ "990005",
         TRUE ~ keep_code(.)
      )
   ) %>%
   rename(
      FIRST                         = FIRST_NAME,
      MIDDLE                        = MIDDLE_NAME,
      LAST                          = LAST_NAME,
      UIC                           = UNIQUE_IDENTIFIER_CODE_UIC,
      AGE_MO                        = AGE_IN_MONTHS,
      IS_PREGNANT                   = IS_CLIENT_PREGNANT,
      SELF_IDENT                    = GENDER_IDENTITY,
      SELF_IDENT_OTHER              = GENDER_IDENTITY_OTHER,
      EDUC_LEVEL                    = HIGHEST_EDUCATIONAL_ATTAINMENT,
      IS_STUDENT                    = IS_CLIENT_A_STUDENT,
      LIVING_WITH_PARTNER           = IS_CLIENT_LIVING_WITH_PARTNER,
      CHILDREN                      = HOW_MANY_CHILDREN,
      PHILHEALTH_NO                 = PHILHEALTH_NUMBER,
      CLIENT_MOBILE                 = MOBILE_NUMBER,
      CLIENT_EMAIL                  = EMAIL_ADDRESS,
      WORK                          = OCCUPATION_JOB,
      IS_EMPLOYED                   = IS_CLIENT_EMPLOYED,
      IS_OFW                        = IS_CLIENT_AN_OFW,
      OFW_COUNTRY                   = FROM_WHAT_COUNTRY,
      OFW_YR_RET                    = IF_YES_YEAR_OF_RETURN,
      OFW_STATION                   = SHIP_OR_LAND,
      EXPOSE_HIV_MOTHER             = DID_BIRTH_MOTHER_HAVE_HIV,
      EXPOSE_SEX_M                  = DID_CLIENT_HAVE_SEX_WITH_MALES,
      EXPOSE_SEX_M_AV_DATE          = DATE_OF_LAST_ANAL_SEX_WITH_MALES,
      EXPOSE_SEX_M_AV_NOCONDOM_DATE = DATE_OF_LAST_CONDOMLESS_ANAL_SEX_WITH_MALES,
      EXPOSE_SEX_F                  = DID_CLIENT_HAVE_SEX_WITH_FEMALES,
      EXPOSE_SEX_F_AV_DATE          = DATE_OF_LAST_ANAL_VAGINAL_SEX_WITH_FEMALES,
      EXPOSE_SEX_F_AV_NOCONDOM_DATE = DATE_OF_LAST_CONDOMLESS_ANAL_VAGINAL_SEX_WITH_FEMALES,
      EXPOSE_SEX_PAYING             = DID_CLIENT_PAY_FOR_SEX_IN_CASH_KIND,
      EXPOSE_SEX_PAYING_DATE        = DATE_OF_LAST_PAID_FOR_SEX_IN_CASH_KIND,
      EXPOSE_SEX_PAYMENT            = DID_CLIENT_RECEIVE_PAYMENT_FOR_SEX_CASH_KIND,
      EXPOSE_SEX_PAYMENT_DATE       = DATE_OF_LAST_RECEIVED_PAYMENT_FOR_SEX_CASH_KIND,
      EXPOSE_SEX_DRUGS              = DID_CLIENT_HAVE_SEX_UNDER_INFLUENCE_OF_DRUGS,
      EXPOSE_SEX_DRUGS_DATE         = DATE_OF_LAST_SEX_UNDER_INFLUENCE_OF_DRUGS,
      EXPOSE_DRUG_INJECT            = DID_CLIENT_SHARE_NEEDLES_IN_DRUG_INJECTION,
      EXPOSE_DRUG_INJECT_DATE       = DATE_OF_LAST_SHARED_NEEDLES_IN_DRUG_INJECTION,
      EXPOSE_BLOOD_TRANSFUSE        = DID_CLIENT_RECEIVE_BLOOD_TRANSFUSION,
      EXPOSE_BLOOD_TRANSFUSE_DATE   = DATE_OF_LAST_RECEIVED_BLOOD_TRANSUSION,
      EXPOSE_OCCUPATION             = DID_CLIENT_GET_OCCUPATIONAL_EXPOSURE_NEEDLES_SHARPS,
      EXPOSE_OCCUPATION_DATE        = DATE_OF_LAST_OCCUPATIONAL_EXPOSURE,
      TEST_REASON_HIV_EXPOSE        = REASON_FOR_HIV_TESTING_POSSIBLE_EXPOSURE,
      TEST_REASON_PHYSICIAN         = REASON_FOR_HIV_TESTING_PHYSICIAN_RECOMMENDATION,
      TEST_REASON_PEER_ED           = REASON_FOR_HIV_TESTING_REFERRED_BY_PEER_EDUCATOR,
      TEST_REASON_EMPLOY_OFW        = REASON_FOR_HIV_TESTING_OFW_EMPLOYMENT,
      TEST_REASON_EMPLOY_LOCAL      = REASON_FOR_HIV_TESTING_LOCAL_EMPLOYMENT,
      TEST_REASON_TEXT_EMAIL        = REASON_FOR_HIV_TESTING_RECEIVED_A_TEXT_EMAIL_TO_GET_TESTED,
      TEST_REASON_INSURANCE         = REASON_FOR_HIV_TESTING_INSURANCE_REQUIREMENT,
      TEST_REASON_NO_REASON         = REASON_FOR_HIV_TESTING_NO_REASON,
      TEST_REASON_OTHER_TEXT        = WHAT_IS_THE_OTHER_REASON_FOR_HIV_TESTING,
      PREV_TESTED                   = HAS_CLIENT_BEEN_PREVIOUSLY_TESTED,
      PREV_TEST_DATE                = MOST_RECENT_TEST_DATE,
      PREV_TEST_FACI                = PREVIOUS_TEST_FACILITY_NAME,
      PREV_TEST_RESULT              = PREVIOUS_TEST_RESULT,
      MED_TB_PX                     = IS_CLIENT_A_CURRENT_TB_PATIENT,
      MED_STI                       = WAS_CLIENT_DIAGNOSED_WITH_OTHER_STI,
      MED_HEP_B                     = DOES_CLIENT_HAVE_HEPATITIS_B,
      MED_HEP_C                     = DOES_CLIENT_HAVE_HEPATITIS_C,
      MED_PREP_PX                   = HAS_CLIENT_TAKEN_PREP,
      MED_PEP_PX                    = HAS_CLIENT_TAKEN_PEP,
      CLINICAL_PIC                  = CLINICAL_PICTURE,
      SYMPTOMS                      = SIGNS_SYMPTOMS,
      WHO_CLASS                     = W_H_O_STAGING,
      SCREEN_AGREED                 = DID_CLIENT_AGREE_TO_HIV_TESTING,
      TEST_REFUSE_OTHER_TEXT        = WHAT_IS_THE_REASON_FOR_HIV_TESTING_REFUSAL_IF_ANY,
      MODALITY                      = TESTING_MODALITY,
      T0_DATE                       = DATE_PERFORMED_YYYY_MM_DD,
      T0_RESULT                     = SCREENING_RESULT,
      REFER_ART                     = WAS_CLIENT_REFERRED_TO_ART,
      REFER_CONFIRM                 = WAS_CLIENT_REFERRED_TO_CONFIRMATORY,
      REFER_RETEST                  = WAS_CLIENT_ADVISED_TO_RE_TEST,
      RETEST_MOS                    = RE_TEST_IN_MONTH_S,
      RETEST_WKS                    = RE_TEST_IN_WEEK_S,
      RETEST_DATE                   = RE_TEST_DATE,
      SERVICE_HIV_101               = OTHER_SERVICES_PROVIDED_HIV_101,
      SERVICE_IEC_MATS              = OTHER_SERVICES_PROVIDED_IEC_MATERIALS,
      SERVICE_RISK_COUNSEL          = OTHER_SERVICES_PROVIDED_RISK_REDUCTION_PLANNING,
      SERVICE_PREP_REFER            = OTHER_SERVICES_REFERRED_TO_PREP,
      SERVICE_SSNT_OFFER            = OTHER_SERVICES_OFFERED_SSNT,
      SERVICE_SSNT_ACCEPT           = OTHER_SERVICES_ACCEPTED_SSNT,
      SERVICE_CONDOMS               = OTHER_SERVICES_NUMBER_OF_CONDOMS_GIVEN,
      SERVICE_LUBES                 = OTHER_SERVICES_NUMBER_OF_LUBES_GIVEN,
      PROVIDER_TYPE                 = PRIMARY_HTS_PROVIDER_TYPE,
      CLINIC_NOTES                  = CLINICAL_NOTES,
      COUNSEL_NOTES                 = COUNSELING_NOTES,
      CURR_ADDR                     = CURRENT_ADDRESS,
      PERM_ADDR                     = PERMANENT_ADDRESS,
      BIRTH_ADDR                    = BIRTHPLACE_ADDRESS,
      HIV_SERVICE_ADDR              = HIV_SERVICE_ADDRESS,
   ) %>%
   mutate(
      PROVIDER      = toupper(coalesce(PERFORMED_BY, NAME_OF_SERVICE_PROVIDER)),
      HTS_FACI      = coalesce(TESTING_FACILITY, RECORD_FACI, "990005"),
      MODE_OF_REACH = str_squish(toupper(MODE_OF_REACH)),
      REACH_ONLINE  = if_else(str_detect(MODE_OF_REACH, "ONLINE"), "1_Yes", NA_character_),
      REACH_VENUE   = if_else(MODE_OF_REACH == "OUTREACH", "1_Yes", NA_character_),
      REACH_SSNT    = if_else(MODE_OF_REACH == "SSNT", "1_Yes", NA_character_),
   ) %>%
   left_join(
      y  = aiha$convert$staff,
      by = join_by(PROVIDER)
   ) %>%
   separate_wider_delim(
      CURRENT_RESIDENCE_DROPDOWN,
      "|",
      names   = c("CURR_NAME_MUNC", "CURR_NAME_PROV", "CURR_NAME_REG"),
      too_few = "align_start"
   ) %>%
   left_join(
      y  = ohasis$ref_addr %>%
         select(
            CURR_PSGC_REG  = PSGC_REG,
            CURR_PSGC_PROV = PSGC_PROV,
            CURR_PSGC_MUNC = PSGC_MUNC,
            CURR_NAME_REG  = NAME_REG,
            CURR_NAME_PROV = NAME_PROV,
            CURR_NAME_MUNC = NAME_MUNC
         ),
      by = join_by(CURR_NAME_REG, CURR_NAME_PROV, CURR_NAME_MUNC)
   ) %>%
   left_join(
      y  = aiha$convert$addr %>%
         select(
            CURRENT_ADDRESS_REGION   = ADDRESS_REG,
            CURRENT_ADDRESS_PROVINCE = ADDRESS_PROV,
            CURRENT_ADDRESS_MUNCITY  = ADDRESS_MUNC,
            CORR_PSGC_REG            = PSGC_REG,
            CORR_PSGC_PROV           = PSGC_PROV,
            CORR_PSGC_MUNC           = PSGC_MUNC
         ),
      by = join_by(CURRENT_ADDRESS_REGION, CURRENT_ADDRESS_PROVINCE, CURRENT_ADDRESS_MUNCITY)
   ) %>%
   mutate(
      use_corr       = if_else(is.na(CURR_PSGC_REG) & !is.na(CORR_PSGC_REG), 1, 0, 0),
      CURR_PSGC_REG  = if_else(use_corr == 1, CORR_PSGC_REG, CURR_PSGC_REG),
      CURR_PSGC_PROV = if_else(use_corr == 1, CORR_PSGC_PROV, CURR_PSGC_PROV),
      CURR_PSGC_MUNC = if_else(use_corr == 1, CORR_PSGC_MUNC, CURR_PSGC_MUNC),
   ) %>%
   select(-starts_with("CORR_PSGC")) %>%
   separate_wider_delim(
      PERMANENT_RESIDENCE_DROPDOWN,
      "|",
      names   = c("PERM_NAME_MUNC", "PERM_NAME_PROV", "PERM_NAME_REG"),
      too_few = "align_start"
   ) %>%
   left_join(
      y  = ohasis$ref_addr %>%
         select(
            PERM_PSGC_REG  = PSGC_REG,
            PERM_PSGC_PROV = PSGC_PROV,
            PERM_PSGC_MUNC = PSGC_MUNC,
            PERM_NAME_REG  = NAME_REG,
            PERM_NAME_PROV = NAME_PROV,
            PERM_NAME_MUNC = NAME_MUNC
         ),
      by = join_by(PERM_NAME_REG, PERM_NAME_PROV, PERM_NAME_MUNC)
   ) %>%
   left_join(
      y  = aiha$convert$addr %>%
         select(
            PERMANENT_ADDRESS_REGION   = ADDRESS_REG,
            PERMANENT_ADDRESS_PROVINCE = ADDRESS_PROV,
            PERMANENT_ADDRESS_MUNICITY = ADDRESS_MUNC,
            CORR_PSGC_REG              = PSGC_REG,
            CORR_PSGC_PROV             = PSGC_PROV,
            CORR_PSGC_MUNC             = PSGC_MUNC
         ),
      by = join_by(PERMANENT_ADDRESS_REGION, PERMANENT_ADDRESS_PROVINCE, PERMANENT_ADDRESS_MUNICITY)
   ) %>%
   mutate(
      use_corr       = if_else(is.na(PERM_PSGC_REG) & !is.na(CORR_PSGC_REG), 1, 0, 0),
      PERM_PSGC_REG  = if_else(use_corr == 1, CORR_PSGC_REG, PERM_PSGC_REG),
      PERM_PSGC_PROV = if_else(use_corr == 1, CORR_PSGC_PROV, PERM_PSGC_PROV),
      PERM_PSGC_MUNC = if_else(use_corr == 1, CORR_PSGC_MUNC, PERM_PSGC_MUNC),
   ) %>%
   select(-starts_with("CORR_PSGC")) %>%
   separate_wider_delim(
      BIRTHPLACE_DROPDOWN,
      "|",
      names   = c("BIRTH_NAME_MUNC", "BIRTH_NAME_PROV", "BIRTH_NAME_REG"),
      too_few = "align_start"
   ) %>%
   left_join(
      y  = ohasis$ref_addr %>%
         select(
            BIRTH_PSGC_REG  = PSGC_REG,
            BIRTH_PSGC_PROV = PSGC_PROV,
            BIRTH_PSGC_MUNC = PSGC_MUNC,
            BIRTH_NAME_REG  = NAME_REG,
            BIRTH_NAME_PROV = NAME_PROV,
            BIRTH_NAME_MUNC = NAME_MUNC
         ),
      by = join_by(BIRTH_NAME_REG, BIRTH_NAME_PROV, BIRTH_NAME_MUNC)
   ) %>%
   left_join(
      y  = aiha$convert$addr %>%
         select(
            BIRTHPLACE_REGION   = ADDRESS_REG,
            BIRTHPLACE_PROVINCE = ADDRESS_PROV,
            BIRTHPLACE_MUNCITY  = ADDRESS_MUNC,
            CORR_PSGC_REG       = PSGC_REG,
            CORR_PSGC_PROV      = PSGC_PROV,
            CORR_PSGC_MUNC      = PSGC_MUNC
         ),
      by = join_by(BIRTHPLACE_REGION, BIRTHPLACE_PROVINCE, BIRTHPLACE_MUNCITY)
   ) %>%
   mutate(
      use_corr        = if_else(is.na(BIRTH_PSGC_REG) & !is.na(CORR_PSGC_REG), 1, 0, 0),
      BIRTH_PSGC_REG  = if_else(use_corr == 1, CORR_PSGC_REG, BIRTH_PSGC_REG),
      BIRTH_PSGC_PROV = if_else(use_corr == 1, CORR_PSGC_PROV, BIRTH_PSGC_PROV),
      BIRTH_PSGC_MUNC = if_else(use_corr == 1, CORR_PSGC_MUNC, BIRTH_PSGC_MUNC),
   ) %>%
   select(-starts_with("CORR_PSGC")) %>%
   separate_wider_delim(
      HIV_SERVICE_ADDRESS_DROPDOWN,
      "|",
      names   = c("HIV_SERVICE_NAME_MUNC", "HIV_SERVICE_NAME_PROV", "HIV_SERVICE_NAME_REG"),
      too_few = "align_start"
   ) %>%
   left_join(
      y  = ohasis$ref_addr %>%
         select(
            HIV_SERVICE_PSGC_REG  = PSGC_REG,
            HIV_SERVICE_PSGC_PROV = PSGC_PROV,
            HIV_SERVICE_PSGC_MUNC = PSGC_MUNC,
            HIV_SERVICE_NAME_REG  = NAME_REG,
            HIV_SERVICE_NAME_PROV = NAME_PROV,
            HIV_SERVICE_NAME_MUNC = NAME_MUNC
         ),
      by = join_by(HIV_SERVICE_NAME_REG, HIV_SERVICE_NAME_PROV, HIV_SERVICE_NAME_MUNC)
   ) %>%
   left_join(
      y  = aiha$convert$addr %>%
         select(
            HIV_SERVICE_FACILITY_REGION   = ADDRESS_REG,
            HIV_SERVICE_FACILITY_PROVINCE = ADDRESS_PROV,
            HIV_SERVICE_FACILITY_MUNICTY  = ADDRESS_MUNC,
            CORR_PSGC_REG                 = PSGC_REG,
            CORR_PSGC_PROV                = PSGC_PROV,
            CORR_PSGC_MUNC                = PSGC_MUNC
         ),
      by = join_by(HIV_SERVICE_FACILITY_REGION, HIV_SERVICE_FACILITY_PROVINCE, HIV_SERVICE_FACILITY_MUNICTY)
   ) %>%
   mutate(
      use_corr              = if_else(is.na(HIV_SERVICE_PSGC_REG) & !is.na(CORR_PSGC_REG), 1, 0, 0),
      HIV_SERVICE_PSGC_REG  = if_else(use_corr == 1, CORR_PSGC_REG, HIV_SERVICE_PSGC_REG),
      HIV_SERVICE_PSGC_PROV = if_else(use_corr == 1, CORR_PSGC_PROV, HIV_SERVICE_PSGC_PROV),
      HIV_SERVICE_PSGC_MUNC = if_else(use_corr == 1, CORR_PSGC_MUNC, HIV_SERVICE_PSGC_MUNC),
   ) %>%
   select(-starts_with("CORR_PSGC")) %>%
   mutate_at(
      .vars = vars(contains("DATE")),
      ~if_else(StrIsNumeric(.), as.Date(excel_numeric_to_date(as.numeric(.))), as.Date(parse_date_time(., c("YmdHMS", "Ymd", "mdY", "mdy", "mY")))) %>% as.character()
   ) %>%
   mutate(
      RECORD_DATE      = coalesce(RECORD_DATE, T0_DATE, CREATED_DATE),
      SELF_IDENT_OTHER = case_when(
         is.na(SELF_IDENT_OTHER) & SELF_IDENT == "TGW" ~ "TGW",
         TRUE ~ SELF_IDENT_OTHER
      ),
      SELF_IDENT       = case_when(
         SELF_IDENT == "TGW" ~ "3_Other",
         TRUE ~ SELF_IDENT
      ),

      EDUC_LEVEL       = case_when(
         EDUC_LEVEL %in% c("High School", "High Scool") ~ "3_High School",
         EDUC_LEVEL %in% c("Post Graduate", "Post-Graduate") ~ "6_Post-Graduate",
         EDUC_LEVEL == "college" ~ "4_College",
         EDUC_LEVEL == "Vocational" ~ "5_Vocational",
         TRUE ~ EDUC_LEVEL
      ),

      T0_RESULT        = case_when(
         T0_RESULT == "Reactive" ~ "1_Reactive",
         T0_RESULT == "REACTIVE" ~ "1_Reactive",
         T0_RESULT == "Non-reactive" ~ "2_Non-reactive",
         TRUE ~ T0_RESULT
      ),
      PREV_TEST_RESULT = case_when(
         PREV_TEST_RESULT == "Reactive" ~ "1_Positive",
         PREV_TEST_RESULT == "POSITIVE" ~ "1_Positive",
         PREV_TEST_RESULT == "Non-reactive" ~ "2_Negative",
         PREV_TEST_RESULT == "NEGATIVE" ~ "2_Negative",
         PREV_TEST_RESULT == "Negative" ~ "2_Negative",
         PREV_TEST_RESULT == "NR" ~ "2_Negative",
         PREV_TEST_RESULT == "DON'T KNOW" ~ "4_Was not able to get result",
         TRUE ~ PREV_TEST_RESULT
      ),
      PREV_TEST_FACI   = remove_code(PREV_TEST_FACI),
      PROVIDER_TYPE    = case_when(
         PROVIDER_TYPE == "CBS Motivator" ~ "3_CBS Motivator",
         PROVIDER_TYPE == "HIV Counselor" ~ "2_HIV Counselor",
         PROVIDER_TYPE == "Others" ~ "8888_Other",
         PROVIDER_TYPE == "PN" ~ "8888_Other",
         PROVIDER_TYPE == "Peer Navigator" ~ "8888_Other",
         PROVIDER_TYPE == "PeerNavegator" ~ "8888_Other",
         TRUE ~ PROVIDER_TYPE
      ),
      CLIENT_TYPE      = case_when(
         CLIENT_TYPE == "Mobile HTS" ~ "3_Mobile HTS Client",
         CLIENT_TYPE == "Outpatient/Walk-in" ~ "2_Walk-in / Outpatient",
         TRUE ~ CLIENT_TYPE
      ),
      MODALITY         = "101103_Community-based (CBS)",
      row_id           = row_number(),
   ) %>%
   mutate_at(
      .vars = vars(FIRST, MIDDLE, LAST, SUFFIX, PHILHEALTH_NO, UIC, PATIENT_CODE, CONFIRMATORY_CODE),
      ~na_if(clean_pii(.), "0")
   )

#  uploaded --------------------------------------------------------------------

db               <- "ohasis_warehouse"
lw_conn          <- ohasis$conn("lw")
aiha$prev_upload <- dbTable(
   lw_conn,
   db,
   "form_hts",
   raw_where = TRUE,
   where     = glue(r"(
         (RECORD_DATE BETWEEN '{min(as.character(aiha$hts$RECORD_DATE))}' AND '{max(as.character(aiha$hts$RECORD_DATE))}') AND
            FACI_ID = '990005'
   )")
)
aiha$prev_upload %<>%
   mutate_if(
      .predicate = is.POSIXct,
      ~as.Date(.)
   ) %>%
   mutate_all(~as.character(.))
TIMESTAMP <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
dbDisconnect(lw_conn)

#  uploaded --------------------------------------------------------------------

aiha$hts %<>%
   select(-any_of("PATIENT_ID")) %>%
   mutate(
      UIC = StrLeft(UIC, 14)
   ) %>%
   # get records id if existing
   left_join(
      y  = aiha$prev_upload %>%
         select(
            PATIENT_ID,
            CONFIRMATORY_CODE,
            UIC,
            FIRST,
            MIDDLE,
            LAST,
            SUFFIX,
            SEX,
            BIRTHDATE,
            PHILHEALTH_NO,
            PHILSYS_ID,
            CLIENT_MOBILE,
            CLIENT_EMAIL
         ) %>%
         distinct(
            CONFIRMATORY_CODE,
            UIC,
            FIRST,
            MIDDLE,
            LAST,
            SUFFIX,
            SEX,
            BIRTHDATE,
            PHILHEALTH_NO,
            PHILSYS_ID,
            CLIENT_MOBILE,
            CLIENT_EMAIL,
            .keep_all = TRUE
         ),
      by = join_by(
         CONFIRMATORY_CODE,
         UIC,
         FIRST,
         MIDDLE,
         LAST,
         SUFFIX,
         SEX,
         BIRTHDATE,
         PHILHEALTH_NO,
         PHILSYS_ID,
         CLIENT_MOBILE,
         CLIENT_EMAIL
      )
   ) %>%
   # get records id if existing
   left_join(
      y  = aiha$prev_upload %>%
         select(
            REC_ID,
            PATIENT_ID,
            CREATED_BY,
            CREATED_AT,
            RECORD_DATE
         ),
      by = join_by(PATIENT_ID, RECORD_DATE)
   ) %>%
   mutate(
      CREATED_BY   = coalesce(CREATED_BY, STAFF_ID, "1300000048"),
      CREATED_AT   = coalesce(CREATED_AT, RECORD_DATE),
      # CREATED_AT   = format(CREATED_AT, "%Y-%m-%d 00:00:00"),

      HTS_SUB_FACI = ""
   ) %>%
   left_join(
      y  = ohasis$ref_faci %>%
         select(
            HTS_FACI     = FACI_ID,
            HTS_SUB_FACI = SUB_FACI_ID,
            FACI_PSGC_REG,
            FACI_PSGC_PROV,
            FACI_PSGC_MUNC,
         ),
      by = join_by(HTS_FACI, HTS_SUB_FACI)
   ) %>%
   mutate(
      use_faci_location     = if_else(coalesce(HIV_SERVICE_PSGC_MUNC, "") == "", 1, 0, 0),
      HIV_SERVICE_PSGC_REG  = if_else(use_faci_location == 1, FACI_PSGC_REG, HIV_SERVICE_PSGC_REG),
      HIV_SERVICE_PSGC_PROV = if_else(use_faci_location == 1, FACI_PSGC_PROV, HIV_SERVICE_PSGC_PROV),
      HIV_SERVICE_PSGC_MUNC = if_else(use_faci_location == 1, FACI_PSGC_MUNC, HIV_SERVICE_PSGC_MUNC),
   ) %>%
   # fix risk
   mutate_at(
      .vars = vars(
         starts_with("EXPOSE") & !contains("DATE"),
         starts_with("TEST_REASON") & !contains("TEXT"),
         starts_with("MED_") & !contains("TEXT"),
         starts_with("REFER_") & !contains("TEXT"),
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
         IS_OFW,
         IS_STUDENT,
         IS_PREGNANT,
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
         TRUE ~ .
      )
   ) %>%
   # fix risk
   mutate_at(
      .vars = vars(
         starts_with("MED_") & !contains("TEXT"),
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
   mutate(
      SCREEN_AGREED = case_when(
         SCREEN_AGREED == "1_Agreed" ~ "1_Yes",
         SCREEN_AGREED == "Accepted HIV Testing" ~ "1_Yes",
      ),
      CIVIL_STATUS  = case_when(
         CIVIL_STATUS == "Single" ~ "1_Single",
         CIVIL_STATUS == "Married" ~ "2_Married",
         CIVIL_STATUS == "Separated" ~ "3_Separated",
         CIVIL_STATUS == "Widowed" ~ "4_Widowed",
         CIVIL_STATUS == "1_Yes" ~ NA_character_,
         TRUE ~ CIVIL_STATUS
      ),
      NATIONALITY   = case_when(
         NATIONALITY == 'Austrian' ~ 'Austria',
         NATIONALITY == 'Cambodian' ~ 'Cambodia',
         NATIONALITY == 'Filipino' ~ 'Philippines',
         TRUE ~ NATIONALITY
      ),
      OFW_STATION   = case_when(
         OFW_STATION == 'Ship' ~ '1_On a ship',
         OFW_STATION == 'SHIP' ~ '1_On a ship',
         OFW_STATION == 'Land' ~ '2_Land',
         TRUE ~ OFW_STATION
      )
   )

aiha$check <- list(
   addr  = bind_rows(
      aiha$hts %>%
         mutate(
            PERMANENT_ADDRESS_REGION = coalesce(PERMANENT_ADDRESS_REGION, PERM_NAME_REG),
            PERMANENT_ADDRESS_PROVINCE = coalesce(PERMANENT_ADDRESS_PROVINCE, PERM_NAME_PROV),
            PERMANENT_ADDRESS_MUNICITY = coalesce(PERMANENT_ADDRESS_REGION, PERM_NAME_MUNC),
         ) %>%
         filter(
            is.na(PERM_PSGC_REG),
            if_any(c(PERMANENT_ADDRESS_REGION, PERMANENT_ADDRESS_PROVINCE, PERMANENT_ADDRESS_MUNICITY), ~!is.na(.))
         ) %>%
         distinct(
            ENCODED_REG  = PERMANENT_ADDRESS_REGION,
            ENCODED_PROV = PERMANENT_ADDRESS_PROVINCE,
            ENCODED_MUNC = PERMANENT_ADDRESS_MUNICITY
         ),
      aiha$hts %>%
         filter(
            is.na(CURR_PSGC_REG),
            if_any(c(CURRENT_ADDRESS_REGION, CURRENT_ADDRESS_PROVINCE, CURRENT_ADDRESS_MUNCITY), ~!is.na(.))
         ) %>%
         distinct(
            ENCODED_REG  = CURRENT_ADDRESS_REGION,
            ENCODED_PROV = CURRENT_ADDRESS_PROVINCE,
            ENCODED_MUNC = CURRENT_ADDRESS_MUNCITY
         ),
      aiha$hts %>%
         filter(
            is.na(BIRTH_PSGC_REG),
            if_any(c(BIRTHPLACE_REGION, BIRTHPLACE_PROVINCE, BIRTHPLACE_MUNCITY), ~!is.na(.))
         ) %>%
         distinct(
            ENCODED_REG  = BIRTHPLACE_REGION,
            ENCODED_PROV = BIRTHPLACE_PROVINCE,
            ENCODED_MUNC = BIRTHPLACE_MUNCITY
         ),
      aiha$hts %>%
         filter(
            is.na(HIV_SERVICE_PSGC_REG),
            if_any(c(HIV_SERVICE_FACILITY_REGION, HIV_SERVICE_FACILITY_PROVINCE, HIV_SERVICE_FACILITY_MUNICTY), ~!is.na(.))
         ) %>%
         distinct(
            ENCODED_REG  = HIV_SERVICE_FACILITY_REGION,
            ENCODED_PROV = HIV_SERVICE_FACILITY_PROVINCE,
            ENCODED_MUNC = HIV_SERVICE_FACILITY_MUNICTY
         )
   ) %>%
      distinct_all(),
   staff = aiha$hts %>%
      filter(
         is.na(STAFF_ID),
         !is.na(PROVIDER)
      ) %>%
      distinct(PROVIDER)
)

aiha$import <- aiha$hts %>%
   select(row_id, HTS_FACI, any_of(names(aiha$prev_upload))) %>%
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
   mutate(
      WORK_TEXT    = WORK,
      MODULE       = "2_Testing",
      DISEASE      = "HIV",
      FACI_ID      = "990005",
      old_rec      = if_else(!is.na(REC_ID), 1, 0, 0),
      # UPDATED_BY   = if_else(old_rec == 1, "1300000048", NA_character_),
      # UPDATED_AT   = if_else(old_rec == 1, TIMESTAMP, NA_character_),
      PROVIDER_ID  = CREATED_BY,
      SERVICE_TYPE = MODALITY,
      TEST_TYPE    = "10",
      TEST_NUM     = 1,
      RESULT       = T0_RESULT,
      DATE_PERFORM = T0_DATE,
      PERFORM_BY   = CREATED_BY,
      FORM         = "HTS Form",
      VERSION      = "2021",
   )

aiha$import %<>%
   filter(!is.na(PATIENT_ID)) %>%
   bind_rows(
      batch_px_ids(aiha$import %>% filter(is.na(PATIENT_ID)), PATIENT_ID, FACI_ID, "row_id")
   )

aiha$import %<>%
   filter(!is.na(REC_ID)) %>%
   bind_rows(
      batch_rec_ids(aiha$import %>% filter(is.na(REC_ID)), REC_ID, CREATED_BY, "row_id")
   )

match_vars <- intersect(names(aiha$import), names(aiha$prev_upload))
# aiha$import %<>%
#    anti_join(aiha$prev_upload, join_by(PATIENT_ID, RECORD_DATE))
aiha$import %<>%
   select(-HTS_FACI) %>%
   anti_join(
      y  = aiha$prev_upload %>%
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

aiha$prev_upload %>%
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

aiha$import %>%
   select(any_of(match_vars)) %>%
   filter(PATIENT_ID == "20231201990005003E") %>%
   write_clip()

aiha$import %<>%
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

aiha$tables <- deconstruct_hts(aiha$import)
wide        <- c("px_test_refuse", "px_other_service", "px_reach", "px_med_profile", "px_test_reason")
delete      <- aiha$tables$px_record$data %>% select(REC_ID)

db_conn <- ohasis$conn("db")
lapply(wide, function(table) dbxDelete(db_conn, Id(schema = "ohasis_interim", table = table), delete))
# lapply(wide, function(table) dbExecute(conn = db_conn, statement = glue("DELETE FROM ohasis_interim.{table} WHERE REC_ID IN (?)"), params = list(aiha$import$REC_ID)))

lapply(aiha$tables, function(ref, db_conn) {
   log_info("Uploading {green(ref$name)}.")
   table_space <- Id(schema = "ohasis_interim", table = ref$name)
   dbxUpsert(db_conn, table_space, ref$data, ref$pk)
   # dbExecute(db_conn, glue("DELETE FROM ohasis_interim.{ref$name} WHERE REC_ID IN (?)"), params = list(unique(ref$data$REC_ID)))
}, db_conn)
dbDisconnect(db_conn)
