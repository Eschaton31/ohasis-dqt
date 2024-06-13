hts_logsheet <- list.files(
   path       = "C:/Users/Administrator/Downloads/HTS Logheet",
   pattern    = "*.xlsx",
   recursive  = TRUE,
   full.names = TRUE
)

conso_logsheet <- lapply(hts_logsheet, function(file) {
   sheets   <- excel_sheets(file)
   logsheet <- list()
   for (sheet in sheets) {
      test <- read_excel(file, sheet, n_max = 3)

      if (ncol(test) == 121 | ncol(test) == 79) {
         logsheet[[sheet]] <- read_excel(file, sheet, skip = 2) %>%
            mutate_all(as.character)
      }
   }
   return(logsheet %>% bind_rows(.id = 'sheet'))
}) %>%
   bind_rows(.id = "id") %>%
   filter(!is.na(`Encoded By: (Full Name)`))

convert_addr <- conso_logsheet %>%
   rename(
      PERM_NAME_REG   = 'Permanent Residence: Region',
      PERM_NAME_PROV  = 'Permanent Residence: Province',
      PERM_NAME_MUNC  = 'Permanent Residence:  City/Municipality',
      CURR_NAME_REG   = '8. Current Residence: Region',
      CURR_NAME_PROV  = 'Current Residence: Province',
      CURR_NAME_MUNC  = 'Current Residence:  City/Municipality',
      BIRTH_NAME_REG  = 'Place of Birth: Region',
      BIRTH_NAME_PROV = 'Place of Birth: Province',
      BIRTH_NAME_MUNC = 'Place of Birth:  City/Municipality',
      VENUE_NAME_REG  = 'Venue: Region',
      VENUE_NAME_PROV = 'Venue: Province',
      VENUE_NAME_MUNC = 'Venue: City/Municipality'
   ) %>%
   left_join(
      y  = ohasis$ref_addr %>%
         select(
            PERM_NAME_REG  = 'NAME_REG',
            PERM_NAME_PROV = 'NAME_PROV',
            PERM_NAME_MUNC = 'NAME_MUNC',
            PERM_PSGC_REG  = 'PSGC_REG',
            PERM_PSGC_PROV = 'PSGC_PROV',
            PERM_PSGC_MUNC = 'PSGC_MUNC'
         ),
      by = join_by(PERM_NAME_REG, PERM_NAME_PROV, PERM_NAME_MUNC)
   ) %>%
   select(
      -PERM_NAME_REG, -PERM_NAME_PROV, -PERM_NAME_MUNC
   ) %>%
   left_join(
      y  = ohasis$ref_addr %>%
         select(CURR_NAME_REG  = 'NAME_REG',
                CURR_NAME_PROV = 'NAME_PROV',
                CURR_NAME_MUNC = 'NAME_MUNC',
                CURR_PSGC_REG  = 'PSGC_REG',
                CURR_PSGC_PROV = 'PSGC_PROV',
                CURR_PSGC_MUNC = 'PSGC_MUNC'
         ),
      by = join_by(CURR_NAME_REG, CURR_NAME_PROV, CURR_NAME_MUNC)
   ) %>%
   left_join(
      y  = ohasis$ref_addr %>%
         select(
            BIRTH_NAME_REG  = 'NAME_REG',
            BIRTH_NAME_PROV = 'NAME_PROV',
            BIRTH_NAME_MUNC = 'NAME_MUNC',
            BIRTH_PSGC_REG  = 'PSGC_REG',
            BIRTH_PSGC_PROV = 'PSGC_PROV',
            BIRTH_PSGC_MUNC = 'PSGC_MUNC'
         ),
      by = join_by(BIRTH_NAME_REG, BIRTH_NAME_PROV, BIRTH_NAME_MUNC)
   ) %>%
   left_join(
      y  = ohasis$ref_addr %>%
         select(
            VENUE_NAME_REG  = 'NAME_REG',
            VENUE_NAME_PROV = 'NAME_PROV',
            VENUE_NAME_MUNC = 'NAME_MUNC',
            VENUE_PSGC_REG  = 'PSGC_REG',
            VENUE_PSGC_PROV = 'PSGC_PROV',
            VENUE_PSGC_MUNC = 'PSGC_MUNC'
         ),
      by = join_by(VENUE_NAME_REG, VENUE_NAME_PROV, VENUE_NAME_MUNC)
   ) %>%
   rename(
      CREATED_AT                  = "Encoded On",
      CREATED_BY                  = "Encoded By: (Full Name)",
      RECORD_DATE                 = "1. Date of Test/Reach",
      PHILHEALTH_NO               = "2. PhilHealth No.",
      PHILSYS_ID                  = "3. PhilSys ID",
      CONFIRM_CODE                = "HIV Confirmatory Code",
      PATIENT_CODE                = "Patient Code",
      FIRST                       = "4. First Name",
      MIDDLE                      = "Middle Name",
      LAST                        = "Last Name",
      SUFFIX                      = "Suffix (Jr., Sr., III, etc.)",
      UIC                         = "UIC",
      BIRTHDATE                   = "6. Birth Date",
      AGE                         = "Age (in years)",
      AGE_MO                      = "Age (in months)",
      SEX                         = "7. Sex (at birth)",
      gender_identity             = "Gender Identity",
      CURR_REG                    = "CURR_NAME_REG",
      CURR_PROV                   = "CURR_NAME_PROV",
      CURR_MUNC                   = "CURR_NAME_MUNC",
      PERM_REG                    = "PERM_NAME_REG",
      PERM_PROV                   = "PERM_NAME_PROV",
      PERM_MUNC                   = "PERM_NAME_MUNC",
      BIRTH_REG                   = "BIRTH_NAME_REG",
      BIRTH_PROV                  = "BIRTH_NAME_PROV",
      BIRTH_MUNC                  = "BIRTH_NAME_MUNC",
      NATIONALITY                 = "9. Nationality",
      CIVIL_STATUS                = "10. Civil Status",
      LIVING_WITH_PARTNER         = "11. Currently living with a partner?",
      CHILDREN                    = "No. Of Children",
      IS_PREGNANT                 = "12. Currently Pregnant?",
      EDUC_LEVEL                  = "13. Highest Education Attainment",
      IS_STUDENT                  = "14. Currently in school?",
      IS_EMPLOYED                 = "15. Currently working?",
      WORK                        = "Occupation",
      OFW_YR_RET                  = "Year last returned",
      OFW_STATION                 = "Where were you based?",
      OFW_COUNTRY                 = "Country last worked in",
      EXPOSE_HIV_MOTHER           = "17. Birth mother had HIV",
      EXPOSE_SEX_M                = "Sex w/ Male (Yes/No)",
      EXPOSE_SEX_M_AV             = "Anal/Neovaginal Sex w/ Male (Date Most Recent)",
      EXPOSE_SEX_M_AV_NOCONDOM    = "Anal/Neovaginal Sex w/ Male (Date Most Recent Condomless)",
      EXPOSE_SEX_F                = "Sex w/ Female (Yes/No)",
      EXPOSE_SEX_F_AV             = "Anal/Neovaginal Sex w/ Female (Date Most Recent)",
      EXPOSE_SEX_F_AV_NOCONDOM    = "Anal/Neovaginal Sex w/ Female (Date Most Recent Condomless)",
      EXPOSE_SEX_PAYMENT          = "Paid for Sex (Yes/No)",
      EXPOSE_SEX_PAYMENT_DATE     = "Paid for Sex (Date Most Recent)",
      EXPOSE_SEX_PAYING           = "Paying for Sex (Yes/No)",
      EXPOSE_SEX_PAYING_DATE      = "Paying for Sex (Date Most Recent)",
      EXPOSE_SEX_DRUGS            = "Sex under influence of drugs (Yes/No)",
      EXPOSE_SEX_DRUGS_DATE       = "Sex under influence of drugs (Date Most Recent)",
      EXPOSE_NEEDLE_SHARE         = "Shared needles during drug injection (Yes/No)",
      EXPOSE_NEEDLE_SHARE_DATE    = "Shared needles during drug injection (Date Most Recent)",
      EXPOSE_BLOOD_TRANSFUSE      = "Received blood transfusion (Yes/No)",
      EXPOSE_BLOOD_TRANSFUSE_DATE = "Received blood transfusion (Date Most Recent)",
      EXPOSE_OCCUPATION           = "Occupational Exposure",
      EXPOSE_OCCUPATION_DATE      = "Occupational Exposure  (Date Most Recent)",
      TEST_REASON_HIV_EXPOSE      = "18. Possible exposure to HIV",
      TEST_REASON_PHYSICIAN       = "Recommended by physician/nurse/midwife",
      TEST_REASON_PEER_ED         = "Referred by a peer educator",
      TEST_REASON_EMPLOY_OFW      = "Employment - Overseas",
      TEST_REASON_EMPLOY_LOCAL    = "Employment - Local",
      TEST_REASON_TEXT_EMAIL      = "Received a text message/email",
      TEST_REASON_INSURANCE       = "Requirement for insurance",
      TEST_REASON_OTHER           = "Other reasons (Specify)",
      PREV_TESTED                 = "19. Ever been tested for HIV?",
      PREV_TEST_DATE              = "Date of most recent HIV test",
      PREV_TEST_FACI              = "Site/Organization or City/Municipality where you got tested",
      PREV_TEST_RESULT            = "Result of last test",
      MED_TB_PX                   = "20. Current TB patient",
      MED_HEP_B                   = "With Hepatitis B",
      MED_HEP_C                   = "With Hepatitis C",
      MED_STI                     = "Diagnosed with other STIs",
      MED_PEP_PX                  = "Taken PEP",
      MED_PREP_PX                 = "Taking PrEP",
      CLINICAL_PIC                = "21. Clinical Picture",
      SYMPTOMS                    = "Describe S/Sx",
      WHO_CLASS                   = "WHO Staging",
      CLIENT_TYPE                 = "22. Client Type",
      REACH_CLINICAL              = "23. Clinical",
      REACH_ONLINE                = "Online",
      REACH_INDEX_TESTING         = "Index testing",
      REACH_SSNT                  = "SSNT",
      REACH_VENUE                 = "Outreach",
      SCREEN_AGREED               = "24. HIV Test (Accept/Refuse)",
      REFER_ART                   = "Refer to ART",
      REFER_CONFIRM               = "Refer for Confirmatory",
      RETEST_MOS                  = "Advise for retesting in: Months",
      RETEST_WKS                  = "Advise for retesting in: Weeks",
      SERVICE_HIV_101             = "25. HIV 101",
      SERVICE_IEC_MATS            = "IEC materials",
      SERVICE_RISK_COUNSEL        = "Risk reduction planning",
      SERVICE_PREP_REFER          = "Referred to PrEP or given PEP",
      SERVICE_SSNT_OFFER          = "Offered SSNT",
      SERVICE_SSNT_ACCEPT         = "Accepted SSNT",
      SERVICE_CONDOMS             = "Condoms: # distributed",
      SERVICE_LUBES               = "Lubricants: # distributed"
   ) %>%
   mutate(
      CLIENT_MOBILE = "",
      CLIENT_EMAIL = "",
      REC_ID = row_number(),
      SERVICE_TYPE = '101103_CBS',
      TEST_REFUSE_REASON_OTHER_TEXT = NA_character_,
      CLINIC_NOTES = NA_character_,
      COUNSEL_NOTES = NA_character_,
   )

final_logsheet <- deconstruct_hts(convert_addr)




