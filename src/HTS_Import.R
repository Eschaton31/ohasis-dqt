dir   <- "H:/hts-imports/20241106"
files <- list.files(dir, ".xlsx", recursive = TRUE, full.names = TRUE)

HtsLogsheet <- R6Class(
   'HtsLogsheet',
   public = list(
      files          = NULL,
      data           = list(
         ref       = tibble(),
         raw       = tibble(),
         filtered  = tibble(),
         converted = tibble()
      ),
      corr           = list(
         staff = tibble(),
         site  = tibble(),
         date  = tibble(),
         addr  = tibble()
      ),
      issues         = list(),

      read           = function(file) {
         sheets   <- excel_sheets(file)
         sheets   <- sheets[!str_detect(sheets, "Chart")]
         logsheet <- list()
         for (sheet in sheets) {
            test <- read_excel(file, sheet, n_max = 3, .name_repair = "unique_quiet")

            if (ncol(test) == 121 |
               ncol(test) == 79 |
               ncol(test) == 122 |
               ncol(test) == 80) {
               logsheet[[sheet]] <- read_excel(file, sheet, skip = 2, .name_repair = "unique_quiet") %>%
                  mutate_all(as.character) %>%
                  mutate_all(~na_if(., "N/A")) %>%
                  mutate_all(~na_if(., "n/a")) %>%
                  rename_all(
                     ~case_when(
                        . == "Encoded On" ~ "CREATED_AT",
                        . == "Encoded By: (Full Name)" ~ "CREATED_BY",
                        . == "Signed Consent" ~ "SIGNATURE",
                        . == "Verbal Consent" ~ "VERBAL_CONSENT",
                        . == "1. Date of Test/Reach" ~ "RECORD_DATE",
                        . == "2. PhilHealth No." ~ "PHILHEALTH_NO",
                        . == "3. PhilSys ID" ~ "PHILSYS_ID",
                        . == "HIV Confirmatory Code" ~ "CONFIRM_CODE",
                        . == "Patient Code" ~ "PATIENT_CODE",
                        . == "4. First Name" ~ "FIRST",
                        . == "Middle Name" ~ "MIDDLE",
                        . == "Last Name" ~ "LAST",
                        . == "Suffix (Jr., Sr., III, etc.)" ~ "SUFFIX",
                        . == "UIC" ~ "UIC",
                        . == "6. Birth Date" ~ "BIRTHDATE",
                        . == "Age (in years)" ~ "AGE",
                        . == "Age (in months)" ~ "AGE_MO",
                        . == "7. Sex (at birth)" ~ "SEX",
                        . == "Gender Identity" ~ "SELF_IDENT",
                        . == "Gender Identity (Others)" ~ "SELF_IDENT_OTHER",
                        . == "9. Nationality" ~ "NATIONALITY",
                        . == "10. Civil Status" ~ "CIVIL_STATUS",
                        . == "11. Currently living with a partner?" ~ "LIVING_WITH_PARTNER",
                        . == "No. Of Children" ~ "CHILDREN",
                        . == "12. Currently Pregnant?" ~ "IS_PREGNANT",
                        . == "13. Highest Education Attainment" ~ "EDUC_LEVEL",
                        . == "14. Currently in school?" ~ "IS_STUDENT",
                        . == "15. Currently working?" ~ "IS_EMPLOYED",
                        . == "16. Worked/Resided abroad/overseas in the past 5 years?" ~ "IS_OFW",
                        . == "Occupation" ~ "WORK_TEXT",
                        . == "Year last returned" ~ "OFW_YR_RET",
                        . == "Where were you based?" ~ "OFW_STATION",
                        . == "Country last worked in" ~ "OFW_COUNTRY",
                        . == "17. Birth mother had HIV" ~ "EXPOSE_HIV_MOTHER",
                        . == "Sex w/ Male (Yes/No)" ~ "EXPOSE_SEX_M",
                        . == "17. Sex w/ Male (Yes/No)" ~ "EXPOSE_SEX_M",
                        . == "Anal/Neovaginal Sex w/ Male (Date Most Recent)" ~ "EXPOSE_SEX_M_AV_DATE",
                        . == "Anal/Neovaginal Sex w/ Male (Date Most Recent Condomless)" ~ "EXPOSE_SEX_M_AV_NOCONDOM_DATE",
                        . == "Sex w/ Female (Yes/No)" ~ "EXPOSE_SEX_F",
                        . == "Anal/Neovaginal Sex w/ Female (Date Most Recent)" ~ "EXPOSE_SEX_F_AV_DATE",
                        . == "Anal/Neovaginal Sex w/ Female (Date Most Recent Condomless)" ~ "EXPOSE_SEX_F_AV_NOCONDOM_DATE",
                        . == "Paid for Sex (Yes/No)" ~ "EXPOSE_SEX_PAYMENT",
                        . == "Paid for Sex (Date Most Recent)" ~ "EXPOSE_SEX_PAYMENT_DATE",
                        . == "Paying for Sex (Yes/No)" ~ "EXPOSE_SEX_PAYING",
                        . == "Paying for Sex (Date Most Recent)" ~ "EXPOSE_SEX_PAYING_DATE",
                        . == "Sex under influence of drugs (Yes/No)" ~ "EXPOSE_SEX_DRUGS",
                        . == "Sex under influence of drugs (Date Most Recent)" ~ "EXPOSE_SEX_DRUGS_DATE",
                        . == "Shared needles during drug injection (Yes/No)" ~ "EXPOSE_DRUG_INJECT",
                        . == "Shared needles during drug injection (Date Most Recent)" ~ "EXPOSE_DRUG_INJECT_DATE",
                        . == "Received blood transfusion (Yes/No)" ~ "EXPOSE_BLOOD_TRANSFUSE",
                        . == "Received blood transfusion (Date Most Recent)" ~ "EXPOSE_BLOOD_TRANSFUSE_DATE",
                        . == "Occupational Exposure" ~ "EXPOSE_OCCUPATION",
                        . == "Occupational Exposure  (Date Most Recent)" ~ "EXPOSE_OCCUPATION_DATE",
                        . == "Occupational Exposure (Date Most Recent)" ~ "EXPOSE_OCCUPATION_DATE",
                        . == "18. Possible exposure to HIV" ~ "TEST_REASON_HIV_EXPOSE",
                        . == "Recommended by physician/nurse/midwife" ~ "TEST_REASON_PHYSICIAN",
                        . == "Referred by a peer educator" ~ "TEST_REASON_PEER_ED",
                        . == "Employment - Overseas" ~ "TEST_REASON_EMPLOY_OFW",
                        . == "Employment - Local" ~ "TEST_REASON_EMPLOY_LOCAL",
                        . == "Received a text message/email" ~ "TEST_REASON_TEXT_EMAIL",
                        . == "Requirement for insurance" ~ "TEST_REASON_INSURANCE",
                        . == "Other reasons (Specify)" ~ "TEST_REASON_OTHER",
                        . == "19. Ever been tested for HIV?" ~ "PREV_TESTED",
                        . == "Date of most recent HIV test" ~ "PREV_TEST_DATE",
                        . == "Site/Organization or City/Municipality where you got tested" ~ "PREV_TEST_FACI",
                        . == "Result of last test" ~ "PREV_TEST_RESULT",
                        . == "20. Current TB patient" ~ "MED_TB_PX",
                        . == "With Hepatitis B" ~ "MED_HEP_B",
                        . == "With Hepatitis C" ~ "MED_HEP_C",
                        . == "Diagnosed with other STIs" ~ "MED_STI",
                        . == "Taken PEP" ~ "MED_PEP_PX",
                        . == "Taking PrEP" ~ "MED_PREP_PX",
                        . == "21. Clinical Picture" ~ "CLINICAL_PIC",
                        . == "Describe S/Sx" ~ "SYMPTOMS",
                        . == "WHO Staging" ~ "WHO_CLASS",
                        . == "22. Client Type" ~ "CLIENT_TYPE",
                        . == "23. Clinical" ~ "REACH_CLINICAL",
                        . == "Online" ~ "REACH_ONLINE",
                        . == "Index testing" ~ "REACH_INDEX",
                        . == "SSNT" ~ "REACH_SSNT",
                        . == "Outreach" ~ "REACH_VENUE",
                        . == "24. HIV Test (Accept/Refuse)" ~ "SCREEN_AGREED",
                        . == "Refer to ART" ~ "REFER_ART",
                        . == "Refer for Confirmatory" ~ "REFER_CONFIRM",
                        . == "Advise for retesting in: Months" ~ "RETEST_MOS",
                        . == "Advise for retesting in: Weeks" ~ "RETEST_WKS",
                        . == "25. HIV 101" ~ "SERVICE_HIV_101",
                        . == "IEC materials" ~ "SERVICE_IEC_MATS",
                        . == "Risk reduction planning" ~ "SERVICE_RISK_COUNSEL",
                        . == "Referred to PrEP or given PEP" ~ "SERVICE_PREP_REFER",
                        . == "Offered SSNT" ~ "SERVICE_SSNT_OFFER",
                        . == "Accepted SSNT" ~ "SERVICE_SSNT_ACCEPT",
                        . == "Condoms: # distributed" ~ "SERVICE_CONDOMS",
                        . == "Lubricants: # distributed" ~ "SERVICE_LUBES",
                        . == "HIV testing modality" ~ "SERVICE_TYPE",
                        . == "HIV test result" ~ "T0_RESULT",
                        . == "Reason for refusal" ~ "TEST_REFUSE_REASON_OTHER_TEXT",
                        . == "26. Name of Testing Site/Organization" ~ "HTS_FACI",
                        . == "27. Primary HTS Provider" ~ "PROVIDER_ID",
                        . == "HTS Provider Type" ~ "PROVIDER_TYPE",
                        . == "HTS Provider Type (Others)" ~ "PROVIDER_TYPE_OTHER",
                        . == "Clinical Notes" ~ "CLINIC_NOTES",
                        . == "Counseling Notes" ~ "COUNSEL_NOTES",
                        . == "Permanent Residence: Region" ~ "PERM_NAME_REG",
                        . == "Permanent Residence: Province" ~ "PERM_NAME_PROV",
                        . == "Permanent Residence:  City/Municipality" ~ "PERM_NAME_MUNC",
                        . == "8. Current Residence: Region" ~ "CURR_NAME_REG",
                        . == "Current Residence: Province" ~ "CURR_NAME_PROV",
                        . == "Current Residence:  City/Municipality" ~ "CURR_NAME_MUNC",
                        . == "Place of Birth: Region" ~ "BIRTH_NAME_REG",
                        . == "Place of Birth: Province" ~ "BIRTH_NAME_PROV",
                        . == "Place of Birth:  City/Municipality" ~ "BIRTH_NAME_MUNC",
                        . == "Venue: Region" ~ "HIV_SERVICE_NAME_REG",
                        . == "Venue: Province" ~ "HIV_SERVICE_NAME_PROV",
                        . == "Venue: City/Municipality" ~ "HIV_SERVICE_NAME_MUNC",
                        . == "Venue: Details" ~ "HIV_SERVICE_ADDR",
                        . == "Record ID" ~ "REC_ID",
                        TRUE ~ .
                     )
                  ) %>%
                  mutate(
                     row_num = as.character(row_number())
                  )
            }
         }
         return(logsheet %>% bind_rows(.id = 'sheet'))
      },
      batchRead      = function(path) {
         files         <- list.files(path, ".xlsx", recursive = TRUE, full.names = TRUE)
         self$data$raw <- pbsapply(files, self$read, simplify = FALSE, USE.NAMES = TRUE) %>%
            bind_rows(.id = "id") %>%
            mutate_all(~na_if(., "#REF!")) %>%
            mutate(
               id     = basename(id),
               row_id = stri_c(basename(id), "-", sheet, "-", row_num)
            )

         invisible(self)
      },
      getExisting    = function() {
         con <- connect("ohasis-lw")

         if ("REC_ID" %in% names(data)) {
            self$data$ref <- QB$new(con)$from("ohasis_warehouse.form_hts")$whereIn("REC_ID", data$REC_ID)$get()
         } else {
            self$data$ref <- QB$new(con)$from("ohasis_warehouse.form_hts")$limit(0)$get()
         }

         dbDisconnect(con)

         invisible(self)
      },
      getRefs        = function() {
         local_gs4_quiet()

         ss <- "1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc"

         self$corr$staff <- range_speedread(ss, "staff", show_col_types = FALSE, col_types = cols(.default = "c"), name_repair = "unique_quiet")
         self$corr$site  <- range_speedread(ss, "site", show_col_types = FALSE, col_types = cols(.default = "c"), name_repair = "unique_quiet")
         self$corr$addr  <- range_speedread(ss, "addr", show_col_types = FALSE, col_types = cols(.default = "c"), name_repair = "unique_quiet")
         self$corr$date  <- range_speedread(ss, "file-no_created_at", show_col_types = FALSE, col_types = cols(.default = "c"), name_repair = "unique_quiet") %>%
            select(
               id                    = FILE,
               HIV_SERVICE_NAME_REG  = VENUE_REG,
               HIV_SERVICE_NAME_PROV = VENUE_PROV,
               HIV_SERVICE_NAME_MUNC = VENUE_MUNC,
               DATE_SUBMIT           = "DATE SUBMITTED TO MEO
(yyyy-mm-dd)"
            )

         invisible(self)
      },
      convert        = function() {
         if ("REC_ID" %in% names(self$data$raw)) {
            self$data$convert <- self$data$raw %>%
               select(-CREATED_BY, -CREATED_AT) %>%
               left_join(
                  y  = self$data$ref %>%
                     select(REC_ID, PATIENT_ID, CREATED_BY, CREATED_AT),
                  by = join_by(REC_ID)
               ) %>%
               filter(!is.na(PATIENT_ID))

            pii_cols <- c(
               "FIRST",
               "MIDDLE",
               "LAST",
               "SUFFIX",
               "BIRTHDATE",
               "SEX",
               "UIC",
               "PHILHEALTH_NO",
               "SELF_IDENT",
               "SELF_IDENT_OTHER",
               "PHILSYS_ID",
               "CIVIL_STATUS",
               "NATIONALITY",
               "EDUC_LEVEL",
               "CLIENT_MOBILE",
               "CLIENT_EMAIL"
            )

            for (col in pii_cols) {
               col <- as.name(col)
               self$data$convert %<>%
                  left_join(
                     y  = self$data$ref %>%
                        select(REC_ID, CORR = {{col}}),
                     by = join_by(REC_ID)
                  ) %>%
                  mutate(
                     {{col}} := coalesce({{col}}, as.character(CORR))
                  ) %>%
                  select(-CORR)
            }
         } else {
            self$data$convert <- self$data$raw %>%
               mutate(
                  REC_ID        = NA_character_,
                  PATIENT_ID    = NA_character_,
                  CLIENT_MOBILE = NA_character_,
                  CLIENT_EMAIL  = NA_character_,
               )
         }

         self$data$convert %<>%
            left_join(
               y  = self$corr$date,
               by = join_by(id, HIV_SERVICE_NAME_REG, HIV_SERVICE_NAME_PROV, HIV_SERVICE_NAME_MUNC)
            ) %>%
            mutate(
               CREATED_AT    = coalesce(CREATED_AT, DATE_SUBMIT, RECORD_DATE),
               CLIENT_MOBILE = NA_character_,
               CLIENT_EMAIL  = NA_character_,
            ) %>%
            mutate_at(
               .vars = vars(
                  PERM_NAME_REG,
                  PERM_NAME_PROV,
                  PERM_NAME_MUNC,
                  CURR_NAME_REG,
                  CURR_NAME_PROV,
                  CURR_NAME_MUNC,
                  BIRTH_NAME_REG,
                  BIRTH_NAME_PROV,
                  BIRTH_NAME_MUNC,
                  HIV_SERVICE_NAME_REG,
                  HIV_SERVICE_NAME_PROV,
                  HIV_SERVICE_NAME_MUNC
               ),
               ~coalesce(na_if(str_squish(toupper(.)), ""), "UNKNOWN")
            ) %>%
            left_join(
               y  = self$corr$addr %>%
                  select(
                     PERM_NAME_REG  = NAME_REG,
                     PERM_NAME_PROV = NAME_PROV,
                     PERM_NAME_MUNC = NAME_MUNC,
                     CORR_NAME_REG,
                     CORR_NAME_PROV,
                     CORR_NAME_MUNC
                  ),
               by = join_by(PERM_NAME_REG, PERM_NAME_PROV, PERM_NAME_MUNC)
            ) %>%
            mutate(
               PERM_NAME_REG  = coalesce(CORR_NAME_REG, PERM_NAME_REG),
               PERM_NAME_PROV = coalesce(CORR_NAME_PROV, PERM_NAME_PROV),
               PERM_NAME_MUNC = coalesce(CORR_NAME_MUNC, PERM_NAME_MUNC),
            ) %>%
            select(-starts_with("CORR_NAME_")) %>%
            left_join(
               y  = self$corr$addr %>%
                  select(
                     CURR_NAME_REG  = NAME_REG,
                     CURR_NAME_PROV = NAME_PROV,
                     CURR_NAME_MUNC = NAME_MUNC,
                     CORR_NAME_REG,
                     CORR_NAME_PROV,
                     CORR_NAME_MUNC
                  ),
               by = join_by(CURR_NAME_REG, CURR_NAME_PROV, CURR_NAME_MUNC)
            ) %>%
            mutate(
               CURR_NAME_REG  = coalesce(CORR_NAME_REG, CURR_NAME_REG),
               CURR_NAME_PROV = coalesce(CORR_NAME_PROV, CURR_NAME_PROV),
               CURR_NAME_MUNC = coalesce(CORR_NAME_MUNC, CURR_NAME_MUNC),
            ) %>%
            select(-starts_with("CORR_NAME_")) %>%
            left_join(
               y  = self$corr$addr %>%
                  select(
                     BIRTH_NAME_REG  = NAME_REG,
                     BIRTH_NAME_PROV = NAME_PROV,
                     BIRTH_NAME_MUNC = NAME_MUNC,
                     CORR_NAME_REG,
                     CORR_NAME_PROV,
                     CORR_NAME_MUNC
                  ),
               by = join_by(BIRTH_NAME_REG, BIRTH_NAME_PROV, BIRTH_NAME_MUNC)
            ) %>%
            mutate(
               BIRTH_NAME_REG  = coalesce(CORR_NAME_REG, BIRTH_NAME_REG),
               BIRTH_NAME_PROV = coalesce(CORR_NAME_PROV, BIRTH_NAME_PROV),
               BIRTH_NAME_MUNC = coalesce(CORR_NAME_MUNC, BIRTH_NAME_MUNC),
            ) %>%
            select(-starts_with("CORR_NAME_")) %>%
            left_join(
               y  = self$corr$addr %>%
                  select(
                     HIV_SERVICE_NAME_REG  = NAME_REG,
                     HIV_SERVICE_NAME_PROV = NAME_PROV,
                     HIV_SERVICE_NAME_MUNC = NAME_MUNC,
                     CORR_NAME_REG,
                     CORR_NAME_PROV,
                     CORR_NAME_MUNC
                  ),
               by = join_by(HIV_SERVICE_NAME_REG, HIV_SERVICE_NAME_PROV, HIV_SERVICE_NAME_MUNC)
            ) %>%
            mutate(
               HIV_SERVICE_NAME_REG  = coalesce(CORR_NAME_REG, HIV_SERVICE_NAME_REG),
               HIV_SERVICE_NAME_PROV = coalesce(CORR_NAME_PROV, HIV_SERVICE_NAME_PROV),
               HIV_SERVICE_NAME_MUNC = coalesce(CORR_NAME_MUNC, HIV_SERVICE_NAME_MUNC),
            ) %>%
            select(-starts_with("CORR_NAME_")) %>%
            left_join(
               y  = ohasis$ref_addr %>%
                  mutate_at(
                     .vars = vars(NAME_REG, NAME_PROV, NAME_MUNC),
                     ~str_squish(toupper(.))
                  ) %>%
                  select(
                     PERM_NAME_REG  = NAME_REG,
                     PERM_NAME_PROV = NAME_PROV,
                     PERM_NAME_MUNC = NAME_MUNC,
                     PERM_REG       = PSGC_REG,
                     PERM_PROV      = PSGC_PROV,
                     PERM_MUNC      = PSGC_MUNC
                  ),
               by = join_by(PERM_NAME_REG, PERM_NAME_PROV, PERM_NAME_MUNC)
            ) %>%
            left_join(
               y  = ohasis$ref_addr %>%
                  mutate_at(
                     .vars = vars(NAME_REG, NAME_PROV, NAME_MUNC),
                     ~str_squish(toupper(.))
                  ) %>%
                  select(
                     CURR_NAME_REG  = NAME_REG,
                     CURR_NAME_PROV = NAME_PROV,
                     CURR_NAME_MUNC = NAME_MUNC,
                     CURR_REG       = PSGC_REG,
                     CURR_PROV      = PSGC_PROV,
                     CURR_MUNC      = PSGC_MUNC
                  ),
               by = join_by(CURR_NAME_REG, CURR_NAME_PROV, CURR_NAME_MUNC)
            ) %>%
            left_join(
               y  = ohasis$ref_addr %>%
                  mutate_at(
                     .vars = vars(NAME_REG, NAME_PROV, NAME_MUNC),
                     ~str_squish(toupper(.))
                  ) %>%
                  select(
                     BIRTH_NAME_REG  = NAME_REG,
                     BIRTH_NAME_PROV = NAME_PROV,
                     BIRTH_NAME_MUNC = NAME_MUNC,
                     BIRTH_REG       = PSGC_REG,
                     BIRTH_PROV      = PSGC_PROV,
                     BIRTH_MUNC      = PSGC_MUNC
                  ),
               by = join_by(BIRTH_NAME_REG, BIRTH_NAME_PROV, BIRTH_NAME_MUNC)
            ) %>%
            left_join(
               y  = ohasis$ref_addr %>%
                  mutate_at(
                     .vars = vars(NAME_REG, NAME_PROV, NAME_MUNC),
                     ~str_squish(toupper(.))
                  ) %>%
                  select(
                     HIV_SERVICE_NAME_REG  = NAME_REG,
                     HIV_SERVICE_NAME_PROV = NAME_PROV,
                     HIV_SERVICE_NAME_MUNC = NAME_MUNC,
                     HIV_SERVICE_REG       = PSGC_REG,
                     HIV_SERVICE_PROV      = PSGC_PROV,
                     HIV_SERVICE_MUNC      = PSGC_MUNC
                  ),
               by = join_by(HIV_SERVICE_NAME_REG, HIV_SERVICE_NAME_PROV, HIV_SERVICE_NAME_MUNC)
            ) %>%
            rename(COUNTRY_NAME = NATIONALITY) %>%
            left_join(select(ohasis$ref_country, COUNTRY_NAME = NATIONALITY, NATIONALITY = COUNTRY_CODE), join_by(COUNTRY_NAME)) %>%
            rename(NATIONALITY_RAW = COUNTRY_NAME) %>%
            rename(COUNTRY_NAME = OFW_COUNTRY) %>%
            left_join(select(ohasis$ref_country, COUNTRY_NAME, OFW_COUNTRY = COUNTRY_CODE), join_by(COUNTRY_NAME)) %>%
            rename(OFW_COUNTRY_RAW = COUNTRY_NAME) %>%
            rename(STAFF_NAME = CREATED_BY) %>%
            mutate(STAFF_NAME = toupper(STAFF_NAME)) %>%
            left_join(select(self$corr$staff, STAFF_NAME, CREATED_BY = USER_ID) %>% distinct(STAFF_NAME, .keep_all = TRUE), join_by(STAFF_NAME)) %>%
            rename(STAFF_NAME_RAW = STAFF_NAME) %>%
            rename(STAFF_NAME = PROVIDER_ID) %>%
            mutate(STAFF_NAME = toupper(STAFF_NAME)) %>%
            left_join(select(self$corr$staff, STAFF_NAME, PROVIDER_ID = USER_ID) %>% distinct(STAFF_NAME, .keep_all = TRUE), join_by(STAFF_NAME)) %>%
            rename(PROVIDER_RAW = STAFF_NAME) %>%
            filter(!is.na(HTS_FACI)) %>%
            left_join(self$corr$site %>% distinct(HTS_FACI, .keep_all = TRUE), join_by(HTS_FACI)) %>%
            mutate(
               # FACI_ID          = coalesce(FACI_ID, substr(CREATED_BY, 1, 6)),
               SERVICE_FACI     = FACI_ID,
               SERVICE_SUB_FACI = SUB_FACI_ID,
            ) %>%
            mutate(
               DISEASE                  = "101000",
               MODULE                   = "2_Testing",
               SEX                      = case_when(
                  SEX == "Male" ~ "1_Male",
                  SEX == "MALE" ~ "1_Male",
                  SEX == "Man" ~ "1_Male",
                  SEX == "Female" ~ "2_Female",
                  TRUE ~ SEX
               ),
               SELF_IDENT               = case_when(
                  SELF_IDENT == "Man" ~ "1_Man",
                  SELF_IDENT == "MAN" ~ "1_Man",
                  SELF_IDENT == "Woman" ~ "2_Woman",
                  SELF_IDENT == "WOMAN" ~ "2_Woman",
                  SELF_IDENT == "Others" ~ "3_Other",
                  SELF_IDENT == "Other" ~ "3_Other",
                  TRUE ~ SELF_IDENT
               ),
               BIRTHDATE                = if_else(
                  str_length(UIC) == 14 & is.na(BIRTHDATE),
                  stri_c(sep = "-", StrRight(UIC, 4), substr(UIC, 7, 8), substr(UIC, 9, 10)),
                  BIRTHDATE,
                  BIRTHDATE
               ),

               EDUC_LEVEL               = case_when(
                  EDUC_LEVEL == "None" ~ "1_None",
                  EDUC_LEVEL == "Elementary" ~ "2_Elementary",
                  EDUC_LEVEL == "High School" ~ "3_High School",
                  EDUC_LEVEL == "HIGHSCHOOL" ~ "3_High School",
                  EDUC_LEVEL == "College" ~ "4_College",
                  EDUC_LEVEL == "COLLEGE" ~ "4_College",
                  EDUC_LEVEL == "Vocational" ~ "5_Vocational",
                  EDUC_LEVEL == "Post-Graduate" ~ "6_Post-Graduate",
                  EDUC_LEVEL == "Post-graduate" ~ "6_Post-Graduate",
                  EDUC_LEVEL == "Pre-school" ~ "7_Pre-school",
                  TRUE ~ EDUC_LEVEL
               ),
               CIVIL_STATUS             = case_when(
                  CIVIL_STATUS == "Single" ~ "1_Single",
                  CIVIL_STATUS == "SIngle" ~ "1_Single",
                  CIVIL_STATUS == "SINGLE" ~ "1_Single",
                  CIVIL_STATUS == "Married" ~ "2_Married",
                  CIVIL_STATUS == "MARRIED" ~ "2_Married",
                  CIVIL_STATUS == "Separated" ~ "3_Separated",
                  CIVIL_STATUS == "Widowed" ~ "4_Widowed",
                  CIVIL_STATUS == "Divorced" ~ "5_Divorced",
                  TRUE ~ CIVIL_STATUS
               ),

               SERVICE_TYPE             = case_when(
                  SERVICE_TYPE == "Mortality" ~ "*00001",
                  SERVICE_TYPE == "Facility-based Testing (FBT)" ~ "101101",
                  SERVICE_TYPE == "FBT" ~ "101101",
                  SERVICE_TYPE == "Community-based (CBS)" ~ "101103",
                  SERVICE_TYPE == "CBS" ~ "101103",
                  SERVICE_TYPE == "Non-laboratory FBT (FBS)" ~ "101104",
                  SERVICE_TYPE == "Non-lab FBT" ~ "101104",
                  SERVICE_TYPE == "Self-testing" ~ "101105",
                  SERVICE_TYPE == "Anti-Retroviral Treatment (ART)" ~ "101201",
                  SERVICE_TYPE == "Pre-Exposure Prophylaxis (PrEP)" ~ "101301",
                  SERVICE_TYPE == "Prevention of Mother-to-Child Transmission (PMTCT)" ~ "101303",
                  SERVICE_TYPE == "Reach" ~ "101304",
                  TRUE ~ coalesce(SERVICE_TYPE, '101304')
               ),
               PROVIDER_TYPE            = case_when(
                  PROVIDER_TYPE == "Medical Technologist" ~ "1_Medical Technologist",
                  PROVIDER_TYPE == "MedTech" ~ "1_Medical Technologist",
                  PROVIDER_TYPE == "HIV Counselor" ~ "2_HIV Counselor",
                  PROVIDER_TYPE == "CBS Motivator" ~ "3_CBS Motivator",
                  PROVIDER_TYPE == "Other" ~ "8888_Other",
                  PROVIDER_TYPE == "PEER NAVIGATOR" ~ "8888_Other",
                  PROVIDER_TYPE == "Peer Navigator" ~ "8888_Other",
                  PROVIDER_TYPE == "Others" ~ "8888_Other",
                  PROVIDER_TYPE == "Case Finder" ~ "8888_Other",
                  TRUE ~ PROVIDER_TYPE
               ),
               CLIENT_TYPE              = case_when(
                  CLIENT_TYPE == "Inpatient" ~ "1_Inpatient",
                  CLIENT_TYPE == "Walk-in / Outpatient" ~ "2_Walk-in / Outpatient",
                  CLIENT_TYPE == "Walk-in" ~ "2_Walk-in / Outpatient",
                  CLIENT_TYPE == "WALK-IN" ~ "2_Walk-in / Outpatient",
                  CLIENT_TYPE == "Outpatient" ~ "2_Walk-in / Outpatient",
                  CLIENT_TYPE == "Mobile HTS Client" ~ "3_Mobile HTS Client",
                  CLIENT_TYPE == "MOBIle HTS Client" ~ "3_Mobile HTS Client",
                  CLIENT_TYPE == "mobile HTS Client" ~ "3_Mobile HTS Client",
                  CLIENT_TYPE == "Satellite Client" ~ "5_Satellite Client",
                  CLIENT_TYPE == "Referral" ~ "4_Referral",
                  CLIENT_TYPE == "Transient" ~ "6_Transient",
                  CLIENT_TYPE == "Persons Deprived of Liberty" ~ "7_Persons Deprived of Liberty",
                  CLIENT_TYPE == "Courier" ~ "8_Courier",
                  TRUE ~ CLIENT_TYPE
               ),

               FORM                     = 'HTS Form',
               VERSION                  = 2021,

               T0_DATE                  = RECORD_DATE,

               SCREEN_AGREED            = case_when(
                  SCREEN_AGREED == "Accept" ~ "1_Yes",
               ),

               # TODO: Add conversion for ofw data
               NATIONALITY = case_when(
                  toupper(NATIONALITY_RAW) == "FILIPINO" ~ "PH",
                  TRUE ~ NATIONALITY
               ),

               OFW_STATION              = case_when(
                  OFW_STATION == "On a ship" ~ "1_On a ship",
                  OFW_STATION == "Land" ~ "2_Land",
                  TRUE ~ as.character(OFW_STATION)
               ),

               EXPOSE_SEX_M_AV          = if_else(!is.na(EXPOSE_SEX_M_AV_DATE), "1_Yes", NA_character_),
               EXPOSE_SEX_M_AV_NOCONDOM = if_else(!is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE), "1_Yes", NA_character_),

               EXPOSE_SEX_F_AV          = if_else(!is.na(EXPOSE_SEX_F_AV_DATE), "1_Yes", NA_character_),
               EXPOSE_SEX_F_AV_NOCONDOM = if_else(!is.na(EXPOSE_SEX_F_AV_NOCONDOM_DATE), "1_Yes", NA_character_),

               PREV_TEST_RESULT         = case_when(
                  PREV_TEST_RESULT == "Reactive" ~ "1_Reactive",
                  PREV_TEST_RESULT == "Non-reactive" ~ "2_Non-reactive",
                  PREV_TEST_RESULT == "NA" ~ "4_Was not able to get result",
                  PREV_TEST_RESULT == "Was not able to get result" ~ "4_Was not able to get result",
                  TRUE ~ PREV_TEST_RESULT
               ),

               CLINICAL_PIC             = case_when(
                  CLINICAL_PIC == "Asymptomatic" ~ "1_Symptomatic",
                  CLINICAL_PIC == "Symptomatic" ~ "2_Symptomatic",
                  TRUE ~ CLINICAL_PIC
               ),
               WHO_CLASS                = case_when(
                  WHO_CLASS == "I" ~ "1_I",
                  WHO_CLASS == "II" ~ "2_II",
                  WHO_CLASS == "III" ~ "3_III",
                  WHO_CLASS == "IV" ~ "4_IV",
                  TRUE ~ WHO_CLASS
               ),

               TEST_REFUSE_REASON_OTHER = if_else(!is.na(TEST_REFUSE_REASON_OTHER_TEXT), "Yes", NA_character_),

               T0_RESULT                = case_when(
                  T0_RESULT == "NON-REACTIVE" ~ "2_Non-reactive",
                  T0_RESULT == "Non-reactive" ~ "2_Non-reactive",
                  T0_RESULT == "REACTIVE" ~ "1_Reactive",
                  T0_RESULT == "Reactive" ~ "1_Reactive",
                  T0_RESULT == "CBS" ~ NA_character_,
                  TRUE ~ T0_RESULT
               )
            ) %>%
            mutate_at(
               .vars = vars(
                  LIVING_WITH_PARTNER,
                  IS_PREGNANT,
                  IS_EMPLOYED,
                  IS_STUDENT,
                  IS_OFW,
                  SIGNATURE,
                  VERBAL_CONSENT,
                  PREV_TESTED,
                  starts_with("EXPOSE_"),
                  starts_with("TEST_REASON_"),
                  starts_with("MED_"),
                  starts_with("REACH_"),
                  starts_with("REFER_"),
                  starts_with("SERVICE_"),
               ),
               ~case_when(
                  str_squish(toupper(.)) == "YES" ~ "1_Yes",
                  str_squish(toupper(.)) == "NO" ~ "0_No",
                  TRUE ~ .
               )
            ) %>%
            mutate_at(
               .vars = vars(
                  starts_with("REACH_"),
                  starts_with("SERVICE_"),
               ),
               ~na_if(., "0_No")
            ) %>%
            mutate_at(
               .vars = vars(contains("_DATE"), contains("DATE_"), BIRTHDATE),
               ~as.character(as.Date(parse_date_time(., c("Ymd", "mdY", "mY", "Ym", "Y"))))
            ) %>%
            mutate(
               UPDATED_BY = "1300000048",
               UPDATED_AT = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
            ) %>%
            relocate(any_of(names(self$data$ref)), .before = 1)

         invisible(self)
      },
      checkIssues    = function() {
         self$issues <- list(
            `file-no_created_at` = self$data$raw %>%
               filter(is.na(CREATED_AT), !is.na(RECORD_DATE)) %>%
               distinct(
                  `FILE NAME` = id,
                  VENUE_REG   = HIV_SERVICE_NAME_REG,
                  VENUE_PROV  = HIV_SERVICE_NAME_PROV,
                  VENUE_MUNC  = HIV_SERVICE_NAME_MUNC
               ) %>%
               arrange(`FILE NAME`, VENUE_REG, VENUE_PROV, VENUE_MUNC),
            `per-file_faci`      = self$data$raw %>%
               filter(RECORD_DATE != "YYYY-MM-DD") %>%
               mutate(id = basename(id)) %>%
               distinct(HTS_FACI, FILE = id) %>%
               arrange(HTS_FACI, FILE),

            clean_addr           = self$data$convert %>%
               filter(is.na(PERM_REG)) %>%
               distinct(NAME_REG = PERM_NAME_REG, NAME_PROV = PERM_NAME_PROV, NAME_MUNC = PERM_NAME_MUNC) %>%
               bind_rows(
                  self$data$convert %>%
                     filter(is.na(CURR_REG)) %>%
                     distinct(NAME_REG = CURR_NAME_REG, NAME_PROV = CURR_NAME_PROV, NAME_MUNC = CURR_NAME_MUNC),
                  self$data$convert %>%
                     filter(is.na(BIRTH_REG)) %>%
                     distinct(NAME_REG = BIRTH_NAME_REG, NAME_PROV = BIRTH_NAME_PROV, NAME_MUNC = BIRTH_NAME_MUNC),
                  self$data$convert %>%
                     filter(is.na(HIV_SERVICE_REG)) %>%
                     distinct(NAME_REG = HIV_SERVICE_NAME_REG, NAME_PROV = HIV_SERVICE_NAME_PROV, NAME_MUNC = HIV_SERVICE_NAME_MUNC)
               ) %>%
               distinct() %>%
               arrange(NAME_REG, NAME_PROV, NAME_MUNC),

            clean_country        = self$data$convert %>%
               filter(is.na(NATIONALITY)) %>%
               distinct(COUNTRY = NATIONALITY_RAW) %>%
               bind_rows(
                  self$data$convert %>%
                     filter(is.na(OFW_COUNTRY_RAW)) %>%
                     distinct(COUNTRY = OFW_COUNTRY_RAW)
               ) %>%
               distinct() %>%
               filter(!is.na(COUNTRY)) %>%
               arrange(COUNTRY),

            clean_faci           = self$data$convert %>%
               filter(!is.na(HTS_FACI), is.na(FACI_ID)) %>%
               distinct(HTS_FACI),

            clean_staff          = self$data$convert %>%
               filter(!is.na(STAFF_NAME_RAW), is.na(CREATED_BY)) %>%
               distinct(STAFF = STAFF_NAME_RAW) %>%
               bind_rows(
                  self$data$convert %>%
                     filter(!is.na(PROVIDER_RAW), is.na(PROVIDER_ID)) %>%
                     distinct(STAFF = PROVIDER_RAW)
               ) %>%
               distinct() %>%
               arrange(STAFF)
         )

         invisible(self)
      },
      removeBlanks   = function() {
         self$data$filtered <- self$data$convert %>%
            filter(!is.na(RECORD_DATE)) %>%
            filter(CREATED_AT != "Auto-fill") %>%
            select(
               -ends_with("NAME_REG"),
               -ends_with("NAME_PROV"),
               -ends_with("NAME_MUNC"),
            )


         self$data$filtered %<>%
            filter(!is.na(PATIENT_ID)) %>%
            bind_rows(
               batch_px_ids(self$data$filtered %>% filter(is.na(PATIENT_ID)), PATIENT_ID, FACI_ID, "row_id")
            )

         self$data$filtered %<>%
            filter(!is.na(REC_ID)) %>%
            bind_rows(
               batch_rec_ids(self$data$filtered %>% filter(is.na(REC_ID)), REC_ID, CREATED_BY, "row_id")
            )

         invisible(self)
      },
      compareRecords = function(rec_id) {
         match_vars <- intersect(names(self$data$filtered), names(self$data$ref))
         match_vars <- match_vars[!(match_vars %in% c("PRIME", "UPDATED_BY", "UPDATED_AT", "DELETED_BY", "DELETED_AT", "FORM_VERSION", "DISEASE", "HTS_MSM", "HTS_TGW", "SNAPSHOT", "CONFIRMATORY_CODE", "PHILHEALTH_NO", "PHILSYS_ID"))]
         match_vars <- match_vars[!(match_vars %in% c("EXPOSE_HIV_MOTHER", "EXPOSE_SEX_M_AV_NOCONDOM", "EXPOSE_SEX_M_AV_NOCONDOM_DATE", "EXPOSE_SEX_F_AV_NOCONDOM", "EXPOSE_SEX_F_AV_NOCONDOM_DATE", "SERVICE_BY", "TEST_REASON_OTHER_TEXT"))]
         match_vars <- match_vars[!(match_vars %in% c("AGE_MO", "GENDER_AFFIRM_THERAPY", "IS_PREGNANT"))]
         self$data$filtered %>%
            filter(REC_ID == rec_id) %>%
            mutate(type = "new", .before = 1) %>%
            mutate_all(as.character) %>%
            bind_rows(self$data$ref %>%
                         filter(REC_ID == rec_id) %>%
                         mutate_all(as.character) %>%
                         mutate(type = "old", .before = 1)) %>%
            select(any_of(match_vars)) %>%
            View('review')
      }
   )
)

import <- HtsLogsheet$new()
import$batchRead("H:/hts-imports/20241106")
import$getExisting()
import$getRefs()
import$convert()
import$checkIssues()
import$removeBlanks()

tables <- deconstruct_hts(import$data$filtered)
long   <- c("px_test_refuse", "px_other_service", "px_reach", "px_med_profile", "px_test_reason")
delete <- tables$px_record$data %>% select(REC_ID)

db_conn <- ohasis$conn("db")
pblapply(long, function(table) dbxDelete(db_conn, Id(schema = "ohasis_interim", table = table), delete))
pblapply(tables, function(ref, db_conn) {
   log_info("Uploading {green(ref$name)}.")
   table_space <- Id(schema = "ohasis_interim", table = ref$name)
   dbxUpsert(db_conn, table_space, ref$data, ref$pk)
   # dbExecute(db_conn, glue("DELETE FROM ohasis_interim.{ref$name} WHERE REC_ID IN (?)"), params = list(unique(ref$data$REC_ID)))
}, db_conn)
dbDisconnect(db_conn)


delete <- tables$px_record$data %>% select(REC_ID)
pblapply(names(tables), function(table) dbxDelete(db_conn, Id(schema = "ohasis_interim", table = table), delete))
