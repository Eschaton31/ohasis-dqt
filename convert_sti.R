tables <- c("px_pii", "px_faci_info", "px_ob", "px_occupation", "px_key_pop", "px_sti_sx", "px_remarks", "px_sti_testing", "px_sti_treat", "px_egasp", "px_vaccine")
lapply(tables, function(tbl) ohasis$data_factory("lake", tbl, "upsert", TRUE))

tables <- c("form_sti", "id_registry")
lapply(tables, function(tbl) ohasis$data_factory("warehouse", tbl, "upsert", TRUE))

con      <- ohasis$conn("lw")
id_reg   <- QB$new(con)$from("ohasis_warehouse.id_registry")$select("CENTRAL_ID", "PATIENT_ID")$get()
form_sti <- QB$new(con)$from("ohasis_warehouse.form_sti")$get()
dbDisconnect(con)

convert_type <- "name"
sti          <- form_sti %>%
   get_cid(id_reg, PATIENT_ID) %>%
   mutate_at(
      .vars = vars(FIRST, MIDDLE, LAST, SUFFIX, PATIENT_CODE, UIC, PHILHEALTH_NO, PHILSYS_ID, CLIENT_MOBILE, CLIENT_EMAIL),
      ~clean_pii(.)
   ) %>%
   mutate_if(
      .predicate = is.Date,
      ~if_else(. <= -25567, NA_Date_, ., .)
   ) %>%
   mutate(
      SERVICE_FACI     = coalesce(SERVICE_FACI, FACI_ID),
      SERVICE_SUB_FACI = if_else(SERVICE_FACI %in% c("130001", "130605", "040200", "130023"), SERVICE_SUB_FACI, NA_character_)
   ) %>%
   get_latest_pii(
      "CENTRAL_ID",
      c(
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
         "NATIONALITY",
         "CURR_PSGC_REG",
         "CURR_PSGC_PROV",
         "CURR_PSGC_MUNC",
         "PERM_PSGC_REG",
         "PERM_PSGC_PROV",
         "PERM_PSGC_MUNC",
         "CLIENT_MOBILE",
         "CLIENT_EMAIL"
      )
   ) %>%
   rename(
      CREATED = CREATED_BY,
      UPDATED = UPDATED_BY,
   ) %>%
   ohasis$get_faci(
      list(REPORT_FACI = c("FACI_ID", "SUB_FACI_ID")),
      convert_type
   ) %>%
   ohasis$get_faci(
      list(STI_FACI = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
      convert_type,
      c("STI_REG", "STI_PROV", "STI_MUNC")
   ) %>%
   ohasis$get_addr(
      c(
         PERM_REG  = "PERM_PSGC_REG",
         PERM_PROV = "PERM_PSGC_PROV",
         PERM_MUNC = "PERM_PSGC_MUNC"
      ),
      convert_type
   ) %>%
   ohasis$get_addr(
      c(
         CURR_REG  = "CURR_PSGC_REG",
         CURR_PROV = "CURR_PSGC_PROV",
         CURR_MUNC = "CURR_PSGC_MUNC"
      ),
      convert_type
   ) %>%
   ohasis$get_staff(c(CREATED_BY = "CREATED")) %>%
   ohasis$get_staff(c(UPDATED_BY = "UPDATED")) %>%
   ohasis$get_staff(c(STI_PROVIDER = "SERVICE_BY")) %>%
   mutate_at(
      .vars = vars(
         VISIT_TYPE,
         SEX,
         IS_PREGNANT,
         SELF_IDENT,
         KP_REGISTERED_SW,
         KP_FREELANCE_SW,
         KP_MSM,
         KP_TG,
         KP_PWID,
         KP_PDL,
         KP_OFW,
         KP_PARTNER,
         KP_OFW_PARTNER,
         KP_GENPOP,
         CLIENT_TYPE,
         STI_SX_DISCHARGE_VAGINAL,
         STI_SX_DISCHARGE_ANAL,
         STI_SX_DISCHARGE_URETHRAL,
         STI_SX_DISCHARGE_OROPHARYNGEAL,
         STI_SX_SWOLLEN_SCROTUM,
         STI_SX_PAIN_URINE,
         STI_SX_ULCER_GENITAL,
         STI_SX_WARTS_GENITAL,
         STI_SX_PAIN_ABDOMEN,
         STI_SX_BLISTER,
         STI_SX_ULCERS,
         STI_SX_WARTS,
         STI_SX_MASS,
         STI_SX_FOUL_ODOR,
         STI_SX_RASHES,
         STI_SX_REDNESS,
         STI_SX_LICE,
         STI_SX_ABSCESS,
         SYPH_RPR_RESULT,
         SYPH_TPPATPHA_RESULT,
         SYPH_OTHER_RESULT,
         SYPH_TREATMENT,
         SPECIMEN_MALE_UREHTRA,
         SPECIMEN_FEMALE_CERVICAL,
         SPECIMEN_PHARYNX,
         SPECIMEN_RECTUM,
         GONO_GRAM_PUS,
         GONO_GRAM_NEG_EXTRA,
         GONO_GRAM_NEG_INTRA,
         IS_SYNDROMIC,
         GONO_TREATMENT,
         BACVAG_GRAM_STAIN_RESULT,
         BACVAG_OTHER_RESULT,
         BACVAG_TREATMENT,
         TRICHO_WET_MOUNT_RESULT,
         TRICHO_OTHER_RESULT,
         TRICHO_TREATMENT,
         HIV_TEST_RESULT,
         HEPB_TEST_RESULT,
         VAX_HEPB_1ST_DONE,
         HEPC_TEST_RESULT,
      ),
      ~remove_code(.)
   ) %>%
   select(
      `Record ID`                                                                        = REC_ID,
      `Central ID`                                                                       = CENTRAL_ID,
      `Patient ID`                                                                       = PATIENT_ID,
      `Encoded by:`                                                                      = CREATED_BY,
      `Encoded on:`                                                                      = CREATED_AT,
      `Updated by:`                                                                      = UPDATED_BY,
      `Updated no:`                                                                      = UPDATED_AT,
      `Facility Visited`                                                                 = REPORT_FACI,
      `Visit Date`                                                                       = RECORD_DATE,
      `STI Facility`                                                                     = STI_FACI,
      `Name of Primary STI Provider`                                                     = STI_PROVIDER,
      `Visit Type`                                                                       = VISIT_TYPE,
      `HIV Confirmatory Code`                                                            = CONFIRMATORY_CODE,
      `UIC`                                                                              = UIC,
      `PhilHealth No.`                                                                   = PHILHEALTH_NO,
      `Sex`                                                                              = SEX,
      `Current Pregnant?`                                                                = IS_PREGNANT,
      `Birth Date`                                                                       = BIRTHDATE,
      `Patient Code`                                                                     = PATIENT_CODE,
      `PhilSys No.`                                                                      = PHILSYS_ID,
      `First Name`                                                                       = FIRST,
      `Middle Name`                                                                      = MIDDLE,
      `Last Name`                                                                        = LAST,
      `Suffix (Jr., Sr., III, etc)`                                                      = SUFFIX,
      `Age`                                                                              = AGE,
      `Age (in months)`                                                                  = AGE_MO,
      `Self-Identity`                                                                    = SELF_IDENT,
      `Self-Identity (Other)`                                                            = SELF_IDENT_OTHER,
      `Nationality`                                                                      = NATIONALITY,
      `Occupation: Job/Work/Occupation`                                                  = WORK,
      `Occupation: Establishment`                                                        = WORK_ESTABLISHMENT,
      `Curr. Address: Region`                                                            = CURR_REG,
      `Curr. Address: Province`                                                          = CURR_PROV,
      `Curr. Address: City/Municipality`                                                 = CURR_MUNC,
      `Curr. Address: Complete Address`                                                  = CURR_ADDR,
      `Perm. Address: Region`                                                            = PERM_REG,
      `Perm. Address: Province`                                                          = PERM_PROV,
      `Perm. Address: City/Municipality`                                                 = PERM_MUNC,
      `Perm. Address: Complete Address`                                                  = PERM_ADDR,
      `Contact No.`                                                                      = CLIENT_MOBILE,
      `Email Address`                                                                    = CLIENT_EMAIL,
      `Key Population: Registered Sex Worker`                                            = KP_REGISTERED_SW,
      `Key Population: Freelance Sex Worker`                                             = KP_FREELANCE_SW,
      `Key Population: Men who have sex with men (MSM)"`                                 = KP_MSM,
      `Key Population: Transgender (TG)`                                                 = KP_TG,
      `Key Population: People who inject drugs (PWID)`                                   = KP_PWID,
      `Key Population: Person Deprived of Liberty (PDL)`                                 = KP_PDL,
      `Key Population: Overseas Filipino`                                                = KP_OFW,
      `Key Population: Partner of MSM/PWID`                                              = KP_PARTNER,
      `Key Population: Partners of OFW`                                                  = KP_OFW_PARTNER,
      `Key Population: General Population`                                               = KP_GENPOP,
      `Key Population: Other KP`                                                         = KP_OTHER,
      `Patient Type`                                                                     = CLIENT_TYPE,
      `Physical Examination: Vaginal discharge`                                          = STI_SX_DISCHARGE_VAGINAL,
      `Physical Examination: Anal discharge`                                             = STI_SX_DISCHARGE_ANAL,
      `Physical Examination: Urethral discharge`                                         = STI_SX_DISCHARGE_URETHRAL,
      `Physical Examination: Oropharyngeal discharge`                                    = STI_SX_DISCHARGE_OROPHARYNGEAL,
      `Physical Examination: Scrotal swelling / tenderness`                              = STI_SX_SWOLLEN_SCROTUM,
      `Physical Examination: Painful urination`                                          = STI_SX_PAIN_URINE,
      `Physical Examination: Genital ulcer`                                              = STI_SX_ULCER_GENITAL,
      `Physical Examination: Oral ulcer`                                                 = STI_SX_ULCER_ORAL,
      `Physical Examination: Genital warts`                                              = STI_SX_WARTS_GENITAL,
      `Physical Examination: Lower abdominal pain`                                       = STI_SX_PAIN_ABDOMEN,
      `Physical Examination: Blister`                                                    = STI_SX_BLISTER,
      `Physical Examination: Ulcers`                                                     = STI_SX_ULCERS,
      `Physical Examination: Warts`                                                      = STI_SX_WARTS,
      `Physical Examination: Mass`                                                       = STI_SX_MASS,
      `Physical Examination: Foul Odor`                                                  = STI_SX_FOUL_ODOR,
      `Physical Examination: Rashes`                                                     = STI_SX_RASHES,
      `Physical Examination: Redness`                                                    = STI_SX_REDNESS,
      `Physical Examination: Presence of Lice`                                           = STI_SX_LICE,
      `Physical Examination: Presence of Abscess`                                        = STI_SX_ABSCESS,
      `Physical Examination: Other STI signs & symptoms`                                 = STI_SX_OTHER_TEXT,
      `Physical Examination: Diagnosis`                                                  = PE_DIAGNOSIS,
      `Physical Examination: Remarks`                                                    = PE_REMARKS,
      `Syphilis: RPR Test Date`                                                          = SYPH_RPR_DATE,
      `Syphilis: RPR Test Result`                                                        = SYPH_RPR_RESULT,
      `Syphilis: RPR Quantitative Result`                                                = SYPH_RPR_QUANTI,
      `Syphilis: TPPA/TPHA Test Date`                                                    = SYPH_TPPATPHA_DATE,
      `Syphilis: TPPA/TPHA Test Result`                                                  = SYPH_TPPATPHA_RESULT,
      `Syphilis: TPPA/TPHA Quantitative Result`                                          = SYPH_TPPATPHA_QUANTI,
      `Syphilis: Name of Other Test `                                                    = SYPH_OTHER_NAME,
      `Syphilis: Other Test Date`                                                        = SYPH_OTHER_DATE,
      `Syphilis: Other Test Result`                                                      = SYPH_OTHER_RESULT,
      `Syphilis: Other Test Quantitative Result`                                         = SYPH_OTHER_QUANTI,
      `Syphilis: Given Treatment`                                                        = SYPH_TREATMENT,
      `Gonorrhea and Non-Gonococcal Infections (NGI): Specimen Type - Male urethra`      = SPECIMEN_MALE_UREHTRA,
      `Gonorrhea and Non-Gonococcal Infections (NGI): Specimen Type - Female cervical`   = SPECIMEN_FEMALE_CERVICAL,
      `Gonorrhea and Non-Gonococcal Infections (NGI): Specimen Type - Pharynx`           = SPECIMEN_PHARYNX,
      `Gonorrhea and Non-Gonococcal Infections (NGI): Specimen Type - Rectum`            = SPECIMEN_RECTUM,
      `Gonorrhea and Non-Gonococcal Infections (NGI): Specimen Type - Others`            = SPECIMEN_TYPE_OTHER,
      `Gonorrhea and Non-Gonococcal Infections (NGI): Gram Staining Date`                = GONO_GRAM_STAIN_DATE,
      `Gonorrhea and Non-Gonococcal Infections (NGI): Gram Staining Pus Cells`           = GONO_GRAM_PUS,
      `Gonorrhea and Non-Gonococcal Infections (NGI): Gram (-) Intracellular Diplococci` = GONO_GRAM_NEG_EXTRA,
      `Gonorrhea and Non-Gonococcal Infections (NGI): Gram (-) Extracellular Diplococci` = GONO_GRAM_NEG_INTRA,
      `Gonorrhea and Non-Gonococcal Infections (NGI): Syndromic Assessment`              = IS_SYNDROMIC,
      `Gonorrhea and Non-Gonococcal Infections (NGI): Syndromic Assessment (Specify)`    = SYNDROMIC_ASSESSMENT,
      `Gonorrhea and Non-Gonococcal Infections (NGI): Given Treatment`                   = GONO_TREATMENT,
      `Bacterial Vaginosis: Gram Staining Date`                                          = BACVAG_GRAM_STAIN_DATE,
      `Bacterial Vaginosis: Gram Staining Result`                                        = BACVAG_GRAM_STAIN_RESULT,
      `Bacterial Vaginosis: Gram Staining Quantitative Result`                           = BACVAG_GRAM_STAIN_QUANTI,
      `Bacterial Vaginosis: Name of Other Test `                                         = BACVAG_OTHER_NAME,
      `Bacterial Vaginosis: Other Test Date`                                             = BACVAG_OTHER_DATE,
      `Bacterial Vaginosis: Other Test Result`                                           = BACVAG_OTHER_RESULT,
      `Bacterial Vaginosis: Other Test Quantitative Result`                              = BACVAG_OTHER_QUANTI,
      `Bacterial Vaginosis: Given Treatment`                                             = BACVAG_TREATMENT,
      `Trichomoniasis: Wet Mount Test Dagte`                                             = TRICHO_WET_MOUNT_DATE,
      `Trichomoniasis: Wet Mount Test Result`                                            = TRICHO_WET_MOUNT_RESULT,
      `Trichomoniasis: Wet Mount Quantitative Result`                                    = TRICHO_WET_MOUNT_QUANTI,
      `Trichomoniasis: Name of Other Test `                                              = TRICHO_OTHER_NAME,
      `Trichomoniasis: Other Test Date`                                                  = TRICHO_OTHER_DATE,
      `Trichomoniasis: Other Test Result`                                                = TRICHO_OTHER_RESULT,
      `Trichomoniasis: Other Test Quantitative Result`                                   = TRICHO_OTHER_QUANTI,
      `Trichomoniasis: Given Treatment`                                                  = TRICHO_TREATMENT,
      `HIV: Test Date`                                                                   = HIV_TEST_DATE,
      `HIV: Test Result`                                                                 = HIV_TEST_RESULT,
      `HIV: Quantitative Result`                                                         = HIV_TEST_QUANTI,
      `Hepatitis B: Test Date`                                                           = HEPB_TEST_DATE,
      `Hepatitis B: Test Result`                                                         = HEPB_TEST_RESULT,
      `Hepatitis B: Quantitative Result`                                                 = HEPB_TEST_QUANTI,
      `Hepatitis B: Given Hepatitis B Vaccination?`                                      = VAX_HEPB_1ST_DONE,
      `Hepatitis C: Test Date`                                                           = HEPC_TEST_DATE,
      `Hepatitis C: Test Result`                                                         = HEPC_TEST_RESULT,
      `Hepatitis C: Quantitative Result`                                                 = HEPC_TEST_QUANTI,
      `Clinic Notes`                                                                     = REPORT_NOTES,
      `Region of STI Facility`                                                           = STI_REG,
      `Province of STI Facility`                                                         = STI_PROV,
      `City/Municipality of STI Facility`                                                = STI_MUNC,
   ) %>%
   mutate_at(
      .vars = vars(contains("Test Date")),
      ~as.Date(.)
   )

sti_qc <- sti %>%
   filter(`City/Municipality of STI Facility` == "Quezon City")

write_xlsx(list(`QC STI Forms` = sti_qc), "H:/20240809_QC-STI_ever.xlsx")
write_xlsx(list(`PH STI Forms` = sti), "H:/20240809_QC-All_ever.xlsx")
