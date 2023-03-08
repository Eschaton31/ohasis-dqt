##  prepare dataset for kp_prev & hts indicators -------------------------------

prepare_hts <- function(forms, harp, coverage) {
   data <- process_hts(forms$form_hts, forms$form_a, forms$form_cfbs) %>%
      filter(
         hts_date %within% interval(coverage$min, coverage$max)
      ) %>%
      get_cid(forms$id_reg, PATIENT_ID) %>%
      mutate_if(
         .predicate = is.POSIXct,
         ~null_dates(., "POSIXct")
      ) %>%
      mutate_if(
         .predicate = is.Date,
         ~null_dates(., "Date")
      ) %>%
      mutate(
         hts_priority = case_when(
            CONFIRM_RESULT %in% c(1, 2, 3) ~ 1,
            hts_result != "(no data)" & src %in% c("hts2021", "a2017") ~ 2,
            hts_result != "(no data)" & hts_modality == "FBT" ~ 3,
            hts_result != "(no data)" & hts_modality == "CBS" ~ 4,
            hts_result != "(no data)" & hts_modality == "FBS" ~ 5,
            hts_result != "(no data)" & hts_modality == "ST" ~ 6,
            TRUE ~ 9999
         )
      ) %>%
      left_join(
         y  = harp$dx %>%
            select(
               CENTRAL_ID,
               idnum,
               transmit,
               sexhow,
               confirm_date,
               ref_report,
               HARPDX_BIRTHDATE  = bdate,
               HARPDX_SEX        = sex,
               HARPDX_SELF_IDENT = self_identity,
               HARPDX_FACI,
               HARPDX_SUB_FACI
            ),
         by = join_by(CENTRAL_ID)
      ) %>%
      left_join(
         y  = harp$tx$new %>%
            select(
               CENTRAL_ID,
               art_id,
               artstart_date
            ),
         by = join_by(CENTRAL_ID)
      )

   return(data)
}

consolidate_risks <- function(data) {
   risk <- data %>%
      select(
         REC_ID,
         contains("risk", ignore.case = FALSE)
      ) %>%
      pivot_longer(
         cols = contains("risk", ignore.case = FALSE)
      ) %>%
      group_by(REC_ID) %>%
      summarise(
         risks = stri_c(collapse = ", ", unique(sort(value)))
      )

   return(risk)
}

clean_hts <- function(data, risk) {
   data %<>%
      left_join(y = risk, by = join_by(REC_ID)) %>%
      mutate(
         # tag if central to be used
         use_harpdx        = if_else(
            condition = !is.na(idnum),
            true      = 1,
            false     = 0,
            missing   = 0
         ),

         # old confirmed
         old_dx            = case_when(
            confirm_date >= as.Date(pepfar$coverage$min) ~ 0,
            ref_report < as.Date(pepfar$coverage$min) ~ 1,
            TRUE ~ 0
         ),

         # tag those without form faci
         use_record_faci   = if_else(
            condition = is.na(SERVICE_FACI),
            true      = 1,
            false     = 0
         ),

         # tag which test to be used
         FINAL_FACI        = if_else(
            condition = use_record_faci == 1,
            true      = FACI_ID,
            false     = SERVICE_FACI
         ),
         FINAL_SUB_FACI    = case_when(
            use_record_faci == 1 & FACI_ID == "130000" ~ SPECIMEN_SUB_SOURCE,
            !(SERVICE_FACI %in% c("130001", "130605", "040200")) ~ NA_character_,
            TRUE ~ SERVICE_SUB_FACI
         ),

         HTS_TST_RESULT    = case_when(
            hts_result == "R" ~ "Reactive",
            hts_result == "NR" ~ "Non-reactive",
            hts_result == "IND" ~ "Indeterminate",
            is.na(hts_result) ~ "(no data)",
            TRUE ~ hts_result
         ),

         FINAL_TEST_RESULT = case_when(
            old_dx == 1 & !is.na(idnum) ~ "Confirmed: Known Pos",
            old_dx == 0 & !is.na(idnum) ~ "Confirmed: Positive",
            CONFIRM_RESULT == 1 ~ "Confirmed: Positive",
            CONFIRM_RESULT == 2 ~ "Confirmed: Negative",
            CONFIRM_RESULT == 3 ~ "Confirmed: Indeterminate",
            hts_modality == "FBT" ~ paste0("Tested: ", HTS_TST_RESULT),
            hts_modality == "FBS" ~ paste0("Tested: ", HTS_TST_RESULT),
            hts_modality == "CBS" ~ paste0("CBS: ", HTS_TST_RESULT),
            hts_modality == "ST" ~ paste0("Self-Testing: ", HTS_TST_RESULT),
         ),
      )

   return(data)
}

tag_indicators <- function(data) {
   confirm_data <- data %>%
      mutate(
         FINAL_CONFIRM_DATE = case_when(
            old_dx == 0 & hts_priority == 1 ~ as.Date(coalesce(DATE_CONFIRM, T3_DATE, T2_DATE, T1_DATE)),
            old_dx == 1 ~ confirm_date,
            TRUE ~ NA_Date_
         )
      ) %>%
      filter(!is.na(FINAL_CONFIRM_DATE)) %>%
      select(CENTRAL_ID, FINAL_CONFIRM_DATE) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE)

   data %<>%
      left_join(y = confirm_data, by = join_by(CENTRAL_ID)) %>%
      mutate(
         # tag specific indicators
         KP_PREV        = 1,
         HTS_TST        = if_else(
            condition = HTS_TST_RESULT == "(no data)",
            true      = 0,
            false     = 1,
            missing   = 0
         ),
         HTS_TST_POS    = if_else(
            condition = FINAL_TEST_RESULT == "Confirmed: Positive",
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         HTS_TST_VERIFY = if_else(
            condition = hts_modality %in% c("CBS", "ST") & FINAL_CONFIRM_DATE >= hts_date,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         TX_NEW_VERIFY  = if_else(
            condition = hts_modality %in% c("CBS", "ST") & artstart_date >= hts_date,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         PREP_OFFER     = if_else(
            condition = hts_result %in% c("IND", "NR") & keep_code(SERVICE_PREP_REFER) == "1",
            true      = 1,
            false     = 0,
            missing   = 0
         ),
      )

   return(data)
}

generate_disagg <- function(data) {
   data %<>%
      mutate(
         # sex
         Sex             = coalesce(StrLeft(coalesce(HARPDX_SEX, remove_code(SEX)), 1), "(no data)"),

         # KAP
         msm             = case_when(
            use_harpdx == 1 &
               Sex == "M" &
               sexhow %in% c("HOMOSEXUAL", "BISEXUAL") ~ 1,
            use_harpdx == 0 &
               Sex == "M" &
               grepl("yes-", risk_sexwithm) ~ 1,
            TRUE ~ 0
         ),
         tgw             = case_when(
            use_harpdx == 1 &
               msm == 1 &
               HARPDX_SELF_IDENT %in% c("FEMALE", "OTHERS") ~ 1,
            use_harpdx == 0 &
               msm == 1 &
               keep_code(SELF_IDENT) %in% c("2", "3") ~ 1,
            TRUE ~ 0
         ),
         hetero          = case_when(
            use_harpdx == 1 & sexhow == "HETEROSEXUAL" ~ 1,
            use_harpdx == 0 &
               Sex == "M" &
               !grepl("yes-", risk_sexwithm) &
               grepl("yes-", risk_sexwithf) ~ 1,
            use_harpdx == 0 &
               Sex == "F" &
               grepl("yes-", risk_sexwithm) &
               !grepl("yes-", risk_sexwithf) ~ 1,
            TRUE ~ 0
         ),
         pwid            = case_when(
            use_harpdx == 1 & transmit == "IVDU" ~ 1,
            use_harpdx == 0 & grepl("yes-", risk_injectdrug) ~ 1,
            TRUE ~ 0
         ),
         unknown         = case_when(
            transmit == "UNKNOWN" ~ 1,
            risks == "(no data)" ~ 1,
            TRUE ~ 0
         ),
         `KP Population` = case_when(
            msm == 1 & tgw == 0 ~ "MSM",
            msm == 1 & tgw == 1 ~ "TGW",
            pwid == 1 ~ "PWID",
            Sex == "F" ~ "(not included)",
            unknown == 1 ~ "(no data)",
            TRUE ~ "Non-MSM"
         ),

         # Age Band
         curr_age        = calc_age(coalesce(HARPDX_BIRTHDATE, BIRTHDATE), hts_date),
         curr_age        = floor(coalesce(curr_age, AGE)),
         Age_Band        = case_when(
            curr_age >= 0 & curr_age < 5 ~ "01_0-4",
            curr_age >= 5 & curr_age < 10 ~ "02_5-9",
            curr_age >= 10 & curr_age < 15 ~ "03_10-14",
            curr_age >= 15 & curr_age < 20 ~ "04_15-19",
            curr_age >= 20 & curr_age < 25 ~ "05_20-24",
            curr_age >= 25 & curr_age < 30 ~ "06_25-29",
            curr_age >= 30 & curr_age < 35 ~ "07_30-34",
            curr_age >= 35 & curr_age < 40 ~ "08_35-39",
            curr_age >= 40 & curr_age < 45 ~ "09_40-44",
            curr_age >= 45 & curr_age < 50 ~ "10_45-49",
            curr_age >= 50 & curr_age < 55 ~ "11_50-54",
            curr_age >= 55 & curr_age < 60 ~ "12_55-59",
            curr_age >= 60 & curr_age < 65 ~ "13_60-64",
            curr_age >= 65 & curr_age < 1000 ~ "14_65+",
            TRUE ~ "99_(no data)"
         ),
         `DATIM Age`     = if_else(
            condition = curr_age < 15,
            true      = "<15",
            false     = ">=15",
            missing   = "(no data)"
         ),
      ) %>%
      left_join(
         y  = coverage$sites %>%
            select(
               FACI_ID,
               starts_with("site_")
            ) %>%
            distinct_all(),
         by = join_by(FINAL_FACI == FACI_ID)
      ) %>%
      ohasis$get_faci(
         list(`Site/Organization` = c("FINAL_FACI", "FINAL_SUB_FACI")),
         "name",
         c("Site Region", "Site Province", "Site City")
      ) %>%
      mutate(
         CONFIRM_RESULT = case_when(
            CONFIRM_RESULT == 1 ~ "1_Positive",
            CONFIRM_RESULT == 2 ~ "2_Negative",
            CONFIRM_RESULT == 3 ~ "3_Indeterminate",
            CONFIRM_RESULT == 4 ~ "4_Pending",
            CONFIRM_RESULT == 5 ~ "5_Duplicate",
         )
      ) %>%
      ohasis$get_faci(
         list(SPECIMEN_SOURCE_FACI = c("SPECIMEN_SOURCE", "SPECIMEN_SUB_SOURCE")),
         "name"
      ) %>%
      ohasis$get_faci(
         list(CONFIRM_LAB = c("CONFIRM_FACI", "CONFIRM_SUB_FACI")),
         "name"
      ) %>%
      ohasis$get_addr(
         c(
            PERM_REG  = "PERM_PSGC_REG",
            PERM_PROV = "PERM_PSGC_PROV",
            PERM_MUNC = "PERM_PSGC_MUNC"
         ),
         "name"
      ) %>%
      ohasis$get_addr(
         c(
            CURR_REG  = "CURR_PSGC_REG",
            CURR_PROV = "CURR_PSGC_PROV",
            CURR_MUNC = "CURR_PSGC_MUNC"
         ),
         "name"
      ) %>%
      ohasis$get_addr(
         c(
            BIRTH_REG  = "BIRTH_PSGC_REG",
            BIRTH_PROV = "BIRTH_PSGC_PROV",
            BIRTH_MUNC = "BIRTH_PSGC_MUNC"
         ),
         "name"
      ) %>%
      ohasis$get_addr(
         c(
            CBS_REG  = "HIV_SERVICE_PSGC_REG",
            CBS_PROV = "HIV_SERVICE_PSGC_PROV",
            CBS_MUNC = "HIV_SERVICE_PSGC_MUNC"
         ),
         "name"
      ) %>%
      mutate(
         CBS_VENUE   = toupper(str_squish(HIV_SERVICE_ADDR)),
         ONLINE_APP  = case_when(
            grepl("GRINDR", CBS_VENUE) ~ "GRINDR",
            grepl("GRNDR", CBS_VENUE) ~ "GRINDR",
            grepl("GRINDER", CBS_VENUE) ~ "GRINDR",
            grepl("TWITTER", CBS_VENUE) ~ "TWITTER",
            grepl("FACEBOOK", CBS_VENUE) ~ "FACEBOOK",
            grepl("MESSENGER", CBS_VENUE) ~ "FACEBOOK",
            grepl("\\bFB\\b", CBS_VENUE) ~ "FACEBOOK",
            grepl("\\bGR\\b", CBS_VENUE) ~ "GRINDR",
         ),
         CONFIRM_LAB = if_else(
            CONFIRM_TYPE == "1_Central NRL",
            "NRL-SACCL",
            CONFIRM_LAB,
            CONFIRM_LAB
         )
      ) %>%
      rename(
         CREATED = CREATED_BY,
         UPDATED = UPDATED_BY,
         DELETED = DELETED_BY,
      ) %>%
      ohasis$get_staff(c(CREATED_BY = "CREATED")) %>%
      ohasis$get_staff(c(UPDATED_BY = "UPDATED")) %>%
      ohasis$get_staff(c(DELETED_BY = "DELETED")) %>%
      ohasis$get_staff(c(HTS_PROVIDER = "SERVICE_BY")) %>%
      generate_gender_identity(SEX, SELF_IDENT, SELF_IDENT_OTHER, gender_identity) %>%
      rename(
         HTS_PROVIDER_TYPE       = PROVIDER_TYPE,
         HTS_PROVIDER_TYPE_OTHER = PROVIDER_TYPE_OTHER,
      ) %>%
      select(
         -FACI_ID,
         -SUB_FACI_ID,
         -SERVICE_FACI,
         -MODALITY,
         -use_record_faci,
         -IDNUM,
         -PERM_ADDR,
         -CURR_ADDR,
         -BIRTH_ADDR,
         -any_of(c(
            "FIRST",
            "MIDDLE",
            "LAST",
            "SUFFIX",
            "CLIENT_EMAIL",
            "CLIENT_MOBILE",
            "UIC",
            "PHILHEALTH_NO",
            "PATIENT_CODE",
            "PHILSYS_ID",
            "CONFIRMATORY_CODE",
            "SNAPSHOT",
            "PRIME",
            "PATIENT_ID",
            "RECORD_DATE",
            "DISEASE",
            "DELETED_AT",
            "DELETED_BY",
            "BIRTHDATE",
            "HIV_SERVICE_TYPE",
            "GENDER_AFFIRM_THERAPY",
            "HIV_SERVICE_ADDR",
            "src",
            "MODULE"
         )),
         -c(
            starts_with("SIGNATURE", ignore.case = FALSE),
            ends_with("SUB_FACI", ignore.case = FALSE),
            ends_with("MSM", ignore.case = FALSE),
            ends_with("TGW", ignore.case = FALSE),
            ends_with("FSW", ignore.case = FALSE),
            ends_with("PWID", ignore.case = FALSE),
            ends_with("GENPOP", ignore.case = FALSE),
            ends_with("_NA", ignore.case = FALSE),
            starts_with("SIGNATORY_", ignore.case = FALSE)
         )
      ) %>%
      mutate(
         RT_AGREED       = NA_character_,
         RT_SPECIMEN     = NA_character_,
         RT_RESULT       = NA_character_,
         RT_VL_REQUESTED = NA_character_,
         RT_VL_DATE      = NA_Date_,
         RT_VL_RESULT    = NA_character_,
         RITA_RESULT     = NA_character_,
      ) %>%
      mutate(
         `CBS Region`   = if_else(
            hts_modality %in% c("CBS", "FBS", "ST"),
            CBS_REG,
            NA_character_,
            NA_character_
         ),
         `CBS Province` = if_else(
            hts_modality %in% c("CBS", "FBS", "ST"),
            CBS_PROV,
            NA_character_,
            NA_character_
         ),
         `CBS City`     = if_else(
            hts_modality %in% c("CBS", "FBS", "ST"),
            CBS_MUNC,
            NA_character_,
            NA_character_
         )
      )

   return(data)
}

.init <- function(envir = parent.env(environment())) {
   p     <- envir
   reach <- prepare_hts(p$forms, p$harp, p$coverage)
   risks <- consolidate_risks(reach)

   p$linelist$reach <- clean_hts(reach, risks) %>%
      tag_indicators() %>%
      generate_disagg()
}