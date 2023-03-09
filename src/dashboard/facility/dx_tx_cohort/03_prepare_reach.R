##  prepare dataset for kp_prev & hts indicators -------------------------------

prepare_hts <- function(oh, harp, coverage) {
   data <- process_hts(oh$forms$form_hts, oh$forms$form_a, oh$forms$form_cfbs) %>%
      filter(
         hts_date %within% interval(as.Date("2021-01-01"), coverage$max)
      ) %>%
      get_cid(oh$id_reg, PATIENT_ID) %>%
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
            get_cid(oh$id_reg, PATIENT_ID) %>%
            mutate(
               ref_report = as.Date(stri_c(sep = "-", year, stri_pad_left(month, 2, "0"), "01"))
            ) %>%
            select(
               CENTRAL_ID,
               idnum,
               transmit,
               sexhow,
               confirm_date,
               ref_report,
               pregnant,
               HARPDX_BIRTHDATE  = bdate,
               HARPDX_SEX        = sex,
               HARPDX_SELF_IDENT = self_identity
            ),
         by = join_by(CENTRAL_ID)
      ) %>%
      left_join(
         y  = harp$tx %>%
            get_cid(oh$id_reg, PATIENT_ID) %>%
            select(
               CENTRAL_ID,
               art_id,
               artstart_date
            ),
         by = join_by(CENTRAL_ID)
      ) %>%
      left_join(
         y  = harp$prep %>%
            get_cid(oh$id_reg, PATIENT_ID) %>%
            select(
               CENTRAL_ID,
               prep_id,
               prepstart_date
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

clean_hts <- function(data, risk, coverage) {
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
            confirm_date >= as.Date(coverage$min) ~ 0,
            ref_report < as.Date(coverage$min) ~ 1,
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
            condition = HTS_TST_RESULT != "(no data)",
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
         sex           = coalesce(StrLeft(coalesce(HARPDX_SEX, remove_code(SEX)), 1), "(no data)"),

         # KAP
         msm           = case_when(
            use_harpdx == 1 &
               sex == "M" &
               sexhow %in% c("HOMOSEXUAL", "BISEXUAL") ~ 1,
            use_harpdx == 0 &
               sex == "M" &
               grepl("yes-", risk_sexwithm) ~ 1,
            TRUE ~ 0
         ),
         tgw           = case_when(
            use_harpdx == 1 &
               msm == 1 &
               HARPDX_SELF_IDENT %in% c("FEMALE", "OTHERS") ~ 1,
            use_harpdx == 0 &
               msm == 1 &
               keep_code(SELF_IDENT) %in% c("2", "3") ~ 1,
            TRUE ~ 0
         ),
         hetero        = case_when(
            use_harpdx == 1 & sexhow == "HETEROSEXUAL" ~ 1,
            use_harpdx == 0 &
               sex == "M" &
               !grepl("yes-", risk_sexwithm) &
               grepl("yes-", risk_sexwithf) ~ 1,
            use_harpdx == 0 &
               sex == "F" &
               grepl("yes-", risk_sexwithm) &
               !grepl("yes-", risk_sexwithf) ~ 1,
            TRUE ~ 0
         ),
         pwid          = case_when(
            use_harpdx == 1 & transmit == "IVDU" ~ 1,
            use_harpdx == 0 & grepl("yes-", risk_injectdrug) ~ 1,
            TRUE ~ 0
         ),
         unknown       = case_when(
            transmit == "UNKNOWN" ~ 1,
            risks == "(no data)" ~ 1,
            TRUE ~ 0
         ),

         # kap
         sex           = case_when(
            sex == "M" ~ "Male",
            sex == "F" ~ "Female",
            TRUE ~ "(no data)"
         ),
         kap_type      = case_when(
            transmit == "IVDU" ~ "PWID",
            msm == 1 & tgw == 0 ~ "MSM",
            msm == 1 & tgw == 1 ~ "MSM-TGW",
            sex == "Male" ~ "Other Males",
            sex == "Female" & pregnant == 1 ~ "Pregnant WLHIV",
            sex == "Female" ~ "Other Females",
            TRUE ~ "Other"
         ),

         # Age Band
         curr_age      = calc_age(coalesce(HARPDX_BIRTHDATE, BIRTHDATE), hts_date),
         curr_age      = floor(coalesce(curr_age, AGE)),
         reeach_age_c1 = gen_agegrp(curr_age, "harp"),
         reeach_age_c2 = gen_agegrp(curr_age, "5yr"),
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
      mutate(
         CBS_VENUE      = toupper(str_squish(HIV_SERVICE_ADDR)),
         ONLINE_APP     = case_when(
            grepl("GRINDR", CBS_VENUE) ~ "GRINDR",
            grepl("GRNDR", CBS_VENUE) ~ "GRINDR",
            grepl("GRINDER", CBS_VENUE) ~ "GRINDR",
            grepl("TWITTER", CBS_VENUE) ~ "TWITTER",
            grepl("FACEBOOK", CBS_VENUE) ~ "FACEBOOK",
            grepl("MESSENGER", CBS_VENUE) ~ "FACEBOOK",
            grepl("\\bFB\\b", CBS_VENUE) ~ "FACEBOOK",
            grepl("\\bGR\\b", CBS_VENUE) ~ "GRINDR",
         ),
         REACH_ONLINE   = if_else(!is.na(ONLINE_APP), "1_Yes", REACH_ONLINE, REACH_ONLINE),
         REACH_CLINICAL = if_else(
            condition = if_all(starts_with("REACH_"), ~is.na(.)) & hts_modality == "FBT",
            true      = "1_Yes",
            false     = REACH_CLINICAL,
            missing   = REACH_CLINICAL
         ),
      ) %>%
      select(,
         REC_ID,
         CENTRAL_ID,
         FORM_VERSION,
         UIC,
         PHILHEALTH_NO,
         PATIENT_CODE,
         PHILSYS_ID,
         FIRST,
         MIDDLE,
         LAST,
         SUFFIX,
         SEX     = sex,
         SELF_IDENT,
         SELF_IDENT_OTHER,
         REACH_ONLINE,
         REACH_SSNT,
         REACH_CLINICAL,
         REACH_INDEX_TESTING,
         REACH_VENUE,
         REFER_ART,
         REFER_CONFIRM,
         REFER_RETEST,
         hts_date,
         hts_result,
         hts_modality,
         risk_motherhashiv,
         risk_sexwithf,
         risk_sexwithf_nocdm,
         risk_sexwithm,
         risk_sexwithm_nocdm,
         risk_payingforsex,
         risk_paymentforsex,
         risk_sexwithhiv,
         risk_injectdrug,
         risk_needlestick,
         risk_bloodtransfuse,
         risk_illicitdrug,
         risk_chemsex,
         risk_tattoo,
         risk_sti,
         hts_priority,
         idnum,
         confirm_date,
         art_id,
         artstart_date,
         prep_id,
         prepstart_date,
         FACI_ID = FINAL_FACI,
         FINAL_TEST_RESULT,
         FINAL_CONFIRM_DATE,
         KP_PREV,
         HTS_TST,
         HTS_TST_POS,
         HTS_TST_VERIFY,
         TX_NEW_VERIFY,
         PREP_OFFER,
         kap_type,
         curr_age,
         reeach_age_c1,
         reeach_age_c2,
         ONLINE_APP,
      ) %>%
      mutate_at(
         .vars = vars(starts_with("REACH"), starts_with("REFER")),
         ~as.integer(keep_code(.))
      )

   return(data)
}

.init <- function(envir = parent.env(environment())) {
   p     <- envir
   reach <- prepare_hts(p$oh, p$harp, p$params)
   risks <- consolidate_risks(reach)

   reach <- clean_hts(reach, risks, p$params) %>%
      tag_indicators() %>%
      generate_disagg()

   p$data$reach <- reach %>%
      arrange(desc(hts_date)) %>%
      distinct(CENTRAL_ID, FACI_ID, .keep_all = TRUE)

   p$data$hts <- reach %>%
      filter(HTS_TST == 1) %>%
      arrange(desc(year(hts_date)), hts_priority) %>%
      distinct(CENTRAL_ID, FACI_ID, .keep_all = TRUE)

   p$data$nr <- reach %>%
      filter(
         HTS_TST == 1,
         grepl("Non-reactive$", FINAL_TEST_RESULT) | grepl("Negative$", FINAL_TEST_RESULT),
         is.na(idnum)
      ) %>%
      anti_join(
         y  = reach %>%
            filter(FINAL_TEST_RESULT == "Confirmed: Positive") %>%
            select(CENTRAL_ID),
         by = join_by(CENTRAL_ID)
      ) %>%
      arrange(desc(year(hts_date)), hts_priority) %>%
      distinct(CENTRAL_ID, FACI_ID, .keep_all = TRUE)
}