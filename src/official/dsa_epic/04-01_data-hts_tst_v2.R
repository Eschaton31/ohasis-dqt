##  prepare dataset for HTS_TST ------------------------------------------------

epic$linelist$hts_tst <- process_hts(epic$forms$form_hts, epic$forms$form_a, epic$forms$form_cfbs) %>%
   mutate(
      priority = case_when(
         !is.na(CONFIRM_RESULT) ~ 1,
         hts_result != "(no data)" & src %in% c("hts2021", "a2017") ~ 2,
         hts_result != "(no data)" & hts_modality == "FBT" ~ 3,
         hts_result != "(no data)" & hts_modality == "CBS" ~ 4,
         hts_result != "(no data)" & hts_modality == "FBS" ~ 5,
         hts_result != "(no data)" & hts_modality == "ST" ~ 6,
         TRUE ~ 9999
      )
   ) %>%
   filter(
      (hts_date >= as.Date(epic$coverage$min) & hts_date <= as.Date(epic$coverage$max))
   ) %>%
   get_cid(epic$forms$id_registry, PATIENT_ID) %>%
   arrange(priority, hts_date) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   left_join(
      y  = epic$harp$dx %>%
         mutate(
            ref_report = as.Date(paste(sep = "-", year, stri_pad_left(month, 2, "0"), "01"))
         ) %>%
         left_join(
            y  = bind_rows(epic$forms$form_a, epic$forms$form_hts) %>%
               select(
                  REC_ID,
                  CENTRAL_FACI     = SERVICE_FACI,
                  CENTRAL_SUB_FACI = SERVICE_SUB_FACI,
               ),
            by = "REC_ID"
         ) %>%
         select(
            CENTRAL_ID,
            idnum,
            bdate,
            reg_sex = sex,
            transmit,
            sexhow,
            self_identity,
            confirm_date,
            ref_report,
            CENTRAL_FACI,
            CENTRAL_SUB_FACI
         ),
      by = "CENTRAL_ID"
   )

risk <- epic$linelist$hts_tst %>%
   select(
      REC_ID,
      contains("risk", ignore.case = FALSE)
   ) %>%
   pivot_longer(
      cols = contains("risk", ignore.case = FALSE)
   ) %>%
   group_by(REC_ID) %>%
   summarise(
      risks = paste0(collapse = ", ", unique(sort(value)))
   ) %>%
   ungroup()

epic$linelist$hts_tst %<>%
   left_join(y = risk, by = "REC_ID") %>%
   mutate(
      # tag if central to be used
      use_central        = if_else(
         condition = !is.na(idnum),
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      # old confirmed
      old_dx             = case_when(
         confirm_date >= as.Date(epic$coverage$min) ~ 0,
         ref_report < as.Date(epic$coverage$min) ~ 1,
         TRUE ~ 0
      ),

      # tag those without form faci
      use_record_faci    = if_else(
         condition = is.na(SERVICE_FACI),
         true      = 1,
         false     = 0
      ),

      # tag which test to be used
      FINAL_FACI         = if_else(
         condition = use_record_faci == 1,
         true      = FACI_ID,
         false     = SERVICE_FACI
      ),
      FINAL_SUB_FACI     = SERVICE_SUB_FACI,

      LATEST_TEST_RESULT = case_when(
         hts_result == "R" ~ "reactive",
         hts_result == "NR" ~ "Non-reactive",
         hts_result == "IND" ~ "Indeterminate",
         TRUE ~ "(no data)"
      ),

      FINAL_TEST_RESULT  = case_when(
         old_dx == 0 & !is.na(idnum) ~ "Confirmed: Positive",
         CONFIRM_RESULT == 1 ~ "Confirmed: Positive",
         CONFIRM_RESULT == 2 ~ "Confirmed: Negative",
         CONFIRM_RESULT == 3 ~ "Confirmed: Indeterminate",
         hts_modality == "FBT" ~ paste0("Tested: ", LATEST_TEST_RESULT),
         hts_modality == "FBS" ~ paste0("Tested: ", LATEST_TEST_RESULT),
         hts_modality == "CBS" ~ paste0("CBS: ", LATEST_TEST_RESULT),
         hts_modality == "ST" ~ paste0("Self-Testing: ", LATEST_TEST_RESULT),
      ),
   ) %>%
   mutate(
      # sex
      Sex             = case_when(
         use_central == 1 ~ StrLeft(reg_sex, 1),
         use_central == 0 ~ substr(SEX, 3, 3),
         TRUE ~ "(no data)"
      ),

      # KAP
      msm             = case_when(
         use_central == 1 &
            Sex == "M" &
            sexhow %in% c("HOMSOEXUAL", "BISEXUAL") ~ 1,
         use_central == 0 &
            Sex == "M" &
            grepl("yes-", risk_sexwithm) ~ 1,
         TRUE ~ 0
      ),
      tgw             = case_when(
         use_central == 1 &
            msm == 1 &
            self_identity %in% c("FEMALE", "OTHERS") ~ 1,
         use_central == 0 &
            msm == 1 &
            StrLeft(SELF_IDENT, 1) %in% c("2", "3") ~ 1,
         TRUE ~ 0
      ),
      hetero          = case_when(
         use_central == 1 & sexhow == "HETEROSEXUAL" ~ 1,
         use_central == 0 &
            Sex == "M" &
            !grepl("yes-", risk_sexwithm) &
            grepl("yes-", risk_sexwithf) ~ 1,
         use_central == 0 &
            Sex == "F" &
            grepl("yes-", risk_sexwithm) &
            !grepl("yes-", risk_sexwithf) ~ 1,
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
         Sex == "F" ~ "(not included)",
         unknown == 1 ~ "(no data)",
         TRUE ~ "Non-MSM"
      ),

      # Age Band
      curr_age        = case_when(
         use_central == 1 & !is.na(bdate) ~ calc_age(bdate, hts_date),
         use_central == 0 & !is.na(BIRTHDATE) ~ calc_age(BIRTHDATE, hts_date),
         TRUE ~ as.integer(AGE)
      ),
      curr_age        = floor(curr_age),
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

      `DISAG 2`       = case_when(
         hts_modality == "CBS" ~ "CBO (CBS)",
         hts_modality == "ST" ~ "Self-Testing",
         TRUE ~ "Facility (walk-in)"
      ),
      `DISAG 3`       = case_when(
         old_dx == 1 & !is.na(CONFIRM_RESULT) ~ "Confirmed: Known Pos",
         old_dx == 1 & hts_modality == "FBT" ~ "Tested: Known Pos",
         old_dx == 1 & hts_modality == "FBS" ~ "Tested: Known Pos",
         old_dx == 1 & hts_modality == "CBS" ~ "CBS: Known Pos",
         old_dx == 1 & hts_modality == "ST" ~ "Self-Testing: Known Pos",
         TRUE ~ FINAL_TEST_RESULT
      ),
      `DISAG 4`       = if_else(
         REACH_SSNT == "1_Yes",
         "Reached via SSNT",
         "",
         ""
      ),
      `DISAG 5`       = if_else(
         REACH_INDEX_TESTING == "1_Yes",
         "Index Testing",
         "",
         ""
      ),
      `DISAG 6`       = if_else(
         REACH_ONLINE == "1_Yes",
         "Online Reach",
         "",
         ""
      ),
   ) %>%
   left_join(
      y  = epic$sites %>%
         select(
            FINAL_FACI = FACI_ID,
            site_epic_2021,
            site_epic_2022
         ) %>%
         distinct_all(),
      by = "FINAL_FACI"
   ) %>%
   ohasis$get_faci(
      list("Site/Organization" = c("FINAL_FACI", "FINAL_SUB_FACI")),
      "name",
      c("Site Region", "Site Province", "Site City")
   ) %>%
   filter(
      !stri_detect_regex(FINAL_TEST_RESULT, ": \\(no data\\)$")
   )