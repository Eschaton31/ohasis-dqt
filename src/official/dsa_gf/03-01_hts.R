##  prepare dataset for HTS_TST ------------------------------------------------

gf$linelist$hts <- gf$logsheet$combined %>%
   filter(
      !is.na(test_date),
      kap_type != "OTHERS"
   ) %>%
   mutate(
      birthdate        = paste(
         sep = "-",
         StrRight(uic, 4),
         substr(uic, 7, 8),
         substr(uic, 9, 10)
      ) %>% as.Date(),

      # Sex,
      Sex              = case_when(
         sex == "M" ~ "Male",
         sex == "F" ~ "Female",
         TRUE ~ "(no data)"
      ),

      #
      Date_Start       = paste(
         sep = "-",
         year(test_date),
         stri_pad_left(month(test_date), 2, "0"),
         "01"
      ),
      Date_End         = as.character((as.Date(Date_Start) %m+% months(1)) - 1),

      # Age_Band
      curr_age         = floor((reach_date - birthdate) / 365.25) %>% as.numeric(),
      curr_age         = floor(curr_age),
      `Age Group`      = case_when(
         curr_age %in% seq(0, 14) ~ "01_<15",
         curr_age %in% seq(15, 17) ~ "02_15-17",
         curr_age %in% seq(18, 19) ~ "03_18-19",
         curr_age %in% seq(20, 24) ~ "04_20-24",
         curr_age %in% seq(25, 29) ~ "05_25-29",
         curr_age %in% seq(30, 34) ~ "06_30-34",
         curr_age %in% seq(35, 49) ~ "07_35-49",
         curr_age %in% seq(50, 54) ~ "08_50-54",
         curr_age %in% seq(55, 59) ~ "09_55-59",
         curr_age %in% seq(60, 64) ~ "10_60-64",
         curr_age %in% seq(65, 69) ~ "11_65-69",
         curr_age %in% seq(70, 74) ~ "12_70-74",
         curr_age %in% seq(75, 79) ~ "13_75-79",
         curr_age %in% seq(80, 1000) ~ "14_80+",
         TRUE ~ "99_(no data)"
      ),

      # kp
      `Key Population` = kap_type,


      RECORD_P6M       = reach_date %m-% months(6),
      RECORD_P12M      = reach_date %m-% months(12),
      `DISAG 1`        = case_when(
         analp12m == "w/in 6m" ~ "1) Sexual Risk (anal sex) past 6 months",
         analp12m == "w/in 12m" ~ "2) Sexual Risk (anal sex) past 12 months",
         analp12m == ">p12m" ~ "3) Sexual Risk (anal sex) >12 months",
         date_last_sex_msm >= RECORD_P6M ~ "1) Sexual Risk (anal sex) past 6 months",
         date_last_sex_msm >= RECORD_P12M ~ "2) Sexual Risk (anal sex) past 12 months",
         date_last_sex_msm < RECORD_P12M ~ "3) Sexual Risk (anal sex) >12 months",
         TRUE ~ "(no data)"
      ),
      tested           = toupper(tested),
      `DISAG 2`        = case_when(
         stri_detect_regex(tested, "^FACILITY") ~ "Facility/Clinic (by MedTech)",
         stri_detect_regex(tested, "^OUTREACH") ~ "Outreach/Community (by MedTech)",
         stri_detect_regex(tested, "^0UTREACH") ~ "Outreach/Community (by MedTech)",
         stri_detect_regex(tested, "^COMMUNITY") ~ "Outreach/Community (by MedTech)",
         stri_detect_regex(tested, "^OUT REACH") ~ "Outreach/Community (by MedTech)",
         stri_detect_regex(tested, "^CBS") ~ "CBS",
         stri_detect_regex(tested, "^SELF") ~ "Self-testing",
         stri_detect_regex(tested, "^SOCIAL AND SEXUAL NETWORK TESTING") ~ "SSNT",
         TRUE ~ "Facility/Clinic (by MedTech)"
      ),
      `DISAG 3`        = case_when(
         confirm_date < reach_date ~ "Known Pos",
         reactive == "Y" ~ "Reactive",
         reactive == "N" ~ "Non-reactive",
         TRUE ~ "(no data)"
      ),
      `DISAG 3.1`      = case_when(
         data_src == "OHASIS" & `DISAG 2` == "Facility/Clinic (by MedTech)" ~ "T1 Complete",
         data_src == "Both" & t1_complete == "Y" ~ "T1 Complete",
         data_src == "PSGI" & t1_complete == "Y" ~ "T1 Complete",
         TRUE ~ "(no data on T1)"
      ),

      tat_confirm_art  = floor(difftime(artstart_date, confirm_date, units = "days")),
      `DISAG 4`        = case_when(
         !is.na(artstart_date) ~ "Started treatment",
         is.na(artstart_date) & !is.na(link2care_date) ~ "Linked to care",
         TRUE ~ "(not enrolled)"
      ),
      `DISAG 5`        = case_when(
         tat_confirm_art < 0 ~ "0) Tx before dx",
         tat_confirm_art == 0 ~ "1) Same day",
         tat_confirm_art >= 1 & tat_confirm_art <= 7 ~ "2) 1 - 7 days",
         tat_confirm_art >= 8 & tat_confirm_art <= 14 ~ "3) 8 - 14 days",
         tat_confirm_art >= 15 & tat_confirm_art <= 30 ~ "4) 15 - 30 days",
         tat_confirm_art >= 31 ~ "5) More than 30 days",
         TRUE ~ "(no confirm date)",
      )
   ) %>%
   rename(
      `Site/Organization` = site_name,
      `Site Region`       = site_region,
      `Site Province`     = site_province,
      `Site City`         = site_muncity,
      `Data Source`       = data_src
   ) %>%
   mutate(
      drop = case_when(
         `Age Group` == "01_<15" & `Data Source` == "OHASIS" ~ 1,
         `DISAG 1` == "(no data)" & `Data Source` == "OHASIS" ~ 1,
         TRUE ~ 0
      )
   ) %>%
   filter(
      Sex != "Female",
      drop == 0
   )