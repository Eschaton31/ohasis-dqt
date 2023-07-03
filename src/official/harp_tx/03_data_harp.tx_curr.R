##  Append to the previous art registry ----------------------------------------

get_latest_visit <- function(art_last, new_reg, params) {
   log_info("Processing latest visit.")
   data <- new_reg %>%
      select(-PATIENT_ID) %>%
      rename(artstart_rec = REC_ID) %>%
      left_join(
         y  = art_last %>%
            arrange(desc(LATEST_NEXT_DATE)) %>%
            mutate(LATEST_VISIT = VISIT_DATE) %>%
            distinct(CENTRAL_ID, .keep_all = TRUE),
         by = join_by(CENTRAL_ID)
      ) %>%
      mutate_if(
         .predicate = is.POSIXct,
         ~as.Date(.)
      ) %>%
      mutate(
         # Age
         AGE                = coalesce(AGE, AGE_MO / 12),
         AGE_DTA            = calc_age(birthdate, VISIT_DATE),

         # tag those without ART_FACI
         use_record_faci    = if_else(is.na(SERVICE_FACI), 1, 0, 0),
         SERVICE_FACI       = if_else(use_record_faci == 1, FACI_ID, SERVICE_FACI),

         # convert to HARP facility
         ACTUAL_FACI        = SERVICE_FACI,
         ACTUAL_SUB_FACI    = SERVICE_SUB_FACI,

         # tag special clinics
         special_clinic     = case_when(
            SERVICE_FACI %in% params$tly ~ "tly",
            SERVICE_FACI %in% params$sail ~ "sail",
            TRUE ~ NA_character_
         ),
         SERVICE_FACI       = case_when(
            special_clinic == "tly" ~ "130001",
            special_clinic == "sail" ~ "130025",
            TRUE ~ SERVICE_FACI
         ),

         # satellite
         SATELLITE_FACI     = if_else(
            condition = StrLeft(CLIENT_TYPE, 1) == "5",
            true      = FACI_DISP,
            false     = NA_character_
         ),
         SATELLITE_SUB_FACI = if_else(
            condition = StrLeft(CLIENT_TYPE, 1) == "5",
            true      = SUB_FACI_DISP,
            false     = NA_character_
         ),

         # transient
         TRANSIENT_FACI     = if_else(
            condition = StrLeft(CLIENT_TYPE, 1) == "6",
            true      = FACI_DISP,
            false     = NA_character_
         ),
         TRANSIENT_SUB_FACI = if_else(
            condition = StrLeft(CLIENT_TYPE, 1) == "6",
            true      = SUB_FACI_DISP,
            false     = NA_character_
         ),
      )

   return(data)
}

##  Sort by earliest visit of client for the report ----------------------------

get_final_visit <- function(data) {
   log_info("Using last visited facility.")
   data %<>%
      arrange(desc(VISIT_DATE), desc(LATEST_NEXT_DATE), CENTRAL_ID) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      rename(
         ART_FACI     = SERVICE_FACI,
         ART_SUB_FACI = SERVICE_SUB_FACI,
      )
   
   return(data)
}

##  Facilities -----------------------------------------------------------------

convert_faci_addr <- function(data) {
   # record faci
   data %<>%
      ohasis$get_faci(
         list(FACI_CODE = c("FACI_ID", "SUB_FACI_ID")),
         "code"
      ) %>%
      # art faci
      ohasis$get_faci(
         list(ART_FACI_CODE = c("ART_FACI", "ART_SUB_FACI")),
         "code",
         c("tx_reg", "tx_prov", "tx_munc")
      ) %>%
      # epic / gf faci
      ohasis$get_faci(
         list(ACTUAL_FACI_CODE = c("ACTUAL_FACI", "ACTUAL_SUB_FACI")),
         "code",
         c("real_reg", "real_prov", "real_munc")
      ) %>%
      # satellite
      ohasis$get_faci(
         list(SATELLITE_FACI_CODE = c("SATELLITE_FACI", "SATELLITE_SUB_FACI")),
         "code"
      ) %>%
      # satellite
      ohasis$get_faci(
         list(TRANSIENT_FACI_CODE = c("TRANSIENT_FACI", "TRANSIENT_SUB_FACI")),
         "code"
      ) %>%
      mutate(
         ART_BRANCH    = ART_FACI_CODE,
         ACTUAL_BRANCH = ACTUAL_FACI_CODE,
      ) %>%
      mutate(
         across(
            names(select(., ends_with("_BRANCH", ignore.case = FALSE))),
            ~if_else(nchar(.) > 3, ., NA_character_)
         )
      ) %>%
      mutate(
         across(
            names(select(., ends_with("_BRANCH", ignore.case = FALSE))),
            ~if_else(. == "TLY", "TLY-ANGLO", ., .)
         )
      ) %>%
      mutate(
         ART_BRANCH = case_when(
            special_clinic == "sail" ~ ACTUAL_BRANCH,
            special_clinic == "tly" & is.na(ACTUAL_BRANCH) ~ "TLY-ANGLO",
            special_clinic == "tly" & ACTUAL_BRANCH == "TLY" ~ "TLY-ANGLO",
            special_clinic == "tly" & ACTUAL_BRANCH != "TLY" ~ ACTUAL_BRANCH,
            TRUE ~ ART_BRANCH
         ),
      ) %>%
      arrange(ART_FACI_CODE, VISIT_DATE, LATEST_NEXT_DATE) %>%
      ohasis$get_addr(
         c(
            "artstart_reg"  = "CURR_PSGC_REG",
            "artstart_prov" = "CURR_PSGC_PROV",
            "artstart_munc" = "CURR_PSGC_MUNC"
         ),
         "nhsss"
      )

   return(data)
}

##  Generate subset variables --------------------------------------------------

# updated outcomes
tag_curr_data <- function(data, prev_outcome, art_first, last_disp, params) {
   last_disp %<>%
      arrange(desc(LATEST_NEXT_DATE)) %>%
      filter(VISIT_DATE <= ohasis$next_date) %>%
      mutate(LATEST_VISIT = VISIT_DATE) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      mutate(
         # tag those without ART_FACI
         use_record_faci    = if_else(
            condition = is.na(SERVICE_FACI),
            true      = 1,
            false     = 0
         ),
         SERVICE_FACI       = if_else(
            condition = use_record_faci == 1,
            true      = FACI_ID,
            false     = SERVICE_FACI
         ),

         # tag sail clinics as ship
         sail_clinic        = case_when(
            FACI_ID == "040200" ~ 1,
            SERVICE_FACI == "040200" ~ 1,
            FACI_ID == "040211" ~ 1,
            SERVICE_FACI == "040211" ~ 1,
            FACI_ID == "130748" ~ 1,
            SERVICE_FACI == "130748" ~ 1,
            FACI_ID == "130814" ~ 1,
            SERVICE_FACI == "130814" ~ 1,
            TRUE ~ 0
         ),

         # tag tly clinic
         tly_clinic         = case_when(
            FACI_ID == "070021" ~ 1,
            FACI_ID == "040198" ~ 1,
            FACI_ID == "070021" ~ 1,
            FACI_ID == "130173" ~ 1,
            FACI_ID == "130707" ~ 1,
            FACI_ID == "130708" ~ 1,
            FACI_ID == "130749" ~ 1,
            FACI_ID == "130751" ~ 1,
            FACI_ID == "130001" ~ 1,
            SERVICE_FACI == "070021" ~ 1,
            SERVICE_FACI == "040198" ~ 1,
            SERVICE_FACI == "070021" ~ 1,
            SERVICE_FACI == "130173" ~ 1,
            SERVICE_FACI == "130707" ~ 1,
            SERVICE_FACI == "130708" ~ 1,
            SERVICE_FACI == "130749" ~ 1,
            SERVICE_FACI == "130751" ~ 1,
            SERVICE_FACI == "130001" ~ 1,
            TRUE ~ 0
         ),

         # convert to HARP facility
         ACTUAL_FACI        = SERVICE_FACI,
         ACTUAL_SUB_FACI    = SERVICE_SUB_FACI,
         SERVICE_FACI       = case_when(
            tly_clinic == 1 ~ "130001",
            sail_clinic == 1 ~ "130025",
            TRUE ~ SERVICE_FACI
         ),

         # satellite
         SATELLITE_FACI     = if_else(
            condition = StrLeft(CLIENT_TYPE, 1) == "5",
            true      = FACI_DISP,
            false     = NA_character_
         ),
         SATELLITE_SUB_FACI = if_else(
            condition = StrLeft(CLIENT_TYPE, 1) == "5",
            true      = SUB_FACI_DISP,
            false     = NA_character_
         ),

         # transient
         TRANSIENT_FACI     = if_else(
            condition = StrLeft(CLIENT_TYPE, 1) == "6",
            true      = FACI_DISP,
            false     = NA_character_
         ),
         TRANSIENT_SUB_FACI = if_else(
            condition = StrLeft(CLIENT_TYPE, 1) == "6",
            true      = SUB_FACI_DISP,
            false     = NA_character_
         ),
      ) %>%
      ohasis$get_faci(
         list(ART_FACI_CODE = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
         "code"
      ) %>%
      ohasis$get_faci(
         list(ACTUAL_FACI_CODE = c("ACTUAL_FACI", "ACTUAL_SUB_FACI")),
         "code",
      ) %>%
      mutate(
         ART_BRANCH = if_else(
            condition = nchar(ART_FACI_CODE) > 3,
            true      = ART_FACI_CODE,
            false     = NA_character_
         ),
         .after     = ART_FACI_CODE
      ) %>%
      mutate(
         ACTUAL_BRANCH = if_else(
            condition = nchar(ACTUAL_FACI_CODE) > 3,
            true      = ACTUAL_FACI_CODE,
            false     = NA_character_
         ),
         .after        = ACTUAL_FACI_CODE
      ) %>%
      mutate(
         ART_BRANCH    = case_when(
            ART_FACI_CODE == "TLY" & is.na(ART_BRANCH) ~ "TLY-ANGLO",
            TRUE ~ ART_BRANCH
         ),
         ACTUAL_BRANCH = case_when(
            ACTUAL_FACI_CODE == "TLY" & is.na(ACTUAL_BRANCH) ~ "TLY-ANGLO",
            TRUE ~ ACTUAL_BRANCH
         ),
      ) %>%
      mutate_at(
         .vars = vars(ACTUAL_FACI_CODE, ART_FACI_CODE),
         ~case_when(
            stri_detect_regex(., "^SAIL") ~ "SAIL",
            stri_detect_regex(., "^TLY") ~ "TLY",
            TRUE ~ .
         )
      ) %>%
      mutate(
         ART_BRANCH = case_when(
            sail_clinic == 1 ~ ACTUAL_BRANCH,
            tly_clinic == 1 & is.na(ACTUAL_BRANCH) ~ "TLY-ANGLO",
            tly_clinic == 1 & ACTUAL_BRANCH == "TLY" ~ "TLY-ANGLO",
            tly_clinic == 1 & ACTUAL_BRANCH != "TLY" ~ ACTUAL_BRANCH,
            TRUE ~ ART_BRANCH
         ),
      ) %>%
      arrange(ART_FACI_CODE, VISIT_DATE, LATEST_NEXT_DATE) %>%
      mutate(
         ART_BRANCH    = case_when(
            ART_FACI_CODE == "SHP" & is.na(ART_BRANCH) ~ "SHIP-MAKATI",
            ART_FACI_CODE == "TLY" & is.na(ART_BRANCH) ~ "TLY-ANGLO",
            TRUE ~ ART_BRANCH
         ),
         ACTUAL_BRANCH = case_when(
            ACTUAL_FACI_CODE == "SHP" & is.na(ACTUAL_BRANCH) ~ "SHIP-MAKATI",
            ACTUAL_FACI_CODE == "TLY" & is.na(ACTUAL_BRANCH) ~ "TLY-ANGLO",
            TRUE ~ ACTUAL_BRANCH
         ),
      )

   data <- data %>%
      # get mortality data
      left_join(
         y  = dead %>%
            select(
               # CENTRAL_ID,
               mort_id,
               ref_death_date
            ),
         by = "mort_id"
      ) %>%
      # get latest outcome data
      left_join(
         y  = prev_outcome %>%
            mutate(hub = toupper(hub)) %>%
            select(
               art_id,
               prev_rec            = REC_ID,
               prev_class          = class,
               prev_outcome        = outcome,
               prev_ffup           = latest_ffupdate,
               prev_pickup         = latest_nextpickup,
               prev_regimen        = latest_regimen,
               prev_line           = line,
               prev_artreg         = art_reg,
               prev_hub            = hub,
               prev_branch         = branch,
               prev_sathub         = sathub,
               prev_transhub       = transhub,
               prev_realhub        = realhub,
               prev_realhub_branch = realhub_branch,
               prev_age            = curr_age,
            ),
         by = "art_id"
      ) %>%
      # get ohasis earliest visits
      left_join(
         y  = art_first %>%
            select(CENTRAL_ID, EARLIEST_REC = REC_ID, EARLIEST_VISIT = VISIT_DATE) %>%
            arrange(EARLIEST_VISIT) %>%
            distinct(CENTRAL_ID, .keep_all = TRUE),
         by = "CENTRAL_ID"
      ) %>%
      # get ohasis latest dispense
      left_join(
         y  = last_disp %>%
            select(
               CENTRAL_ID,
               LASTDISP_REC           = REC_ID,
               LASTDISP_HUB           = ART_FACI_CODE,
               LASTDISP_BRANCH        = ART_BRANCH,
               LASTDISP_ACTUAL_HUB    = ACTUAL_FACI_CODE,
               LASTDISP_ACTUAL_BRANCH = ACTUAL_BRANCH,
               LASTDISP_REC           = REC_ID,
               LASTDISP_VISIT         = VISIT_DATE,
               LASTDISP_NEXT_DATE     = LATEST_NEXT_DATE,
               LASTDISP_ARV           = MEDICINE_SUMMARY
            ) %>%
            distinct(CENTRAL_ID, .keep_all = TRUE),
         by = "CENTRAL_ID"
      ) %>%
      mutate(
         # fix date formats
         LATEST_NEXT_DATE    = as.Date(LATEST_NEXT_DATE),

         # self identity for the period, if available
         self_identity       = if_else(
            condition = !is.na(SELF_IDENT),
            true      = substr(stri_trans_toupper(SELF_IDENT), 3, stri_length(SELF_IDENT)),
            false     = NA_character_
         ),
         self_identity_other = toupper(SELF_IDENT_OTHER),
         self_identity       = case_when(
            self_identity_other == "N/A" ~ NA_character_,
            self_identity_other == "no answer" ~ NA_character_,
            self_identity == "OTHER" ~ "OTHERS",
            self_identity == "MAN" ~ "MALE",
            self_identity == "WOMAN" ~ "FEMALE",
            self_identity == "MALE" ~ "MALE",
            self_identity == "FEMALE" ~ "FEMALE",
            TRUE ~ self_identity
         ),
         self_identity_other = case_when(
            self_identity_other == "NO ANSWER" ~ NA_character_,
            self_identity_other == "N/A" ~ NA_character_,
            TRUE ~ self_identity_other
         ),

         # clinical pic
         who_staging         = StrLeft(WHO_CLASS, 1) %>% as.integer(),

         # tag if new data is to be used
         new_report          = if_else(is.na(prev_outcome), 1, 0, 0),
         use_type            = case_when(
            !is.na(MEDICINE_SUMMARY) & LATEST_NEXT_DATE >= -25567 ~ "latest",
            !is.na(LASTDISP_ARV) & LASTDISP_NEXT_DATE >= -25567 ~ "lastdisp",
            TRUE ~ NA_character_
         ),
         use_db              = case_when(
            new_report == 1 & use_type == "latest" ~ 1,
            new_report == 1 & use_type == "lastdisp" ~ 2,
            LATEST_VISIT > prev_ffup & use_type == "latest" ~ 1,
            LATEST_NEXT_DATE > prev_pickup & use_type == "latest" ~ 1,
            LASTDISP_VISIT > prev_ffup & use_type == "lastdisp" ~ 2,
            LASTDISP_NEXT_DATE > prev_pickup & use_type == "lastdisp" ~ 2,
            LATEST_VISIT == prev_ffup & MEDICINE_SUMMARY != prev_regimen ~ 1,
            TRUE ~ 0
         ),

         # current age for class
         curr_age            = case_when(
            !is.na(birthdate) & use_db == 1 ~ abs(as.numeric(difftime(LATEST_VISIT, birthdate, units = "days")) / 365.25),
            is.na(birthdate) & use_db == 1 & !is.na(age) ~ age + (year(LATEST_VISIT) - year(artstart_date)),
            is.na(birthdate) & use_db == 1 & is.na(age) ~ prev_age + (year(LATEST_VISIT) - year(prev_ffup)),
            !is.na(birthdate) & use_db == 2 ~ abs(as.numeric(difftime(LASTDISP_VISIT, birthdate, units = "days")) / 365.25),
            is.na(birthdate) & use_db == 2 & !is.na(age) ~ age + (year(LASTDISP_VISIT) - year(artstart_date)),
            is.na(birthdate) & use_db == 2 & is.na(age) ~ prev_age + (year(LASTDISP_VISIT) - year(prev_ffup)),
            !is.na(birthdate) & use_db == 0 ~ abs(as.numeric(difftime(prev_ffup, birthdate, units = "days")) / 365.25),
            is.na(birthdate) & use_db == 0 & !is.na(age) ~ age + (year(prev_ffup) - year(artstart_date)),
            is.na(birthdate) & use_db == 0 & is.na(age) ~ prev_age + (year(prev_ffup) - year(artstart_date)),
         ) %>% floor(),
         curr_class          = case_when(
            curr_age >= 10 ~ "Adult Patient",
            curr_age < 10 ~ "Pediatric Patient",
         ),

         # current outcome
         curr_outcome        = case_when(
            use_db == 1 & (LATEST_VISIT <= ref_death_date) ~ "dead",
            use_db == 1 & LATEST_NEXT_DATE >= params$cutoff_date ~ "alive on arv",
            use_db == 1 & LATEST_NEXT_DATE < params$cutoff_date ~ "lost to follow up",
            use_db == 2 & (LASTDISP_VISIT <= ref_death_date) ~ "dead",
            use_db == 2 & LASTDISP_NEXT_DATE >= params$cutoff_date ~ "alive on arv",
            use_db == 2 & LASTDISP_NEXT_DATE < params$cutoff_date ~ "lost to follow up",
            use_db == 0 & prev_ffup <= ref_death_date ~ "dead",
            use_db == 0 &
               stri_detect_fixed(prev_outcome, "dead") &
               is.na(ref_death_date) ~ prev_outcome,
            use_db == 0 & stri_detect_fixed(prev_outcome, "stopped") ~ prev_outcome,
            use_db == 0 & stri_detect_fixed(prev_outcome, "overseas") ~ prev_outcome,
            use_db == 0 & prev_pickup >= params$cutoff_date ~ "alive on arv",
            use_db == 0 & prev_pickup < params$cutoff_date ~ "lost to follow up",
            use_db == 0 ~ prev_outcome
         ),

         # count number of drugs in regimen
         prev_num_drugs      = if_else(
            condition = !is.na(prev_regimen),
            true      = stri_count_fixed(prev_regimen, "+") + 1,
            false     = 0
         ),
         curr_num_drugs      = if_else(
            condition = !is.na(MEDICINE_SUMMARY),
            true      = stri_count_fixed(MEDICINE_SUMMARY, "+") + 1,
            false     = 0
         ),

         # check for multi-month clients
         days_to_pickup      = case_when(
            use_db == 1 ~ abs(as.numeric(difftime(LATEST_NEXT_DATE, LATEST_VISIT, units = "days"))),
            use_db == 2 ~ abs(as.numeric(difftime(LASTDISP_NEXT_DATE, LASTDISP_VISIT, units = "days"))),
            use_db == 0 ~ abs(as.numeric(difftime(prev_pickup, prev_ffup, units = "days"))),
         ),
         arv_worth           = case_when(
            days_to_pickup == 0 ~ '0_No ARVs',
            days_to_pickup > 0 & days_to_pickup < 90 ~ '1_<3 months worth of ARVs',
            days_to_pickup >= 90 &
               days_to_pickup < 180 ~ '2_3-5 months worth of ARVs',
            days_to_pickup >= 180 & days_to_pickup <= 365.25 ~ '3_6-12 months worth of ARVs',
            days_to_pickup > 365.25 ~ '4_More than 1 yr worth of ARVs',
            TRUE ~ '5_(no data)'
         ),

         # regimen disagg
         arv_reg             = stri_trans_tolower(MEDICINE_SUMMARY),
         azt1                = if_else(stri_detect_fixed(arv_reg, "azt"), "azt", NA_character_),
         tdf1                = if_else(stri_detect_fixed(arv_reg, "tdf"), "tdf", NA_character_),
         d4t1                = if_else(stri_detect_fixed(arv_reg, "d4t"), "d4t", NA_character_),
         xtc1                = if_else(stri_detect_fixed(arv_reg, "3tc"), "3tc", NA_character_),
         efv1                = if_else(stri_detect_fixed(arv_reg, "efv"), "efv", NA_character_),
         nvp1                = if_else(stri_detect_fixed(arv_reg, "nvp"), "nvp", NA_character_),
         lpvr1               = if_else(stri_detect_fixed(arv_reg, "lpv/r"), "lpv/r", NA_character_),
         lpvr1               = if_else(stri_detect_fixed(arv_reg, "lpvr"), "lpvr", lpvr1),
         ind1                = if_else(stri_detect_fixed(arv_reg, "ind"), "ind", NA_character_),
         ral1                = if_else(stri_detect_fixed(arv_reg, "ral"), "ral", NA_character_),
         abc1                = if_else(stri_detect_fixed(arv_reg, "abc"), "abc", NA_character_),
         ril1                = if_else(stri_detect_fixed(arv_reg, "ril"), "ril", NA_character_),
         dtg1                = if_else(stri_detect_fixed(arv_reg, "dtg"), "dtg", NA_character_),
      )

   return(data)
}

##  Finalize -------------------------------------------------------------------

final_conversion <- function(data) {
   data %<>%
      unite(
         col   = 'art_reg1',
         sep   = ' ',
         na.rm = T,
         azt1,
         tdf1,
         d4t1,
         xtc1,
         efv1,
         nvp1,
         lpvr1,
         ind1,
         ral1,
         abc1,
         ril1,
         dtg1
      ) %>%
      mutate(
         curr_line = case_when(
            art_reg1 == "tdf 3tc efv" ~ 1,
            art_reg1 == "tdf 3tc nvp" ~ 1,
            art_reg1 == "azt 3tc efv" ~ 1,
            art_reg1 == "azt 3tc nvp" ~ 1,
            art_reg1 == "d4t 3tc efv" ~ 1,
            art_reg1 == "d4t 3tc nvp" ~ 1,
            art_reg1 == "tdf 3tc dtg" ~ 1,
            art_reg1 == "3tc nvp abc" ~ 1,
            art_reg1 == "3tc efv abc" ~ 1,
            art_reg1 == "3tc abc ril" ~ 1,
            art_reg1 == "tdf 3tc ril" ~ 1,
            art_reg1 == "azt 3tc ril" ~ 1,
            art_reg1 == "azt 3tc lpv/r" ~ 2,
            art_reg1 == "tdf 3tc lpv/r" ~ 2,
            art_reg1 == "d4t 3tc lpv/r" ~ 2,
            art_reg1 == "d4t 3tc idv" ~ 2,
            art_reg1 == "azt 3tc idv" ~ 2,
            art_reg1 == "tdf 3tc idv" ~ 2,
            art_reg1 == "3tc lpv/r abc" ~ 2,
            !is.na(art_reg1) ~ 3
         ),
      ) %>%
      # same vars as registry
      select(
         REC_ID,
         CENTRAL_ID,
         art_id,
         idnum,
         prep_id,
         mort_id,
         year,
         month,
         confirmatory_code,
         px_code,
         uic,
         first,
         middle,
         last,
         suffix,
         birthdate,
         sex,
         initials,
         philsys_id,
         philhealth_no,
         who_staging,
         use_db,
         artstart_date,
         artstart_rec,
         oh_artstart_rec     = EARLIEST_REC,
         oh_artstart         = EARLIEST_VISIT,
         prev_rec,
         prev_hub,
         prev_branch,
         prev_sathub,
         prev_transhub,
         prev_realhub,
         prev_realhub_branch,
         prev_class,
         prev_outcome,
         prev_line,
         prev_ffup,
         prev_pickup,
         prev_regimen,
         prev_artreg,
         prev_num_drugs,
         curr_hub            = ART_FACI_CODE,
         curr_branch         = ART_BRANCH,
         curr_sathub         = SATELLITE_FACI_CODE,
         curr_transhub       = TRANSIENT_FACI_CODE,
         curr_age,
         curr_class,
         curr_outcome,
         curr_line,
         curr_ffup           = LATEST_VISIT,
         curr_pickup         = LATEST_NEXT_DATE,
         curr_regimen        = MEDICINE_SUMMARY,
         lastdisp_rec        = LASTDISP_REC,
         lastdisp_hub        = LASTDISP_HUB,
         lastdisp_branch     = LASTDISP_BRANCH,
         lastdisp_realhub    = LASTDISP_ACTUAL_HUB,
         lastdisp_realbranch = LASTDISP_ACTUAL_BRANCH,
         lastdisp_ffup       = LASTDISP_VISIT,
         lastdisp_pickup     = LASTDISP_NEXT_DATE,
         lastdisp_regimen    = LASTDISP_ARV,
         lastdisp_rec        = LASTDISP_REC,
         curr_artreg         = art_reg1,
         curr_num_drugs,
         tx_reg,
         tx_prov,
         tx_munc,
         curr_realhub        = ACTUAL_FACI_CODE,
         curr_realhub_branch = ACTUAL_BRANCH,
         real_reg,
         real_prov,
         real_munc,
         days_to_pickup,
         arv_worth,
         ref_death_date,
      ) %>%
      # remove codes
      mutate_at(
         .vars = vars(contains("date")),
         ~if_else(
            condition = !is.na(.),
            true      = as.Date(.),
            false     = NA_Date_
         )
      )

   return(data)
}
##  Flag data for validation ---------------------------------------------------

get_checks <- function(data) {
   check  <- list()
   update <- input(
      prompt  = "Run `outcome.initial` validations?",
      options = c("1" = "yes", "2" = "no"),
      default = "1"
   )
   update <- substr(toupper(update), 1, 1)
   if (update == "1") {
      # initialize checking layer

      view_vars <- c(
         "REC_ID",
         "CENTRAL_ID",
         "FACI_CODE",
         "ACTUAL_FACI_CODE",
         "ACTUAL_BRANCH",
         "FORM_VERSION",
         "art_id",
         "confirmatory_code",
         "uic",
         "px_code",
         "philhealth_no",
         "philsys_id",
         "first",
         "middle",
         "last",
         "suffix",
         "birthdate",
         "sex",
         "TX_STATUS",
         "VISIT_DATE",
         "LATEST_NEXT_DATE",
         "MEDICINE_SUMMARY",
         "CLINIC_NOTES",
         "COUNSEL_NOTES"
      )

      # dates
      date_vars <- c(
         "VISIT_DATE",
         "DISP_DATE"
      )
      check     <- check_dates(data, check, view_vars, date_vars)

      # non-negotiable variables
      nonnegotiables <- c(
         "ART_FACI_CODE",
         "MEDICINE_SUMMARY"
      )
      check          <- check_nonnegotiables(data, check, view_vars, nonnegotiables)

      log_info("Checking for dispensing later than next pick-up.")
      check[["disp_>_next"]] <- data %>%
         filter(
            VISIT_DATE > LATEST_NEXT_DATE
         ) %>%
         select(
            any_of(view_vars),
            LATEST_NEXT_DATE
         )

      log_info("Checking for mismatch record vs art faci.")
      check[["mismatch_faci"]] <- data %>%
         filter(
            FACI_CODE != ART_FACI_CODE,
            sail_clinic != 1
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for mismatch visit vs disp date.")
      check[["mismatch_disp"]] <- data %>%
         mutate(
            diff = abs(as.numeric(difftime(RECORD_DATE, as.Date(DISP_DATE), units = "days")))
         ) %>%
         filter(
            diff > 21
         ) %>%
         select(
            any_of(view_vars),
            FACI_CODE,
            ART_FACI_CODE,
            RECORD_DATE,
            DISP_DATE,
            diff
         )

      log_info("Checking for possible PMTCT-N clients.")
      check[["possible_pmtct"]] <- data %>%
         filter(
            (NUM_OF_DRUGS == 1 & stri_detect_fixed(MEDICINE_SUMMARY, "syr")) |
               AGE <= 5 |
               AGE_DTA <= 5
         ) %>%
         select(
            any_of(view_vars),
            MEDICINE_SUMMARY,
         )

      log_info("Checking for possible PrEP clients.")
      check[["possible_prep"]] <- data %>%
         filter(
            stri_detect_fixed(MEDICINE_SUMMARY, "FTC")
         ) %>%
         select(
            any_of(view_vars),
            MEDICINE_SUMMARY,
         )

      log_info("Checking calculated age vs computed age.")
      check[["mismatch_age"]] <- data %>%
         filter(
            AGE != AGE_DTA
         ) %>%
         select(
            any_of(view_vars),
            AGE,
            AGE_DTA
         )

      log_info("Checking ART reports tagged as DOH-EB.")
      check[["art_eb"]] <- data %>%
         filter(
            ART_FACI_CODE == "DOH"
         ) %>%
         select(
            any_of(view_vars),
         )

      # range-median
      tabstat <- c(
         "encoded_date",
         "VISIT_DATE",
         "DISP_DATE",
         "LATEST_NEXT_DATE",
         "BIRTHDATE",
         "AGE",
         "AGE_MO"
      )
      check   <- check_tabstat(data, check, tabstat)
   }
   return(check)
}

##  Actual flow ----------------------------------------------------------------

.init <- function() {
   p <- parent.env(environment())
   local(envir = p, {
      data <- get_latest_visit(.GlobalEnv$nhsss$harp_tx$forms$art_last, .GlobalEnv$nhsss$harp_tx$official$new_reg) %>%
         get_final_visit() %>%
         convert_faci_addr()

      write_rds(data, file.path(wd, "outcome.initial.RDS"))

      check <- get_checks(data)
   })

   local(envir = .GlobalEnv, flow_validation(nhsss$harp_tx, "outcome.initial", ohasis$ym))
}