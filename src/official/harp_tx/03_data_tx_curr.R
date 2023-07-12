##  Append to the previous art registry ----------------------------------------

get_latest_record <- function(form_data, new_reg, params) {
   log_info("Processing latest visit.")
   data <- new_reg %>%
      select(-PATIENT_ID) %>%
      rename(artstart_rec = REC_ID) %>%
      left_join(
         y  = form_data %>%
            arrange(desc(LATEST_NEXT_DATE)) %>%
            mutate(LATEST_VISIT = VISIT_DATE) %>%
            distinct(CENTRAL_ID, .keep_all = TRUE),
         by = join_by(CENTRAL_ID)
      ) %>%
      mutate_if(
         .predicate = is.POSIXct,
         ~as.Date(.)
      ) %>%
      mutate_if(
         .predicate = is.Date,
         ~if_else(. <= -25567, NA_Date_, ., .)
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
      )

   data %<>%
      mutate_at(
         .vars = vars(ends_with("_FACI_CODE", ignore.case = FALSE)),
         ~case_when(
            str_detect(., "^TLY") ~ "TLY",
            str_detect(., "^SHIP") ~ "SHP",
            str_detect(., "^SAIL") ~ "SAIL",
            TRUE ~ .
         )
      ) %>%
      mutate(
         across(
            names(select(., ends_with("_BRANCH", ignore.case = FALSE))),
            ~case_when(
               pull(data, str_replace(cur_column(), "_BRANCH", "_FACI_CODE")) == "TLY" & is.na(.) ~ "TLY-ANGLO",
               pull(data, str_replace(cur_column(), "_BRANCH", "_FACI_CODE")) == "SHP" & is.na(.) ~ "SHIP-MAKATI",
               TRUE ~ .
            )
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
      arrange(ART_FACI_CODE, VISIT_DATE, LATEST_NEXT_DATE)

   return(data)
}

##  Generate subset variables --------------------------------------------------

# updated outcomes
tag_curr_data <- function(data, prev_outcome, art_first, last_disp, last_vl, params) {
   log_info("Converting to final HARP variables.")
   data %<>%
      # get latest outcome data
      left_join(
         y  = prev_outcome %>%
            mutate(hub = toupper(hub)) %>%
            select(
               art_id,
               prev_rec            = REC_ID,
               prev_class          = class,
               prev_outcome        = outcome,
               prev_ffupdate       = latest_ffupdate,
               prev_nextpickup     = latest_nextpickup,
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
         by = join_by(art_id)
      ) %>%
      # get ohasis earliest visits
      left_join(
         y  = art_first %>%
            select(CENTRAL_ID, EARLIEST_REC = REC_ID, EARLIEST_VISIT = VISIT_DATE) %>%
            arrange(EARLIEST_VISIT) %>%
            distinct(CENTRAL_ID, .keep_all = TRUE),
         by = join_by(CENTRAL_ID)
      ) %>%
      # get ohasis latest vl
      left_join(
         y  = last_vl %>%
            select(CENTRAL_ID, LAST_VL_DATE = LAB_VIRAL_DATE, LAST_VL_RESULT = LAB_VIRAL_RESULT) %>%
            arrange(desc(LAST_VL_DATE)) %>%
            distinct(CENTRAL_ID, .keep_all = TRUE),
         by = join_by(CENTRAL_ID)
      ) %>%
      # get ohasis latest dispense
      left_join(
         y  = last_disp %>%
            select(
               art_id,
               LASTDISP_REC           = REC_ID,
               LASTDISP_HUB           = ART_FACI_CODE,
               LASTDISP_BRANCH        = ART_BRANCH,
               LASTDISP_ACTUAL_HUB    = ACTUAL_FACI_CODE,
               LASTDISP_ACTUAL_BRANCH = ACTUAL_BRANCH,
               LASTDISP_SATHUB        = SATELLITE_FACI_CODE,
               LASTDISP_TRANSHUB      = TRANSIENT_FACI_CODE,
               LASTDISP_REC           = REC_ID,
               LASTDISP_VISIT         = VISIT_DATE,
               LASTDISP_NEXT_DATE     = LATEST_NEXT_DATE,
               LASTDISP_ARV           = MEDICINE_SUMMARY
            ) %>%
            distinct(art_id, .keep_all = TRUE),
         by = join_by(art_id)
      ) %>%
      mutate(
         # clinical pic
         who_staging    = as.integer(keep_code(WHO_CLASS)),

         # pregnant
         pregnant       = as.integer(keep_code(IS_PREGNANT)),

         # tag if new data is to be used
         new_report     = if_else(is.na(prev_outcome), 1, 0, 0),
         use_type       = case_when(
            !is.na(MEDICINE_SUMMARY) & LATEST_NEXT_DATE >= -25567 ~ "latest",
            !is.na(LASTDISP_ARV) & LASTDISP_NEXT_DATE >= -25567 ~ "lastdisp",
            TRUE ~ NA_character_
         ),
         use_db         = case_when(
            new_report == 1 & use_type == "latest" ~ 1,
            new_report == 1 & use_type == "lastdisp" ~ 2,
            LATEST_VISIT > prev_ffupdate & use_type == "latest" ~ 1,
            LATEST_NEXT_DATE > prev_nextpickup & use_type == "latest" ~ 1,
            LASTDISP_VISIT > prev_ffupdate & use_type == "lastdisp" ~ 2,
            LASTDISP_NEXT_DATE > prev_nextpickup & use_type == "lastdisp" ~ 2,
            LATEST_VISIT == prev_ffupdate & MEDICINE_SUMMARY != prev_regimen ~ 1,
            TRUE ~ 0
         ),

         # current age for class
         curr_age       = case_when(
            !is.na(birthdate) & use_db == 1 ~ calc_age(birthdate, LATEST_VISIT),
            is.na(birthdate) & use_db == 1 & !is.na(age) ~ age + (year(LATEST_VISIT) - year(artstart_date)),
            is.na(birthdate) & use_db == 1 & is.na(age) ~ prev_age + (year(LATEST_VISIT) - year(prev_ffupdate)),
            !is.na(birthdate) & use_db == 2 ~ calc_age(birthdate, LASTDISP_VISIT),
            is.na(birthdate) & use_db == 2 & !is.na(age) ~ age + (year(LASTDISP_VISIT) - year(artstart_date)),
            is.na(birthdate) & use_db == 2 & is.na(age) ~ prev_age + (year(LASTDISP_VISIT) - year(prev_ffupdate)),
            !is.na(birthdate) & use_db == 0 ~ calc_age(prev_ffupdate, LASTDISP_VISIT),
            is.na(birthdate) & use_db == 0 & !is.na(age) ~ age + (year(prev_ffupdate) - year(artstart_date)),
            is.na(birthdate) & use_db == 0 & is.na(age) ~ prev_age + (year(prev_ffupdate) - year(artstart_date)),
         ),
         curr_age       = as.integer(floor(age)),
         curr_class     = case_when(
            curr_age >= 10 ~ "Adult Patient",
            curr_age < 10 ~ "Pediatric Patient",
         ),

         # current outcome
         curr_outcome   = case_when(
            use_db == 1 & (LATEST_VISIT <= ref_death_date) ~ "dead",
            use_db == 1 & LATEST_NEXT_DATE >= params$max ~ "alive on arv",
            use_db == 1 & LATEST_NEXT_DATE < params$max ~ "lost to follow up",
            use_db == 2 & (LASTDISP_VISIT <= ref_death_date) ~ "dead",
            use_db == 2 & LASTDISP_NEXT_DATE >= params$max ~ "alive on arv",
            use_db == 2 & LASTDISP_NEXT_DATE < params$max ~ "lost to follow up",
            use_db == 0 & prev_ffupdate <= ref_death_date ~ "dead",
            use_db == 0 &
               stri_detect_fixed(prev_outcome, "dead") &
               is.na(ref_death_date) ~ prev_outcome,
            use_db == 0 & stri_detect_fixed(prev_outcome, "stopped") ~ prev_outcome,
            use_db == 0 & stri_detect_fixed(prev_outcome, "overseas") ~ prev_outcome,
            use_db == 0 & prev_nextpickup >= params$max ~ "alive on arv",
            use_db == 0 & prev_nextpickup < params$max ~ "lost to follow up",
            use_db == 0 ~ prev_outcome
         ),

         # count number of drugs in regimen
         prev_num_drugs = if_else(
            condition = !is.na(prev_regimen),
            true      = stri_count_fixed(prev_regimen, "+") + 1,
            false     = 0
         ),
         curr_num_drugs = if_else(
            condition = !is.na(MEDICINE_SUMMARY),
            true      = stri_count_fixed(MEDICINE_SUMMARY, "+") + 1,
            false     = 0
         ),

         # check for multi-month clients
         days_to_pickup = case_when(
            use_db == 1 ~ abs(as.numeric(difftime(LATEST_NEXT_DATE, LATEST_VISIT, units = "days"))),
            use_db == 2 ~ abs(as.numeric(difftime(LASTDISP_NEXT_DATE, LASTDISP_VISIT, units = "days"))),
            use_db == 0 ~ abs(as.numeric(difftime(prev_nextpickup, prev_ffupdate, units = "days"))),
         ),
         arv_worth      = case_when(
            days_to_pickup == 0 ~ '0_No ARVs',
            days_to_pickup > 0 & days_to_pickup < 90 ~ '1_<3 months worth of ARVs',
            days_to_pickup >= 90 &
               days_to_pickup < 180 ~ '2_3-5 months worth of ARVs',
            days_to_pickup >= 180 & days_to_pickup <= 365.25 ~ '3_6-12 months worth of ARVs',
            days_to_pickup > 365.25 ~ '4_More than 1 yr worth of ARVs',
            TRUE ~ '5_(no data)'
         ),

         # regimen disagg
         arv_reg        = stri_trans_tolower(MEDICINE_SUMMARY),
         azt1           = if_else(stri_detect_fixed(arv_reg, "azt"), "azt", NA_character_),
         tdf1           = if_else(stri_detect_fixed(arv_reg, "tdf"), "tdf", NA_character_),
         d4t1           = if_else(stri_detect_fixed(arv_reg, "d4t"), "d4t", NA_character_),
         xtc1           = if_else(stri_detect_fixed(arv_reg, "3tc"), "3tc", NA_character_),
         efv1           = if_else(stri_detect_fixed(arv_reg, "efv"), "efv", NA_character_),
         nvp1           = if_else(stri_detect_fixed(arv_reg, "nvp"), "nvp", NA_character_),
         lpvr1          = if_else(stri_detect_fixed(arv_reg, "lpv/r"), "lpv/r", NA_character_),
         lpvr1          = if_else(stri_detect_fixed(arv_reg, "lpvr"), "lpvr", lpvr1),
         ind1           = if_else(stri_detect_fixed(arv_reg, "ind"), "ind", NA_character_),
         ral1           = if_else(stri_detect_fixed(arv_reg, "ral"), "ral", NA_character_),
         abc1           = if_else(stri_detect_fixed(arv_reg, "abc"), "abc", NA_character_),
         ril1           = if_else(stri_detect_fixed(arv_reg, "ril"), "ril", NA_character_),
         dtg1           = if_else(stri_detect_fixed(arv_reg, "dtg"), "dtg", NA_character_),
      )

   return(data)
}

##  Finalize -------------------------------------------------------------------

final_conversion <- function(data) {
   log_info("Selecting final dataset structure.")
   data %<>%
      unite(
         col   = "art_reg1",
         sep   = " ",
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
         oh_artstart_rec         = EARLIEST_REC,
         oh_artstart             = EARLIEST_VISIT,
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
         prev_ffupdate,
         prev_nextpickup,
         prev_regimen,
         prev_artreg,
         prev_num_drugs,
         curr_hub                = ART_FACI_CODE,
         curr_branch             = ART_BRANCH,
         curr_sathub             = SATELLITE_FACI_CODE,
         curr_transhub           = TRANSIENT_FACI_CODE,
         curr_age,
         curr_class,
         curr_outcome,
         curr_line,
         curr_ffupdate           = LATEST_VISIT,
         curr_nextpickup         = LATEST_NEXT_DATE,
         curr_regimen            = MEDICINE_SUMMARY,
         curr_vl_date            = LAST_VL_DATE,
         curr_vl_result          = LAST_VL_RESULT,
         lastdisp_rec            = LASTDISP_REC,
         lastdisp_hub            = LASTDISP_HUB,
         lastdisp_branch         = LASTDISP_BRANCH,
         lastdisp_realhub        = LASTDISP_ACTUAL_HUB,
         lastdisp_realhub_branch = LASTDISP_ACTUAL_BRANCH,
         lastdisp_sathub         = LASTDISP_SATHUB,
         lastdisp_transhub       = LASTDISP_TRANSHUB,
         lastdisp_ffupdate       = LASTDISP_VISIT,
         lastdisp_nextpickup     = LASTDISP_NEXT_DATE,
         lastdisp_regimen        = LASTDISP_ARV,
         lastdisp_rec            = LASTDISP_REC,
         curr_artreg             = art_reg1,
         curr_num_drugs,
         prev_num_drugs,
         tx_reg,
         tx_prov,
         tx_munc,
         curr_realhub            = ACTUAL_FACI_CODE,
         curr_realhub_branch     = ACTUAL_BRANCH,
         real_reg,
         real_prov,
         real_munc,
         days_to_pickup,
         arv_worth,
         ref_death_date,
         confirm_date,
         confirm_result,
         confirm_remarks
      )

   return(data)
}

##  Update outcomes ------------------------------------------------------------

finalize_outcomes <- function(data, params) {
   log_info("Finalizing treatment outcomes.")
   data %<>%
      arrange(art_id) %>%
      mutate(
         REC_ID = case_when(
            use_db == 1 ~ REC_ID,
            use_db == 2 ~ lastdisp_rec,
            use_db == 0 ~ REC_ID,
            TRUE ~ prev_rec
         )
      ) %>%
      mutate(
         hub            = NA_character_,
         branch         = NA_character_,
         sathub         = NA_character_,
         sathub         = NA_character_,
         transhub       = NA_character_,
         realhub        = NA_character_,
         realhub_branch = NA_character_,
         ffupdate       = NA_Date_,
         nextpickup     = NA_Date_,
         regimen        = NA_character_,
      )

   data %<>%
      # use latest data
      mutate(
         across(
            c("hub", "branch", "sathub", "transhub", "realhub", "realhub_branch", "ffupdate", "nextpickup", "regimen"),
            ~case_when(
               use_db == 1 ~ pull(data, str_c("curr_", cur_column())),
               use_db == 2 ~ pull(data, str_c("lastdisp_", cur_column())),
               use_db == 0 ~ pull(data, str_c("prev_", cur_column()))
            )
         ),
      ) %>%
      rename_at(
         .vars = vars(regimen, ffupdate, nextpickup),
         ~str_c("latest_", .)
      ) %>%
      # specific taggings
      mutate(
         curr_outcome = case_when(
            prev_outcome == "dead" ~ "dead",
            TRUE ~ curr_outcome
         ),
         newonart     = if_else(
            condition = artstart_date %within% interval(params$min, params$max),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         curr_class   = coalesce(curr_class, prev_class),
      ) %>%
      # final outcome
      mutate(
         outcome = hiv_tx_outcome(curr_outcome, latest_nextpickup, params$max, 30),
         onart   = if_else(
            condition = outcome == "alive on arv",
            true      = 1,
            false     = 0,
            missing   = 0
         ),
      )

   return(data)
}

finalize_faci <- function(data) {
   log_info("Finalizing facility data.")
   data %<>%
      select(
         -ends_with("_reg"),
         -ends_with("_prov"),
         -ends_with("_munc"),
      ) %>%
      mutate(
         # finalize realhub data
         realhub_branch = if_else(
            condition = is.na(realhub),
            true      = branch,
            false     = realhub_branch
         ),
         realhub        = if_else(
            condition = is.na(realhub),
            true      = hub,
            false     = realhub
         )
      ) %>%
      mutate(
         branch         = case_when(
            hub == "TLY" & is.na(branch) ~ "TLY-ANGLO",
            TRUE ~ branch
         ),
         realhub_branch = case_when(
            realhub == "TLY" & is.na(realhub_branch) ~ "TLY-ANGLO",
            TRUE ~ realhub_branch
         ),
      ) %>%
      mutate_at(
         .vars = vars(hub, realhub),
         ~case_when(
            stri_detect_regex(., "^SAIL") ~ "SAIL",
            stri_detect_regex(., "^SHIP") ~ "SHP",
            stri_detect_regex(., "^TLY") ~ "TLY",
            TRUE ~ .
         )
      ) %>%
      mutate(
         branch         = case_when(
            hub == "SHP" & is.na(branch) ~ "SHIP-MAKATI",
            hub == "TLY" & is.na(branch) ~ "TLY-ANGLO",
            TRUE ~ branch
         ),
         realhub_branch = case_when(
            realhub == "SHP" & is.na(realhub_branch) ~ "SHIP-MAKATI",
            realhub == "TLY" & is.na(realhub_branch) ~ "TLY-ANGLO",
            TRUE ~ realhub_branch
         ),
         realhub        = case_when(
            stri_detect_regex(realhub_branch, "^SAIL") ~ "SAIL",
            TRUE ~ realhub
         ),
      ) %>%
      faci_code_to_id(
         ohasis$ref_faci_code %>% distinct(FACI_CODE, SUB_FACI_CODE, .keep_all = TRUE),
         c(FACI_ID = "hub", SUB_FACI_ID = "branch")
      ) %>%
      left_join(
         y  = ohasis$ref_faci %>%
            select(
               FACI_ID,
               SUB_FACI_ID,
               tx_reg  = FACI_NHSSS_REG,
               tx_prov = FACI_NHSSS_PROV,
               tx_munc = FACI_NHSSS_MUNC,
            ),
         by = join_by(FACI_ID, SUB_FACI_ID)
      ) %>%
      select(-FACI_ID, -SUB_FACI_ID) %>%
      faci_code_to_id(
         ohasis$ref_faci_code %>% distinct(FACI_CODE, SUB_FACI_CODE, .keep_all = TRUE),
         c(FACI_ID = "realhub", SUB_FACI_ID = "realhub_branch")
      ) %>%
      left_join(
         y  = ohasis$ref_faci %>%
            select(
               FACI_ID,
               SUB_FACI_ID,
               real_reg  = FACI_NHSSS_REG,
               real_prov = FACI_NHSSS_PROV,
               real_munc = FACI_NHSSS_MUNC,
            ),
         by = join_by(FACI_ID, SUB_FACI_ID)
      ) %>%
      select(
         REC_ID,
         CENTRAL_ID,
         art_id,
         idnum,
         prep_id,
         mort_id,
         sex,
         curr_age,
         hub,
         branch,
         sathub,
         transhub,
         tx_reg,
         tx_prov,
         tx_munc,
         realhub,
         realhub_branch,
         real_reg,
         real_prov,
         real_munc,
         artstart_date,
         class               = curr_class,
         outcome,
         latest_ffupdate,
         latest_nextpickup,
         latest_regimen,
         previous_ffupdate   = prev_ffupdate,
         previous_nextpickup = prev_nextpickup,
         previous_regimen    = prev_regimen,
         newonart,
         onart
      ) %>%
      distinct_all() %>%
      arrange(art_id, desc(latest_nextpickup)) %>%
      distinct(art_id, .keep_all = TRUE) %>%
      mutate(central_id = CENTRAL_ID) %>%
      mutate_at(
         .vars = vars(ends_with("_ffupdate"), ends_with("_nextpickup")),
         ~as.Date(.)
      )

   return(data)
}

reg_disagg <- function(data, regimen_col, reg_disagg_col, reg_line_col) {
   concat_col     <- reg_disagg_col
   regimen_col    <- as.name(regimen_col)
   reg_disagg_col <- as.name(reg_disagg_col)
   reg_line_col   <- as.name(reg_line_col)

   data %<>%
      mutate(
         # reg disagg
         !!reg_disagg_col := toupper(str_squish(!!regimen_col)),
         r_abc            = if_else(
            stri_detect_fixed(!!reg_disagg_col, "ABC") &
               !stri_detect_fixed(!!reg_disagg_col, "ABCSYR"),
            "ABC",
            NA_character_
         ),
         r_abcsyr         = if_else(stri_detect_fixed(!!reg_disagg_col, "ABCSYR"), "ABCsyr", NA_character_),
         r_azt_3tc        = if_else(stri_detect_fixed(!!reg_disagg_col, "AZT/3TC"), "AZT/3TC", NA_character_),
         r_azt            = if_else(
            stri_detect_fixed(!!reg_disagg_col, "AZT") &
               !stri_detect_fixed(!!reg_disagg_col, "AZT/3TC") &
               !stri_detect_fixed(!!reg_disagg_col, "AZTSYR"),
            "AZT",
            NA_character_
         ),
         r_aztsyr         = if_else(stri_detect_fixed(!!reg_disagg_col, "AZTSYR"), "AZTsyr", NA_character_),
         r_tdf            = if_else(
            stri_detect_fixed(!!reg_disagg_col, "TDF") &
               !stri_detect_fixed(!!reg_disagg_col, "TDF/3TC") &
               !stri_detect_fixed(!!reg_disagg_col, "TDF100MG"),
            "TDF",
            NA_character_
         ),
         r_tdf_3tc        = if_else(
            stri_detect_fixed(!!reg_disagg_col, "TDF/3TC") &
               !stri_detect_fixed(!!reg_disagg_col, "TDF/3TC/EFV") &
               !stri_detect_fixed(!!reg_disagg_col, "TDF/3TC/DTG"),
            "TDF/3TC",
            NA_character_
         ),
         r_tdf_3tc_efv    = if_else(stri_detect_fixed(!!reg_disagg_col, "TDF/3TC/EFV"), "TDF/3TC/EFV", NA_character_),
         r_tdf_3tc_dtg    = if_else(stri_detect_fixed(!!reg_disagg_col, "TDF/3TC/DTG"), "TDF/3TC/DTG", NA_character_),
         r_tdf100         = if_else(stri_detect_fixed(!!reg_disagg_col, "TDF100MG"), "TDF100mg", NA_character_),
         r_xtc            = case_when(
            stri_detect_fixed(!!reg_disagg_col, "3TC") &
               !stri_detect_fixed(!!reg_disagg_col, "/3TC") &
               !stri_detect_fixed(!!reg_disagg_col, "3TCSYR") ~ "3TC",
            stri_detect_fixed(!!reg_disagg_col, "D4T/3TC") ~ "D4T/3TC",
            TRUE ~ NA_character_
         ),
         r_xtcsyr         = if_else(stri_detect_fixed(!!reg_disagg_col, "3TCSYR"), "3TCsyr", NA_character_),
         r_nvp            = if_else(
            stri_detect_fixed(!!reg_disagg_col, "NVP") &
               !stri_detect_fixed(!!reg_disagg_col, "NVPSYR"),
            "NVP",
            NA_character_
         ),
         r_nvpsyr         = if_else(stri_detect_fixed(!!reg_disagg_col, "NVPSYR"), "NVPsyr", NA_character_),
         r_efv            = if_else(
            stri_detect_fixed(!!reg_disagg_col, "EFV") &
               !stri_detect_fixed(!!reg_disagg_col, "/EFV") &
               !stri_detect_fixed(!!reg_disagg_col, "EFV50MG") &
               !stri_detect_fixed(!!reg_disagg_col, "EFV200MG") &
               !stri_detect_fixed(!!reg_disagg_col, "EFVSYR"),
            "EFV",
            NA_character_
         ),
         r_efv50          = if_else(stri_detect_fixed(!!reg_disagg_col, "EFV50MG"), "EFV50mg", NA_character_),
         r_efv200         = if_else(stri_detect_fixed(!!reg_disagg_col, "EFV200MG"), "EFV200mg", NA_character_),
         r_efvsyr         = if_else(stri_detect_fixed(!!reg_disagg_col, "EFVSYR"), "EFVsyr", NA_character_),
         r_dtg            = if_else(
            stri_detect_fixed(!!reg_disagg_col, "DTG") &
               !stri_detect_fixed(!!reg_disagg_col, "/DTG"),
            "DTG",
            NA_character_
         ),
         r_lpvr           = if_else(
            (stri_detect_fixed(!!reg_disagg_col, "LPV/R") |
               stri_detect_fixed(!!reg_disagg_col, "LPVR")) &
               (!stri_detect_fixed(!!reg_disagg_col, "RSYR") &
                  !stri_detect_fixed(!!reg_disagg_col, "R PEDIA")),
            "LPV/r",
            NA_character_
         ),
         r_lpvr_pedia     = if_else(
            stri_detect_fixed(!!reg_disagg_col, "LPV") &
               (stri_detect_fixed(!!reg_disagg_col, "RSYR") |
                  stri_detect_fixed(!!reg_disagg_col, "R PEDIA")),
            "LPV/rsyr",
            NA_character_
         ),
         r_ril            = if_else(stri_detect_fixed(!!reg_disagg_col, "RIL"), "RIL", NA_character_),
         r_ral            = if_else(stri_detect_fixed(!!reg_disagg_col, "RAL"), "RAL", NA_character_),
         r_ftc            = if_else(stri_detect_fixed(!!reg_disagg_col, "FTC"), "FTC", NA_character_),
         r_idv            = if_else(stri_detect_fixed(!!reg_disagg_col, "IDV"), "IDV", NA_character_)
      ) %>%
      unite(
         col   = concat_col,
         sep   = '+',
         na.rm = T,
         starts_with("r_", ignore.case = FALSE)
      ) %>%
      mutate(
         # reg disagg
         !!reg_line_col := case_when(
            !!reg_disagg_col == "AZT/3TC+NVP" ~ 1,
            !!reg_disagg_col == "AZT+3TC+NVP" ~ 1,
            !!reg_disagg_col == "AZT/3TC+EFV" ~ 1,
            !!reg_disagg_col == "AZT+3TC+EFV" ~ 1,
            !!reg_disagg_col == "TDF/3TC/EFV" ~ 1,
            !!reg_disagg_col == "TDF/3TC+EFV" ~ 1,
            !!reg_disagg_col == "TDF+3TC+EFV" ~ 1,
            !!reg_disagg_col == "TDF/3TC+NVP" ~ 1,
            !!reg_disagg_col == "TDF+3TC+NVP" ~ 1,
            !!reg_disagg_col == "ABC+3TC+NVP" ~ 1,
            !!reg_disagg_col == "ABC+3TC+EFV" ~ 1,
            !!reg_disagg_col == "ABC+3TC+DTG" ~ 1,
            !!reg_disagg_col == "ABC+3TC+RTV" ~ 1,
            !!reg_disagg_col == "TDF+3TC+RTV" ~ 1,
            !!reg_disagg_col == "TDF/3TC+RTV" ~ 1,
            !!reg_disagg_col == "AZT/3TC+RTV" ~ 1,
            !!reg_disagg_col == "AZT+3TC+RTV" ~ 1,
            !!reg_disagg_col == "TDF/3TC/DTG" ~ 1,
            !!reg_disagg_col == "TDF/3TC+DTG" ~ 1,
            !!reg_disagg_col == "TDF+3TC+DTG" ~ 1,
            !!reg_disagg_col == "AZT/3TC+LPV/r" ~ 2,
            !!reg_disagg_col == "AZT+3TC+LPV/r" ~ 2,
            !!reg_disagg_col == "TDF/3TC+LPV/r" ~ 2,
            !!reg_disagg_col == "TDF+3TC+LPV/r" ~ 2,
            !!reg_disagg_col == "ABC+3TC+LPV/r" ~ 2,
            !!reg_disagg_col == "AZT+3TC+DTG" ~ 2,
            !!reg_disagg_col == "AZT/3TC+DTG" ~ 2,
            !!reg_disagg_col == "ABC+3TC+LPV/r" ~ 2,
            !!reg_disagg_col == "AZT/3TC+NVPsyr" ~ 2,
            !stri_detect_fixed(!!reg_disagg_col, "syr") & !stri_detect_fixed(!!reg_disagg_col, "pedia") ~ 3,
            stri_detect_fixed(!!reg_disagg_col, "syr") | stri_detect_fixed(!!reg_disagg_col, "pedia") ~ 4
         )
      )
   return(data)
}

get_reg_disagg <- function(data, col) {
   data %<>%
      # retag regimen
      mutate(
         arv_reg = stri_trans_tolower(!!as.name(col)),
         azt1    = if_else(stri_detect_fixed(arv_reg, "azt"), "azt", NA_character_),
         tdf1    = if_else(stri_detect_fixed(arv_reg, "tdf"), "tdf", NA_character_),
         d4t1    = if_else(stri_detect_fixed(arv_reg, "d4t"), "d4t", NA_character_),
         xtc1    = if_else(stri_detect_fixed(arv_reg, "3tc"), "3tc", NA_character_),
         efv1    = if_else(stri_detect_fixed(arv_reg, "efv"), "efv", NA_character_),
         nvp1    = if_else(stri_detect_fixed(arv_reg, "nvp"), "nvp", NA_character_),
         lpvr1   = if_else(stri_detect_fixed(arv_reg, "lpv/r"), "lpv/r", NA_character_),
         lpvr1   = if_else(stri_detect_fixed(arv_reg, "lpvr"), "lpvr", lpvr1),
         ind1    = if_else(stri_detect_fixed(arv_reg, "ind"), "ind", NA_character_),
         ral1    = if_else(stri_detect_fixed(arv_reg, "ral"), "ral", NA_character_),
         abc1    = if_else(stri_detect_fixed(arv_reg, "abc"), "abc", NA_character_),
         ril1    = if_else(stri_detect_fixed(arv_reg, "ril"), "ril", NA_character_),
         dtg1    = if_else(stri_detect_fixed(arv_reg, "dtg"), "dtg", NA_character_),
      ) %>%
      unite(
         col   = 'art_reg',
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
         # old line
         line = case_when(
            art_reg == "tdf 3tc efv" ~ 1,
            art_reg == "tdf 3tc nvp" ~ 1,
            art_reg == "azt 3tc efv" ~ 1,
            art_reg == "azt 3tc nvp" ~ 1,
            art_reg == "d4t 3tc efv" ~ 1,
            art_reg == "d4t 3tc nvp" ~ 1,
            art_reg == "tdf 3tc dtg" ~ 1,
            art_reg == "3tc nvp abc" ~ 1,
            art_reg == "3tc efv abc" ~ 1,
            art_reg == "3tc abc ril" ~ 1,
            art_reg == "tdf 3tc ril" ~ 1,
            art_reg == "azt 3tc ril" ~ 1,
            art_reg == "azt 3tc lpv/r" ~ 2,
            art_reg == "tdf 3tc lpv/r" ~ 2,
            art_reg == "d4t 3tc lpv/r" ~ 2,
            art_reg == "d4t 3tc idv" ~ 2,
            art_reg == "azt 3tc idv" ~ 2,
            art_reg == "tdf 3tc idv" ~ 2,
            art_reg == "3tc lpv/r abc" ~ 2,
            !is.na(art_reg) ~ 3
         ),

         # new line
         line = case_when(
            art_reg == "azt 3tc nvp" ~ 1,
            art_reg == "azt 3tc efv" ~ 1,
            art_reg == "tdf 3tc efv" ~ 1,
            art_reg == "tdf 3tc nvp" ~ 1,
            art_reg == "abc 3tc nvp" ~ 1,
            art_reg == "abc 3tc efv" ~ 1,
            art_reg == "abc 3tc dtg" ~ 1,
            art_reg == "abc 3tc rtv" ~ 1,
            art_reg == "tdf 3tc rtv" ~ 1,
            art_reg == "azt 3tc rtv" ~ 1,
            art_reg == "tdf 3tc dtg" ~ 1,
            art_reg == "azt 3tc lpv/r" ~ 2,
            art_reg == "tdf 3tc lpv/r" ~ 2,
            art_reg == "abc 3tc lpv/r" ~ 2,
            art_reg == "azt 3tc dtg" ~ 2,
            !is.na(art_reg) ~ 3
         ),
      ) %>%
      select(-central_id)
   return(data)
}

get_reg_order <- function(data, col) {
   regimen_col <- as.name(col)

   arvs <- data %>%
      select(art_id, arv_reg = !!regimen_col) %>%
      separate_longer_delim(
         cols  = arv_reg,
         delim = "+"
      ) %>%
      distinct(art_id, arv_reg) %>%
      mutate(
         arv_order = case_when(
            arv_reg == "TDF/3TC/EFV" ~ 1,
            arv_reg == "TDF/3TC/DTG" ~ 2,
            arv_reg == "AZT/3TC/NVP" ~ 3,
            arv_reg == "TDF/3TC" ~ 4,
            arv_reg == "TDF/FTC" ~ 5,
            arv_reg == "AZT/3TC" ~ 6,
            arv_reg == "3TC/d4T" ~ 7,
            arv_reg == "TDF" ~ 8,
            arv_reg == "TDFsyr" ~ 9,
            arv_reg == "AZT" ~ 10,
            arv_reg == "AZTsyr" ~ 11,
            arv_reg == "3TC" ~ 12,
            arv_reg == "3TCsyr" ~ 13,
            arv_reg == "FTC" ~ 14,
            arv_reg == "EFV" ~ 15,
            arv_reg == "EFVsyr" ~ 16,
            arv_reg == "DTG" ~ 17,
            arv_reg == "NVP" ~ 18,
            arv_reg == "NVPsyr" ~ 19,
            arv_reg == "LPV/r" ~ 20,
            arv_reg == "LPV/rsyr" ~ 21,
            arv_reg == "IND" ~ 22,
            arv_reg == "RAL" ~ 23,
            arv_reg == "ABC" ~ 24,
            arv_reg == "ABCsyr" ~ 25,
            arv_reg == "RIL" ~ 26,
            arv_reg == "TAF" ~ 27,
            TRUE ~ 9999
         )
      ) %>%
      filter(arv_reg != "NA") %>%
      arrange(arv_order) %>%
      group_by(art_id) %>%
      summarise(!!regimen_col := stri_c(arv_reg, collapse = "+")) %>%
      ungroup()

   data %<>%
      select(-!!regimen_col) %>%
      left_join(
         y  = arvs,
         by = join_by(art_id)
      )

   return(data)
}

##  Attach additional data from forms ------------------------------------------

get_form_data <- function(data, forms) {
   log_info("Attaching clinical data from latest record.")
   data %<>%
      select(
         -any_of(c(
            "tb_status",
            "oi_syph",
            "oi_hepb",
            "oi_hepc",
            "oi_pcp",
            "oi_cmv",
            "oi_orocand",
            "oi_herpes",
            "oi_other",
            "who_staging"
         ))
      ) %>%
      left_join(
         y  = forms %>%
            select(
               REC_ID,
               who_staging = WHO_CLASS,
               tb_status   = TB_STATUS,
               oi_syph     = OI_SYPH_PRESENT,
               oi_hepb     = OI_HEPB_PRESENT,
               oi_hepc     = OI_HEPC_PRESENT,
               oi_pcp      = OI_PCP_PRESENT,
               oi_cmv      = OI_CMV_PRESENT,
               oi_orocand  = OI_OROCAND_PRESENT,
               oi_herpes   = OI_HERPES_PRESENT,
               oi_other    = OI_OTHER_TEXT
            ) %>%
            distinct_all(),
         by = join_by(REC_ID)
      ) %>%
      mutate_at(
         .vars = vars(
            oi_syph,
            oi_hepb,
            oi_hepc,
            oi_pcp,
            oi_cmv,
            oi_orocand,
            oi_herpes
         ),
         ~as.integer(keep_code(.))
      ) %>%
      mutate(
         tb_status   = remove_code(tolower(tb_status)),
         who_staging = case_when(
            who_staging %in% c("1_I", "I") ~ 1,
            who_staging %in% c("2_II", "II") ~ 2,
            who_staging %in% c("3_III", "III") ~ 3,
            who_staging %in% c("4_IV", "IV") ~ 4,
         )
      )

   return(data)
}

##  Flag data for validation ---------------------------------------------------

get_checks <- function(step_data, new_outcome, new_reg, params, run_checks = NULL) {
   check      <- list()
   run_checks <- ifelse(
      !is.null(run_checks),
      run_checks,
      input(
         prompt  = "Run `tx_curr` validations?",
         options = c("1" = "yes", "2" = "no"),
         default = "1"
      )
   )

   if (run_checks == "1") {
      step_data %<>%
         arrange(real_reg, curr_realhub, curr_realhub_branch, art_id)

      view_vars <- c(
         "REC_ID",
         "CENTRAL_ID",
         "real_reg",
         "curr_realhub",
         "curr_realhub_branch",
         "art_id",
         "idnum",
         "prep_id",
         "mort_id",
         "year",
         "month",
         "confirmatory_code",
         "px_code",
         "uic",
         "first",
         "middle",
         "last",
         "suffix",
         "birthdate",
         "curr_age",
         "sex",
         "initials",
         "philsys_id",
         "philhealth_no",
         "use_db",
         "artstart_date",
         "artstart_rec",
         "oh_artstart",
         "oh_artstart_rec",
         "prev_hub",
         "prev_realhub",
         "prev_realhub_branch",
         "curr_hub",
         "prev_class",
         "prev_ffupdate",
         "prev_nextpickup",
         "prev_regimen",
         "prev_outcome",
         "curr_ffupdate",
         "curr_nextpickup",
         "curr_regimen",
         "lastdisp_hub",
         "lastdisp_ffupdate",
         "lastdisp_nextpickup",
         "lastdisp_regimen",
         "lastdisp_rec",
         "curr_outcome",
         "ref_death_date"
      )
      check     <- check_pii(step_data, check, view_vars, first = first, middle = middle, last = last, birthdate = birthdate, sex = sex)

      # non-negotiable variables
      nonnegotiables <- c("curr_age", "uic")
      check          <- check_nonnegotiables(step_data, check, view_vars, nonnegotiables)

      # special checks
      log_info("Checking for missing dispensing data.")
      check[["no_latest_data"]] <- step_data %>%
         filter(
            if_any(c(curr_ffupdate, curr_nextpickup, curr_regimen, curr_outcome, curr_class, curr_line, curr_realhub), ~is.na(.))
         ) %>%
         filter(
            !(art_id %in% c(21673, 21582, 3124, 539, 3534, 140, 654, 663, 3229, 8906, 10913, 15817, 25716, 3124, 539, 3534, 140, 654, 663, 3229, 8906, 10913, 15817, 25716))
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for missing dispensing data.")
      check[["art_recs_gone"]] <- step_data %>%
         filter(is.na(REC_ID)) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for dispensing later than next pick-up.")
      check[["disp_>_next"]] <- step_data %>%
         filter(
            curr_ffupdate > curr_nextpickup
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for possible PMTCT-N clients.")
      check[["possible_pmtct"]] <- step_data %>%
         filter(
            (curr_num_drugs == 1 & str_detect(curr_regimen, "syr")) |
               curr_age <= 5
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for possible PrEP clients.")
      check[["possible_prep"]] <- step_data %>%
         filter(
            stri_detect_fixed(curr_regimen, "FTC")
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking ART reports tagged as DOH-EB.")
      check[["art_eb"]] <- step_data %>%
         filter(
            curr_realhub == "DOH"
         ) %>%
         select(
            any_of(view_vars),
         )

      # special checks
      log_info("Checking for extreme dispensing.")
      check[["mmd"]] <- step_data %>%
         mutate(
            months_to_pickup = floor(days_to_pickup / 30)
         ) %>%
         filter(
            months_to_pickup >= 7
         ) %>%
         select(
            any_of(view_vars),
            months_to_pickup
         )

      log_info("Checking for mismatch facility.")
      check[["mismatch_faci"]] <- step_data %>%
         filter(
            prev_ffupdate == curr_ffupdate,
            curr_hub != prev_hub
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for resurrected clients.")
      check[["resurrect"]] <- step_data %>%
         filter(
            (prev_outcome == "dead" & (curr_outcome != "dead" | is.na(curr_outcome))) |
               (use_db == 1 &
                  prev_outcome == "dead" &
                  prev_outcome != curr_outcome)
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for mismatch artstart dates.")
      check[["oh_earlier_start"]] <- step_data %>%
         filter(
            artstart_date > oh_artstart
         ) %>%
         mutate(
            start_visit_diffdy = abs(floor(interval(artstart_date, oh_artstart) / days(1))),
            start_visit_diffmo = abs(floor(interval(artstart_date, oh_artstart) / months(1)))
         ) %>%
         select(
            any_of(view_vars),
            start_visit_diffdy,
            start_visit_diffmo
         )

      check[["oh_startrec_diff"]] <- step_data %>%
         filter(
            artstart_rec != oh_artstart_rec | is.na(artstart_rec)
         )

      check[["oh_later_start"]] <- step_data %>%
         filter(
            artstart_date < oh_artstart
         ) %>%
         mutate(
            start_visit_diffdy = abs(floor(interval(artstart_date, oh_artstart) / days(1))),
            start_visit_diffmo = abs(floor(interval(artstart_date, oh_artstart) / months(1)))
         ) %>%
         select(
            any_of(view_vars),
            start_visit_diffdy,
            start_visit_diffmo
         )

      log_info("Checking new mortalities.")
      check[["new_mort"]] <- step_data %>%
         select(
            any_of(view_vars),
         ) %>%
         filter(
            curr_outcome == "dead" & (prev_outcome != "dead" | is.na(prev_outcome))
         )

      log_info("Checking for weird vl date.")
      check[["vl_data"]] <- step_data %>%
         filter(
            curr_vl_date > curr_ffupdate |
               curr_vl_date > params$max |
               year(curr_vl_date) < 2002
         ) %>%
         filter(
            curr_outcome == "alive on arv"
         ) %>%
         select(
            any_of(view_vars),
            curr_vl_date,
            curr_vl_result
         )

      log_info("Checking for no confirmatories.")
      check[["not_confirmed"]] <- step_data %>%
         filter(
            is.na(idnum) | is.na(confirm_result),
            curr_outcome == "alive on arv"
         ) %>%
         select(
            any_of(view_vars),
            confirm_date,
            confirm_result,
            confirm_remarks
         )

      log_info("Checking for non-positives.")
      check[["not_positive"]] <- step_data %>%
         filter(
            confirm_result != "1_Positive"
         ) %>%
         select(
            any_of(view_vars),
            confirm_date,
            confirm_result,
            confirm_remarks
         )

      log_info("Checking for dead not in mortality dataset.")
      check[["dead_not_in_mort"]] <- step_data %>%
         filter(
            is.na(mort_id),
            curr_outcome == "dead"
         ) %>%
         select(
            any_of(view_vars),
         )

      data <- new_outcome %>%
         left_join(
            y  = new_reg %>%
               select(
                  art_id,
                  confirmatory_code,
                  uic,
                  px_code,
                  philhealth_no,
                  philsys_id,
                  first,
                  middle,
                  last,
                  suffix,
                  birthdate
               ),
            by = join_by(art_id)
         ) %>%
         relocate(
            .after = art_id,
            prep_id,
            mort_id,
            confirmatory_code,
            uic,
            px_code,
            philhealth_no,
            philsys_id,
            first,
            middle,
            last,
            suffix,
            birthdate
         ) %>%
         relocate(real_reg, realhub, realhub_branch, .after = art_id) %>%
         arrange(real_reg, realhub)

      # special checks
      log_info("Checking for shift out of TLD.")
      check[["tld to lte"]] <- data %>%
         filter(
            previous_regimen == "TDF/3TC/DTG" & latest_regimen == "TDF/3TC/EFV"
         )

      log_info("Checking for no reg_line.")
      check[["missing reg_line"]] <- data %>%
         filter(
            is.na(reg_line)
         )

      log_info("Checking for line 3.")
      check[["line 3"]] <- data %>%
         filter(
            reg_line == 3,
            onart == 1
         )

      log_info("Checking for line 4.")
      check[["line 4"]] <- data %>%
         filter(
            reg_line == 4,
            onart == 1
         )

      log_info("Checking for 2in1+1 and 1+1+1.")
      check[["2in1+1 and 1+1+1"]] <- data %>%
         filter(
            art_reg %in% c("tdf 3tc efv", "tdf 3tc dtg") & stri_detect_fixed(regimen, "+"),
            onart == 1
         )

      # range-median
      tabstat <- c(
         "curr_age",
         "birthdate",
         "artstart_date",
         "oh_artstart",
         "prev_ffupdate",
         "prev_nextpickup",
         "curr_ffupdate",
         "curr_nextpickup",
         "days_to_pickup",
         "ref_death_date",
         "prev_num_drugs",
         "curr_num_drugs"
      )
      check   <- check_tabstat(step_data, check, tabstat)
   }

   return(check)
}

##  Stata Labels ---------------------------------------------------------------

label_stata <- function(data, stata_labels) {
   labels <- split(stata_labels$lab_def, ~label_name)
   labels <- lapply(labels, function(data) {
      final_labels        <- as.integer(data[["value"]])
      names(final_labels) <- as.character(data[["label"]])
      return(final_labels)
   })

   for (i in seq_len(nrow(stata_labels$lab_val))) {
      var   <- stata_labels$lab_val[i,]$variable
      label <- stata_labels$lab_val[i,]$label_name

      if (var %in% names(data))
         data[[var]] <- labelled(
            data[[var]],
            labels[[label]]
         )
   }

   return(data)
}

##  Output Stata Datasets ------------------------------------------------------

output_dta <- function(official, params, save = "2") {
   if (save == "1") {
      log_info("Checking output directory.")
      version <- format(Sys.time(), "%Y%m%d")
      dir     <- Sys.getenv("HARP_TX")
      check_dir(dir)

      log_info("Saving in Stata data format.")
      period_ext <- str_c(params$yr, "-", stri_pad_left(params$mo, 2, "0"), ".dta")
      files      <- list(
         new_reg            = file.path(dir, str_c(version, "_reg-art_", period_ext)),
         new_outcome        = file.path(dir, str_c(version, "_onart_", period_ext)),
         dropped_notyet     = file.path(dir, str_c(version, "_dropped_notyet_", period_ext)),
         dropped_duplicates = file.path(dir, str_c(version, "_dropped_duplicates_", period_ext))
      )
      for (output in intersect(names(files), names(official))) {
         if (nrow(official[[output]]) > 0) {
            official[[output]] %>%
               format_stata() %>%
               write_dta(files[[output]])

            compress_stata(files[[output]])
         }
      }
   }
}

##  Actual flow ----------------------------------------------------------------

.init <- function(envir = parent.env(environment()), ...) {
   step <- parent.env(environment())
   p    <- envir
   vars <- match.call(expand.dots = FALSE)$`...`

   last_visit <- get_latest_record(p$forms$art_last, p$official$new_reg, p$params)
   last_visit <- get_final_visit(last_visit)
   last_visit <- convert_faci_addr(last_visit)

   last_disp <- get_latest_record(p$forms$art_lastdisp, p$official$new_reg, p$params)
   last_disp <- get_final_visit(last_disp)
   last_disp <- convert_faci_addr(last_disp)

   data <- tag_curr_data(last_visit, p$official$old_outcome, p$forms$art_first, last_disp, p$forms$vl_last, p$params)
   data <- final_conversion(data)

   new_outcome <- finalize_outcomes(data, p$params)
   new_outcome <- finalize_faci(new_outcome)
   new_outcome <- new_outcome %>%
      get_reg_order("latest_regimen") %>%
      reg_disagg("latest_regimen", "regimen", "reg_line") %>%
      get_reg_order("previous_regimen") %>%
      reg_disagg("latest_regimen", "latest_regdisagg", "latest_regline") %>%
      get_reg_disagg("latest_regimen") %>%
      reg_disagg("previous_regimen", "previous_regdisagg", "previous_regline")

   new_outcome <- get_form_data(new_outcome, p$forms$form_art_bc)
   # new_outcome <- label_stata(new_outcome, p$corr$stata_labels)

   step$check <- get_checks(data, new_outcome, p$official$new_reg, p$params, run_checks = vars$run_checks)
   step$data  <- data

   p$official$new_outcome <- new_outcome
   output_dta(p$official, p$params, vars$save)

   flow_validation(p, "tx_curr", p$params$ym, upload = vars$upload)
   log_success("Done.")
}