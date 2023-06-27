##  Generate subset variables --------------------------------------------------

# updated outcomes
tag_curr_data <- function(data, prev_outcome, art_first, last_disp, params) {
   dead <- hs_data("harp_dead", "reg", ohasis$yr, ohasis$mo) %>%
      read_dta(
         col_select = c(
            PATIENT_ID,
            mort_id,
            date_of_death,
            year,
            month
         )
      ) %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      ) %>%
      left_join(
         y  = nhsss$harp_tx$forms$id_registry,
         by = "PATIENT_ID"
      ) %>%
      mutate(
         CENTRAL_ID       = if_else(
            condition = is.na(CENTRAL_ID),
            true      = PATIENT_ID,
            false     = CENTRAL_ID
         ),
         proxy_death_date = as.Date(ceiling_date(as.Date(paste(sep = '-', year, month, '01')), unit = 'month')) - 1,
         ref_death_date   = if_else(
            condition = is.na(date_of_death),
            true      = proxy_death_date,
            false     = date_of_death
         )
      )

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
         use_db              = case_when(
            is.na(prev_outcome) &
               !is.na(MEDICINE_SUMMARY) &
               !is.na(LATEST_NEXT_DATE) ~ 1,
            is.na(prev_outcome) &
               !is.na(LASTDISP_ARV) &
               !is.na(LASTDISP_NEXT_DATE) ~ 2,
            LATEST_VISIT > prev_ffup &
               !is.na(MEDICINE_SUMMARY) &
               !is.na(LATEST_NEXT_DATE) ~ 1,
            LATEST_NEXT_DATE > prev_pickup &
               !is.na(MEDICINE_SUMMARY) &
               !is.na(LATEST_NEXT_DATE) ~ 1,
            LASTDISP_VISIT > prev_ffup &
               !is.na(LASTDISP_ARV) &
               !is.na(LATEST_NEXT_DATE) ~ 2,
            LASTDISP_NEXT_DATE > prev_pickup &
               !is.na(LASTDISP_ARV) &
               !is.na(LASTDISP_NEXT_DATE) ~ 2,
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
      prompt  = "Run `converted` validations?",
      options = c("1" = "yes", "2" = "no"),
      default = "1"
   )
   update <- substr(toupper(update), 1, 1)
   if (update == "1") {
      # initialize checking layer
      view_vars <- c(
         "REC_ID",
         "CENTRAL_ID",
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
         "curr_realhub",
         "curr_realhub_branch",
         "prev_class",
         "prev_ffup",
         "prev_pickup",
         "prev_regimen",
         "prev_outcome",
         "curr_ffup",
         "curr_pickup",
         "curr_regimen",
         "lastdisp_hub",
         "lastdisp_ffup",
         "lastdisp_pickup",
         "lastdisp_regimen",
         "lastdisp_rec",
         "curr_outcome",
         "ref_death_date"
      )

      # non-negotiable variables
      nonnegotiables <- c(
         "curr_age",
         "curr_hub",
         "curr_class",
         "curr_outcome",
         "curr_line"
      )
      check          <- check_nonnegotiables(data, check, view_vars, nonnegotiables)

      vars <- c(
         "curr_regimen",
         "curr_ffup",
         "curr_pickup",
         "artstart_date",
         "oh_artstart"
      )
      log_info("Checking if ffup variables are missing.")
      for (var in vars) {
         var          <- as.symbol(var)
         check[[var]] <- data %>%
            filter(
               is.na(!!var),
               !(art_id %in% c(21673, 21582, 3124, 539, 3534, 140, 654, 663, 3229, 8906, 10913, 15817, 25716, 3124, 539, 3534, 140, 654, 663, 3229, 8906, 10913, 15817, 25716))
            ) %>%
            arrange(curr_realhub)
      }

      # duplicate id variables
      vars <- c(
         "art_id",
         "idnum",
         "CENTRAL_ID"
      )
      log_info("Checking if id variables are duplicated.")
      for (var in vars) {
         var                                        <- as.symbol(var)
         check[[paste0("dup_", as.character(var))]] <- data %>%
            select(
               any_of(view_vars),
            ) %>%
            filter(
               !is.na(!!var),
            ) %>%
            get_dupes(!!var) %>%
            arrange(curr_hub)
      }

      # unknown data
      vars <- c(
         "tx_reg",
         "tx_prov",
         "tx_munc"
      )
      log_info("Checking if required variables have UNKNOWN data or unpaired NHSSS versions.")
      for (var in vars) {
         var          <- as.symbol(var)
         check[[var]] <- data %>%
            filter(
               !!var %in% c("UNKNOWN", "OTHERS", NA_character_),
               !(art_id %in% c(21673, 21582, 3124, 539, 3534, 140, 654, 663, 3229, 8906, 10913, 15817, 25716, 3124, 539, 3534, 140, 654, 663, 3229, 8906, 10913, 15817, 25716))
            ) %>%
            select(
               any_of(view_vars),
               tx_reg,
               tx_prov,
               tx_munc,
            ) %>%
            arrange(curr_hub)
      }

      # special checks
      log_info("Checking for extreme dispensing.")
      check[["mmd"]] <- data %>%
         mutate(
            months_to_pickup = floor(days_to_pickup / 30)
         ) %>%
         filter(
            months_to_pickup >= 7
         ) %>%
         select(
            any_of(view_vars),
            months_to_pickup
         ) %>%
         arrange(curr_hub)

      log_info("Checking for extreme dispensing.")
      check[["mismatch_faci"]] <- data %>%
         filter(
            prev_ffup == curr_ffup,
            curr_hub != prev_hub
         ) %>%
         select(
            any_of(view_vars),
         ) %>%
         arrange(curr_hub)

      log_info("Checking for resurrected clients.")
      check[["resurrect"]] <- data %>%
         filter(
            (prev_outcome == "dead" & (curr_outcome != "dead" | is.na(curr_outcome))) |
               (use_db == 1 &
                  prev_outcome == "dead" &
                  prev_outcome != curr_outcome)
         ) %>%
         select(
            any_of(view_vars),
         ) %>%
         arrange(curr_hub)

      log_info("Checking for mismatch artstart dates.")
      check[["oh_earlier_start"]] <- data %>%
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
         ) %>%
         arrange(curr_hub)

      check[["oh_startrec_diff"]] <- data %>%
         filter(
            artstart_rec != oh_artstart_rec | is.na(artstart_rec)
         ) %>%
         arrange(curr_hub)

      check[["oh_later_start"]] <- data %>%
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
         ) %>%
         arrange(curr_hub)

      log_info("Checking new mortalities.")
      check[["new_mort"]] <- data %>%
         select(
            any_of(view_vars),
         ) %>%
         filter(
            curr_outcome == "dead" & (prev_outcome != "dead" | is.na(prev_outcome))
         ) %>%
         arrange(curr_hub)

      # range-median
      tabstat <- c(
         "curr_age",
         "birthdate",
         "artstart_date",
         "oh_artstart",
         "prev_ffup",
         "prev_pickup",
         "curr_ffup",
         "curr_pickup",
         "days_to_pickup",
         "ref_death_date",
         "prev_num_drugs",
         "curr_num_drugs"
      )
      check   <- check_tabstat(data, check, tabstat)
   }
   return(check)
}

##  Actual flow ----------------------------------------------------------------

.init <- function() {
   p <- parent.env(environment())
   local(envir = p, {
      data <- read_rds(file.path(wd, "outcome.initial.RDS"))
      data <- tag_curr_data(data,
                            .GlobalEnv$nhsss$harp_tx$official$old_outcome,
                            .GlobalEnv$nhsss$harp_tx$forms$art_first,
                            .GlobalEnv$nhsss$harp_tx$forms$art_lastdisp,
                            .GlobalEnv$nhsss$harp_tx$params) %>%
         final_conversion()

      write_rds(data, file.path(wd, "outcome.converted.RDS"))

      check <- get_checks(data)
   })

   local(envir = .GlobalEnv, flow_validation(nhsss$harp_tx, "outcome.converted", ohasis$ym))
}