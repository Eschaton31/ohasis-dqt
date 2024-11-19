##  Append to the previous art registry ----------------------------------------

get_records <- function(form_data, new_reg) {
   log_info("Processing latest visit.")
   data <- new_reg %>%
      rename(
         firstscreen_date = prep_first_screen,
         firstscreen_rec  = REC_ID,
      ) %>%
      select(
         -(starts_with("prep") &
            !matches("prep_id")),
         -starts_with("hts"),
         -starts_with("lab"),
         -ends_with("screen"),
         -any_of(
            c(
               "PATIENT_ID",
               "self_identity",
               "self_identity_other",
               "gender_identity",
               "sti_visit",
               "eligible",
               "with_hts",
               "dispensed",
               "curr_reg",
               "curr_prov",
               "curr_munc",
               "perm_reg",
               "perm_prov",
               "perm_munc"
            )
         )
      ) %>%
      left_join(
         y  = form_data,
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
      # get_latest_pii(
      #    "CENTRAL_ID",
      #    c(
      #       "SELF_IDENT",
      #       "SELF_IDENT_OTHER",
      #       "CURR_PSGC_REG",
      #       "CURR_PSGC_PROV",
      #       "CURR_PSGC_MUNC",
      #       "PERM_PSGC_REG",
      #       "PERM_PSGC_PROV",
      #       "PERM_PSGC_MUNC"
      #    )
      # ) %>%
      mutate(
         # Age
         AGE             = coalesce(AGE, AGE_MO / 12),
         AGE_DTA         = calc_age(birthdate, VISIT_DATE),

         # tag those without PREP_FACI
         use_record_faci = if_else(is.na(SERVICE_FACI), 1, 0, 0),
         SERVICE_FACI    = if_else(use_record_faci == 1, FACI_ID, SERVICE_FACI),
      )

   return(data)
}

##  Sort by earliest/latest visit of client for the report ----------------------------

get_final_visit <- function(data) {
   log_info("Using last visited facility.")
   data %<>%
      arrange(desc(VISIT_DATE), desc(LATEST_NEXT_DATE), CENTRAL_ID) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      rename(
         PREP_FACI     = SERVICE_FACI,
         PREP_SUB_FACI = SERVICE_SUB_FACI,
      )

   return(data)
}

get_first_visit <- function(data) {
   log_info("Using first visited facility.")
   data %<>%
      arrange(VISIT_DATE, desc(LATEST_NEXT_DATE), CENTRAL_ID) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      rename(
         PREP_FACI     = SERVICE_FACI,
         PREP_SUB_FACI = SERVICE_SUB_FACI,
      )

   return(data)
}

##  Facilities -----------------------------------------------------------------

convert_faci_addr <- function(data) {
   log_info("Converting address & facility data.")
   # record faci
   data %<>%
      # prep faci
      ohasis$get_faci(
         list(PREP_FACI_CODE = c("PREP_FACI", "PREP_SUB_FACI")),
         "code",
         c("prep_reg", "prep_prov", "prep_munc")
      ) %>%
      mutate(
         PREP_BRANCH = PREP_FACI_CODE,
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
            ~if_else(nchar(.) > 3, ., NA_character_)
         )
      ) %>%
      ohasis$get_addr(
         c(
            perm_reg  = "PERM_PSGC_REG",
            perm_prov = "PERM_PSGC_PROV",
            perm_munc = "PERM_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      ohasis$get_addr(
         c(
            curr_reg  = "CURR_PSGC_REG",
            curr_prov = "CURR_PSGC_PROV",
            curr_munc = "CURR_PSGC_MUNC"
         ),
         "nhsss"
      )

   data %<>%
      mutate_at(
         .vars = vars(ends_with("_FACI_CODE", ignore.case = FALSE)),
         ~case_when(
            str_detect(., "^TLY") ~ "TLY",
            str_detect(., "^SHIP") ~ "SHP",
            str_detect(., "^HASH") ~ "HASH",
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
               pull(data, str_replace(cur_column(), "_BRANCH", "_FACI_CODE")) == "HASH" & is.na(.) ~ "HASH-QC",
               TRUE ~ .
            )
         )
      ) %>%
      arrange(PREP_FACI_CODE, VISIT_DATE, LATEST_NEXT_DATE)

   return(data)
}

##  Generate subset variables --------------------------------------------------

# updated outcomes
tag_curr_data <- function(data, prev_outcome, prep_first, prepdisp_first, prep_last, prep_disc, prep_reinit, params) {
   log_info("Converting to final HARP variables.")
   data %<>%
      # get latest outcome data
      left_join(
         y  = prev_outcome %>%
            select(
               prep_id,
               prepstart_date,
               prev_rec        = REC_ID,
               prev_reinit     = prep_reinit_date,
               prev_prep_plan  = prep_plan,
               prev_prep_type  = prep_type,
               prev_outcome    = outcome,
               prev_ffupdate   = latest_ffupdate,
               prev_nextpickup = latest_nextpickup,
               prev_regimen    = latest_regimen,
            ),
         by = join_by(prep_id)
      ) %>%
      left_join(
         y  = prep_reinit %>%
            select(
               CENTRAL_ID,
               INITIATION_DATE
            ),
         by = join_by(CENTRAL_ID)
      ) %>%
      # prepare screening taggings
      mutate(
         # reinitiation
         prep_reinit_date          = if_else(
            condition = INITIATION_DATE > prepstart_date,
            true      = INITIATION_DATE,
            false     = NA_Date_
         ),

         # demographics
         initials                  = str_squish(stri_c(str_left(FIRST, 1), str_left(MIDDLE, 1), str_left(LAST, 1))),
         SEX                       = remove_code(stri_trans_toupper(SEX)),
         self_identity             = remove_code(stri_trans_toupper(SELF_IDENT)),
         self_identity_other       = toupper(SELF_IDENT_OTHER),
         self_identity             = remove_code(stri_trans_toupper(SELF_IDENT)),
         self_identity             = case_when(
            self_identity == "OTHER" ~ "OTHERS",
            self_identity == "MAN" ~ "MALE",
            self_identity == "WOMAN" ~ "FEMALE",
            self_identity == "MALE" ~ "MALE",
            self_identity == "FEMALE" ~ "FEMALE",
            TRUE ~ self_identity
         ),
         self_identity_other       = stri_trans_toupper(SELF_IDENT_OTHER),
         self_identity_other_sieve = str_replace_all(self_identity_other, "[^[:alnum:]]", ""),

         FIRST_TIME                = as.integer(keep_code(FIRST_TIME)),
         FIRST_TIME                = case_when(
            FIRST_TIME == 1 ~ FIRST_TIME,
            FIRST_TIME == 0 ~ NA_integer_,
            TRUE ~ NA_integer_
         )
      ) %>%
      # get ohasis latest dispense
      left_join(
         y  = prep_last %>%
            select(
               CENTRAL_ID,
               LASTDISP_REC    = REC_ID,
               LASTDISP_HUB    = PREP_FACI_CODE,
               LASTDISP_BRANCH = PREP_BRANCH,
               LASTDISP_REC    = REC_ID,
               LASTDISP_VISIT  = VISIT_DATE,
            ) %>%
            distinct(CENTRAL_ID, .keep_all = TRUE),
         by = "CENTRAL_ID"
      ) %>%
      # get ohasis latest discontinue
      left_join(
         y  = prep_disc %>%
            select(
               CENTRAL_ID,
               LASTDISC_REC    = REC_ID,
               LASTDISC_HUB    = PREP_FACI_CODE,
               LASTDISC_BRANCH = PREP_BRANCH,
               LASTDISC_REC    = REC_ID,
               LASTDISC_VISIT  = VISIT_DATE,
            ) %>%
            distinct(CENTRAL_ID, .keep_all = TRUE),
         by = "CENTRAL_ID"
      ) %>%
      mutate(
         # status as of current report
         prep_status = case_when(
            # !is.na(mort_id) ~ "dead",
            PREP_STATUS == 0 ~ "refused",
            PREP_CONTINUED == 0 ~ "discontinued",
            LASTDISC_VISIT > VISIT_DATE ~ "discontinued",
            LATEST_NEXT_DATE >= -25567 & LATEST_NEXT_DATE < params$min ~ "ltfu",
            prepstart_date == VISIT_DATE & is.na(MEDICINE_SUMMARY) ~ "for initiation",
            !is.na(prepstart_date) & is.na(MEDICINE_SUMMARY) ~ "no dispense",
            prepstart_date == VISIT_DATE ~ "enrollment",
            !is.na(prep_reinit_date) & VISIT_DATE == prep_reinit_date ~ "reinitiation",
            LATEST_NEXT_DATE >= params$min ~ "on prep",
            prep_on == "not screened" ~ "insufficient screening",
            is.na(prepstart_date) & eligible == 0 ~ "ineligible",
         )
      ) %>%
      # get ohasis earliest visits
      left_join(
         y  = prepdisp_first %>%
            select(
               CENTRAL_ID,
               EARLIESTDISP_REC  = REC_ID,
               EARLIESTDISP_DATE = VISIT_DATE
            ) %>%
            arrange(EARLIESTDISP_DATE) %>%
            distinct(CENTRAL_ID, .keep_all = TRUE),
         by = "CENTRAL_ID"
      ) %>%
      left_join(
         y  = prep_first %>%
            select(
               CENTRAL_ID,
               EARLIEST_REC  = REC_ID,
               EARLIEST_DATE = VISIT_DATE,
            ),
         by = join_by(CENTRAL_ID)
      ) %>%
      mutate(
         use_db         = 1,

         prepstart_rec  = if ("prepstart_rec" %in% names(.)) coalesce(prepstart_rec, EARLIESTDISP_REC) else EARLIESTDISP_DATE,
         prepstart_date = coalesce(prepstart_date, EARLIESTDISP_DATE),

         # current age for class
         curr_age       = calc_age(birthdate, VISIT_DATE),

         # current outcome
         curr_outcome   = case_when(
            prep_status == "on prep" ~ "1_on prep",
            prep_status == "enrollment" ~ "1_on prep",
            prep_status == "reinitiation" ~ "1_on prep",
            prep_status == "ltfu" ~ "2_ltfu",
            prep_status == "discontinued" ~ "3_discontinued",
            prep_status == "refused" ~ "4_refused",
            prep_status == "ineligible" ~ "0_ineligible",
            TRUE ~ "5_not on prep"
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
         days_to_pickup = abs(as.numeric(difftime(LATEST_NEXT_DATE, VISIT_DATE, units = "days"))),
         arv_worth      = case_when(
            days_to_pickup == 0 ~ '0_No ARVs',
            days_to_pickup > 0 & days_to_pickup < 90 ~ '1_<3 months worth of ARVs',
            days_to_pickup >= 90 &
               days_to_pickup < 180 ~ '2_3-5 months worth of ARVs',
            days_to_pickup >= 180 & days_to_pickup <= 365.25 ~ '3_6-12 months worth of ARVs',
            days_to_pickup > 365.25 ~ '4_More than 1 yr worth of ARVs',
            TRUE ~ '5_(no data)'
         ),
      ) %>%
      generate_gender_identity(SEX, SELF_IDENT, SELF_IDENT_OTHER, gender_identity) %>%
      rename_at(
         .vars = vars(
            prep_plan,
            prep_type,
            prep_status
         ),
         ~paste0("curr_", .)
      ) %>%
      rename_at(
         .vars = vars(starts_with("KP_", ignore.case = FALSE)),
         ~tolower(.)
      ) %>%
      # same vars as registry
      select(
         REC_ID,
         CENTRAL_ID,
         PATIENT_ID,
         prep_id,
         idnum,
         art_id,
         mort_id,
         year,
         month,
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
         initials,
         self_identity,
         self_identity_other,
         gender_identity,
         mobile,
         email,
         weight              = WEIGHT,
         body_temp           = BODY_TEMP,
         perm_reg,
         perm_prov,
         perm_munc,
         curr_reg,
         curr_prov,
         curr_munc,
         risk_screen,
         ars_screen,
         sti_screen,
         sti_visit,
         starts_with("lab_", ignore.case = FALSE),
         clin_screen,
         dispensed,
         eligible,
         with_hts,
         HTS_REC,
         hts_form,
         hts_modality,
         hts_result,
         hts_date,
         prep_hts_date       = PREP_HIV_DATE,
         contains("risk_", ignore.case = FALSE),
         starts_with("kp_", ignore.case = FALSE),
         # add prepstart_rec
         prepstart_date,
         oh_prepstart_rec    = EARLIESTDISP_REC,
         oh_prepstart        = EARLIESTDISP_DATE,
         firstscreen_rec,
         firstscreen_date,
         oh_firstscreen_rec  = EARLIEST_REC,
         oh_firstscreen_date = EARLIEST_DATE,
         prep_reinit_date,
         starts_with("prev_", ignore.case = FALSE),
         curr_faci           = PREP_FACI_CODE,
         curr_branch         = PREP_BRANCH,
         prep_first_time     = FIRST_TIME,
         starts_with("curr_", ignore.case = FALSE),
         curr_ffupdate       = VISIT_DATE,
         curr_nextpickup     = LATEST_NEXT_DATE,
         curr_regimen        = MEDICINE_SUMMARY,
         lastdisp_rec        = LASTDISP_REC,
         lastvisit_faci      = LASTDISP_HUB,
         lastvisit_branch    = LASTDISP_BRANCH,
         lastvisit_date      = LASTDISP_VISIT,
         lastvisit_rec       = LASTDISP_REC,
         lastdisc_rec        = LASTDISC_REC,
         lastdisc_faci       = LASTDISC_HUB,
         lastdisc_branch     = LASTDISC_BRANCH,
         lastdisc_ffup       = LASTDISC_VISIT,
         lastdisc_rec        = LASTDISC_REC,
         prep_reg,
         prep_prov,
         prep_munc,
         days_to_pickup,
         arv_worth,
         ref_death_date,
      ) %>%
      distinct_all()

   return(data)
}

# first outcomes
tag_first_data <- function(data) {
   log_info("Converting to final HARP variables.")
   data %<>%
      # prepare screening taggings
      mutate(
         # demographics
         initials                  = str_squish(stri_c(str_left(FIRST, 1), str_left(MIDDLE, 1), str_left(LAST, 1))),
         SEX                       = remove_code(stri_trans_toupper(SEX)),
         self_identity             = remove_code(stri_trans_toupper(SELF_IDENT)),
         self_identity_other       = toupper(SELF_IDENT_OTHER),
         self_identity             = remove_code(stri_trans_toupper(SELF_IDENT)),
         self_identity             = case_when(
            self_identity == "OTHER" ~ "OTHERS",
            self_identity == "MAN" ~ "MALE",
            self_identity == "WOMAN" ~ "FEMALE",
            self_identity == "MALE" ~ "MALE",
            self_identity == "FEMALE" ~ "FEMALE",
            TRUE ~ self_identity
         ),
         self_identity_other       = stri_trans_toupper(SELF_IDENT_OTHER),
         self_identity_other_sieve = str_replace_all(self_identity_other, "[^[:alnum:]]", ""),

         FIRST_TIME                = as.integer(keep_code(FIRST_TIME)),
         FIRST_TIME                = case_when(
            FIRST_TIME == 1 ~ FIRST_TIME,
            FIRST_TIME == 0 ~ NA_integer_,
            TRUE ~ NA_integer_
         )
      ) %>%
      mutate(
         use_db         = 1,

         prepstart_rec  = REC_ID,
         prepstart_date = VISIT_DATE,

         # current age for class
         prepstart_age  = calc_age(birthdate, VISIT_DATE),
      ) %>%
      generate_gender_identity(SEX, SELF_IDENT, SELF_IDENT_OTHER, gender_identity) %>%
      rename_at(
         .vars = vars(
            prep_plan,
            prep_type
         ),
         ~paste0("curr_", .)
      ) %>%
      rename_at(
         .vars = vars(starts_with("KP_", ignore.case = FALSE)),
         ~tolower(.)
      ) %>%
      # same vars as registry
      select(
         REC_ID,
         CENTRAL_ID,
         PATIENT_ID,
         prep_id,
         idnum,
         art_id,
         mort_id,
         year,
         month,
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
         initials,
         self_identity,
         self_identity_other,
         gender_identity,
         mobile,
         email,
         weight               = WEIGHT,
         body_temp            = BODY_TEMP,
         perm_reg,
         perm_prov,
         perm_munc,
         curr_reg,
         curr_prov,
         curr_munc,
         risk_screen,
         ars_screen,
         sti_screen,
         sti_visit,
         starts_with("lab_", ignore.case = FALSE),
         clin_screen,
         dispensed,
         eligible,
         with_hts,
         HTS_REC,
         hts_form,
         hts_modality,
         hts_result,
         hts_date,
         prep_hts_date        = PREP_HIV_DATE,
         contains("risk_", ignore.case = FALSE),
         starts_with("kp_", ignore.case = FALSE),
         # add prepstart_rec
         prepstart_date,
         prep_first_time      = FIRST_TIME,
         starts_with("curr_", ignore.case = FALSE),
         prepstart_faci       = PREP_FACI_CODE,
         prepstart_branch     = PREP_BRANCH,
         prepstart_ffupdate   = VISIT_DATE,
         prepstart_nextpickup = LATEST_NEXT_DATE,
         prepstart_regimen    = MEDICINE_SUMMARY,
         prepstart_reg        = prep_reg,
         prepstart_prov       = prep_prov,
         prepstart_munc       = prep_munc,
      ) %>%
      distinct_all()

   return(data)
}

##  Append w/ old Registry -----------------------------------------------------

finalize_outcomes <- function(data, params) {
   log_info("Finalizing PrEP outcomes.")
   data %<>%
      arrange(prep_id) %>%
      mutate(
         curr_outcome     = case_when(
            prev_outcome == "2_ltfu" & curr_outcome == "5_not on prep" ~ "2_ltfu",
            TRUE ~ curr_outcome
         ),
         newonprep        = if_else(
            condition = prepstart_date %within% interval(params$min, params$max),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         onprep           = if_else(
            condition = curr_outcome == "1_on prep",
            true      = 1,
            false     = 0,
            missing   = 0
         ),

         reinit_diff      = interval(prev_reinit, prep_reinit_date) %/% months(1),
         prep_reinit_date = case_when(
            prep_reinit_date > prev_reinit & (reinit_diff > 0 & reinit_diff <= 3) ~ prev_reinit,
            prep_reinit_date < prev_reinit & (reinit_diff > 3) ~ prep_reinit_date,
            TRUE ~ prep_reinit_date
         )
      )
   return(data)
}

finalize_faci <- function(data) {
   data %<>%
      select(
         -ends_with("_reg"),
         -ends_with("_prov"),
         -ends_with("_munc"),
      ) %>%
      rename(
         faci   = curr_faci,
         branch = curr_branch,
      ) %>%
      mutate(
         branch = case_when(
            faci == "HASH" & branch == "HASH" ~ "HASH-QC",
            faci == "HASH" & is.na(branch) ~ "HASH-QC",
            faci == "TLY" & is.na(branch) ~ "TLY-ANGLO",
            TRUE ~ branch
         ),
      ) %>%
      mutate_at(
         .vars = vars(faci),
         ~case_when(
            stri_detect_regex(., "^HASH") ~ "HASH",
            stri_detect_regex(., "^SAIL") ~ "SAIL",
            stri_detect_regex(., "^TLY") ~ "TLY",
            TRUE ~ .
         )
      ) %>%
      mutate(
         branch = case_when(
            faci == "HASH" & is.na(branch) ~ "HASH-QC",
            faci == "TLY" & is.na(branch) ~ "TLY-ANGLO",
            faci == "SHP" & is.na(branch) ~ "SHIP-MAKATI",
            TRUE ~ branch
         ),
      ) %>%
      faci_code_to_id(
         ohasis$ref_faci_code %>% distinct(FACI_CODE, SUB_FACI_CODE, .keep_all = TRUE),
         c(FACI_ID = "faci", SUB_FACI_ID = "branch")
      ) %>%
      left_join(
         y  = ohasis$ref_faci %>%
            select(
               FACI_ID,
               SUB_FACI_ID,
               prep_reg  = FACI_NHSSS_REG,
               prep_prov = FACI_NHSSS_PROV,
               prep_munc = FACI_NHSSS_MUNC,
            ),
         by = join_by(FACI_ID, SUB_FACI_ID)
      ) %>%
      select(-FACI_ID, -SUB_FACI_ID) %>%
      select(
         -starts_with("prev_", ignore.case = FALSE),
         -any_of("age")
      ) %>%
      rename(
         latest_ffupdate   = curr_ffupdate,
         latest_nextpickup = curr_nextpickup,
         latest_regimen    = curr_regimen,
      ) %>%
      rename_at(
         .var = vars(starts_with("curr_")),
         ~stri_replace_all_regex(., "^curr_", "")
      ) %>%
      distinct_all() %>%
      arrange(prep_id) %>%
      mutate(central_id = CENTRAL_ID)

   return(data)
}

##  Flag data for validation ---------------------------------------------------

get_checks <- function(data, run_checks = NULL) {
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
      data %<>%
         mutate(
            reg_order = prep_reg,
            reg_order = case_when(
               reg_order == "1" ~ 1,
               reg_order == "2" ~ 2,
               reg_order == "CAR" ~ 3,
               reg_order == "3" ~ 4,
               reg_order == "NCR" ~ 5,
               reg_order == "4A" ~ 6,
               reg_order == "4B" ~ 7,
               reg_order == "5" ~ 8,
               reg_order == "6" ~ 9,
               reg_order == "7" ~ 10,
               reg_order == "8" ~ 11,
               reg_order == "9" ~ 12,
               reg_order == "10" ~ 13,
               reg_order == "11" ~ 14,
               reg_order == "12" ~ 15,
               reg_order == "CARAGA" ~ 16,
               reg_order == "ARMM" ~ 17,
               reg_order == "BARMM" ~ 17,
               TRUE ~ 9999
            ),
         ) %>%
         arrange(reg_order, curr_faci, curr_branch, prep_id) %>%
         select(-reg_order)

      view_vars <- c(
         "REC_ID",
         "CENTRAL_ID",
         "prep_reg",
         "curr_faci",
         "curr_branch",
         "prep_form",
         "hts_form",
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
         "initials",
         "philsys_id",
         "philhealth_no",
         "mobile",
         "email",
         "prepstart_date",
         "prepstart_rec",
         "oh_prepstart",
         "oh_prepstart_rec",
         "with_hts",
         "risk_screen",
         "ars_screen",
         "sti_screen",
         "eligible",
         "dispensed",
         "curr_ffupdate",
         "curr_nextpickup",
         "curr_regimen",
         "curr_prep_plan",
         "curr_prep_type",
         "prev_ffupdate",
         "prev_nextpickup",
         "prev_regimen",
         "prev_prep_plan",
         "prev_prep_type"
      )
      # check     <- check_pii(data, check, view_vars, first = first, middle = middle, last = last, birthdate = birthdate, sex = sex)

      # non-negotiable variables
      # nonnegotiables <- c("curr_age", "uic")
      # check          <- check_nonnegotiables(data, check, view_vars, nonnegotiables)

      # special checks
      log_info("Checking for missing dispensing data.")
      check[["no_latest_data"]] <- data %>%
         filter(
            (if_any(c(curr_ffupdate, curr_nextpickup, curr_regimen), ~is.na(.)) & !is.na(prepstart_date)) | is.na(curr_faci),
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for missing dispensing data.")
      check[["prep_recs_gone"]] <- data %>%
         filter(is.na(REC_ID)) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for dispensing later than next pick-up.")
      check[["disp_>_next"]] <- data %>%
         filter(
            curr_ffupdate > curr_nextpickup
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for no hts form.")
      check[["no_hts"]] <- data %>%
         filter(
            is.na(with_hts)
         ) %>%
         select(
            any_of(view_vars)
         )

      log_info("Checking for reactive.")
      check[["hts_reactive"]] <- data %>%
         filter(
            hts_result == "R"
         ) %>%
         select(
            any_of(view_vars),
            HTS_REC,
            hts_result
         )

      log_info("Checking for incomplete prep info.")
      check[["inc_prep"]] <- data %>%
         filter(
            dispensed == 1,
            curr_prep_type == "(no data)" | curr_prep_plan == "(no data)"
         ) %>%
         select(
            any_of(view_vars)
         )

      log_info("Checking for possible PMTCT-N clients.")
      check[["possible_pmtct"]] <- data %>%
         filter(
            (curr_num_drugs == 1 & str_detect(curr_regimen, "syr")) |
               curr_age <= 5
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for possible ART clients.")
      check[["possible_art"]] <- data %>%
         filter(
            !stri_detect_fixed(curr_regimen, "FTC")
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for young clients.")
      check[["young_prep"]] <- data %>%
         filter(
            curr_age < 15
         ) %>%
         select(
            any_of(view_vars),
         )

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
         )

      log_info("Checking for no dispensing.")
      check[["no_disp"]] <- data %>%
         filter(
            curr_prep_status == "no dispense"
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for no prep plan.")
      check[["no_plan"]] <- data %>%
         filter(
            !(str_left(curr_outcome, 1) %in% c("0", "3")) | !is.na(prepstart_date),
            curr_prep_plan == "(no data)"
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for no prep type.")
      check[["no_type"]] <- data %>%
         filter(
            !(str_left(curr_outcome, 1) %in% c("0", "3")) | !is.na(prepstart_date),
            curr_prep_type == "(no data)"
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for supposedly not on prep but with plan/type.")
      check[["not_but_on_prep"]] <- data %>%
         filter(
            str_left(curr_outcome, 1) == "5",
            curr_prep_type != "(no data)" | curr_prep_plan != "(no data)"
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for updated outcomes.")
      check[["updated_outcome"]] <- data %>%
         filter(
            prev_outcome != curr_outcome
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for mismatch prepstart dates.")
      check[["oh_earlier_start"]] <- data %>%
         filter(
            prepstart_date > oh_prepstart
         ) %>%
         mutate(
            start_visit_diffdy = abs(floor(interval(prepstart_date, oh_prepstart) / days(1))),
            start_visit_diffmo = abs(floor(interval(prepstart_date, oh_prepstart) / months(1)))
         ) %>%
         select(
            any_of(view_vars),
            start_visit_diffdy,
            start_visit_diffmo
         )

      check[["oh_later_start"]] <- data %>%
         filter(
            prepstart_date < oh_prepstart
         ) %>%
         mutate(
            start_visit_diffdy = abs(floor(interval(prepstart_date, oh_prepstart) / days(1))),
            start_visit_diffmo = abs(floor(interval(prepstart_date, oh_prepstart) / months(1)))
         ) %>%
         select(
            any_of(view_vars),
            start_visit_diffdy,
            start_visit_diffmo
         )

      check[["oh_never_prep"]] <- data %>%
         filter(
            !is.na(prepstart_date) & is.na(oh_prepstart)
         ) %>%
         select(
            any_of(view_vars),
         )

      all_issues <- combine_validations(data, check, "prep_id") %>%
         mutate(
            reg_order = prep_reg,
            reg_order = case_when(
               reg_order == "1" ~ 1,
               reg_order == "2" ~ 2,
               reg_order == "CAR" ~ 3,
               reg_order == "3" ~ 4,
               reg_order == "NCR" ~ 5,
               reg_order == "4A" ~ 6,
               reg_order == "4B" ~ 7,
               reg_order == "5" ~ 8,
               reg_order == "6" ~ 9,
               reg_order == "7" ~ 10,
               reg_order == "8" ~ 11,
               reg_order == "9" ~ 12,
               reg_order == "10" ~ 13,
               reg_order == "11" ~ 14,
               reg_order == "12" ~ 15,
               reg_order == "CARAGA" ~ 16,
               reg_order == "ARMM" ~ 17,
               reg_order == "BARMM" ~ 17,
               TRUE ~ 9999
            ),
         ) %>%
         arrange(reg_order, curr_faci, curr_branch, prep_id) %>%
         select(-reg_order)

      check <- list(all_issues = all_issues)

      # range-median
      tabstat <- c(
         "curr_ffupdate",
         "curr_nextpickup",
         "birthdate",
         "curr_age"
      )
      check   <- check_tabstat(data, check, tabstat)
   }
   return(check)
}

##  Output Stata Datasets ------------------------------------------------------

output_dta <- function(official, params, save = "2") {
   if (save == "1") {
      log_info("Checking output directory.")
      version <- format(Sys.time(), "%Y%m%d")
      dir     <- Sys.getenv("PREP")
      check_dir(dir)

      log_info("Saving in Stata data format.")
      period_ext <- str_c(params$yr, "-", stri_pad_left(params$mo, 2, "0"), ".dta")
      files      <- list(
         new_reg            = file.path(dir, str_c(version, "_reg-prep_", period_ext)),
         new_outcome        = file.path(dir, str_c(version, "_onprep_", period_ext)),
         prepstart          = file.path(dir, str_c(version, "_prepstart_", period_ext)),
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

      flow_dta(official$new_reg, "prep", "reg", params$yr, params$mo)
      flow_dta(official$new_outcome %>%
                  select(-any_of("central_id")), "prep", "outcome", params$yr, params$mo)
   }
}

##  Actual flow ----------------------------------------------------------------

.init <- function(envir = parent.env(environment()), ...) {
   step <- parent.env(environment())
   p    <- envir
   vars <- as.list(list(...))

   first_disp <- get_records(p$forms$prepdisp_first, p$official$new_reg)
   first_disp <- get_final_visit(first_disp)
   first_disp <- convert_faci_addr(first_disp)

   last_disp <- get_records(p$forms$prepdisp_last, p$official$new_reg)
   last_disp <- get_final_visit(last_disp)
   last_disp <- convert_faci_addr(last_disp)

   last_disc <- get_records(p$forms$prepdisc_last, p$official$new_reg)
   last_disc <- get_final_visit(last_disc)
   last_disc <- convert_faci_addr(last_disc)

   last_visit <- p$forms$prep_last %>%
      bind_rows(
         p$forms$form_prep %>%
            get_cid(p$forms$id_registry, PATIENT_ID) %>%
            anti_join(select(p$forms$prep_last, CENTRAL_ID), join_by(CENTRAL_ID))
      )
   last_visit <- get_records(last_visit, p$official$new_reg)
   last_visit <- get_final_visit(last_visit)
   last_visit <- convert_faci_addr(last_visit)

   data <- tag_curr_data(last_visit, p$official$old_outcome, p$forms$prep_first, p$forms$prepdisp_first, last_disp, last_disc, p$forms$prep_init_p12m, p$params)

   new_outcome <- finalize_outcomes(data, p$params)
   new_outcome <- finalize_faci(new_outcome)

   step$check <- get_checks(data, run_checks = vars$run_checks)
   step$data  <- data

   p$official$new_outcome <- new_outcome %>% arrange(prep_id)
   p$official$prepstart   <- tag_first_data(first_disp) %>% arrange(prep_id)
   output_dta(p$official, p$params, vars$save)

   flow_validation(p, "prep_curr", p$params$ym, upload = vars$upload)
   log_success("Done.")
}