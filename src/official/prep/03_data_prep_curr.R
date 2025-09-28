##  Append to the previous art registry ----------------------------------------

get_records <- function(form_data, new_reg) {
   log_info("Processing latest visit.")
   remove_cols <- names(new_reg)
   remove_cols <- remove_cols[remove_cols != 'central_id']
   remove_cols <- remove_cols[remove_cols != 'rec_id']
   remove_cols <- remove_cols[!grepl('^prep', remove_cols)]
   remove_cols <- remove_cols[!grepl('^hts', remove_cols)]
   remove_cols <- remove_cols[!grepl('^lab', remove_cols)]
   remove_cols <- remove_cols[!grepl('screen$', remove_cols)]
   remove_cols <- remove_cols[remove_cols != 'sti_visit']
   remove_cols <- remove_cols[remove_cols != 'eligible']
   remove_cols <- remove_cols[remove_cols != 'with_hts']
   remove_cols <- remove_cols[remove_cols != 'dispensed']
   remove_cols <- remove_cols[remove_cols != 'age']

   data <- new_reg %>%
      rename(
         firstscreen_date = prep_first_screen,
         firstscreen_rec  = rec_id,
      ) %>%
      select(
         -(starts_with("prep") &
            !matches("prep_id")),
         -starts_with("hts"),
         -starts_with("lab"),
         -ends_with("screen"),
         -any_of(
            c(
               "self_identity",
               "self_identity_other",
               "gender_identity",
               "sti_visit",
               "eligible",
               "with_hts",
               "dispensed",
               "age"
            )
         ),
      ) %>%
      left_join(
         y  = form_data %>%
            select(-any_of(remove_cols)),
         by = join_by(central_id)
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
      #    "central_id",
      #    c(
      #       "self_ident",
      #       "self_ident_other",
      #       "curr_reg",
      #       "curr_prov",
      #       "curr_munc",
      #       "perm_reg",
      #       "perm_prov",
      #       "perm_munc"
      #    )
      # ) %>%
      mutate(
         # Age
         age             = coalesce(age, age_mo / 12),
         age_dta         = calc_age(birthdate, visit_date),

         # tag those without prep_faci
         use_record_faci = if_else(is.na(service_faci), 1, 0, 0),
         service_faci    = if_else(use_record_faci == 1, faci_id, service_faci),
      )

   return(data)
}

##  Sort by earliest/latest visit of client for the report ----------------------------

get_final_visit <- function(data) {
   log_info("Using last visited facility.")
   data %<>%
      arrange(desc(visit_date), desc(latest_next_date), central_id) %>%
      distinct(central_id, .keep_all = TRUE) %>%
      rename(
         prep_faci     = service_faci,
         prep_sub_faci = service_sub_faci,
      )

   return(data)
}

get_first_visit <- function(data) {
   log_info("Using first visited facility.")
   data %<>%
      arrange(visit_date, desc(latest_next_date), central_id) %>%
      distinct(central_id, .keep_all = TRUE) %>%
      rename(
         prep_faci     = service_faci,
         prep_sub_faci = service_sub_faci,
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
         list(prep_faci_code = c("prep_faci", "prep_sub_faci")),
         "code",
         c("prep_reg", "prep_prov", "prep_munc")
      ) %>%
      mutate(
         prep_branch = prep_faci_code,
      ) %>%
      mutate(
         across(
            names(select(., ends_with("_branch", ignore.case = FALSE))),
            ~if_else(nchar(.) > 3, ., NA_character_)
         )
      ) %>%
      mutate(
         across(
            names(select(., ends_with("_branch", ignore.case = FALSE))),
            ~if_else(nchar(.) > 3, ., NA_character_)
         )
      ) %>%
      get_addr(
         c(
            perm_reg  = "perm_reg",
            perm_prov = "perm_prov",
            perm_munc = "perm_munc"
         ),
         "nhsss"
      ) %>%
      get_addr(
         c(
            curr_reg  = "curr_reg",
            curr_prov = "curr_prov",
            curr_munc = "curr_munc"
         ),
         "nhsss"
      )

   data %<>%
      mutate_at(
         .vars = vars(ends_with("_faci_code", ignore.case = FALSE)),
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
            names(select(., ends_with("_branch", ignore.case = FALSE))),
            ~case_when(
               pull(data, str_replace(cur_column(), "_branch", "_faci_code")) == "TLY" & is.na(.) ~ "TLY-ANGLO",
               pull(data, str_replace(cur_column(), "_branch", "_faci_code")) == "SHP" & is.na(.) ~ "SHIP-MAKATI",
               pull(data, str_replace(cur_column(), "_branch", "_faci_code")) == "HASH" & is.na(.) ~ "HASH-QC",
               TRUE ~ .
            )
         )
      ) %>%
      arrange(prep_faci_code, visit_date, latest_next_date)

   return(data)
}

##  Generate subset variables --------------------------------------------------

# updated outcomes
tag_curr_data <- function(data, prev_outcome, prep_first, prepdisp_first, prep_last, prep_disc, prep_reinit, params) {
   log_info("Converting to final harp variables.")
   data %<>%
      # get latest outcome data
      left_join(
         y  = prev_outcome %>%
            select(
               prep_id,
               prepstart_date,
               prev_rec        = rec_id,
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
      # left_join(
      #    y  = prep_reinit %>%
      #       select(
      #          central_id,
      #          initiation_date
      #       ),
      #    by = join_by(central_id)
      # ) %>%
      # prepare screening taggings
      mutate(
         # reinitiation
         # prep_reinit_date          = if_else(
         #    condition = initiation_date > prepstart_date,
         #    true      = initiation_date,
         #    false     = NA_Date_
         # ),
         prep_reinit_date          = NA_Date_,

         # demographics
         initials                  = str_squish(stri_c(str_left(first, 1), str_left(middle, 1), str_left(last, 1))),
         sex                       = remove_code(stri_trans_toupper(sex)),
         self_identity             = remove_code(stri_trans_toupper(self_ident)),
         self_identity_other       = toupper(self_ident_other),
         self_identity             = remove_code(stri_trans_toupper(self_ident)),
         self_identity             = case_when(
            self_identity == "OTHER" ~ "OTHERS",
            self_identity == "MAN" ~ "MALE",
            self_identity == "WOMAN" ~ "FEMALE",
            self_identity == "MALE" ~ "MALE",
            self_identity == "FEMALE" ~ "FEMALE",
            TRUE ~ self_identity
         ),
         self_identity_other       = stri_trans_toupper(self_ident_other),
         self_identity_other_sieve = str_replace_all(self_identity_other, "[^[:alnum:]]", ""),

         first_time                = as.integer(keep_code(first_time)),
         first_time                = case_when(
            first_time == 1 ~ first_time,
            first_time == 0 ~ NA_integer_,
            TRUE ~ NA_integer_
         )
      ) %>%
      # get ohasis latest dispense
      left_join(
         y  = prep_last %>%
            select(
               central_id,
               lastdisp_rec    = rec_id,
               lastdisp_hub    = prep_faci_code,
               lastdisp_branch = prep_branch,
               lastdisp_rec    = rec_id,
               lastdisp_visit  = visit_date,
            ) %>%
            distinct(central_id, .keep_all = TRUE),
         by = "central_id"
      ) %>%
      # get ohasis latest discontinue
      left_join(
         y  = prep_disc %>%
            select(
               central_id,
               lastdisc_rec    = rec_id,
               lastdisc_hub    = prep_faci_code,
               lastdisc_branch = prep_branch,
               lastdisc_rec    = rec_id,
               lastdisc_visit  = visit_date,
            ) %>%
            distinct(central_id, .keep_all = TRUE),
         by = "central_id"
      ) %>%
      mutate(
         # status as of current report
         prep_status = case_when(
            # !is.na(mort_id) ~ "dead",
            prep_status == 0 ~ "refused",
            prep_continued == 0 ~ "discontinued",
            lastdisc_visit > visit_date ~ "discontinued",
            latest_next_date >= -25567 & latest_next_date < params$min ~ "ltfu",
            prepstart_date == visit_date & is.na(medicine_summary) ~ "for initiation",
            !is.na(prepstart_date) & is.na(medicine_summary) ~ "no dispense",
            prepstart_date == visit_date ~ "enrollment",
            !is.na(prep_reinit_date) & visit_date == prep_reinit_date ~ "reinitiation",
            latest_next_date >= params$min ~ "on prep",
            prep_on == "not screened" ~ "insufficient screening",
            is.na(prepstart_date) & eligible == 0 ~ "ineligible",
         )
      ) %>%
      # get ohasis earliest visits
      left_join(
         y  = prepdisp_first %>%
            select(
               central_id,
               earliestdisp_rec  = rec_id,
               earliestdisp_date = visit_date
            ) %>%
            arrange(earliestdisp_date) %>%
            distinct(central_id, .keep_all = TRUE),
         by = "central_id"
      ) %>%
      left_join(
         y  = prep_first %>%
            select(
               central_id,
               earliest_rec  = rec_id,
               earliest_date = visit_date,
            ),
         by = join_by(central_id)
      ) %>%
      mutate(
         use_db         = 1,

         prepstart_rec  = if ("prepstart_rec" %in% names(.)) coalesce(prepstart_rec, earliestdisp_rec) else earliestdisp_date,
         prepstart_date = coalesce(prepstart_date, earliestdisp_date),

         # current age for class
         curr_age       = calc_age(birthdate, visit_date),

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
            condition = !is.na(medicine_summary),
            true      = stri_count_fixed(medicine_summary, "+") + 1,
            false     = 0
         ),

         # check for multi-month clients
         days_to_pickup = abs(as.numeric(difftime(latest_next_date, visit_date, units = "days"))),
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
      generate_gender_identity(sex, self_ident, self_ident_other, gender_identity) %>%
      rename_at(
         .vars = vars(
            prep_plan,
            prep_type,
            prep_status
         ),
         ~paste0("curr_", .)
      ) %>%
      rename_at(
         .vars = vars(starts_with("kp_", ignore.case = FALSE)),
         ~tolower(.)
      ) %>%
      # same vars as registry
      select(
         rec_id,
         central_id,
         patient_id,
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
         weight              = weight,
         body_temp           = body_temp,
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
         hts_rec,
         hts_form,
         hts_modality,
         hts_result,
         hts_date,
         prep_hts_date       = prep_hiv_date,
         contains("risk_", ignore.case = FALSE),
         starts_with("kp_", ignore.case = FALSE),
         # add prepstart_rec
         prepstart_date,
         oh_prepstart_rec    = earliestdisp_rec,
         oh_prepstart        = earliestdisp_date,
         firstscreen_rec,
         firstscreen_date,
         oh_firstscreen_rec  = earliest_rec,
         oh_firstscreen_date = earliest_date,
         prep_reinit_date,
         starts_with("prev_", ignore.case = FALSE),
         curr_faci           = prep_faci_code,
         curr_branch         = prep_branch,
         prep_first_time     = first_time,
         starts_with("curr_", ignore.case = FALSE),
         curr_ffupdate       = visit_date,
         curr_nextpickup     = latest_next_date,
         curr_regimen        = medicine_summary,
         lastdisp_rec        = lastdisp_rec,
         lastvisit_faci      = lastdisp_hub,
         lastvisit_branch    = lastdisp_branch,
         lastvisit_date      = lastdisp_visit,
         lastvisit_rec       = lastdisp_rec,
         lastdisc_rec        = lastdisc_rec,
         lastdisc_faci       = lastdisc_hub,
         lastdisc_branch     = lastdisc_branch,
         lastdisc_ffup       = lastdisc_visit,
         lastdisc_rec        = lastdisc_rec,
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
   log_info("Converting to final harp variables.")
   data %<>%
      # prepare screening taggings
      mutate(
         # demographics
         initials                  = str_squish(stri_c(str_left(first, 1), str_left(middle, 1), str_left(last, 1))),
         sex                       = remove_code(stri_trans_toupper(sex)),
         self_identity             = remove_code(stri_trans_toupper(self_ident)),
         self_identity_other       = toupper(self_ident_other),
         self_identity             = remove_code(stri_trans_toupper(self_ident)),
         self_identity             = case_when(
            self_identity == "OTHER" ~ "OTHERS",
            self_identity == "MAN" ~ "MALE",
            self_identity == "WOMAN" ~ "FEMALE",
            self_identity == "MALE" ~ "MALE",
            self_identity == "FEMALE" ~ "FEMALE",
            TRUE ~ self_identity
         ),
         self_identity_other       = stri_trans_toupper(self_ident_other),
         self_identity_other_sieve = str_replace_all(self_identity_other, "[^[:alnum:]]", ""),

         first_time                = as.integer(keep_code(first_time)),
         first_time                = case_when(
            first_time == 1 ~ first_time,
            first_time == 0 ~ NA_integer_,
            TRUE ~ NA_integer_
         )
      ) %>%
      mutate(
         use_db         = 1,

         prepstart_rec  = rec_id,
         prepstart_date = visit_date,

         # current age for class
         prepstart_age  = calc_age(birthdate, visit_date),
      ) %>%
      generate_gender_identity(sex, self_ident, self_ident_other, gender_identity) %>%
      rename_at(
         .vars = vars(
            prep_plan,
            prep_type
         ),
         ~paste0("curr_", .)
      ) %>%
      rename_at(
         .vars = vars(starts_with("kp_", ignore.case = FALSE)),
         ~tolower(.)
      ) %>%
      # same vars as registry
      select(
         rec_id,
         central_id,
         patient_id,
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
         weight               = weight,
         body_temp            = body_temp,
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
         hts_rec,
         hts_form,
         hts_modality,
         hts_result,
         hts_date,
         prep_hts_date        = prep_hiv_date,
         contains("risk_", ignore.case = FALSE),
         starts_with("kp_", ignore.case = FALSE),
         # add prepstart_rec
         prepstart_date,
         prep_first_time      = first_time,
         starts_with("curr_", ignore.case = FALSE),
         prepstart_faci       = prep_faci_code,
         prepstart_branch     = prep_branch,
         prepstart_ffupdate   = visit_date,
         prepstart_nextpickup = latest_next_date,
         prepstart_regimen    = medicine_summary,
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
         ohasis$ref_faci_code %>% distinct(faci_code, sub_faci_code, .keep_all = TRUE),
         c(faci_id = "faci", sub_faci_id = "branch")
      ) %>%
      left_join(
         y  = ohasis$ref_faci %>%
            select(
               faci_id,
               sub_faci_id,
               prep_reg  = addr_nhsss_reg,
               prep_prov = addr_nhsss_prov,
               prep_munc = addr_nhsss_munc,
            ),
         by = join_by(faci_id, sub_faci_id)
      ) %>%
      select(-faci_id, -sub_faci_id) %>%
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
      mutate(central_id = central_id)

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
               reg_order == "car" ~ 3,
               reg_order == "3" ~ 4,
               reg_order == "NCR" ~ 5,
               reg_order == "4a" ~ 6,
               reg_order == "4b" ~ 7,
               reg_order == "5" ~ 8,
               reg_order == "6" ~ 9,
               reg_order == "7" ~ 10,
               reg_order == "8" ~ 11,
               reg_order == "9" ~ 12,
               reg_order == "10" ~ 13,
               reg_order == "11" ~ 14,
               reg_order == "12" ~ 15,
               reg_order == "caraga" ~ 16,
               reg_order == "armm" ~ 17,
               reg_order == "barmm" ~ 17,
               TRUE ~ 9999
            ),
         ) %>%
         arrange(reg_order, curr_faci, curr_branch, prep_id) %>%
         select(-reg_order)

      view_vars <- c(
         "rec_id",
         "central_id",
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
         filter(is.na(rec_id)) %>%
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
            hts_rec,
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
            !stri_detect_fixed(curr_regimen, "ftc")
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
               reg_order == "car" ~ 3,
               reg_order == "3" ~ 4,
               reg_order == "NCR" ~ 5,
               reg_order == "4a" ~ 6,
               reg_order == "4b" ~ 7,
               reg_order == "5" ~ 8,
               reg_order == "6" ~ 9,
               reg_order == "7" ~ 10,
               reg_order == "8" ~ 11,
               reg_order == "9" ~ 12,
               reg_order == "10" ~ 13,
               reg_order == "11" ~ 14,
               reg_order == "12" ~ 15,
               reg_order == "caraga" ~ 16,
               reg_order == "armm" ~ 17,
               reg_order == "barmm" ~ 17,
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
      dir     <- Sys.getenv("prep")
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
               select(
                  -contains("."),
               ) %>%
               write_dta(files[[output]])

            # compress_stata(files[[output]])
         }
      }

      flow_dta(official$new_reg, "prep", "reg", params$yr, params$mo)
      flow_dta(official$new_outcome, "prep", "outcome", params$yr, params$mo)
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
            get_cid(p$forms$id_registry, patient_id) %>%
            anti_join(select(p$forms$prep_last, central_id), join_by(central_id))
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