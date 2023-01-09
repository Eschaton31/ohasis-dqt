##  Generate subset variables --------------------------------------------------

# updated outcomes
tag_curr_data <- function(data, prev_outcome, prepdisp_first, params) {
   dead <- ohasis$get_data("harp_dead", ohasis$yr, ohasis$mo) %>%
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
         y  = nhsss$prep$forms$id_registry,
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

   data %<>%
      # prepare screening taggings
      mutate(
         # reinitiation
         prep_reinit_date    = if_else(
            condition = INITIATION_DATE > PREP_START_DATE,
            true      = INITIATION_DATE,
            false     = NA_Date_
         ),

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
         FIRST_TIME          = keep_code(FIRST_TIME),
         FIRST_TIME          = case_when(
            FIRST_TIME == 1 ~ as.integer(1),
            FIRST_TIME == 0 ~ NA_integer_,
            TRUE ~ NA_integer_
         )
      ) %>%
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
      mutate(
         # status as of current report
         prep_status = case_when(
            !is.na(mort_id) ~ "dead",
            PREP_STATUS == 0 ~ "refused",
            PREP_CONTINUED == 0 ~ "discontinued",
            LATEST_NEXT_DATE < params$cutoff_date ~ "ltfu",
            PREP_START_DATE == VISIT_DATE & is.na(MEDICINE_SUMMARY) ~ "for initiation",
            !is.na(PREP_START_DATE) & is.na(MEDICINE_SUMMARY) ~ "no dispense",
            PREP_START_DATE == VISIT_DATE ~ "enrollment",
            !is.na(prep_reinit_date) & VISIT_DATE == prep_reinit_date ~ "reinitiation",
            LATEST_NEXT_DATE >= params$cutoff_date ~ "on prep",
            prep_on == "not screened" ~ "insufficient screening",
            is.na(PREP_START_DATE) & eligible == 0 ~ "ineligible",
         )
      ) %>%
      # get latest outcome data
      left_join(
         y  = prev_outcome %>%
            select(
               prep_id,
               prev_reinit  = prep_reinit_date,
               prev_outcome = outcome,
            ),
         by = "prep_id"
      ) %>%
      mutate(
         use_db         = 1,

         # current age for class
         curr_age       = calc_age(birthdate, LATEST_VISIT),

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

         # check for multi-month clients
         days_to_pickup = abs(as.numeric(difftime(LATEST_NEXT_DATE, LATEST_VISIT, units = "days"))),
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
      generate_gender_identity(SEX, SELF_IDENT, SELF_IDENT_OTHER, gender_identity)

   return(data)
}

##  Finalize -------------------------------------------------------------------

final_conversion <- function(data) {
   data %<>%
      arrange(prep_id) %>%
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
         mobile          = CLIENT_MOBILE,
         email           = CLIENT_EMAIL,
         weight          = WEIGHT,
         body_temp       = FEVER,
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
         prep_hts_date   = PREP_HIV_DATE,
         contains("risk_", ignore.case = FALSE),
         starts_with("kp_", ignore.case = FALSE),
         prepstart_date  = PREP_START_DATE,
         prep_reinit_date,
         starts_with("prev_", ignore.case = FALSE),
         curr_faci       = PREP_FACI_CODE,
         curr_branch     = PREP_BRANCH,
         prep_first_time = FIRST_TIME,
         starts_with("curr_", ignore.case = FALSE),
         curr_ffup       = LATEST_VISIT,
         curr_pickup     = LATEST_NEXT_DATE,
         curr_regimen    = MEDICINE_SUMMARY,
         prep_reg,
         prep_prov,
         prep_munc,
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
      ) %>%
      distinct_all()

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
      view_vars <- get_names(data)

      # non-negotiable variables
      nonnegotiables <- c(
         "curr_faci",
         "curr_outcome",
         "curr_ffup"
      )
      check          <- check_nonnegotiables(data, check, view_vars, nonnegotiables)

      # duplicate id variables
      vars <- c(
         "prep_id",
         "CENTRAL_ID"
      )
      .log_info("Checking if id variables are duplicated.")
      for (var in vars) {
         var                                        <- as.symbol(var)
         check[[paste0("dup_", as.character(var))]] <- data %>%
            filter(
               !is.na(!!var)
            ) %>%
            get_dupes(!!var) %>%
            arrange(curr_faci)
      }

      # unknown data
      vars <- c(
         "prep_reg",
         "prep_prov",
         "prep_munc"
      )
      .log_info("Checking if required variables have UNKNOWN data or unpaired NHSSS versions.")
      for (var in vars) {
         var          <- as.symbol(var)
         check[[var]] <- data %>%
            filter(
               !!var %in% c("UNKNOWN", "OTHERS", NA_character_)
            ) %>%
            arrange(curr_faci)
      }

      # special checks
      .log_info("Checking for extreme dispensing.")
      check[["mmd"]] <- data %>%
         mutate(
            months_to_pickup = floor(days_to_pickup / 30)
         ) %>%
         filter(
            months_to_pickup >= 7
         ) %>%
         arrange(curr_faci)

      .log_info("Checking for no dispensing.")
      check[["no_disp"]] <- data %>%
         filter(
            curr_prep_status == "no dispense"
         ) %>%
         arrange(curr_faci)

      .log_info("Checking for no prep plan.")
      check[["no_plan"]] <- data %>%
         filter(
            !(StrLeft(curr_outcome, 1) %in% c("0", "3")),
            curr_prep_plan == "(no data)"
         ) %>%
         arrange(curr_faci)

      .log_info("Checking for no prep type.")
      check[["no_type"]] <- data %>%
         filter(
            !(StrLeft(curr_outcome, 1) %in% c("0", "3")),
            curr_prep_type == "(no data)"
         ) %>%
         arrange(curr_faci)

      .log_info("Checking for supposedly not on prep but with plan/type.")
      check[["not_but_on_prep"]] <- data %>%
         filter(
            StrLeft(curr_outcome, 1) == "5",
            curr_prep_type != "(no data)" | curr_prep_plan != "(no data)"
         ) %>%
         arrange(curr_faci)

      .log_info("Checking for possible ART.")
      check[["updated_outcome"]] <- data %>%
         filter(
            prev_outcome != curr_outcome
         ) %>%
         arrange(curr_faci)

      .log_info("Checking for possible ART.")
      check[["possible_art"]] <- data %>%
         filter(
            !stri_detect_fixed(curr_regimen, "TDF/FTC")
         ) %>%
         arrange(curr_faci)

      # range-median
      tabstat <- c(
         "birthdate",
         "prepstart_date",
         "curr_ffup",
         "curr_pickup",
         "ref_death_date"
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
                            .GlobalEnv$nhsss$prep$official$old_outcome,
                            .GlobalEnv$nhsss$prep$forms$art_first,
                            .GlobalEnv$nhsss$prep$params) %>%
         final_conversion()

      write_rds(data, file.path(wd, "outcome.converted.RDS"))

      check <- get_checks(data)
   })

   local(envir = .GlobalEnv, flow_validation(nhsss$prep, "outcome.converted", ohasis$ym))
}