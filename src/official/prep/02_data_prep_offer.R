##  Filter Initial Data & Remove Already Reported ------------------------------

clean_data <- function(forms, old_reg) {
   log_info("Processing new clients.")
   pii_cols <- c(
      "first",
      "middle",
      "last",
      "suffix",
      "birthdate",
      "uic",
      "philhealth_no",
      "self_ident",
      "self_ident_other",
      "philsys_id",
      "civil_status",
      "nationality",
      "curr_reg",
      "curr_prov",
      "curr_munc",
      "perm_reg",
      "perm_prov",
      "perm_munc",
      "client_mobile",
      "client_email",
      "sex"
   )

   # offer-only clients w/o prep visits, excluding those already reported
   prep_offer <- forms$form_prep %>%
      anti_join(
         y  = forms$prep_first %>%
            select(central_id),
         by = join_by(central_id)
      ) %>%
      anti_join(
         y  = old_reg %>%
            select(central_id),
         by = join_by(central_id)
      )

   data <- forms$prep_first %>%
      anti_join(
         y  = old_reg %>%
            select(central_id),
         by = join_by(central_id)
      ) %>%
      bind_rows(prep_offer) %>%
      mutate_at(
         .vars = vars(first, middle, last, suffix, patient_code, uic, philhealth_no, philsys_id, client_mobile, client_email),
         ~clean_pii(.)
      ) %>%
      clean_dates()

   # one pass: get_latest_pii syncs the local pii cache on every call
   pii <- data %>%
      select(rec_id, central_id, patient_id, all_of(pii_cols)) %>%
      get_latest_pii('central_id', pii_cols) %>%
      distinct(rec_id, .keep_all = TRUE) %>%
      select(-central_id, -patient_id)

   data %<>%
      select(-all_of(pii_cols)) %>%
      left_join(pii, join_by(rec_id)) %>%
      mutate(
         # name
         standard_first  = stri_trans_general(first, "latin-ascii"),
         name            = str_squish(stri_c(last, ", ", first, " ", middle, " ", suffix)),

         # Age
         age             = coalesce(age, age_mo / 12),
         age_dta         = calc_age(birthdate, visit_date),

         # tag those without prep_faci
         use_record_faci = if_else(is.na(service_faci), 1, 0, 0),
         service_faci    = if_else(use_record_faci == 1, faci_id, service_faci),
      )

   return(data)
}

##  Sort by earliest visit of client for the report ----------------------------

prioritize_reports <- function(data) {
   log_info("Using first visited facility.")
   return(pick_visit(data))
}

##  Generate subset variables --------------------------------------------------

standardize_data <- function(initial, params) {
   log_info("Converting to final harp variables.")
   data <- initial %>%
      mutate(
         # generate idnum
         prep_id = params$latest_prep_id + row_number(),

         # report date
         year    = params$yr,
         month   = params$mo,
      ) %>%
      tag_demographics()

   return(data)
}

##  Finalize -------------------------------------------------------------------

final_conversion <- function(data) {
   log_info("Selecting final dataset structure.")
   data %<>%
      # same vars as registry
      select(
         rec_id,
         central_id,
         patient_id,
         prep_id,
         year,
         month,
         prep_form         = form_id,
         px_code           = patient_code,
         uic               = uic,
         first             = first,
         middle            = middle,
         last              = last,
         suffix            = suffix,
         age               = age,
         birthdate         = birthdate,
         sex               = sex,
         philsys_id        = philsys_id,
         philhealth_no     = philhealth_no,
         mobile            = client_mobile,
         email             = client_email,
         initials,
         self_identity,
         self_identity_other,
         gender_identity,
         weight            = weight,
         body_temp         = body_temp,
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
         hts_prep_client,
         hts_prep_offer,
         hts_date,
         prep_hts_date     = prep_hiv_date,
         starts_with("prep_risk_", ignore.case = FALSE),
         starts_with("hts_risk_", ignore.case = FALSE),
         prep_first_time   = first_time,
         prep_first_screen = visit_date,
         prep_first_faci   = prep_faci_code,
         prep_first_branch = prep_branch,
         prep_first_reg    = real_reg,
         prep_first_prov   = real_prov,
         prep_first_munc   = real_munc,
         prep_first_arv    = medicine_summary,
      )

   return(data)
}

##  Append w/ old ART Registry -------------------------------------------------

append_data <- function(old, new) {
   log_info("Appending new to final registry.")
   data <- new %>%
      mutate(
         age = as.integer(age)
      ) %>%
      bind_rows(old) %>%
      arrange(prep_id) %>%
      mutate(
         corr_defer = 0,
      ) %>%
      zap_labels()

   return(data)
}

##  Drop using taggings --------------------------------------------------------

remove_drops <- function(data) {
   log_info("Dropping clients w/ issue from final registry.")
   data %<>%
      filter(corr_defer == 0) %>%
      select(-corr_defer) %>%
      mutate(
         # finalize age data
         age_dta = calc_age(birthdate, prep_first_screen),
         age     = coalesce(age, age_dta),
      ) %>%
      distinct_all()

   return(data)
}

##  Merge w/ Dx Registry -------------------------------------------------------

# lookups are de-duplicated by central_id, so registry rows are never dropped here
merge_dx <- function(data, forms, params) {
   dx <- lw_registry("harp_dx", "idnum", params, forms$id_registry) %>%
      arrange(idnum) %>%
      distinct(central_id, .keep_all = TRUE)

   # get idnum
   data %<>%
      select(-any_of("idnum")) %>%
      left_join(dx, join_by(central_id)) %>%
      relocate(idnum, .after = prep_id)

   return(data)
}

##  Merge w/ Tx Registry -------------------------------------------------------

merge_tx <- function(data, forms, params) {
   tx <- lw_registry("harp_tx", "art_id", params, forms$id_registry) %>%
      arrange(art_id) %>%
      distinct(central_id, .keep_all = TRUE)

   # get art_id
   data %<>%
      select(-any_of("art_id")) %>%
      left_join(tx, join_by(central_id)) %>%
      relocate(art_id, .after = idnum)

   return(data)
}

##  Merge w/ Death Registry ----------------------------------------------------

merge_dead <- function(data, forms, params) {
   dead <- lw_registry("harp_dead", c("mort_id", "year", "month", "date_of_death"), params, forms$id_registry) %>%
      mutate(
         proxy_death_date = as.Date(ceiling_date(as.Date(str_c(sep = '-', year, month, '01')), unit = 'month')) - 1,
         ref_death_date   = if_else(
            condition = is.na(date_of_death),
            true      = proxy_death_date,
            false     = date_of_death
         )
      ) %>%
      select(-proxy_death_date, -year, -month, -date_of_death)

   # get mort_id
   data %<>%
      select(-any_of(c("mort_id", "ref_death_date"))) %>%
      left_join(
         y  = dead %>%
            arrange(mort_id) %>%
            distinct(central_id, .keep_all = TRUE),
         by = join_by(central_id)
      ) %>%
      relocate(mort_id, .after = art_id)

   return(data)
}

##  Flag data for validation ---------------------------------------------------

get_checks <- function(data, params, corr, run_checks = NULL, exclude_drops = NULL) {
   check   <- list()
   answers <- ask_checks("prep_offer", run_checks, exclude_drops)

   if (answers$run == "1") {
      data %<>%
         arrange(region_order(prep_first_reg), prep_first_faci, prep_id)

      view_vars <- c(
         "rec_id",
         "central_id",
         "prep_first_reg",
         "prep_first_faci",
         "prep_first_branch",
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
         "age",
         "prep_first_screen",
         "with_hts",
         "hts_date",
         "hts_result",
         "hts_prep_client",
         "hts_prep_offer",
         "risk_screen",
         "ars_screen",
         "sti_screen",
         "eligible",
         "dispensed"
      )
      check     <- check_pii(data, check, view_vars, first = first, middle = middle, last = last, birthdate = birthdate, sex = sex)

      # non-negotiable variables
      nonnegotiables <- c("age", "uic")
      check          <- check_nonnegotiables(data, check, view_vars, nonnegotiables)
      check          <- check_unknown(data, check, "perm_addr", view_vars, perm_reg, perm_prov, perm_munc)
      check          <- check_unknown(data, check, "curr_addr", view_vars, curr_reg, curr_prov, curr_munc)
      check          <- check_age(data, check, view_vars, birthdate = birthdate, age = age, visit_date = prep_first_screen)

      # special checks
      log_info("Checking for dispensed with no meds.")
      check[["dispensed_no_meds"]] <- data %>%
         filter(
            dispensed == 1,
            is.na(prep_first_arv)
         ) %>%
         select(
            any_of(view_vars),
            prep_first_arv,
         )

      log_info("Checking for hts form screening only.")
      check[["no_prep_form"]] <- data %>%
         mutate(
            keep = case_when(
               is.na(prep_form) ~ 1,
               prep_form %like% "cfbs" ~ 1,
               prep_form %like% "Form A" ~ 1,
               prep_form %like% "HTS Form" ~ 1,
               TRUE ~ 1
            )
         ) %>%
         filter(keep == 1) %>%
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

      log_info("Checking for new clients that are not enrollees.")
      check[["late_report"]] <- data %>%
         filter(
            prep_first_screen < params$min
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for possible PMTCT-N clients.")
      check[["possible_pmtct"]] <- data %>%
         filter(
            (prep_first_num_arv == 1 & stri_detect_fixed(prep_first_arv, "syr")) |
               age <= 5
         ) %>%
         select(
            any_of(view_vars),
            prep_first_arv,
         )

      log_info("Checking for possible ART clients.")
      check[["possible_art"]] <- data %>%
         filter(
            !stri_detect_fixed(prep_first_arv, "ftc")
         ) %>%
         select(
            any_of(view_vars),
            prep_first_arv
         )

      log_info("Checking for young clients.")
      check[["young_prep"]] <- data %>%
         filter(
            age < 15
         ) %>%
         select(
            any_of(view_vars),
            prep_first_arv,
         )
      all_issues            <- combine_validations(data, check, "rec_id") %>%
         arrange(region_order(prep_first_reg), prep_first_faci, rec_id)

      check <- list(all_issues = all_issues)

      # range-median
      tabstat <- c(
         "prep_first_screen",
         "birthdate",
         "age"
      )
      check   <- check_tabstat(data, check, tabstat)

      # Remove already tagged data from validation
      if (answers$exclude == "1")
         check <- exclude_deferred(check, corr)
   }

   return(check)
}

##  Actual flow ----------------------------------------------------------------

.init <- function(envir = parent.env(environment()), ...) {
   step <- parent.env(environment())
   p    <- envir
   vars <- as.list(list(...))

   data <- clean_data(p$forms, p$official$old_reg)
   data <- prioritize_reports(data)
   data <- convert_faci_addr(data, c("real_reg", "real_prov", "real_munc"))
   data <- standardize_data(data, p$params)
   data <- final_conversion(data)

   new_reg <- p$official$old_reg %>%
      append_data(data) %>%
      merge_dx(p$forms, p$params) %>%
      merge_tx(p$forms, p$params) %>%
      merge_dead(p$forms, p$params)

   new_reg    <- tag_fordrop(new_reg, p$corr)
   drops      <- subset_drops(new_reg)
   new_reg    <- remove_drops(new_reg)
   step$dupes <- get_reg_dupes(new_reg)

   step$check <- get_checks(data, p$params, p$corr, run_checks = vars$run_checks, exclude_drops = vars$exclude_drops)
   step$data  <- data

   p$official$new_reg       <- new_reg %>% arrange(prep_id)
   p$official[names(drops)] <- drops

   flow_validation(p, "prep_offer", p$params$ym, upload = vars$upload)
   log_success("Done.")
}