##  Filter Initial Data & Remove Already Reported ------------------------------

get_new <- function(forms, old_reg) {
   log_info("Processing new clients.")
   prep_offer <- forms$form_prep %>%
      get_cid(forms$id_registry, PATIENT_ID) %>%
      anti_join(
         y  = forms$prep_first %>%
            select(CENTRAL_ID),
         by = join_by(CENTRAL_ID)
      )

   data <- forms$prep_first %>%
      anti_join(
         y  = old_reg %>%
            select(CENTRAL_ID),
         by = join_by(CENTRAL_ID)
      ) %>%
      bind_rows(prep_offer) %>%
      mutate_at(
         .vars = vars(FIRST, MIDDLE, LAST, SUFFIX, PATIENT_CODE, UIC, PHILHEALTH_NO, PHILSYS_ID, CLIENT_MOBILE, CLIENT_EMAIL),
         ~clean_pii(.)
      ) %>%
      mutate_if(
         .predicate = is.POSIXct,
         ~as.Date(.)
      ) %>%
      mutate_if(
         .predicate = is.Date,
         ~if_else(. <= -25567, NA_Date_, ., .)
      ) %>%
      get_latest_pii(
         "CENTRAL_ID",
         c(
            "FIRST",
            "MIDDLE",
            "LAST",
            "SUFFIX",
            "BIRTHDATE",
            "UIC",
            "PHILHEALTH_NO",
            "SELF_IDENT",
            "SELF_IDENT_OTHER",
            "PHILSYS_ID",
            "CIVIL_STATUS",
            "NATIONALITY",
            "CURR_PSGC_REG",
            "CURR_PSGC_PROV",
            "CURR_PSGC_MUNC",
            "PERM_PSGC_REG",
            "PERM_PSGC_PROV",
            "PERM_PSGC_MUNC",
            "CLIENT_MOBILE",
            "CLIENT_EMAIL"
         )
      ) %>%
      mutate(
         # name
         STANDARD_FIRST  = stri_trans_general(FIRST, "latin-ascii"),
         name            = str_squish(stri_c(LAST, ", ", FIRST, " ", MIDDLE, " ", SUFFIX)),

         # Age
         AGE             = coalesce(AGE, AGE_MO / 12),
         AGE_DTA         = calc_age(BIRTHDATE, VISIT_DATE),

         # tag those without PREP_FACI
         use_record_faci = if_else(is.na(SERVICE_FACI), 1, 0, 0),
         SERVICE_FACI    = if_else(use_record_faci == 1, FACI_ID, SERVICE_FACI),
      )

   return(data)
}

##  Sort by earliest visit of client for the report ----------------------------

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

##  Facilities & Address -------------------------------------------------------

convert_faci_addr <- function(data) {
   log_info("Converting address & facility data.")
   # record faci
   data %<>%
      # prep faci
      ohasis$get_faci(
         list(PREP_FACI_CODE = c("PREP_FACI", "PREP_SUB_FACI")),
         "code",
         c("real_reg", "real_prov", "real_munc")
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
      arrange(PREP_FACI_CODE, VISIT_DATE, LATEST_NEXT_DATE) %>%
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

   return(data)
}

##  Generate subset variables --------------------------------------------------

standardize_data <- function(initial, params) {
   log_info("Converting to final HARP variables.")
   data <- initial %>%
      mutate(
         # generate idnum
         prep_id                   = params$latest_prep_id + row_number(),

         # report date
         year                      = params$yr,
         month                     = params$mo,

         # demographics
         initials                  = str_squish(stri_c(StrLeft(FIRST, 1), StrLeft(MIDDLE, 1), StrLeft(LAST, 1))),
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
      generate_gender_identity(SEX, SELF_IDENT, SELF_IDENT_OTHER, gender_identity)

   return(data)
}

##  Finalize -------------------------------------------------------------------

final_conversion <- function(data) {
   log_info("Selecting final dataset structure.")
   data %<>%
      # same vars as registry
      select(
         REC_ID,
         CENTRAL_ID,
         PATIENT_ID,
         prep_id,
         year,
         month,
         prep_form          = FORM_VERSION,
         px_code            = PATIENT_CODE,
         uic                = UIC,
         first              = FIRST,
         middle             = MIDDLE,
         last               = LAST,
         suffix             = SUFFIX,
         age                = AGE,
         birthdate          = BIRTHDATE,
         sex                = SEX,
         philsys_id         = PHILSYS_ID,
         philhealth_no      = PHILHEALTH_NO,
         mobile             = CLIENT_MOBILE,
         email              = CLIENT_EMAIL,
         initials,
         self_identity,
         self_identity_other,
         gender_identity,
         weight             = WEIGHT,
         body_temp          = FEVER,
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
         hts_prep_client,
         hts_prep_offer,
         hts_date,
         prep_hts_date      = PREP_HIV_DATE,
         starts_with("prep_risk_", ignore.case = FALSE),
         starts_with("hts_risk_", ignore.case = FALSE),
         prep_first_time    = FIRST_TIME,
         prep_first_screen  = VISIT_DATE,
         prep_first_faci    = PREP_FACI_CODE,
         prep_first_branch  = PREP_BRANCH,
         prep_first_reg     = real_reg,
         prep_first_prov    = real_prov,
         prep_first_munc    = real_munc,
         prep_first_arv     = MEDICINE_SUMMARY,
         prep_first_num_arv = NUM_OF_DRUGS,
      )

   return(data)
}

##  Append w/ old ART Registry -------------------------------------------------

append_new <- function(old, new) {
   log_info("Appending new to final registry.")
   data <- new %>%
      bind_rows(old) %>%
      arrange(prep_id) %>%
      mutate(
         drop_notyet     = 0,
         drop_duplicates = 0,
         drop_notprep    = 0,
      ) %>%
      zap_labels()

   return(data)
}

##  Tag data to be reported later on and duplicates for dropping ---------------

tag_fordrop <- function(data, corr) {
   log_info("Tagging enrollees for dropping.")
   for (drop_var in c("drop_notyet", "drop_duplicates"))
      if (drop_var %in% names(corr))
         data %<>%
            left_join(
               y  = corr[[drop_var]] %>%
                  distinct(REC_ID) %>%
                  mutate(drop_this = 1),
               by = join_by(REC_ID)
            ) %>%
            mutate_at(
               .vars = vars(matches(drop_var)),
               ~coalesce(drop_this, .)
            ) %>%
            select(-drop_this)

   return(data)
}

##  Subsets for documentation --------------------------------------------------

subset_drops <- function(data) {
   log_info("Archive those for dropping.")
   drops <- list(
      dropped_notyet  = data %>% filter(drop_notyet == 1),
      dropped_notprep = data %>% filter(drop_notprep == 1)
   )

   return(drops)
}

##  Drop using taggings --------------------------------------------------------

remove_drops <- function(data) {
   log_info("Dropping clients w/ issue from final registry.")
   data %<>%
      mutate(
         drop = drop_duplicates + drop_notyet + drop_notprep,
      ) %>%
      filter(drop == 0) %>%
      select(-drop, -drop_duplicates, -drop_notyet, -drop_notprep) %>%
      mutate(
         # finalize age data
         age_dta = calc_age(birthdate, prep_first_screen),
         age     = coalesce(age, age_dta),
      ) %>%
      distinct_all()

   return(data)
}

##  Merge w/ Dx Registry -------------------------------------------------------

merge_dx <- function(data, forms, params) {
   dx <- hs_data("harp_dx", "reg", params$yr, params$mo) %>%
      read_dta(col_select = c(PATIENT_ID, idnum)) %>%
      get_cid(forms$id_registry, PATIENT_ID) %>%
      select(-PATIENT_ID)

   # get idnum
   data %<>%
      select(-any_of("idnum")) %>%
      left_join(dx, join_by(CENTRAL_ID)) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      relocate(idnum, .after = prep_id)

   return(data)
}

##  Merge w/ Tx Registry -------------------------------------------------------

merge_tx <- function(data, forms, params) {
   tx <- hs_data("harp_tx", "reg", params$yr, params$mo) %>%
      read_dta(col_select = c(PATIENT_ID, art_id)) %>%
      get_cid(forms$id_registry, PATIENT_ID) %>%
      select(-PATIENT_ID)

   # get idnum
   data %<>%
      select(-any_of("art_id")) %>%
      left_join(tx, join_by(CENTRAL_ID)) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      relocate(art_id, .after = idnum)

   return(data)
}

##  Merge w/ Death Registry ----------------------------------------------------

merge_dead <- function(data, forms, params) {
   dead <- hs_data("harp_dead", "reg", params$yr, params$mo) %>%
      read_dta(
         col_select = c(
            PATIENT_ID,
            mort_id,
            year,
            month,
            date_of_death
         )
      ) %>%
      get_cid(forms$id_registry, PATIENT_ID) %>%
      select(-PATIENT_ID) %>%
      mutate(
         proxy_death_date = as.Date(ceiling_date(as.Date(str_c(sep = '-', year, month, '01')), unit = 'month')) - 1,
         ref_death_date   = if_else(
            condition = is.na(date_of_death),
            true      = proxy_death_date,
            false     = date_of_death
         )
      ) %>%
      select(-proxy_death_date, -year, -month, -date_of_death)

   # get idnum
   data %<>%
      select(-any_of(c("mort_id", "ref_death_date"))) %>%
      left_join(
         y  = dead %>%
            distinct(CENTRAL_ID, .keep_all = TRUE),
         by = join_by(CENTRAL_ID)
      ) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      relocate(mort_id, .after = art_id)

   return(data)
}

##  Flag data for validation ---------------------------------------------------

get_checks <- function(data, params, corr, run_checks = NULL, exclude_drops = NULL) {
   check         <- list()
   run_checks    <- ifelse(
      !is.null(run_checks),
      run_checks,
      input(
         prompt  = "Run `tx_new` validations?",
         options = c("1" = "yes", "2" = "no"),
         default = "1"
      )
   )
   exclude_drops <- switch(
      run_checks,
      `1`     = ifelse(!is.null(exclude_drops), exclude_drops, input(
         prompt  = "Exclude clients initially tagged for dropping from validations?",
         options = c("1" = "yes", "2" = "no"),
         default = "1"
      )),
      default = "2"
   )

   if (run_checks == "1") {
      data %<>%
         mutate(
            reg_order = prep_first_reg,
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
         arrange(reg_order, prep_first_faci, prep_id) %>%
         select(-reg_order)

      view_vars <- c(
         "REC_ID",
         "CENTRAL_ID",
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
               prep_form %like% "CFBS" ~ 1,
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
            HTS_REC,
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
            !stri_detect_fixed(prep_first_arv, "FTC")
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

      # range-median
      tabstat <- c(
         "prep_first_screen",
         "birthdate",
         "age"
      )
      check   <- check_tabstat(data, check, tabstat)

      # Remove already tagged data from validation
      if (exclude_drops == "1") {
         for (drop in c("drop_notprep", "drop_notyet")) {
            if (drop %in% names(corr))
               for (check_var in names(check)) {
                  if (check_var != "tabstat")
                     check[[check_var]] %<>%
                        anti_join(
                           y  = corr[[drop]],
                           by = "REC_ID"
                        )
               }
         }
      }
   }

   return(check)
}

##  Actual flow ----------------------------------------------------------------

.init <- function(envir = parent.env(environment()), ...) {
   step <- parent.env(environment())
   p    <- envir
   vars <- as.list(list(...))

   data <- get_new(p$forms, p$official$old_reg)
   data <- get_first_visit(data)
   data <- convert_faci_addr(data)
   data <- standardize_data(data, p$params)
   data <- final_conversion(data)

   new_reg <- p$official$old_reg %>%
      append_new(data) %>%
      merge_dx(p$forms, p$params) %>%
      merge_tx(p$forms, p$params) %>%
      merge_dead(p$forms, p$params)

   new_reg <- tag_fordrop(new_reg, p$corr)
   drops   <- subset_drops(new_reg)
   new_reg <- remove_drops(new_reg) %>%
      select(-prep_first_num_arv)

   step$check <- get_checks(data, p$params, p$corr, run_checks = vars$run_checks, exclude_drops = vars$exclude_drops)
   step$data  <- data

   p$official$new_reg <- new_reg
   append(p$official, drops)

   flow_validation(p, "prep_offer", p$params$ym, upload = vars$upload)
   log_success("Done.")
}