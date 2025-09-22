##  Filter Initial Data & Remove Already Reported ------------------------------

get_enrollees <- function(art_first, old_reg, params) {
   log_info("Processing enrollees.")
   data <- art_first %>%
      anti_join(
         y  = old_reg %>%
            select(central_id),
         by = join_by(central_id)
      ) %>%
      mutate_at(
         .vars = vars(first, middle, last, suffix, confirmatory_code, patient_code, uic, philhealth_no, philsys_id),
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
         "central_id",
         c(
            "first",
            "middle",
            "last",
            "suffix",
            "birthdate",
            "sex",
            "uic",
            "philhealth_no",
            "self_ident",
            "self_ident_other",
            "philsys_id",
            "curr_reg",
            "curr_prov",
            "curr_munc",
            "client_mobile",
            "client_email"
         )
      ) %>%
      mutate(
         # name
         standard_first     = stri_trans_general(first, "latin-ascii"),
         name               = str_squish(stri_c(last, ", ", first, " ", middle, " ", suffix)),

         # Age
         age                = coalesce(age, age_mo / 12),
         age_dta            = calc_age(birthdate, visit_date),

         # tag those without art_faci
         use_record_faci    = if_else(is.na(service_faci), 1, 0, 0),
         service_faci       = if_else(use_record_faci == 1, faci_id, service_faci),

         # convert to harp facility
         actual_faci        = service_faci,
         actual_sub_faci    = service_sub_faci,

         # tag special clinics
         special_clinic     = case_when(
            service_faci %in% params$clinics$tly ~ "tly",
            service_faci %in% params$clinics$sail ~ "sail",
            TRUE ~ NA_character_
         ),
         service_faci       = case_when(
            special_clinic == "TLY" ~ "130001",
            special_clinic == "SAIL" ~ "130025",
            TRUE ~ service_faci
         ),

         # satellite
         satellite_faci     = if_else(
            condition = str_left(client_type, 1) == "5",
            true      = faci_disp,
            false     = NA_character_
         ),
         satellite_sub_faci = if_else(
            condition = str_left(client_type, 1) == "5",
            true      = sub_faci_disp,
            false     = NA_character_
         ),

         # transient
         transient_faci     = if_else(
            condition = str_left(client_type, 1) == "6",
            true      = faci_disp,
            false     = NA_character_
         ),
         transient_sub_faci = if_else(
            condition = str_left(client_type, 1) == "6",
            true      = sub_faci_disp,
            false     = NA_character_
         ),
      )

   return(data)
}

##  Sort by earliest visit of client for the report ----------------------------

get_first_visit <- function(data) {
   log_info("Using first visited facility.")
   data %<>%
      arrange(visit_date, desc(latest_next_date), central_id) %>%
      distinct(central_id, .keep_all = TRUE) %>%
      rename(
         art_faci     = service_faci,
         art_sub_faci = service_sub_faci,
      )

   return(data)
}

##  Adding CD4 results ---------------------------------------------------------

get_cd4 <- function(data, lab_cd4) {
   log_info("Attaching baseline cd4.")
   data %<>%
      # get cd4 data
      left_join(
         y  = lab_cd4 %>%
            select(
               cd4_date,
               cd4_result,
               central_id
            ),
         by = join_by(central_id)
      ) %>%
      mutate(
         # calculate distance from confirmatory date
         cd4_date     = as.Date(cd4_date),
         cd4_enroll   = interval(cd4_date, visit_date) / days(1),

         # baseline is within 182 days
         baseline_cd4 = if_else(
            cd4_enroll >= -182 & cd4_enroll <= 182,
            1,
            0
         ),

         # make values absolute to take date nearest to confirmatory
         cd4_enroll   = abs(cd4_enroll),
      ) %>%
      arrange(central_id, cd4_enroll) %>%
      distinct(central_id, .keep_all = TRUE)

   return(data)
}

##  Facilities & Address -------------------------------------------------------

convert_faci_addr <- function(data) {
   log_info("Converting address & facility data.")
   # record faci
   data %<>%
      ohasis$get_faci(
         list(faci_code = c("faci_id", "sub_faci_id")),
         "code"
      ) %>%
      # art faci
      ohasis$get_faci(
         list(art_faci_code = c("art_faci", "art_sub_faci")),
         "code",
         c("tx_reg", "tx_prov", "tx_munc")
      ) %>%
      # epic / gf faci
      ohasis$get_faci(
         list(actual_faci_code = c("actual_faci", "actual_sub_faci")),
         "code",
         c("real_reg", "real_prov", "real_munc")
      ) %>%
      # satellite
      ohasis$get_faci(
         list(satellite_faci_code = c("satellite_faci", "satellite_sub_faci")),
         "code"
      ) %>%
      # satellite
      ohasis$get_faci(
         list(transient_faci_code = c("transient_faci", "transient_sub_faci")),
         "code"
      ) %>%
      mutate(
         art_branch    = art_faci_code,
         actual_branch = actual_faci_code,
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
      )

   data %<>%
      mutate_at(
         .vars = vars(ends_with("_faci_code", ignore.case = FALSE)),
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
               pull(data, str_replace(cur_column(), "_branch", "_faci_code")) == "TLY" & is.na(.) ~ "TLY-ANGLO",
               pull(data, str_replace(cur_column(), "_branch", "_faci_code")) == "SHP" & is.na(.) ~ "SHIP-MAKATI",
               TRUE ~ .
            )
         )
      ) %>%
      mutate(
         art_branch = case_when(
            special_clinic == "SAIL" ~ actual_branch,
            special_clinic == "TLY" & is.na(actual_branch) ~ "TLY-ANGLO",
            special_clinic == "TLY" & actual_branch == "TLY" ~ "TLY-ANGLO",
            special_clinic == "TLY" & actual_branch != "TLY" ~ actual_branch,
            TRUE ~ art_branch
         ),
      ) %>%
      arrange(art_faci_code, visit_date, latest_next_date) %>%
      get_addr(
         c(
            curr_reg  = "curr_reg",
            curr_prov = "curr_prov",
            curr_munc = "curr_munc"
         ),
         "nhsss"
      )

   return(data)
}

##  Generate subset variables --------------------------------------------------

standardize_data <- function(initial, params) {
   log_info("Converting to final harp variables.")
   data <- initial %>%
      mutate(
         # generate idnum
         art_id            = params$latest_art_id + row_number(),

         # report date
         year              = params$yr,
         month             = params$mo,

         # demographics
         initials          = str_squish(stri_c(str_left(first, 1), str_left(middle, 1), str_left(last, 1))),
         sex               = remove_code(stri_trans_toupper(sex)),

         # clinical pic
         artstart_stage    = as.integer(keep_code(who_class)),

         # pregnant
         pregnant          = as.integer(keep_code(is_pregnant)),

         # cd4 tagging
         days_cd4_artstart = interval(cd4_date, visit_date) / days(1),
         cd4_is_baseline   = if_else(condition = days_cd4_artstart <= 182, 1, 0, 0),
         cd4_date          = case_when(
            cd4_is_baseline == 0 ~ NA_Date_,
            is.na(cd4_result) ~ NA_Date_,
            TRUE ~ cd4_date
         ),
         cd4_result        = case_when(
            cd4_is_baseline == 0 ~ NA_character_,
            TRUE ~ cd4_result
         ),
         cd4_result        = stri_replace_all_charclass(cd4_result, "[:alpha:]", "") %>%
            stri_replace_all_fixed(" ", "") %>%
            stri_replace_all_fixed("<", "") %>%
            as.numeric(),
         baseline_cd4      = case_when(
            cd4_result >= 500 ~ 1,
            cd4_result >= 350 & cd4_result < 500 ~ 2,
            cd4_result >= 200 & cd4_result < 350 ~ 3,
            cd4_result >= 50 & cd4_result < 200 ~ 4,
            cd4_result < 50 ~ 5,
         ),
         baseline_cd4      = labelled(
            baseline_cd4,
            c(
               "1_500+ cells/μL"    = 1,
               "2_350-499 cells/μL" = 2,
               "3_200-349 cells/μL" = 3,
               "4_50-199 cells/μL"  = 4,
               "5_below 50"         = 5
            )
         ),
      )

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
         art_id,
         year,
         month,
         confirmatory_code       = confirmatory_code,
         px_code                 = patient_code,
         uic                     = uic,
         first                   = first,
         middle                  = middle,
         last                    = last,
         suffix                  = suffix,
         age                     = age,
         birthdate               = birthdate,
         sex                     = sex,
         initials,
         philhealth_no           = philhealth_no,
         philsys_id              = philsys_id,
         mobile                  = client_mobile,
         email                   = client_email,
         curr_reg,
         curr_prov,
         curr_munc,
         artstart_hub            = art_faci_code,
         artstart_branch         = art_branch,
         artstart_realhub        = actual_faci_code,
         artstart_realhub_branch = actual_branch,
         artstart_reg            = real_reg,
         artstart_prov           = real_prov,
         artstart_munc           = real_munc,
         artstart_stage,
         visit_type              = visit_type,
         tx_status               = tx_status,
         artstart_addr           = curr_addr,
         artstart_date           = visit_date,
         artstart_nextpickup     = latest_next_date,
         artstart_regimen        = medicine_summary,
         baseline_cd4,
         baseline_cd4_date       = cd4_date,
         baseline_cd4_result     = cd4_result,
         pregnant,
         starts_with("curr")
      ) %>%
      mutate(
         age_pregnant = if_else(
            condition = pregnant == 1,
            true      = age,
            false     = as.numeric(NA)
         ),
      )

   return(data)
}

##  Append w/ old ART Registry -------------------------------------------------

append_enrollees <- function(old, new) {
   log_info("Appending enrollees to final registry.")
   data <- new %>%
      mutate(
         corr_defer   = if_else(is.na(artstart_regimen), 1, 0, 0),
         drop_notart  = 0,
         age          = as.integer(age),
         baseline_cd4 = as.numeric(keep_code(to_character(baseline_cd4)))
      ) %>%
      bind_rows(
         old %>%
            mutate(artstart_stage = as.integer(artstart_stage))
      ) %>%
      arrange(art_id) %>%
      mutate(
         corr_defer  = coalesce(corr_defer, 0),
         drop_notart = coalesce(drop_notart, 0),
      ) %>%
      zap_labels()

   return(data)
}

##  Tag data to be reported later on and duplicates for dropping ---------------

tag_fordrop <- function(data, corr) {
   log_info("Tagging enrollees for dropping.")
   for (drop_var in c("corr_defer", "drop_notart"))
      if (drop_var %in% names(corr)) {
         if (nrow(corr[[drop_var]]) > 0) {
            data %<>%
               left_join(
                  y  = corr[[drop_var]] %>%
                     distinct(rec_id) %>%
                     mutate(drop_this = 1),
                  by = join_by(rec_id)
               ) %>%
               mutate_at(
                  .vars = vars(matches(drop_var)),
                  ~coalesce(drop_this, .)
               ) %>%
               select(-drop_this)
         }
      }

   return(data)
}

##  Subsets for documentation --------------------------------------------------

subset_drops <- function(data) {
   log_info("Archive those for dropping.")
   drops <- list(
      dropped_notyet = data %>% filter(corr_defer == 1),
      dropped_notart = data %>% filter(drop_notart == 1)
   )

   return(drops)
}

##  Drop using taggings --------------------------------------------------------

remove_drops <- function(data) {
   log_info("Dropping clients w/ issue from final registry.")
   data %<>%
      mutate(
         drop = corr_defer + drop_notart,
      ) %>%
      filter(drop == 0) %>%
      select(
         -drop,
         -corr_defer,
         -drop_notart,
         -starts_with("curr"),
      )

   return(data)
}

##  Merge w/ Dx Registry -------------------------------------------------------

merge_dx <- function(data, forms, params) {
   dx <- hs_data("harp_dx", "reg", params$yr, params$mo) %>%
      read_dta(
         col_select = c(
            any_of(c('PATIENT_ID', 'patient_id')),
            labcode,
            labcode2,
            idnum,
            uic,
            firstname,
            middle,
            last,
            name_suffix,
            bdate,
            sex,
            pxcode,
            philhealth,
            confirm_date
         )
      ) %>%
      rename_all(tolower) %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      ) %>%
      get_cid(forms$id_registry, patient_id) %>%
      mutate(
         labcode2 = coalesce(labcode2, labcode)
      )

   data %<>%
      select(-starts_with("labcode2"), -starts_with("dxreg")) %>%
      left_join(
         y  = dx %>%
            select(
               central_id,
               dxreg_confirmatory_code = labcode2,
               dxreg_idnum             = idnum,
               dxreg_uic               = uic,
               dxreg_first             = firstname,
               dxreg_middle            = middle,
               dxreg_last              = last,
               dxreg_suffix            = name_suffix,
               dxreg_birthdate         = bdate,
               dxreg_sex               = sex,
               dxreg_initials          = pxcode,
               dxreg_philhealth_no     = philhealth
            ),
         by = join_by(central_id)
      )

   # check these variables if missing in art reg
   cols <- names(select(data, starts_with("dxreg_", ignore.case = FALSE)))
   cols <- str_replace(cols, "dxreg_", "")
   data %<>%
      mutate(
         across(
            all_of(cols),
            ~coalesce(., pull(data, str_c("dxreg_", cur_column())))
         )
      ) %>%
      mutate(
         idnum             = dxreg_idnum,
         confirmatory_code = coalesce(dxreg_confirmatory_code, confirmatory_code)
      )

   # remove dx registry variables
   data %<>%
      select(
         -starts_with("dxreg"),
         -starts_with("labcode2"),
         -starts_with("confirm_date"),
         -starts_with("confirm_remarks"),
         -starts_with("ref_death_date"),
      ) %>%
      left_join(
         y          = dx %>%
            select(idnum, labcode2, confirm_date) %>%
            mutate(
               labcode2 = case_when(
                  idnum == 6978 ~ "R11-06-3387",
                  idnum == 56460 ~ "D18-09-15962",
                  TRUE ~ labcode2
               )
            ),
         by         = join_by(idnum),
         na_matches = "never"
      ) %>%
      # add latest confirmatory data
      select(-any_of(c('confirm_result', 'confirm_remarks'))) %>%
      left_join(forms$confirm_last, join_by(central_id)) %>%
      mutate(
         confirm_date   = coalesce(confirm_date, as.Date(date_confirm)),
         confirm_result = case_when(
            !is.na(idnum) ~ "1_Positive",
            TRUE ~ confirm_result
         )
      ) %>%
      mutate(
         # finalize age data
         age_dta           = calc_age(birthdate, artstart_date),
         age               = coalesce(age, age_dta),
         confirmatory_code = coalesce(labcode2, confirm_code, confirmatory_code, str_c("*", coalesce(uic, px_code))),
         newonart          = if_else(
            condition = year(artstart_date) == params$yr & month(artstart_date) == params$mo,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
      ) %>%
      select(-date_confirm, -confirm_code, -labcode2) %>%
      distinct(central_id, .keep_all = TRUE) %>%
      relocate(idnum, .after = art_id) %>%
      arrange(art_id)

   return(data)
}

##  Merge w/ Tx Registry -------------------------------------------------------

merge_prep <- function(data, forms, params) {
   prep <- hs_data("prep", "reg", params$yr, params$mo) %>%
      read_dta(
         col_select = c(
            any_of(c('PATIENT_ID', 'patient_id')),
            prep_id
         )
      ) %>%
      rename_all(tolower) %>%
      get_cid(forms$id_registry, patient_id) %>%
      select(-patient_id)

   # get prep_id
   data %<>%
      select(-any_of("prep_id")) %>%
      left_join(
         y  = prep %>%
            distinct(central_id, .keep_all = TRUE),
         by = join_by(central_id)
      ) %>%
      relocate(prep_id, .after = idnum)

   return(data)
}

##  Merge w/ Death Registry ----------------------------------------------------

merge_dead <- function(data, forms, params) {
   dead <- hs_data("harp_dead", "reg", params$yr, params$mo) %>%
      read_dta(
         col_select = c(
            any_of(c('PATIENT_ID', 'patient_id')),
            mort_id,
            year,
            month,
            date_of_death
         )
      ) %>%
      rename_all(tolower) %>%
      get_cid(forms$id_registry, patient_id) %>%
      select(-patient_id) %>%
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
      select(-any_of("mort_id")) %>%
      left_join(
         y  = dead %>%
            distinct(central_id, .keep_all = TRUE),
         by = join_by(central_id)
      ) %>%
      relocate(mort_id, .after = prep_id)

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
            reg_order = artstart_reg,
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
         arrange(reg_order, artstart_realhub, artstart_realhub_branch, art_id) %>%
         select(-reg_order)

      view_vars <- c(
         "rec_id",
         "central_id",
         "artstart_reg",
         "artstart_realhub",
         "artstart_realhub_branch",
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
         "age",
         "tx_status",
         "artstart_date",
         "artstart_nextpickup",
         "artstart_regimen"
      )
      check     <- check_pii(data, check, view_vars, first = first, middle = middle, last = last, birthdate = birthdate, sex = sex)
      check     <- check_unknown(data, check, "curr_addr", view_vars, curr_reg, curr_prov, curr_munc)

      # non-negotiable variables
      nonnegotiables <- c("age", "uic")
      check          <- check_nonnegotiables(data, check, view_vars, nonnegotiables)
      check          <- check_preggy(data, check, view_vars, sex = sex)
      check          <- check_age(data, check, view_vars, birthdate = birthdate, age = age, visit_date = artstart_date)

      # special checks
      log_info("Checking for missing dispensing data.")
      check[["no_disp"]] <- data %>%
         filter(
            if_any(c(artstart_date, artstart_regimen, artstart_nextpickup, artstart_hub), ~is.na(.))
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for new clients tagged as refills.")
      check[["refill_enroll"]] <- data %>%
         filter(
            str_left(tx_status, 1) == "2"
         ) %>%
         select(
            any_of(view_vars),
            tx_status,
            visit_type
         )

      log_info("Checking for new clients that are not enrollees.")
      check[["non_enrollee"]] <- data %>%
         filter(
            artstart_date < params$min,
            coalesce(str_left(tx_status, 1), "") != "1"
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for late reported clients.")
      check[["late_report"]] <- data %>%
         filter(
            artstart_date < params$min,
            coalesce(str_left(tx_status, 1), "") == "1"
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for possible PMTCT-N clients.")
      check[["possible_pmtct"]] <- data %>%
         filter(
            (artstart_num_arv == 1 & str_detect(artstart_regimen, "syr")) |
               age <= 5
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for possible PrEP clients.")
      check[["possible_prep"]] <- data %>%
         filter(
            stri_detect_fixed(artstart_regimen, "ftc")
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking ART reports tagged as doh-eb.")
      check[["art_eb"]] <- data %>%
         filter(
            artstart_hub == "doh"
         ) %>%
         select(
            any_of(view_vars),
         )

      all_issues <- combine_validations(data, check, "rec_id") %>%
         mutate(
            reg_order = artstart_reg,
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
         arrange(reg_order, artstart_realhub, artstart_realhub_branch, rec_id) %>%
         select(-reg_order)

      check <- list(all_issues = all_issues)

      # range-median
      tabstat <- c(
         "artstart_date",
         "artstart_nextpickup",
         "birthdate",
         "age"
      )
      check   <- check_tabstat(data, check, tabstat)

      # Remove already tagged data from validation
      if (exclude_drops == "1") {
         for (drop in c("drop_notart", "corr_defer")) {
            if (drop %in% names(corr))
               for (check_var in names(check)) {
                  if (check_var != "tabstat")
                     check[[check_var]] %<>%
                        anti_join(
                           y  = corr[[drop]],
                           by = "rec_id"
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

   data <- get_enrollees(p$forms$art_first, p$official$old_reg, p$params)
   data <- get_first_visit(data)
   data <- get_cd4(data, p$forms$lab_cd4)
   data <- convert_faci_addr(data)
   data <- standardize_data(data, p$params)
   data <- final_conversion(data)

   new_reg <- p$official$old_reg %>%
      append_enrollees(data) %>%
      merge_dx(p$forms, p$params) %>%
      merge_dead(p$forms, p$params) %>%
      merge_prep(p$forms, p$params)

   new_reg <- tag_fordrop(new_reg, p$corr)
   drops   <- subset_drops(new_reg)
   new_reg <- remove_drops(new_reg)

   step$check <- get_checks(data, p$params, p$corr, run_checks = vars$run_checks, exclude_drops = vars$exclude_drops)
   step$data  <- data

   p$official$new_reg <- new_reg
   append(p$official, drops)

   flow_validation(p, "tx_new", p$params$ym, upload = vars$upload)
   log_success("Done.")
}