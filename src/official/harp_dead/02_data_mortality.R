##  Initial Cleaning -----------------------------------------------------------

clean_data <- function(forms, old_reg) {
   # Form D + bc Dead
   log_info("Processing new mortalities.")
   data <- forms$form_d %>%
      get_cid(forms$id_registry, patient_id) %>%
      # keep only patients not in registry
      anti_join(
         y  = old_reg %>%
            select(central_id),
         by = join_by(central_id)
      ) %>%
      mutate_at(
         .vars = vars(first, middle, last, suffix, confirmatory_code, patient_code, uic, philhealth_no, philsys_id, client_mobile, client_email),
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
            "client_email"
         )
      ) %>%
      mutate(
         # date variables
         report_date    = record_date,
         reporting_form = 'Form D (v2017)',

         # name
         standard_first = stri_trans_general(first, "latin-ascii"),
         fullname       = str_squish(stri_c(last, ", ", first, " ", middle, " ", suffix)),

         # Permanent
         perm_prov      = if_else(str_left(perm_reg, 2) == "99", "999900000", perm_prov, perm_prov),
         perm_munc      = if_else(str_left(perm_reg, 2) == "99", "999999000", perm_munc, perm_munc),
         use_curr       = if_else(
            condition = !is.na(curr_munc) & (is.na(perm_munc) | str_left(perm_munc, 2) == "99"),
            true      = 1,
            false     = 0
         ),
         perm_reg       = if_else(
            condition = use_curr == 1,
            true      = curr_reg,
            false     = perm_reg
         ),
         perm_prov      = if_else(
            condition = use_curr == 1,
            true      = curr_prov,
            false     = perm_prov
         ),
         perm_munc      = if_else(
            condition = use_curr == 1,
            true      = curr_munc,
            false     = perm_munc
         ),

         # Age
         age            = coalesce(age, age_mo / 12),
         age_dta        = calc_age(birthdate, coalesce(death_date, record_date)),

         # tag wrong reports
         not_dead       = if_else(
            condition = str_left(eb_validated, 1) == "0",
            true      = 1,
            false     = 0,
            missing   = 0
         )
      )

   return(data)
}

##  Sorting reports ------------------------------------------------------------

prioritize_reports <- function(data) {
   log_info("Using earliest data.")
   data %<>%
      # remove invalid reports
      filter(not_dead == 0) %>%
      # prioritize form d over form bc
      arrange(desc(reporting_form), report_date, death_date) %>%
      distinct(central_id, .keep_all = TRUE) %>%
      filter(
         report_date < ohasis$next_date |
            death_date < ohasis$next_date |
            is.na(report_date)
      ) %>%
      rename(
         mort_faci     = service_faci,
         mort_sub_faci = service_sub_faci,
      )

   return(data)
}

##  Generate subset variables --------------------------------------------------

standardize_data <- function(initial, params) {
   log_info("Converting to final harp variables.")
   data <- initial %>%
      mutate(
         # generate idnum
         mort_id       = params$latest_mort_id + row_number(),

         # report date
         year          = params$yr,
         month         = params$mo,

         # Perm Region (as encoded)
         permonly_reg  = if_else(
            condition = use_curr == 0,
            true      = perm_reg,
            false     = NA_character_
         ),
         permonly_prov = if_else(
            condition = use_curr == 0,
            true      = perm_prov,
            false     = NA_character_
         ),
         permonly_munc = if_else(
            condition = use_curr == 0,
            true      = perm_munc,
            false     = NA_character_
         ),

         # demographics
         pxcode        = str_squish(stri_c(str_left(first, 1), str_left(middle, 1), str_left(last, 1))),

         sex           = remove_code(stri_trans_toupper(sex)),
         civil_status  = remove_code(stri_trans_toupper(civil_status)),
      )

   return(data)
}

##  Facilities & Address -------------------------------------------------------

convert_faci_addr <- function(data) {
   log_info("Converting address & facility data.")
   # rename columns
   data %<>%
      get_addr(
         c(
            region   = "perm_reg",
            province = "perm_prov",
            muncity  = "perm_munc"
         ),
         "nhsss"
      ) %>%
      get_addr(
         c(
            last_residence_region   = "curr_reg",
            last_residence_province = "curr_prov",
            last_residence_muncity  = "curr_munc"
         ),
         "nhsss"
      ) %>%
      get_addr(
         c(
            birthplace_region       = "birth_reg",
            birthplace_province     = "birth_prov",
            birthplace_municipality = "birth_munc"
         ),
         "nhsss"
      ) %>%
      get_addr(
         c(
            place_of_death_region   = "death_reg",
            place_of_death_province = "death_prov",
            place_of_death_muncity  = "death_munc"
         ),
         "nhsss"
      ) %>%
      # dxlab_standard
      mutate(
         mort_faci     = if_else(
            condition = is.na(mort_faci),
            true      = "",
            false     = mort_faci
         ),
         mort_sub_faci = case_when(
            is.na(mort_sub_faci) ~ "",
            str_left(mort_sub_faci, 6) != mort_faci ~ "",
            TRUE ~ mort_sub_faci
         )
      ) %>%
      left_join(
         na_matches = "never",
         y          = ohasis$ref_faci %>%
            select(mort_faci = faci_id, mort_sub_faci = sub_faci_id, pubpriv = ownership) %>%
            mutate(
               pubpriv = case_when(
                  pubpriv == 1 ~ "PUBLIC",
                  pubpriv == 2 ~ "PRIVATE",
               )
            ),
         by         = join_by(mort_faci, mort_sub_faci)
      ) %>%
      ohasis$get_faci(
         list(facility = c("mort_faci", "mort_sub_faci")),
         "nhsss",
         c("facility_region", "facility_province", "facility_muncity")
      )

   return(data)
}

##  Finalize -------------------------------------------------------------------

final_conversion <- function(data) {
   data %<>%
      # same vars as registry
      select(
         central_id,
         patient_id,
         rec_id,
         mort_id,
         form                    = reporting_form,
         year,
         month,
         saccl_lab_code          = confirmatory_code,
         uic                     = uic,
         fname                   = first,
         mname                   = middle,
         lname                   = last,
         sname                   = suffix,
         fullname,
         birthdate               = birthdate,
         pxcode,
         patient_code            = patient_code,
         age                     = age,
         age_months              = age_mo,
         age_death               = age_dta,
         sex                     = sex,
         philhealth              = philhealth_no,
         philsys_id              = philsys_id,
         mobile                  = client_mobile,
         email                   = client_email,
         muncity,
         province,
         region,
         last_residence_muncity,
         last_residence_province,
         last_residence_region,
         birthplace_municipality,
         birthplace_province,
         birthplace_region,
         civil_status            = civil_status,
         was_living_with_partner = living_with_partner,
         living_children         = children,
         immediate_cause         = immediate_causes,
         antecedentcause         = antecedent_causes,
         underlying_cause        = underlying_causes,
         tb                      = disease_tb,
         hepb                    = disease_hepb,
         hepc                    = disease_hepc,
         cmeningitis             = disease_meningitis,
         pcp                     = disease_pcp,
         cmv                     = disease_cmv,
         candidiasis             = disease_orocand,
         toxo                    = disease_toxoplasmosis,
         covid19                 = disease_covid19,
         hiv                     = disease_hiv,
         facility,
         pubpriv,
         facility_region,
         facility_province,
         facility_muncity,
         is_valid                = eb_validated,
         date_of_death           = death_date,
         with_death_cert         = death_certificate,
         place_of_death_region,
         place_of_death_province,
         place_of_death_muncity,
         place_of_death_addr     = death_addr,
         report_date,
         report_notes            = report_notes,
         report_by               = reported_by,
      ) %>%
      # turn into codes
      mutate_at(
         .vars = vars(
            tb,
            hepb,
            hepc,
            cmeningitis,
            pcp,
            cmv,
            candidiasis,
            toxo,
            covid19,
            with_death_cert
         ),
         ~as.integer(keep_code(.))
      ) %>%
      mutate(
         pubpriv = if_else(pubpriv == "0", NA_character_, as.character(pubpriv))
      )

   return(data)
}

##  Append w/ old Registry -----------------------------------------------------

append_data <- function(old, new) {
   log_info("Appending cases to final registry.")
   data <- new %>%
      mutate(living_children = as.character(living_children)) %>%
      # keep only validated
      filter(is_valid == "1_Yes") %>%
      bind_rows(old %>% select(-matches("report_date"))) %>%
      arrange(mort_id) %>%
      mutate(
         drop_notyet     = 0,
         drop_duplicates = 0,
      ) %>%
      relocate(idnum, .after = mort_id)

   return(data)
}

##  Merge w/ Dx Registry -------------------------------------------------------

merge_dx <- function(data, forms, params) {
   dx <- hs_data("harp_dx", "reg", params$yr, params$mo) %>%
      read_dta(
         col_select = c(
            rec_id,
            patient_id,
            labcode,
            labcode2,
            idnum,
            uic,
            firstname,
            middle,
            last,
            name_suffix,
            name,
            bdate,
            sex,
            pxcode,
            philhealth,
            region,
            province,
            muncity
         )
      ) %>%
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
               dxreg_saccl_lab_code = labcode2,
               dxreg_idnum          = idnum,
               dxreg_uic            = uic,
               dxreg_fname          = firstname,
               dxreg_mname          = middle,
               dxreg_lname          = last,
               dxreg_sname          = name_suffix,
               dxreg_fullname       = name,
               dxreg_birthdate      = bdate,
               dxreg_sex            = sex,
               dxreg_pxcode         = pxcode,
               dxreg_philhealth     = philhealth,
               dxreg_region         = region,
               dxreg_province       = province,
               dxreg_muncity        = muncity
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
         idnum          = dxreg_idnum,
         saccl_lab_code = coalesce(dxreg_saccl_lab_code, saccl_lab_code)
      )

   data %<>%
      mutate_at(
         .vars = vars(ends_with("muncity"), ends_with("province"), ends_with("region")),
         ~coalesce(., "unknown")
      ) %>%
      mutate(
         final_region   = if_else(
            condition = dxreg_muncity == "unknown" & (muncity != "unknown" & !is.na(muncity)),
            true      = region,
            false     = dxreg_region,
            missing   = "unknown"
         ),
         final_province = if_else(
            condition = dxreg_muncity == "unknown" & (muncity != "unknown" & !is.na(muncity)),
            true      = province,
            false     = dxreg_province,
            missing   = "unknown"
         ),
         final_muncity  = if_else(
            condition = dxreg_muncity == "unknown" & (muncity != "unknown" & !is.na(muncity)),
            true      = muncity,
            false     = dxreg_muncity,
            missing   = "unknown"
         ),
         final_region   = if_else(
            condition = final_muncity == "unknown" | is.na(final_muncity),
            true      = mort_region,
            false     = final_region,
            missing   = final_region
         ),
         final_province = if_else(
            condition = final_muncity == "unknown" | is.na(final_muncity),
            true      = mort_province,
            false     = final_province,
            missing   = final_province
         ),
         final_muncity  = if_else(
            condition = final_muncity == "unknown" | is.na(final_muncity),
            true      = mort_muncity,
            false     = final_muncity,
            missing   = final_muncity
         ),
         final_region   = coalesce(final_region, region, dxreg_region),
         final_province = coalesce(final_province, province, dxreg_province),
         final_muncity  = coalesce(final_muncity, muncity, dxreg_muncity),
      ) %>%
      # additional process to ensure final_region
      mutate(
         final_region   = if_else(
            condition = final_muncity == "unknown" & mort_muncity != "unknown",
            true      = mort_region,
            false     = final_region,
            missing   = final_region
         ),
         final_province = if_else(
            condition = final_muncity == "unknown" & mort_muncity != "unknown",
            true      = mort_province,
            false     = final_province,
            missing   = final_province
         ),
         final_muncity  = if_else(
            condition = final_muncity == "unknown" & mort_muncity != "unknown",
            true      = mort_muncity,
            false     = final_muncity,
            missing   = final_muncity
         ),
         final_region   = if_else(
            condition = is.na(final_muncity) & mort_muncity != "unknown",
            true      = mort_region,
            false     = final_region,
            missing   = final_region
         ),
         final_province = if_else(
            condition = is.na(final_muncity) & mort_muncity != "unknown",
            true      = mort_province,
            false     = final_province,
            missing   = final_province
         ),
         final_muncity  = if_else(
            condition = is.na(final_muncity) & mort_muncity != "unknown",
            true      = mort_muncity,
            false     = final_muncity,
            missing   = final_muncity
         ),
         final_region   = if_else(
            condition = final_muncity == "unknown" & muncity != "unknown",
            true      = region,
            false     = final_region,
            missing   = final_region
         ),
         final_province = if_else(
            condition = final_muncity == "unknown" & muncity != "unknown",
            true      = province,
            false     = final_province,
            missing   = final_province
         ),
         final_muncity  = if_else(
            condition = final_muncity == "unknown" & muncity != "unknown",
            true      = muncity,
            false     = final_muncity,
            missing   = final_muncity
         ),
      )

   # remove dx registry variables
   data %<>%
      select(
         -starts_with("dxreg"),
         -starts_with("labcode2")
      ) %>%
      left_join(
         y          = dx %>%
            select(idnum, labcode2) %>%
            mutate(
               labcode2 = case_when(
                  idnum == 6978 ~ "r11-06-3387",
                  idnum == 56460 ~ "d18-09-15962",
                  TRUE ~ labcode2
               )
            ),
         by         = join_by(idnum),
         na_matches = "never"
      ) %>%
      mutate(
         # finalize age data
         age_dta        = calc_age(birthdate, coalesce(date_of_death, report_date)),
         age            = coalesce(age, age_dta),
         saccl_lab_code = coalesce(labcode2, saccl_lab_code, str_c("*", coalesce(uic, patient_code))),
      ) %>%
      distinct_all()

   return(data)
}


##  Tag data to be reported later on and duplicates for dropping ---------------

tag_fordrop <- function(data, corr) {
   log_info("Tagging reports for dropping.")
   for (drop_var in c("drop_notyet", "drop_duplicates"))
      if (drop_var %in% names(corr))
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

   return(data)
}

##  Subsets for documentation --------------------------------------------------

subset_drops <- function(data) {
   log_info("Archive those for dropping.")
   drops <- list(
      dropped_notyet     = data %>% filter(drop_notyet == 1),
      dropped_duplicates = data %>% filter(drop_duplicates == 1)
   )

   return(drops)
}

##  Drop using taggings --------------------------------------------------------

remove_drops <- function(data, params) {
   data %<>%
      mutate(
         drop = drop_duplicates + drop_notyet,
      ) %>%
      filter(drop == 0) %>%
      select(-drop, -drop_duplicates, -drop_notyet) %>%
      select(
         -any_of(
            c(
               "transmit",
               "sexhow",
               "labcode2",
               "interval_mort",
               "interval_reg",
               "drop_tag",
               "age_dta",
               "motcat4",
               "motcat"
            )
         )
      )

   return(data)
}

##  Output Stata Datasets ------------------------------------------------------

output_dta <- function(official, params, save = "2") {
   if (save == "1") {
      log_info("Checking output directory.")
      version <- format(Sys.time(), "%Y%m%d")
      dir     <- Sys.getenv("harp_dead")
      check_dir(dir)

      log_info("Saving in Stata data format.")
      period_ext <- str_c(params$yr, "-", stri_pad_left(params$mo, 2, "0"), ".dta")
      files      <- list(
         new                = file.path(dir, str_c(version, "_mort_", period_ext)),
         dropped_notyet     = file.path(dir, str_c(version, "_dropped_notyet_", period_ext)),
         dropped_duplicates = file.path(dir, str_c(version, "_dropped_duplicates_", period_ext))
      )
      for (output in intersect(names(files), names(official))) {
         if (nrow(official[[output]]) > 0) {
            official[[output]] %>%
               format_stata() %>%
               write_dta(files[[output]])

            # compress_stata(files[[output]])
         }
      }

      flow_dta(official$new, "harp_dead", "reg", params$yr, params$mo)
   }
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
            reg_order = facility_region,
            reg_order = case_when(
               reg_order == "1" ~ 1,
               reg_order == "2" ~ 2,
               reg_order == "car" ~ 3,
               reg_order == "3" ~ 4,
               reg_order == "ncr" ~ 5,
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
         arrange(reg_order, facility, mort_id) %>%
         select(-reg_order)

      view_vars <- c(
         "rec_id",
         "central_id",
         "facility_region",
         "facility",
         "form",
         "saccl_lab_code",
         "uic",
         "patient_code",
         "fname",
         "mname",
         "lname",
         "sname",
         "birthdate",
         "sex",
         "report_by",
         "report_date",
         "date_of_death",
         "with_death_cert",
         "immediate_cause",
         "antecedentcause",
         "underlying_cause",
         "report_notes",
         "place_of_death_addr"
      )
      check     <- check_pii(data, check, view_vars, first = fname, middle = mname, last = lname, birthdate = birthdate, sex = sex)

      # dates
      date_vars <- c(
         "report_date",
         "date_of_death",
         "birthdate"
      )
      check     <- check_dates(data, check, view_vars, date_vars)

      # non-negotiable variables
      nonnegotiables <- c(
         "form",
         "age",
         "hiv"
      )
      check          <- check_unknown(data, check, "perm_addr", view_vars, region, province, muncity)
      check          <- check_unknown(data, check, "death_addr", view_vars, place_of_death_region, place_of_death_province, place_of_death_muncity)
      check          <- check_unknown(data, check, "faci_data", view_vars, facility, pubpriv)
      check          <- check_nonnegotiables(data, check, view_vars, nonnegotiables)

      # special checks
      log_info("Checking for data w/o cause of death.")
      check[["no_cause"]] <- data %>%
         filter(
            if_all(c(immediate_cause, antecedentcause, underlying_cause), ~is.na(.))
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for death report from Form bc.")
      check[["formbc_dead"]] <- data %>%
         filter(
            form == "Form bc"
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for death reports that are still for investigation.")
      check[["confirm_if_dead"]] <- data %>%
         filter(
            str_left(is_valid, 1) == "3" | is.na(is_valid)
         ) %>%
         select(
            any_of(view_vars),
         )

      all_issues <- combine_validations(data, check, "rec_id") %>%
         mutate(
            reg_order = facility_region,
            reg_order = case_when(
               reg_order == "1" ~ 1,
               reg_order == "2" ~ 2,
               reg_order == "car" ~ 3,
               reg_order == "3" ~ 4,
               reg_order == "ncr" ~ 5,
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
         arrange(reg_order, facility, rec_id) %>%
         select(-reg_order)

      check <- list(all_issues = all_issues)

      # range-median
      tabstat <- c(
         "report_date",
         "date_of_death",
         "birthdate",
         "age"
      )
      check   <- check_tabstat(data, check, tabstat)
   }

   return(check)
}

##  Actual flow ----------------------------------------------------------------

.init <- function(envir = parent.env(environment()), ...) {
   step <- parent.env(environment())
   p    <- envir
   vars <- as.list(list(...))

   data <- clean_data(p$forms, p$official$old)
   data <- prioritize_reports(data)
   data <- standardize_data(data, p$params)
   data <- convert_faci_addr(data)
   data <- final_conversion(data)

   new_reg <- append_data(p$official$old, data) %>%
      merge_dx(p$forms, p$params)
   new_reg <- tag_fordrop(new_reg, p$corr)
   drops   <- subset_drops(new_reg)
   new_reg <- remove_drops(new_reg, p$params)

   step$check <- get_checks(data, p$pdf_rhivda, p$corr, run_checks = vars$run_checks, exclude_drops = vars$exclude_drops)
   step$data  <- data

   p$official$new <- new_reg
   append(p$official, drops)
   output_dta(p$official, p$params, vars$save)

   flow_validation(p, "mortality", p$params$ym, upload = vars$upload)
   log_success("Done.")
}
