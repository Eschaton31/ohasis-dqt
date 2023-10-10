##  Initial Cleaning -----------------------------------------------------------

clean_data <- function(forms, old_reg) {
   # Form D + BC Dead
   log_info("Processing new mortalities.")
   data <- forms$form_d %>%
      get_cid(forms$id_registry, PATIENT_ID) %>%
      # keep only patients not in registry
      anti_join(
         y  = old_reg %>%
            select(CENTRAL_ID),
         by = join_by(CENTRAL_ID)
      ) %>%
      mutate_at(
         .vars = vars(FIRST, MIDDLE, LAST, SUFFIX, CONFIRMATORY_CODE, PATIENT_CODE, UIC, PHILHEALTH_NO, PHILSYS_ID, CLIENT_MOBILE, CLIENT_EMAIL),
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
            "SEX",
            "UIC",
            "PHILHEALTH_NO",
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
         # date variables
         report_date    = RECORD_DATE,

         # name
         STANDARD_FIRST = stri_trans_general(FIRST, "latin-ascii"),
         fullname       = str_squish(stri_c(LAST, ", ", FIRST, " ", MIDDLE, " ", SUFFIX)),

         # Permanent
         PERM_PSGC_PROV = if_else(StrLeft(PERM_PSGC_REG, 2) == "99", "999900000", PERM_PSGC_PROV, PERM_PSGC_PROV),
         PERM_PSGC_MUNC = if_else(StrLeft(PERM_PSGC_REG, 2) == "99", "999999000", PERM_PSGC_MUNC, PERM_PSGC_MUNC),
         use_curr       = if_else(
            condition = !is.na(CURR_PSGC_MUNC) & (is.na(PERM_PSGC_MUNC) | StrLeft(PERM_PSGC_MUNC, 2) == "99"),
            true      = 1,
            false     = 0
         ),
         PERM_PSGC_REG  = if_else(
            condition = use_curr == 1,
            true      = CURR_PSGC_REG,
            false     = PERM_PSGC_REG
         ),
         PERM_PSGC_PROV = if_else(
            condition = use_curr == 1,
            true      = CURR_PSGC_PROV,
            false     = PERM_PSGC_PROV
         ),
         PERM_PSGC_MUNC = if_else(
            condition = use_curr == 1,
            true      = CURR_PSGC_MUNC,
            false     = PERM_PSGC_MUNC
         ),

         # Age
         AGE            = coalesce(AGE, AGE_MO / 12),
         AGE_DTA        = calc_age(BIRTHDATE, coalesce(DEATH_DATE, RECORD_DATE)),

         # tag wrong reports
         not_dead       = if_else(
            condition = StrLeft(EB_VALIDATED, 1) == "0",
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
      arrange(desc(REPORTING_FORM), report_date, DEATH_DATE) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      filter(
         report_date < ohasis$next_date |
            DEATH_DATE < ohasis$next_date |
            is.na(report_date)
      ) %>%
      rename(
         MORT_FACI     = SERVICE_FACI,
         MORT_SUB_FACI = SERVICE_SUB_FACI,
      )

   return(data)
}

##  Generate subset variables --------------------------------------------------

standardize_data <- function(initial, params) {
   log_info("Converting to final HARP variables.")
   data <- initial %>%
      mutate(
         # generate idnum
         mort_id            = params$latest_mort_id + row_number(),

         # report date
         year               = params$yr,
         month              = params$mo,

         # Perm Region (as encoded)
         PERMONLY_PSGC_REG  = if_else(
            condition = use_curr == 0,
            true      = PERM_PSGC_REG,
            false     = NA_character_
         ),
         PERMONLY_PSGC_PROV = if_else(
            condition = use_curr == 0,
            true      = PERM_PSGC_PROV,
            false     = NA_character_
         ),
         PERMONLY_PSGC_MUNC = if_else(
            condition = use_curr == 0,
            true      = PERM_PSGC_MUNC,
            false     = NA_character_
         ),

         # demographics
         pxcode             = str_squish(stri_c(StrLeft(FIRST, 1), StrLeft(MIDDLE, 1), StrLeft(LAST, 1))),

         SEX                = remove_code(stri_trans_toupper(SEX)),
         CIVIL_STATUS       = remove_code(stri_trans_toupper(CIVIL_STATUS)),
      )

   return(data)
}

##  Facilities & Address -------------------------------------------------------

convert_faci_addr <- function(data) {
   log_info("Converting address & facility data.")
   # rename columns
   data %<>%
      ohasis$get_addr(
         c(
            region   = "PERM_PSGC_REG",
            province = "PERM_PSGC_PROV",
            muncity  = "PERM_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      ohasis$get_addr(
         c(
            last_residence_region   = "CURR_PSGC_REG",
            last_residence_province = "CURR_PSGC_PROV",
            last_residence_muncity  = "CURR_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      ohasis$get_addr(
         c(
            birthplace_region       = "BIRTH_PSGC_REG",
            birthplace_province     = "BIRTH_PSGC_PROV",
            birthplace_municipality = "BIRTH_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      ohasis$get_addr(
         c(
            place_of_death_region   = "DEATH_PSGC_REG",
            place_of_death_province = "DEATH_PSGC_PROV",
            place_of_death_muncity  = "DEATH_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      # dxlab_standard
      mutate(
         MORT_FACI     = if_else(
            condition = is.na(MORT_FACI),
            true      = "",
            false     = MORT_FACI
         ),
         MORT_SUB_FACI = case_when(
            is.na(MORT_SUB_FACI) ~ "",
            StrLeft(MORT_SUB_FACI, 6) != MORT_FACI ~ "",
            TRUE ~ MORT_SUB_FACI
         )
      ) %>%
      left_join(
         na_matches = "never",
         y          = ohasis$ref_faci %>%
            select(
               MORT_FACI     = FACI_ID,
               MORT_SUB_FACI = SUB_FACI_ID,
               pubpriv       = PUBPRIV
            ),
         by         = join_by(MORT_FACI, MORT_SUB_FACI)
      ) %>%
      ohasis$get_faci(
         list(facility = c("MORT_FACI", "MORT_SUB_FACI")),
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
         CENTRAL_ID,
         PATIENT_ID,
         REC_ID,
         mort_id,
         form                    = REPORTING_FORM,
         year,
         month,
         saccl_lab_code          = CONFIRMATORY_CODE,
         uic                     = UIC,
         fname                   = FIRST,
         mname                   = MIDDLE,
         lname                   = LAST,
         sname                   = SUFFIX,
         fullname,
         birthdate               = BIRTHDATE,
         pxcode,
         patient_code            = PATIENT_CODE,
         age                     = AGE,
         age_months              = AGE_MO,
         age_death               = AGE_DTA,
         sex                     = SEX,
         philhealth              = PHILHEALTH_NO,
         philsys_id              = PHILSYS_ID,
         mobile                  = CLIENT_MOBILE,
         email                   = CLIENT_EMAIL,
         muncity,
         province,
         region,
         last_residence_muncity,
         last_residence_province,
         last_residence_region,
         birthplace_municipality,
         birthplace_province,
         birthplace_region,
         civil_status            = CIVIL_STATUS,
         was_living_with_partner = LIVING_WITH_PARTNER,
         living_children         = CHILDREN,
         immediate_cause         = IMMEDIATE_CAUSES,
         antecedentcause         = ANTECEDENT_CAUSES,
         underlying_cause        = UNDERLYING_CAUSES,
         tb                      = DISEASE_TB,
         hepb                    = DISEASE_HEPB,
         hepc                    = DISEASE_HEPC,
         cmeningitis             = DISEASE_MENINGITIS,
         pcp                     = DISEASE_PCP,
         cmv                     = DISEASE_CMV,
         candidiasis             = DISEASE_OROCAND,
         toxo                    = DISEASE_TOXOPLASMOSIS,
         covid19                 = DISEASE_COVID19,
         hiv                     = DISEASE_HIV,
         facility,
         pubpriv,
         facility_region,
         facility_province,
         facility_muncity,
         is_valid                = EB_VALIDATED,
         date_of_death           = DEATH_DATE,
         with_death_cert         = DEATH_CERTIFICATE,
         place_of_death_region,
         place_of_death_province,
         place_of_death_muncity,
         place_of_death_addr     = DEATH_ADDR,
         report_date,
         report_notes            = REPORT_NOTES,
         report_by               = REPORTED_BY,
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
            REC_ID,
            PATIENT_ID,
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
      get_cid(forms$id_registry, PATIENT_ID) %>%
      mutate(
         labcode2 = coalesce(labcode2, labcode)
      )

   data %<>%
      select(-starts_with("labcode2"), -starts_with("dxreg")) %>%
      left_join(
         y  = dx %>%
            select(
               CENTRAL_ID,
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
         by = join_by(CENTRAL_ID)
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
         ~coalesce(., "UNKNOWN")
      ) %>%
      mutate(
         final_region   = if_else(
            condition = dxreg_muncity == "UNKNOWN" & (muncity != "UNKNOWN" & !is.na(muncity)),
            true      = region,
            false     = dxreg_region,
            missing   = "UNKNOWN"
         ),
         final_province = if_else(
            condition = dxreg_muncity == "UNKNOWN" & (muncity != "UNKNOWN" & !is.na(muncity)),
            true      = province,
            false     = dxreg_province,
            missing   = "UNKNOWN"
         ),
         final_muncity  = if_else(
            condition = dxreg_muncity == "UNKNOWN" & (muncity != "UNKNOWN" & !is.na(muncity)),
            true      = muncity,
            false     = dxreg_muncity,
            missing   = "UNKNOWN"
         ),
         final_region   = if_else(
            condition = final_muncity == "UNKNOWN" | is.na(final_muncity),
            true      = mort_region,
            false     = final_region,
            missing   = final_region
         ),
         final_province = if_else(
            condition = final_muncity == "UNKNOWN" | is.na(final_muncity),
            true      = mort_province,
            false     = final_province,
            missing   = final_province
         ),
         final_muncity  = if_else(
            condition = final_muncity == "UNKNOWN" | is.na(final_muncity),
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
            condition = final_muncity == "UNKNOWN" & mort_muncity != "UNKNOWN",
            true      = mort_region,
            false     = final_region,
            missing   = final_region
         ),
         final_province = if_else(
            condition = final_muncity == "UNKNOWN" & mort_muncity != "UNKNOWN",
            true      = mort_province,
            false     = final_province,
            missing   = final_province
         ),
         final_muncity  = if_else(
            condition = final_muncity == "UNKNOWN" & mort_muncity != "UNKNOWN",
            true      = mort_muncity,
            false     = final_muncity,
            missing   = final_muncity
         ),
         final_region   = if_else(
            condition = is.na(final_muncity) & mort_muncity != "UNKNOWN",
            true      = mort_region,
            false     = final_region,
            missing   = final_region
         ),
         final_province = if_else(
            condition = is.na(final_muncity) & mort_muncity != "UNKNOWN",
            true      = mort_province,
            false     = final_province,
            missing   = final_province
         ),
         final_muncity  = if_else(
            condition = is.na(final_muncity) & mort_muncity != "UNKNOWN",
            true      = mort_muncity,
            false     = final_muncity,
            missing   = final_muncity
         ),
         final_region   = if_else(
            condition = final_muncity == "UNKNOWN" & muncity != "UNKNOWN",
            true      = region,
            false     = final_region,
            missing   = final_region
         ),
         final_province = if_else(
            condition = final_muncity == "UNKNOWN" & muncity != "UNKNOWN",
            true      = province,
            false     = final_province,
            missing   = final_province
         ),
         final_muncity  = if_else(
            condition = final_muncity == "UNKNOWN" & muncity != "UNKNOWN",
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
                  idnum == 6978 ~ "R11-06-3387",
                  idnum == 56460 ~ "D18-09-15962",
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
      dir     <- Sys.getenv("HARP_DEAD")
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

            compress_stata(files[[output]])
         }
      }
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
         arrange(reg_order, facility, mort_id) %>%
         select(-reg_order)

      view_vars <- c(
         "REC_ID",
         "CENTRAL_ID",
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

      log_info("Checking for death report from Form BC.")
      check[["formbc_dead"]] <- data %>%
         filter(
            form == "Form BC"
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for death reports that are still for investigation.")
      check[["confirm_if_dead"]] <- data %>%
         filter(
            StrLeft(is_valid, 1) == "3" | is.na(is_valid)
         ) %>%
         select(
            any_of(view_vars),
         )

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
