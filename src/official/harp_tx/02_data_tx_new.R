##  Filter Initial Data & Remove Already Reported ------------------------------

get_enrollees <- function(art_first, old_reg, params) {
   log_info("Processing enrollees.")
   data <- art_first %>%
      anti_join(
         y  = old_reg %>%
            select(CENTRAL_ID),
         by = join_by(CENTRAL_ID)
      ) %>%
      mutate_at(
         .vars = vars(FIRST, MIDDLE, LAST, SUFFIX, CONFIRMATORY_CODE, PATIENT_CODE, UIC, PHILHEALTH_NO, PHILSYS_ID),
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
            "SELF_IDENT",
            "SELF_IDENT_OTHER",
            "PHILSYS_ID",
            "CURR_PSGC_REG",
            "CURR_PSGC_PROV",
            "CURR_PSGC_MUNC",
            "CLIENT_MOBILE",
            "CLIENT_EMAIL"
         )
      ) %>%
      mutate(
         # name
         STANDARD_FIRST     = stri_trans_general(FIRST, "latin-ascii"),
         name               = str_squish(stri_c(LAST, ", ", FIRST, " ", MIDDLE, " ", SUFFIX)),

         # Age
         AGE                = coalesce(AGE, AGE_MO / 12),
         AGE_DTA            = calc_age(BIRTHDATE, VISIT_DATE),

         # tag those without ART_FACI
         use_record_faci    = if_else(is.na(SERVICE_FACI), 1, 0, 0),
         SERVICE_FACI       = if_else(use_record_faci == 1, FACI_ID, SERVICE_FACI),

         # convert to HARP facility
         ACTUAL_FACI        = SERVICE_FACI,
         ACTUAL_SUB_FACI    = SERVICE_SUB_FACI,

         # tag special clinics
         special_clinic     = case_when(
            SERVICE_FACI %in% params$clinics$tly ~ "tly",
            SERVICE_FACI %in% params$clinics$sail ~ "sail",
            TRUE ~ NA_character_
         ),
         SERVICE_FACI       = case_when(
            special_clinic == "tly" ~ "130001",
            special_clinic == "sail" ~ "130025",
            TRUE ~ SERVICE_FACI
         ),

         # satellite
         SATELLITE_FACI     = if_else(
            condition = str_left(CLIENT_TYPE, 1) == "5",
            true      = FACI_DISP,
            false     = NA_character_
         ),
         SATELLITE_SUB_FACI = if_else(
            condition = str_left(CLIENT_TYPE, 1) == "5",
            true      = SUB_FACI_DISP,
            false     = NA_character_
         ),

         # transient
         TRANSIENT_FACI     = if_else(
            condition = str_left(CLIENT_TYPE, 1) == "6",
            true      = FACI_DISP,
            false     = NA_character_
         ),
         TRANSIENT_SUB_FACI = if_else(
            condition = str_left(CLIENT_TYPE, 1) == "6",
            true      = SUB_FACI_DISP,
            false     = NA_character_
         ),
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
         ART_FACI     = SERVICE_FACI,
         ART_SUB_FACI = SERVICE_SUB_FACI,
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
               CD4_DATE,
               CD4_RESULT,
               CENTRAL_ID
            ),
         by = join_by(CENTRAL_ID)
      ) %>%
      mutate(
         # calculate distance from confirmatory date
         CD4_DATE     = as.Date(CD4_DATE),
         CD4_ENROLL   = interval(CD4_DATE, VISIT_DATE) / days(1),

         # baseline is within 182 days
         BASELINE_CD4 = if_else(
            CD4_ENROLL >= -182 & CD4_ENROLL <= 182,
            1,
            0
         ),

         # make values absolute to take date nearest to confirmatory
         CD4_ENROLL   = abs(CD4_ENROLL),
      ) %>%
      arrange(CENTRAL_ID, CD4_ENROLL) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE)

   return(data)
}

##  Facilities & Address -------------------------------------------------------

convert_faci_addr <- function(data) {
   log_info("Converting address & facility data.")
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
      arrange(ART_FACI_CODE, VISIT_DATE, LATEST_NEXT_DATE) %>%
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
         art_id            = params$latest_art_id + row_number(),

         # report date
         year              = params$yr,
         month             = params$mo,

         # demographics
         initials          = str_squish(stri_c(str_left(FIRST, 1), str_left(MIDDLE, 1), str_left(LAST, 1))),
         SEX               = remove_code(stri_trans_toupper(SEX)),

         # clinical pic
         artstart_stage    = as.integer(keep_code(WHO_CLASS)),

         # pregnant
         pregnant          = as.integer(keep_code(IS_PREGNANT)),

         # cd4 tagging
         days_cd4_artstart = interval(CD4_DATE, VISIT_DATE) / days(1),
         cd4_is_baseline   = if_else(condition = days_cd4_artstart <= 182, 1, 0, 0),
         CD4_DATE          = case_when(
            cd4_is_baseline == 0 ~ NA_Date_,
            is.na(CD4_RESULT) ~ NA_Date_,
            TRUE ~ CD4_DATE
         ),
         CD4_RESULT        = case_when(
            cd4_is_baseline == 0 ~ NA_character_,
            TRUE ~ CD4_RESULT
         ),
         CD4_RESULT        = stri_replace_all_charclass(CD4_RESULT, "[:alpha:]", "") %>%
            stri_replace_all_fixed(" ", "") %>%
            stri_replace_all_fixed("<", "") %>%
            as.numeric(),
         baseline_cd4      = case_when(
            CD4_RESULT >= 500 ~ 1,
            CD4_RESULT >= 350 & CD4_RESULT < 500 ~ 2,
            CD4_RESULT >= 200 & CD4_RESULT < 350 ~ 3,
            CD4_RESULT >= 50 & CD4_RESULT < 200 ~ 4,
            CD4_RESULT < 50 ~ 5,
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
         REC_ID,
         CENTRAL_ID,
         PATIENT_ID,
         art_id,
         year,
         month,
         confirmatory_code       = CONFIRMATORY_CODE,
         px_code                 = PATIENT_CODE,
         uic                     = UIC,
         first                   = FIRST,
         middle                  = MIDDLE,
         last                    = LAST,
         suffix                  = SUFFIX,
         age                     = AGE,
         birthdate               = BIRTHDATE,
         sex                     = SEX,
         initials,
         philhealth_no           = PHILHEALTH_NO,
         philsys_id              = PHILSYS_ID,
         mobile                  = CLIENT_MOBILE,
         email                   = CLIENT_EMAIL,
         curr_reg,
         curr_prov,
         curr_munc,
         artstart_hub            = ART_FACI_CODE,
         artstart_branch         = ART_BRANCH,
         artstart_realhub        = ACTUAL_FACI_CODE,
         artstart_realhub_branch = ACTUAL_BRANCH,
         artstart_reg            = real_reg,
         artstart_prov           = real_prov,
         artstart_munc           = real_munc,
         artstart_stage,
         visit_type              = VISIT_TYPE,
         tx_status               = TX_STATUS,
         artstart_addr           = CURR_ADDR,
         artstart_date           = VISIT_DATE,
         artstart_nextpickup     = LATEST_NEXT_DATE,
         artstart_regimen        = MEDICINE_SUMMARY,
         artstart_num_arv        = NUM_OF_DRUGS,
         baseline_cd4,
         baseline_cd4_date       = CD4_DATE,
         baseline_cd4_result     = CD4_RESULT,
         pregnant,
         starts_with("CURR_PSGC")
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
         corr_defer = if_else(is.na(artstart_regimen), 1, 0, 0),
         drop_notart = 0,
      ) %>%
      bind_rows(
         old %>%
            mutate(artstart_stage = as.integer(artstart_stage))
      ) %>%
      arrange(art_id) %>%
      mutate(
         corr_defer = coalesce(corr_defer, 0),
         drop_notart = coalesce(drop_notart, 0),
      ) %>%
      zap_labels()

   return(data)
}

##  Tag data to be reported later on and duplicates for dropping ---------------

tag_fordrop <- function(data, corr) {
   log_info("Tagging enrollees for dropping.")
   for (drop_var in c("corr_defer", "drop_notart"))
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
         -starts_with("CURR_PSGC"),
      )

   return(data)
}

##  Merge w/ Dx Registry -------------------------------------------------------

merge_dx <- function(data, forms, params) {
   dx <- hs_data("harp_dx", "reg", params$yr, params$mo) %>%
      read_dta(
         col_select = c(
            PATIENT_ID,
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
      left_join(forms$confirm_last, join_by(CENTRAL_ID)) %>%
      mutate(
         confirm_date   = coalesce(confirm_date, as.Date(DATE_CONFIRM)),
         confirm_result = case_when(
            !is.na(idnum) ~ "1_Positive",
            TRUE ~ CONFIRM_RESULT
         )
      ) %>%
      rename(
         confirm_remarks = CONFIRM_REMARKS
      ) %>%
      mutate(
         # finalize age data
         age_dta           = calc_age(birthdate, artstart_date),
         age               = coalesce(age, age_dta),
         confirmatory_code = coalesce(labcode2, CONFIRM_CODE, confirmatory_code, str_c("*", coalesce(uic, px_code))),
         newonart          = if_else(
            condition = year(artstart_date) == params$yr & month(artstart_date) == params$mo,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
      ) %>%
      select(-CONFIRM_RESULT, -DATE_CONFIRM, -CONFIRM_CODE, -labcode2) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      relocate(idnum, .after = art_id) %>%
      arrange(art_id)

   return(data)
}

##  Merge w/ Tx Registry -------------------------------------------------------

merge_prep <- function(data, forms, params) {
   prep <- hs_data("prep", "reg", params$yr, params$mo) %>%
      read_dta(
         col_select = c(
            PATIENT_ID,
            prep_id
         )
      ) %>%
      get_cid(forms$id_registry, PATIENT_ID) %>%
      select(-PATIENT_ID)

   # get prep_id
   data %<>%
      select(-any_of("prep_id")) %>%
      left_join(
         y  = prep %>%
            distinct(CENTRAL_ID, .keep_all = TRUE),
         by = join_by(CENTRAL_ID)
      ) %>%
      relocate(prep_id, .after = idnum)

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

   # get mort_id
   data %<>%
      select(-any_of("mort_id")) %>%
      left_join(
         y  = dead %>%
            distinct(CENTRAL_ID, .keep_all = TRUE),
         by = join_by(CENTRAL_ID)
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
         arrange(reg_order, artstart_realhub, artstart_realhub_branch, art_id) %>%
         select(-reg_order)

      view_vars <- c(
         "REC_ID",
         "CENTRAL_ID",
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
            stri_detect_fixed(artstart_regimen, "FTC")
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking ART reports tagged as DOH-EB.")
      check[["art_eb"]] <- data %>%
         filter(
            artstart_hub == "DOH"
         ) %>%
         select(
            any_of(view_vars),
         )

      all_issues <- combine_validations(data, check, "REC_ID") %>%
         mutate(
            reg_order = artstart_reg,
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
         arrange(reg_order, artstart_realhub, artstart_realhub_branch, REC_ID) %>%
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
   new_reg <- remove_drops(new_reg) %>%
      select(-artstart_num_arv)

   step$check <- get_checks(data, p$params, p$corr, run_checks = vars$run_checks, exclude_drops = vars$exclude_drops)
   step$data  <- data

   p$official$new_reg <- new_reg
   append(p$official, drops)

   flow_validation(p, "tx_new", p$params$ym, upload = vars$upload)
   log_success("Done.")
}