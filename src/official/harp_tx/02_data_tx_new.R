##  Filter Initial Data & Remove Already Reported ------------------------------

clean_data <- function(art_first, old_reg, params) {
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
      clean_dates() %>%
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
         standard_first = stri_trans_general(first, "latin-ascii"),
         name           = str_squish(stri_c(last, ", ", first, " ", middle, " ", suffix)),
      ) %>%
      tag_visit_faci(params)

   return(data)
}

##  Sort by earliest visit of client for the report ----------------------------

prioritize_reports <- function(data) {
   log_info("Using first visited facility.")
   return(pick_visit(data))
}

##  Adding CD4 results ---------------------------------------------------------

# all cd4 results of the enrollees, across every patient_id linked to them
download_cd4 <- function(enrollees, id_registry, params) {
   log_info("Downloading {green('CD4 Data')} of enrollees.")
   cids <- unique(enrollees$central_id)
   pids <- id_registry %>%
      filter(central_id %in% cids) %>%
      pull(patient_id) %>%
      # unregistered clients use their patient_id as central_id
      c(cids) %>%
      unique()

   if (length(pids) == 0)
      return(tibble(cd4_date = as.Date(character()), cd4_result = character(), central_id = character()))

   lw_conn <- connect('mariadb-lw')
   on.exit(dbDisconnect(lw_conn), add = TRUE)

   QB$new(lw_conn)$
      from("ohasis_lake.lab_wide as labs")$
      leftJoin('ohasis_lake.px_demographics as pii', 'labs.rec_id', '=', 'pii.rec_id')$
      select("labs.lab_cd4_date as cd4_date", "labs.lab_cd4_result as cd4_result", "pii.patient_id")$
      whereRaw(glue("DATE(labs.lab_cd4_date) <= '{params$max}' and labs.deleted_at is null"))$
      whereIn("pii.patient_id", pids)$
      get(format = 'Arrow') %>%
      get_cid(id_registry, patient_id)
}

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
   data %<>%
      convert_faci() %>%
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
         # baseline is w/in 182 days before or after art start
         cd4_is_baseline   = if_else(condition = abs(days_cd4_artstart) <= 182, 1, 0, 0),
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

append_data <- function(old, new) {
   log_info("Appending enrollees to final registry.")
   data <- new %>%
      mutate(
         # enrollees w/o a regimen are reported once dispensed
         corr_defer   = if_else(is.na(artstart_regimen), 1, 0, 0),
         age          = as.integer(age),
         baseline_cd4 = to_character(baseline_cd4)
      ) %>%
      bind_rows(
         old %>%
            mutate(artstart_stage = as.integer(artstart_stage))
      ) %>%
      arrange(art_id) %>%
      mutate(
         corr_defer = coalesce(corr_defer, 0),
      ) %>%
      zap_labels()

   return(data)
}

##  Drop using taggings --------------------------------------------------------

remove_drops <- function(data) {
   log_info("Dropping clients w/ issue from final registry.")
   data %<>%
      filter(corr_defer == 0) %>%
      select(
         -corr_defer,
         -starts_with("curr"),
      )

   return(data)
}

##  Merge w/ Dx Registry -------------------------------------------------------

# lookups are de-duplicated by central_id, so registry rows are never dropped here
merge_dx <- function(data, forms, params) {
   dx_cols <- c(
      "labcode",
      "labcode2",
      "idnum",
      "uic",
      "firstname",
      "middle",
      "last",
      "name_suffix",
      "bdate",
      "sex",
      "pxcode",
      "philhealth",
      "confirm_date"
   )
   dx      <- lw_registry("harp_dx", dx_cols, params, forms$id_registry) %>%
      mutate(
         labcode2 = coalesce(labcode2, labcode)
      )

   data %<>%
      select(-starts_with("labcode2"), -starts_with("dxreg")) %>%
      left_join(
         y  = dx %>%
            arrange(idnum) %>%
            distinct(central_id, .keep_all = TRUE) %>%
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
            distinct(idnum, .keep_all = TRUE) %>%
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
      left_join(forms$confirm_last %>% distinct(central_id, .keep_all = TRUE), join_by(central_id)) %>%
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
      relocate(idnum, .after = art_id) %>%
      arrange(art_id)

   return(data)
}

##  Merge w/ Tx Registry -------------------------------------------------------

merge_prep <- function(data, forms, params) {
   prep <- lw_registry("prep", "prep_id", params, forms$id_registry)

   # get prep_id
   data %<>%
      select(-any_of("prep_id")) %>%
      left_join(
         y  = prep %>%
            arrange(prep_id) %>%
            distinct(central_id, .keep_all = TRUE),
         by = join_by(central_id)
      ) %>%
      relocate(prep_id, .after = idnum)

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
      select(-any_of("mort_id")) %>%
      left_join(
         y  = dead %>%
            arrange(mort_id) %>%
            distinct(central_id, .keep_all = TRUE),
         by = join_by(central_id)
      ) %>%
      relocate(mort_id, .after = prep_id)

   return(data)
}

##  Flag data for validation ---------------------------------------------------

get_checks <- function(data, params, corr, run_checks = NULL, exclude_drops = NULL) {
   check   <- list()
   answers <- ask_checks("tx_new", run_checks, exclude_drops)

   if (answers$run == "1") {
      data %<>%
         arrange(region_order(artstart_reg), artstart_realhub, artstart_realhub_branch, art_id)

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
         arrange(region_order(artstart_reg), artstart_realhub, artstart_realhub_branch, rec_id)

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

   data <- clean_data(p$forms$art_first, p$official$old_reg, p$params)
   data <- prioritize_reports(data)

   step$lab_cd4 <- download_cd4(data, p$forms$id_registry, p$params)
   data         <- get_cd4(data, step$lab_cd4)
   data         <- convert_faci_addr(data)
   data         <- standardize_data(data, p$params)
   data         <- final_conversion(data)

   new_reg <- p$official$old_reg %>%
      append_data(data) %>%
      merge_dx(p$forms, p$params) %>%
      merge_dead(p$forms, p$params) %>%
      merge_prep(p$forms, p$params)

   new_reg    <- tag_fordrop(new_reg, p$corr)
   drops      <- subset_drops(new_reg)
   new_reg    <- remove_drops(new_reg)
   step$dupes <- get_reg_dupes(new_reg)

   step$check <- get_checks(data, p$params, p$corr, run_checks = vars$run_checks, exclude_drops = vars$exclude_drops)
   step$data  <- data

   p$official$new_reg       <- new_reg
   p$official[names(drops)] <- drops

   flow_validation(p, "tx_new", p$params$ym, upload = vars$upload)
   log_success("Done.")
}