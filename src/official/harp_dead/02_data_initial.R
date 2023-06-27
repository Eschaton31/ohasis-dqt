##  Filter Initial Data & Remove Already Reported ------------------------------

clean_data <- function(forms, old_reg) {
   # open connections
   log_info("Generating `harp_dead`.`initial`.")

   # Form A + HTS Forms
   log_info("Dropping already-reported records.")
   data <- forms$form_d %>%
      get_cid(forms$id_registry, PATIENT_ID) %>%
      # keep only patients not in registry
      anti_join(
         y  = old_reg %>%
            select(CENTRAL_ID),
         by = "CENTRAL_ID"
      ) %>%
      mutate_at(
         .vars = vars(FIRST, MIDDLE, LAST, SUFFIX),
         ~str_squish(coalesce(toupper(.), ""))
      ) %>%
      mutate(
         # date variables
         encoded_date   = as.Date(CREATED_AT),
         report_date    = RECORD_DATE,

         # name
         STANDARD_FIRST = stri_trans_general(FIRST, "latin-ascii"),
         fullname       = str_squish(stri_c(LAST, ", ", FIRST, " ", MIDDLE, " ", SUFFIX)),

         # Permanent
         use_curr       = if_else(
            condition = (is.na(PERM_PSGC_MUNC) & !is.na(CURR_PSGC_MUNC)) | StrLeft(PERM_PSGC_MUNC, 2) == '99',
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
         AGE            = if_else(
            condition = is.na(AGE) & !is.na(AGE_MO),
            true      = AGE_MO / 12,
            false     = as.double(AGE)
         ),
         AGE_DTA        = case_when(
            !is.na(BIRTHDATE) & !is.na(DEATH_DATE) ~ floor((DEATH_DATE - BIRTHDATE) / 365.25),
            !is.na(BIRTHDATE) & is.na(DEATH_DATE) ~ floor((report_date - BIRTHDATE) / 365.25),
         ) %>% as.numeric(),

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
   data %<>%
      # remove invalid reports
      filter(not_dead == 0) %>%
      # prioritize form d over form bc
      arrange(desc(REPORTING_FORM), report_date, DEATH_DATE) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      filter(report_date < ohasis$next_date |
                DEATH_DATE < ohasis$next_date |
                is.na(report_date)) %>%
      rename(
         MORT_FACI     = SERVICE_FACI,
         MORT_SUB_FACI = SERVICE_SUB_FACI,
      )

   return(data)
}

##  Facilities -----------------------------------------------------------------

attach_faci_names <- function(data) {
   faci_ids <- list(
      c("FACI_ID", "SUB_FACI_ID", "FACI_NAME"),
      c("MORT_FACI", "MORT_SUB_FACI", "MORT_FACI_NAME")
   )

   for (i in seq_len(length(faci_ids))) {
      faci_id     <- faci_ids[[i]][1] %>% as.symbol()
      faci_name   <- faci_ids[[i]][3] %>% as.symbol()
      sub_faci_id <- faci_ids[[i]][2] %>% as.symbol()

      # rename columns
      data %<>%
         # clean variables first
         mutate(
            !!faci_id     := if_else(
               condition = is.na(!!faci_id),
               true      = "",
               false     = !!faci_id
            ),
            !!sub_faci_id := case_when(
               is.na(!!sub_faci_id) ~ "",
               StrLeft(!!sub_faci_id, 6) != !!faci_id ~ "",
               TRUE ~ !!sub_faci_id
            )
         ) %>%
         # get referenced data
         left_join(
            y  = ohasis$ref_faci %>%
               select(
                  !!faci_id     := FACI_ID,
                  !!sub_faci_id := SUB_FACI_ID,
                  !!faci_name   := FACI_LABEL
               ),
            by = c(as.character(faci_id), as.character(sub_faci_id))
         ) %>%
         # move then rename to old version
         relocate(!!faci_name, .after = !!sub_faci_id)
   }
   return(data)
}

##  Flag data for validation ---------------------------------------------------

get_checks <- function(data, pdf_rhivda) {
   update <- input(
      prompt  = "Run `initial` validations?",
      options = c("1" = "yes", "2" = "no"),
      default = "1"
   )
   update <- substr(toupper(update), 1, 1)

   check <- list()
   if (update == "1") {
      # initialize checking layer
      view_vars <- c(
         "REC_ID",
         "PATIENT_ID",
         "REPORTING_FORM",
         "CONFIRMATORY_CODE",
         "UIC",
         "PATIENT_CODE",
         "FIRST",
         "MIDDLE",
         "LAST",
         "SUFFIX",
         "BIRTHDATE",
         "SEX",
         "MORT_FACI_NAME",
         "VISIT_DATE",
         "DEATH_DATE",
         "IMMEDIATE_CAUSES",
         "ANTECEDENT_CAUSES",
         "UNDERLYING_CAUSES"
      )

      # dates
      date_vars <- c(
         "encoded_date",
         "report_date",
         "DEATH_DATE",
         "BIRTHDATE"
      )
      check     <- check_dates(data, check, view_vars, date_vars)

      # non-negotiable variables
      nonnegotiables <- c(
         "FORM_VERSION",
         "MORT_FACI",
         "PERM_PSGC_REG",
         "PERM_PSGC_PROV",
         "PERM_PSGC_MUNC",
         "DEATH_PSGC_REG",
         "DEATH_PSGC_PROV",
         "DEATH_PSGC_MUNC",
         "FIRST",
         "LAST",
         "CENTRAL_ID",
         "AGE",
         "SEX",
         "DISEASE_HIV"
      )
      check          <- check_nonnegotiables(data, check, view_vars, nonnegotiables)

      # special checks
      log_info("Checking for data w/o cause of death.")
      check[["no_cause"]] <- data %>%
         filter(
            is.na(IMMEDIATE_CAUSES),
            is.na(ANTECEDENT_CAUSES),
            is.na(UNDERLYING_CAUSES),
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for death report from Form BC.")
      check[["formbc_dead"]] <- data %>%
         filter(
            REPORTING_FORM == "Form BC"
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for death reports that are still for investigation.")
      check[["confirm_if_dead"]] <- data %>%
         filter(
            StrLeft(EB_VALIDATED, 1) == "3" | is.na(EB_VALIDATED)
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for short names.")
      check[["short_name"]] <- data %>%
         mutate(
            n_first  = nchar(FIRST),
            n_middle = nchar(MIDDLE),
            n_last   = nchar(LAST),
            n_name   = n_first + n_middle + n_last,
         ) %>%
         filter(
            n_name <= 10 | n_first <= 3 | n_last <= 3
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking calculated age vs computed age.")
      check[["mismatch_age"]] <- data %>%
         filter(
            AGE != AGE_DTA
         ) %>%
         select(
            any_of(view_vars),
            AGE,
            AGE_DTA
         )

      # range-median
      tabstat <- c(
         "encoded_date",
         "report_date",
         "DEATH_DATE",
         "BIRTHDATE",
         "AGE",
         "AGE_MO"
      )
      check   <- check_tabstat(data, check, tabstat)
   }

   return(check)
}

##  Actual flow ----------------------------------------------------------------

.init <- function() {
   p <- parent.env(environment())
   local(envir = p, {
      data <- clean_data(.GlobalEnv$nhsss$harp_dead$forms, .GlobalEnv$nhsss$harp_dead$official$old) %>%
         prioritize_reports() %>%
         attach_faci_names()

      write_rds(data, file.path(wd, "initial.RDS"))

      check <- get_checks(data, pdf_rhivda)
   })

   local(envir = .GlobalEnv, flow_validation(nhsss$harp_dead, "initial", ohasis$ym))
}
