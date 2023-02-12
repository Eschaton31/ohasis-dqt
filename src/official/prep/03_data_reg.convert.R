##  Generate subset variables --------------------------------------------------

standardize_data <- function(initial) {
   data <- data %>%
      arrange(VISIT_DATE, CREATED_AT) %>%
      mutate(
         # generate idnum
         prep_id             = .GlobalEnv$nhsss$prep$params$latest_prep_id + row_number(),

         # # report date
         year                = ohasis$yr %>% as.integer(),
         month               = ohasis$mo %>% as.integer(),

         # demographics
         initials            = paste0(
            if_else(
               condition = !is.na(FIRST),
               true      = StrLeft(FIRST, 1),
               false     = ""
            ),
            if_else(
               condition = !is.na(MIDDLE),
               true      = StrLeft(MIDDLE, 1),
               false     = ""
            ),
            if_else(
               condition = !is.na(LAST),
               true      = StrLeft(LAST, 1),
               false     = ""
            )
         ),
         SEX                 = stri_trans_toupper(SEX),
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
      generate_gender_identity(SEX, SELF_IDENT, SELF_IDENT_OTHER, gender_identity)

   return(data)
}

##  Address --------------------------------------------------------------------

convert_address <- function(data) {
   data %<>%
      mutate(
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
      ) %>%
      ohasis$get_addr(
         c(
            prep_first_reg  = "PERM_PSGC_REG",
            prep_first_prov = "PERM_PSGC_PROV",
            prep_first_munc = "PERM_PSGC_MUNC"
         ),
         "nhsss"
      )

   return(data)
}

##  Finalize -------------------------------------------------------------------

final_conversion <- function(data) {
   data %<>%
      # same vars as registry
      select(
         REC_ID,
         CENTRAL_ID,
         PATIENT_ID,
         prep_id,
         year,
         month,
         prep_form         = FORM_VERSION,
         px_code           = PATIENT_CODE,
         uic               = UIC,
         first             = FIRST,
         middle            = MIDDLE,
         last              = LAST,
         suffix            = SUFFIX,
         age               = AGE,
         birthdate         = BIRTHDATE,
         sex               = SEX,
         philsys_id        = PHILSYS_ID,
         philhealth_no     = PHILHEALTH_NO,
         initials,
         self_identity,
         self_identity_other,
         gender_identity,
         mobile            = CLIENT_MOBILE,
         email             = CLIENT_EMAIL,
         weight            = WEIGHT,
         body_temp         = FEVER,
         prep_first_reg,
         prep_first_prov,
         prep_first_munc,
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
         prep_hts_date     = PREP_HIV_DATE,
         starts_with("prep_risk_", ignore.case = FALSE),
         starts_with("hts_risk_", ignore.case = FALSE),
         prep_first_time   = FIRST_TIME,
         prep_first_screen = VISIT_DATE,
         prep_first_faci   = PREP_FACI_CODE,
         prep_first_branch = PREP_BRANCH,
      ) %>%
      # remove codes
      mutate_at(
         .vars = vars(
            sex,
         ),
         ~remove_code(.)
      )

   return(data)
}

##  Flag data for validation ---------------------------------------------------

get_checks <- function(data) {
   check  <- list()
   update <- input(
      prompt  = "Run `reg.converted` validations?",
      options = c("1" = "yes", "2" = "no"),
      default = "1"
   )
   update <- substr(toupper(update), 1, 1)

   if (update == "1") {
      # initialize checking layer
      # duplicate id variables
      vars <- c(
         "uic",
         "px_code",
         "philhealth_no",
         "philsys_id"
      )
      .log_info("Checking if id variables are duplicated.")
      for (var in vars) {
         var                                        <- as.symbol(var)
         check[[paste0("dup_", as.character(var))]] <- data %>%
            filter(
               !is.na(!!var)
            ) %>%
            select(
               REC_ID,
               PATIENT_ID,
               uic,
               first,
               middle,
               last,
               suffix,
               birthdate,
               sex,
               prep_first_faci,
               prep_first_reg,
               prep_first_prov,
               prep_first_munc,
               !!var
            ) %>%
            get_dupes(!!var)
      }
   }

   return(check)
}

##  Actual flow ----------------------------------------------------------------

.init <- function() {
   p <- parent.env(environment())
   local(envir = p, {
      data <- read_rds(file.path(wd, "reg.initial.RDS"))
      data <- standardize_data(data) %>%
         convert_address() %>%
         final_conversion()

      write_rds(data, file.path(wd, "reg.converted.RDS"))

      check <- get_checks(data)
   })

   local(envir = .GlobalEnv, flow_validation(nhsss$prep, "reg.converted", ohasis$ym))
}