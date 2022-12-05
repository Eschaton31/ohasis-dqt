##  Generate subset variables --------------------------------------------------

standardize_data <- function(initial) {
   data <- initial %>%
      mutate(
         # generate idnum
         art_id              = nhsss$harp_tx$params$latest_art_id + row_number(),

         # report date
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

         # clinical pic
         artstart_stage      = StrLeft(WHO_CLASS, 1) %>% as.integer(),

         # class
         # class               = case_when(
         #    AGE >= 10 ~ "Adult Patient",
         #    AGE < 10 ~ "Pedaitric Patient",
         #    TRUE ~ "(no data)"
         # ),

         # pregnant
         pregnant            = if_else(
            condition = StrLeft(IS_PREGNANT, 1) == "1",
            true      = 1 %>% as.integer(),
            false     = NA_integer_
         ),


         # cd4 tagging
         CD4_RESULT          = stri_replace_all_charclass(CD4_RESULT, "[:alpha:]", ""),
         CD4_RESULT          = stri_replace_all_fixed(CD4_RESULT, " ", ""),
         CD4_RESULT          = stri_replace_all_fixed(CD4_RESULT, "<", ""),
         CD4_RESULT          = as.numeric(CD4_RESULT),
         baseline_cd4        = case_when(
            CD4_RESULT >= 500 ~ 1,
            CD4_RESULT >= 350 & CD4_RESULT < 500 ~ 2,
            CD4_RESULT >= 200 & CD4_RESULT < 350 ~ 3,
            CD4_RESULT >= 50 & CD4_RESULT < 200 ~ 4,
            CD4_RESULT < 50 ~ 5,
         ),
         CD4_DATE            = if_else(
            condition = is.na(CD4_RESULT),
            true      = NA_Date_,
            false     = CD4_DATE
         ),

         # WHO Case Definition of advanced HIV classification
         CD4_ENROLL          = difftime(VISIT_DATE, CD4_DATE, units = "days") %>% as.numeric(),
         baseline_cd4        = if_else(
            condition = CD4_ENROLL <= 182,
            true      = baseline_cd4,
            false     = as.numeric(NA)
         ),
         CD4_DATE            = if_else(
            condition = CD4_ENROLL <= 182,
            true      = CD4_DATE,
            false     = as.numeric(NA)
         ),
         CD4_RESULT          = if_else(
            condition = CD4_ENROLL <= 182,
            true      = CD4_RESULT,
            false     = as.numeric(NA)
         ),
      )

   return(data)
}

##  Address --------------------------------------------------------------------

convert_address <- function(data) {
   data %<>%
      mutate_at(
         .vars = vars(contains("_PSGC_")),
         ~if_else(is.na(.), "", .)
      ) %>%
      # current
      rename_at(
         .vars = vars(starts_with("CURR_PSGC")),
         ~stri_replace_all_fixed(., "CURR_", "")
      ) %>%
      left_join(
         y  = ohasis$ref_addr %>%
            select(
               PSGC_REG,
               PSGC_PROV,
               PSGC_MUNC,
               artstart_reg  = NHSSS_REG,
               artstart_prov = NHSSS_PROV,
               artstart_munc = NHSSS_MUNC,
            ),
         by = c("PSGC_REG", "PSGC_PROV", "PSGC_MUNC")
      ) %>%
      relocate(artstart_reg, artstart_prov, artstart_munc, .before = PSGC_REG)

   return(data)
}

##  Finalize -------------------------------------------------------------------

final_conversion <- function(data) {
   data %<>%
      mutate(
         # finalize ACTUAL_FACI_CODE data
         ACTUAL_BRANCH    = if_else(
            condition = is.na(ACTUAL_FACI_CODE),
            true      = ART_BRANCH,
            false     = ACTUAL_BRANCH
         ),
         ACTUAL_FACI_CODE = if_else(
            condition = is.na(ACTUAL_FACI_CODE),
            true      = ART_FACI_CODE,
            false     = ACTUAL_FACI_CODE
         )
      ) %>%
      # same vars as registry
      select(
         REC_ID,
         CENTRAL_ID,
         PATIENT_ID,
         art_id,
         year,
         month,
         confirmatory_code   = CONFIRMATORY_CODE,
         px_code             = PATIENT_CODE,
         uic                 = UIC,
         first               = FIRST,
         middle              = MIDDLE,
         last                = LAST,
         suffix              = SUFFIX,
         age                 = AGE,
         birthdate           = BIRTHDATE,
         sex                 = SEX,
         initials,
         philsys_id          = PHILSYS_ID,
         philhealth_no       = PHILHEALTH_NO,
         artstart_hub        = ART_FACI_CODE,
         artstart_branch     = ART_BRANCH,
         artstart_realhub    = ACTUAL_FACI_CODE,
         artstart_realbranch = ACTUAL_BRANCH,
         artstart_stage,
         artstart_reg,
         artstart_prov,
         artstart_munc,
         artstart_addr       = CURR_ADDR,
         artstart_date       = VISIT_DATE,
         baseline_cd4,
         baseline_cd4_date   = CD4_DATE,
         baseline_cd4_result = CD4_RESULT,
         pregnant
      ) %>%
      # turn into codes
      mutate_at(
         .vars = vars(
            pregnant,
         ),
         ~if_else(
            condition = !is.na(.),
            true      = substr(., 1, stri_locate_first_fixed(., "_") - 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # remove codes
      mutate_at(
         .vars = vars(
            sex,
         ),
         ~if_else(
            condition = !is.na(.),
            true      = substr(., stri_locate_first_fixed(., "_") + 1, stri_length(.)),
            false     = NA_character_
         )
      ) %>%
      # convert dates
      mutate_at(
         .vars = vars(contains("date")),
         ~if_else(
            condition = !is.na(.),
            true      = as.Date(.),
            false     = NA_Date_
         )
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
         "confirmatory_code",
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
               confirmatory_code,
               uic,
               first,
               middle,
               last,
               suffix,
               birthdate,
               sex,
               artstart_hub,
               !!var
            ) %>%
            get_dupes(!!var)
      }

      # unknown data
      vars <- c(
         "artstart_reg",
         "artstart_prov",
         "artstart_munc"
      )
      .log_info("Checking if required variables have UNKNOWN data or unpaired NHSSS versions.")
      for (var in vars) {
         var          <- as.symbol(var)
         check[[var]] <- data %>%
            filter(
               !!var %in% c("UNKNOWN", "OTHERS", NA_character_)
            ) %>%
            select(
               REC_ID,
               PATIENT_ID,
               confirmatory_code,
               uic,
               first,
               middle,
               last,
               suffix,
               birthdate,
               sex,
               artstart_hub,
               !!var
            )
      }

      # range-median
      .log_info("Checking range-median of data.")
      check$tabstat <- data %>%
         tabstat(
            age,
            age_pregnant
         )
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

   local(envir = .GlobalEnv, flow_validation(nhsss$harp_tx, "reg.converted", ohasis$ym))
}