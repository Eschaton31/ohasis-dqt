##  Generate subset variables --------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
.log_info("Generating `harp_tx`.`reg.converted`.")

# latest_art_id
.log_info("Getting latest `art_id` for reference.")
nhsss$harp_tx$params$latest_art_id <- max(as.integer(nhsss$harp_tx$official$old_reg$art_id), na.rm = TRUE)

# new clients
.log_info("Performing initial conversion.")
nhsss$harp_tx$reg.converted$data <- nhsss$harp_tx$reg.initial$data %>%
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

##  Address --------------------------------------------------------------------

.log_info("Attaching address names (HARP versions).")
nhsss$harp_tx$reg.converted$data %<>%
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

##  Finalize -------------------------------------------------------------------

.log_info("Finalizing dataframe.")
nhsss$harp_tx$reg.converted$data %<>%
   select(
      -any_of(
         c(
            "artstart_reg",
            "artstart_prov",
            "artstart_munc"
         )
      )
   ) %>%
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
   left_join(
      y  = ohasis$ref_faci %>%
         distinct(FACI_CODE, .keep_all = TRUE) %>%
         select(
            ART_BRANCH    = FACI_CODE,
            artstart_reg  = FACI_NHSSS_REG,
            artstart_prov = FACI_NHSSS_PROV,
            artstart_munc = FACI_NHSSS_MUNC,
         ) %>%
         mutate(
            ART_FACI_CODE = case_when(
               stri_detect_regex(ART_BRANCH, "^SAIL") ~ "SHP",
               stri_detect_regex(ART_BRANCH, "^TLY") ~ "TLY",
               TRUE ~ ART_BRANCH
            ),
            ART_BRANCH    = if_else(
               condition = nchar(ART_BRANCH) == 3,
               true      = NA_character_,
               false     = ART_BRANCH
            ),
            .before       = 1
         ),
      by = c("ART_FACI_CODE", "ART_BRANCH")
   ) %>%
   left_join(
      y  = ohasis$ref_faci %>%
         distinct(FACI_CODE, .keep_all = TRUE) %>%
         select(
            ACTUAL_BRANCH      = FACI_CODE,
            artstart_real_reg  = FACI_NHSSS_REG,
            artstart_real_prov = FACI_NHSSS_PROV,
            artstart_real_munc = FACI_NHSSS_MUNC,
         ) %>%
         mutate(
            ACTUAL_FACI_CODE = case_when(
               stri_detect_regex(ACTUAL_BRANCH, "^TLY") ~ "TLY",
               TRUE ~ ACTUAL_BRANCH
            ),
            ACTUAL_BRANCH    = if_else(
               condition = nchar(ACTUAL_BRANCH) == 3,
               true      = NA_character_,
               false     = ACTUAL_BRANCH
            ),
            .before          = 1
         ),
      by = c("ACTUAL_FACI_CODE", "ACTUAL_BRANCH")
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
      artstart_stage,
      artstart_reg,
      artstart_prov,
      artstart_munc,
      artstart_addr       = CURR_ADDR,
      artstart_date       = VISIT_DATE,
      artstart_realhub    = ACTUAL_FACI_CODE,
      artstart_realbranch = ACTUAL_BRANCH,
      real_reg,
      real_prov,
      real_munc,
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

##  Flag data for validation ---------------------------------------------------

update <- input(
   prompt  = "Run `reg.converted` validations?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
update <- substr(toupper(update), 1, 1)

nhsss$harp_tx$reg.converted$check <- list()
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
      var                                                                    <- as.symbol(var)
      nhsss$harp_tx$reg.converted$check[[paste0("dup_", as.character(var))]] <- nhsss$harp_tx$reg.converted$data %>%
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
      var                                      <- as.symbol(var)
      nhsss$harp_tx$reg.converted$check[[var]] <- nhsss$harp_tx$reg.converted$data %>%
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
   vars <- c(
      "age_pregnant",
      "age"
   )
   .log_info("Checking range-median of data.")
   nhsss$harp_tx$reg.converted$check$tabstat <- data.frame()
   for (var in vars) {
      var <- as.symbol(var)
      df  <- nhsss$harp_tx$reg.converted$data

      nhsss$harp_tx$reg.converted$check$tabstat <- df %>%
         summarise(
            VARIABLE = as.character(var),
            MIN      = suppress_warnings(min(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            MEDIAN   = suppress_warnings(median(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            MAX      = suppress_warnings(max(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            NAs      = sum(if_else(is.na(!!var), 1, 0, 0))
         ) %>%
         mutate_all(~as.character(.)) %>%
         bind_rows(nhsss$harp_tx$reg.converted$check$tabstat)
   }
}

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
data_name <- "reg.converted"
if (!is.empty(nhsss$harp_tx[[data_name]]$check))
   .validation_gsheets(
      data_name   = data_name,
      parent_list = nhsss$harp_tx[[data_name]]$check,
      drive_path  = paste0(nhsss$harp_tx$gdrive$path$report, "Validation/"),
      surv_name   = "HARP Tx"
   )

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
