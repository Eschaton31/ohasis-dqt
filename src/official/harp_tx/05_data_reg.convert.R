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
      )
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
   # same vars as registry
   select(
      REC_ID,
      CENTRAL_ID,
      PATIENT_ID,
      art_id,
      year,
      month,
      confirmatory_code = CONFIRMATORY_CODE,
      px_code           = PATIENT_CODE,
      uic               = UIC,
      first             = FIRST,
      middle            = MIDDLE,
      last              = LAST,
      suffix            = SUFFIX,
      age               = AGE,
      birthdate         = BIRTHDATE,
      sex               = SEX,
      initials,
      philsys_id        = PHILSYS_ID,
      philhealth_no     = PHILHEALTH_NO,
      artstart_hub      = ART_FACI_CODE,
      artstart_stage,
      artstart_reg,
      artstart_prov,
      artstart_munc,
      artstart_addr     = CURR_ADDR,
      artstart_date     = VISIT_DATE,
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
if (update == "1") {
   # initialize checking layer
   nhsss$harp_tx$reg.converted$check <- list()
   # duplicate id variables
   vars                          <- c(
      "confirmatory_code",
      "uic",
      "px_code",
      "philhealth_no",
      "philsys_id"
   )
   .log_info("Checking if id variables are duplicated.")
   for (var in vars) {
      var                                                                <- as.symbol(var)
      nhsss$harp_tx$reg.converted$check[[paste0("dup_", as.character(var))]] <- nhsss$harp_tx$reg.converted$data %>%
         filter(
            is.na(!!var)
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
      var                                  <- as.symbol(var)
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
            MIN      = min(!!var, na.rm = TRUE),
            MEDIAN   = median(!!var, na.rm = TRUE),
            MAX      = max(!!var, na.rm = TRUE),
            NAs      = sum(if_else(is.na(!!var), 1, 0, 0))
         ) %>%
         mutate_all(~as.character(.)) %>%
         bind_rows(nhsss$harp_tx$reg.converted$check$tabstat)
   }
}

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
data_name <- "reg.converted"
if ("check" %in% names(nhsss$harp_tx[[data_name]]))
   .validation_gsheets(
      data_name   = data_name,
      parent_list = nhsss$harp_tx[[data_name]]$check,
      drive_path  = paste0(nhsss$harp_tx$gdrive$path$report, "Validation/"),
      surv_name   = "Tx"
   )

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
