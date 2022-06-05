##  Generate subset variables --------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
.log_info("Generating `prep`.`reg.converted`.")

# latest_prep_id
.log_info("Getting latest `prep_id` for reference.")
nhsss$prep$params$latest_prep_id <- max(as.integer(nhsss$prep$official$old_reg$prep_id), na.rm = TRUE)

# new clients
.log_info("Performing initial conversion.")
nhsss$prep$reg.converted$data <- nhsss$prep$reg.initial$data %>%
   arrange(VISIT_DATE, CREATED_AT) %>%
   mutate(
      # generate idnum
      prep_id             = nhsss$prep$params$latest_prep_id + row_number(),

      # # report date
      # year                = ohasis$yr %>% as.integer(),
      # month               = ohasis$mo %>% as.integer(),
      year                = year(VISIT_DATE) %>% as.integer(),
      month               = month(VISIT_DATE) %>% as.integer(),

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
   )

##  Finalize -------------------------------------------------------------------

.log_info("Finalizing dataframe.")
nhsss$prep$reg.converted$data %<>%
   # same vars as registry
   select(
      REC_ID,
      CENTRAL_ID,
      PATIENT_ID,
      prep_id,
      year,
      month,
      px_code           = PATIENT_CODE,
      uic               = UIC,
      first             = FIRST,
      middle            = MIDDLE,
      last              = LAST,
      suffix            = SUFFIX,
      age               = AGE,
      birthdate         = BIRTHDATE,
      sex               = SEX,
      self_identity,
      self_identity_other,
      philsys_id        = PHILSYS_ID,
      philhealth_no     = PHILHEALTH_NO,
      initials,
      mobile            = CLIENT_MOBILE,
      email             = CLIENT_EMAIL,
      prep_first_screen = VISIT_DATE,
      prep_first_faci   = PREP_FACI_CODE,
      prep_first_branch = PREP_BRANCH,
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
   )

##  Flag data for validation ---------------------------------------------------

update <- input(
   prompt  = "Run `reg.converted` validations?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
update <- substr(toupper(update), 1, 1)

nhsss$prep$reg.converted$check <- list()
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
      var                                                                 <- as.symbol(var)
      nhsss$prep$reg.converted$check[[paste0("dup_", as.character(var))]] <- nhsss$prep$reg.converted$data %>%
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
}

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
data_name <- "reg.converted"
if (!is.empty(nhsss$prep[[data_name]]$check))
   .validation_gsheets(
      data_name   = data_name,
      parent_list = nhsss$prep[[data_name]]$check,
      drive_path  = paste0(nhsss$prep$gdrive$path$report, "Validation/"),
      surv_name   = "HARP Tx"
   )

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
