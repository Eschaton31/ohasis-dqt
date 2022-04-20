##  Generate subset variables --------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
.log_info("Generating `harp_dead`.`converted`.")

# latest_mort_id
.log_info("Getting latest `idnum` for reference.")
nhsss$harp_dead$params$latest_mort_id <- max(as.integer(nhsss$harp_dead$official$old$mort_id), na.rm = TRUE)

# new clients
.log_info("Performing initial conversion.")
nhsss$harp_dead$converted$data <- nhsss$harp_dead$initial$data %>%
   mutate(
      # generate idnum
      mort_id            = nhsss$harp_dead$params$latest_mort_id + row_number(),

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

      # report date
      year               = ohasis$yr %>% as.integer(),
      month              = ohasis$mo %>% as.integer(),

      # demographics
      pxcode             = paste0(
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
      SEX                = stri_trans_toupper(SEX),
      CIVIL_STATUS       = stri_trans_toupper(CIVIL_STATUS),
   )

##  Address --------------------------------------------------------------------

.log_info("Attaching address names (HARP versions).")
nhsss$harp_dead$converted$data %<>%
   mutate_at(
      .vars = vars(contains("_PSGC_")),
      ~if_else(is.na(.), "", .)
   ) %>%
   # permanent
   rename_at(
      .vars = vars(starts_with("PERM_PSGC")),
      ~stri_replace_all_fixed(., "PERM_", "")
   ) %>%
   left_join(
      y  = ohasis$ref_addr %>%
         select(
            PSGC_REG,
            PSGC_PROV,
            PSGC_MUNC,
            region   = NHSSS_REG,
            province = NHSSS_PROV,
            muncity  = NHSSS_MUNC,
         ),
      by = c("PSGC_REG", "PSGC_PROV", "PSGC_MUNC")
   ) %>%
   relocate(muncity, province, region, .before = PSGC_REG) %>%
   select(
      -PSGC_REG,
      -PSGC_PROV,
      -PSGC_MUNC
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
            last_residence_region   = NHSSS_REG,
            last_residence_province = NHSSS_PROV,
            last_residence_muncity  = NHSSS_MUNC,
         ),
      by = c("PSGC_REG", "PSGC_PROV", "PSGC_MUNC")
   ) %>%
   relocate(last_residence_muncity, last_residence_province, last_residence_region, .before = PSGC_REG) %>%
   select(
      -PSGC_REG,
      -PSGC_PROV,
      -PSGC_MUNC
   ) %>%
   # birth
   rename_at(
      .vars = vars(starts_with("BIRTH_PSGC")),
      ~stri_replace_all_fixed(., "BIRTH_", "")
   ) %>%
   left_join(
      y  = ohasis$ref_addr %>%
         select(
            PSGC_REG,
            PSGC_PROV,
            PSGC_MUNC,
            birthplace_region       = NHSSS_REG,
            birthplace_province     = NHSSS_PROV,
            birthplace_municipality = NHSSS_MUNC,
         ),
      by = c("PSGC_REG", "PSGC_PROV", "PSGC_MUNC")
   ) %>%
   relocate(birthplace_municipality, birthplace_province, birthplace_region, .before = PSGC_REG) %>%
   select(
      -PSGC_REG,
      -PSGC_PROV,
      -PSGC_MUNC
   ) %>%
   # death region
   rename_at(
      .vars = vars(starts_with("DEATH_PSGC")),
      ~stri_replace_all_fixed(., "DEATH_", "")
   ) %>%
   left_join(
      y  = ohasis$ref_addr %>%
         select(
            PSGC_REG,
            PSGC_PROV,
            PSGC_MUNC,
            place_of_death_region   = NHSSS_REG,
            place_of_death_province = NHSSS_PROV,
            place_of_death_muncity  = NHSSS_MUNC,
         ),
      by = c("PSGC_REG", "PSGC_PROV", "PSGC_MUNC")
   ) %>%
   relocate(place_of_death_muncity, place_of_death_province, place_of_death_region, .before = PSGC_REG) %>%
   select(
      -PSGC_REG,
      -PSGC_PROV,
      -PSGC_MUNC
   )

##  Facilities -----------------------------------------------------------------

.log_info("Attaching facility names (HARP versions).")
nhsss$harp_dead$converted$data %<>%
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
         left_join(
            y  = ohasis$ref_addr %>%
               select(
                  FACI_PSGC_REG  = PSGC_REG,
                  FACI_PSGC_PROV = PSGC_PROV,
                  FACI_PSGC_MUNC = PSGC_MUNC,
                  dx_region      = NHSSS_REG,
                  dx_province    = NHSSS_PROV,
                  dx_muncity     = NHSSS_MUNC
               ),
            by = c("FACI_PSGC_REG", "FACI_PSGC_PROV", "FACI_PSGC_MUNC")
         ) %>%
         select(
            MORT_FACI     = FACI_ID,
            MORT_SUB_FACI = SUB_FACI_ID,
            facility      = FACI_NAME_CLEAN,
            pubpriv       = PUBPRIV,
            dx_region,
            dx_province,
            dx_muncity
         ),
      by         = c("MORT_FACI", "MORT_SUB_FACI")
   ) %>%
   relocate(facility, .before = MORT_FACI)

##  Finalize -------------------------------------------------------------------

.log_info("Finalizing dataframe.")
nhsss$harp_dead$converted$data %<>%
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
      age                     = AGE,
      age_months              = AGE_MO,
      age_death               = AGE_DTA,
      sex                     = SEX,
      philhealth              = PHILHEALTH_NO,
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
      facility,
      MORT_FACI,
      is_valid                = EB_VALIDATED,
      date_of_death           = DEATH_DATE,
      place_of_death_region,
      place_of_death_province,
      place_of_death_muncity,
      report_date
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
         civil_status,
      ),
      ~if_else(
         condition = !is.na(.),
         true      = substr(., stri_locate_first_fixed(., "_") + 1, stri_length(.)),
         false     = NA_character_
      )
   ) %>%
   # fix dates
   mutate_at(
      .vars = vars(contains("date")),
      ~if_else(
         condition = !is.na(.),
         true      = as.Date(.),
         false     = NA_Date_
      )
   )

##  Flag data for validation ---------------------------------------------------

update <- input(
   prompt  = "Run `converted` validations?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
update <- substr(toupper(update), 1, 1)

nhsss$harp_dead$converted$check <- list()
if (update == "1") {
   # initialize checking layer
   view_vars <- c(
      "REC_ID",
      "PATIENT_ID",
      "form",
      "saccl_lab_code",
      "uic",
      "fname",
      "mname",
      "lname",
      "sname",
      "birthdate",
      "sex",
      "MORT_FACI",
      "facility",
      "is_valid"
   )
   # non-negotiable variables
   vars      <- c(
      "mort_id",
      "age",
      "facility"
   )
   .log_info("Checking if non-negotiable variables are missing.")
   for (var in vars) {
      var                                    <- as.symbol(var)
      nhsss$harp_dead$converted$check[[var]] <- nhsss$harp_dead$converted$data %>%
         filter(
            is.na(!!var)
         ) %>%
         select(
            any_of(view_vars),
            !!var
         )
   }

   # duplicate id variables
   vars <- c(
      "mort_id",
      "saccl_lab_code",
      "uic"
   )
   .log_info("Checking if id variables are duplicated.")
   for (var in vars) {
      var                                                                  <- as.symbol(var)
      nhsss$harp_dead$converted$check[[paste0("dup_", as.character(var))]] <- nhsss$harp_dead$converted$data %>%
         filter(
            is.na(!!var)
         ) %>%
         select(
            any_of(view_vars),
            !!var
         ) %>%
         get_dupes(!!var)
   }

   # unknown data
   vars <- c(
      "region",
      "province",
      "muncity"
   )
   .log_info("Checking if required variables have UNKNOWN data or unpaired NHSSS versions.")
   for (var in vars) {
      var                                    <- as.symbol(var)
      nhsss$harp_dead$converted$check[[var]] <- nhsss$harp_dead$converted$data %>%
         filter(
            !!var %in% c("UNKNOWN", "OTHERS", NA_character_)
         ) %>%
         select(
            any_of(view_vars),
            !!var
         )
   }

   # range-median
   vars <- c(
      "age",
      "age_death",
      "living_children"
   )
   .log_info("Checking range-median of data.")
   nhsss$harp_dead$converted$check$tabstat <- data.frame()
   for (var in vars) {
      var <- as.symbol(var)
      df  <- nhsss$harp_dead$converted$data

      nhsss$harp_dead$converted$check$tabstat <- df %>%
         summarise(
            VARIABLE = as.character(var),
            MIN      = min(!!var, na.rm = TRUE),
            MEDIAN   = median(!!var, na.rm = TRUE),
            MAX      = max(!!var, na.rm = TRUE),
            NAs      = sum(if_else(is.na(!!var), 1, 0, 0))
         ) %>%
         mutate_all(~as.character(.)) %>%
         bind_rows(nhsss$harp_dead$converted$check$tabstat)
   }
}

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
data_name <- "converted"
if (!is.empty(nhsss$harp_dead[[data_name]]$check))
   .validation_gsheets(
      data_name   = data_name,
      parent_list = nhsss$harp_dead[[data_name]]$check,
      drive_path  = paste0(nhsss$harp_dead$gdrive$path$report, "Validation/"),
      surv_name   = "HARP Dead"
   )

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
