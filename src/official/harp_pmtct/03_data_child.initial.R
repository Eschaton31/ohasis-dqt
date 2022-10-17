##  Filter Initial Data & Remove Already Reported ------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
.log_info("Generating `harp_pmtct`.`initial`.")
newchild_dx <- nhsss$harp_pmtct$harp$dx %>%
   filter(
      year == nhsss$harp_pmtct$coverage$yr,
      pregnant == 1,
      month >= as.numeric(month(nhsss$harp_pmtct$coverage$min)),
      month <= as.numeric(nhsss$harp_pmtct$coverage$mo)
   ) %>%
   select(
      CENTRAL_ID,
      PATIENT_ID,
      REC_ID,
      RECORD_DATE = visit_date,
      dxlab_standard
   ) %>%
   left_join(
      y  = nhsss$harp_pmtct$forms$amc_mom %>%
         select(
            REC_ID,
            edd          = EDD,
            lmp          = LMP,
            hub_delivery = DELIVER_FACI
         ),
      by = "REC_ID"
   ) %>%
   left_join(
      y  = ohasis$ref_faci %>%
         select(
            FACI_ID,
            dxlab_standard = FACI_NAME_CLEAN
         ) %>%
         distinct_all(),
      by = "dxlab_standard"
   ) %>%
   select(-dxlab_standard) %>%
   mutate(
      birth_status = "unknown"
   )

newchild_tx <- nhsss$harp_pmtct$forms$form_art_bc %>%
   # get latest central ids
   left_join(
      y  = nhsss$harp_pmtct$forms$id_registry,
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   ) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   select(
      CENTRAL_ID,
      PATIENT_ID,
      REC_ID,
      RECORD_DATE  = VISIT_DATE,
      FACI_ID      = SERVICE_FACI,
      edd          = EDD,
      lmp          = LMP,
      hub_delivery = DELIVER_FACI
   ) %>%
   mutate(
      birth_status = "unknown"
   )

newchild_pmtct <- nhsss$harp_pmtct$forms$form_pmtct %>%
   filter(
      RECORD_DATE >= nhsss$harp_pmtct$coverage$min,
      RECORD_DATE <= nhsss$harp_pmtct$coverage$min
   ) %>%
   select(
      -starts_with("MOM"),
      -starts_with("DAD"),
   ) %>%
   # get latest central ids
   left_join(
      y  = nhsss$harp_pmtct$forms$id_registry,
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   ) %>%
   anti_join(
      y  = nhsss$harp_pmtct$official$old_child %>% select(CENTRAL_ID),
      by = "CENTRAL_ID"
   ) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   rename_at(
      .vars = vars(CONFIRMATORY_CODE, FIRST, MIDDLE, LAST, SUFFIX, UIC, BIRTHDATE, SEX),
      ~tolower(.)
   ) %>%
   select(
      CENTRAL_ID,
      PATIENT_ID,
      REC_ID,
      FACI_ID     = SERVICE_FACI,
      report_date = RECORD_DATE,
      confirmatory_code,
      first,
      middle,
      last,
      suffix,
      uic,
      birthdate,
      sex,
      pxcode      = PATIENT_CODE
   ) %>%
   mutate(
      info_source = "pmtct"
   )

newchild <- bind_rows(newchild_dx, newchild_tx, newchild_pmtct) %>%
   left_join(
      y  = nhsss$harp_pmtct$official$new_mother %>%
         select(CENTRAL_ID, pmtct_mom_id),
      by = "CENTRAL_ID"
   ) %>%
   arrange(RECORD_DATE) %>%
   distinct(pmtct_mom_id, .keep_all = TRUE) %>%
   mutate(
      year  = as.numeric(nhsss$harp_pmtct$coverage$yr),
      month = as.numeric(nhsss$harp_pmtct$coverage$mo),
   ) %>%
   select(
      -PATIENT_ID,
      -CENTRAL_ID,
      -RECORD_DATE
   )

max_id <- max(nhsss$
                 harp_pmtct$
                 official$
                 old_child$
                 pmtct_child_id)

nhsss$harp_pmtct$official$new_child <- nhsss$harp_pmtct$official$old_child %>%
   mutate_at(
      .vars = vars(idnum, pmtct_mom_id, pmtct_child_id),
      ~as.numeric(.)
   ) %>%
   bind_rows(
      newchild %>%
         mutate(
            pmtct_child_id = max_id + row_number()
         )
   ) %>%
   select(-CENTRAL_ID) %>%
   left_join(
      y          = nhsss$harp_pmtct$forms$id_registry,
      by         = "PATIENT_ID",
      na_matches = "never"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   )

.log_info("Updating data using dx registry information.")
nhsss$harp_pmtct$official$new_child %<>%
   select(-starts_with("labcode2")) %>%
   left_join(
      y          = nhsss$harp_pmtct$harp$dx %>%
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
         ),
      by         = "CENTRAL_ID",
      na_matches = "never"
   ) %>%
   mutate(
      # tag clients for updating w/ dx registry data
      use_dxreg         = if_else(
         condition = is.na(idnum) & !is.na(dxreg_idnum),
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      idnum             = if_else(
         condition = use_dxreg == 1,
         true      = dxreg_idnum %>% as.integer(),
         false     = idnum %>% as.integer(),
         missing   = idnum %>% as.integer()
      ),
      confirmatory_code = if_else(
         condition = !is.na(dxreg_confirmatory_code),
         true      = dxreg_confirmatory_code,
         false     = confirmatory_code,
         missing   = confirmatory_code
      ),
      birthdate         = if_else(
         condition = !is.na(dxreg_birthdate),
         true      = dxreg_birthdate,
         false     = birthdate,
         missing   = birthdate
      ),
   )

# update these variables if missing in art reg
reg_vars <- c(
   "uic",
   "first",
   "last",
   "suffix",
   "birthdate",
   "sex",
   "initials"
)
for (var in reg_vars) {
   art_var <- var %>% as.symbol()
   dx_var  <- glue("dxreg_{var}") %>% as.symbol()

   nhsss$harp_pmtct$official$new_child %<>%
      mutate(
         !!art_var := if_else(
            condition = is.na(!!art_var),
            true      = !!dx_var,
            false     = !!art_var,
            missing   = !!art_var
         )
      )
}

# remove dx registry variables
nhsss$harp_pmtct$official$new_child %<>%
   select(
      -use_dxreg,
      -starts_with("dxreg")
   )

.log_info("Updating data using tx registry information.")
nhsss$harp_pmtct$official$new_child %<>%
   select(-starts_with("labcode2")) %>%
   left_join(
      y          = nhsss$harp_pmtct$harp$tx_reg %>%
         select(
            CENTRAL_ID,
            txreg_confirmatory_code = confirmatory_code,
            txreg_art_id            = art_id,
            txreg_uic               = uic,
            txreg_first             = first,
            txreg_middle            = middle,
            txreg_last              = last,
            txreg_suffix            = suffix,
            txreg_birthdate         = birthdate,
            txreg_sex               = sex,
            txreg_initials          = initials,
         ),
      by         = "CENTRAL_ID",
      na_matches = "never"
   )

# update these variables if missing in art reg
reg_vars <- c(
   "uic",
   "art_id",
   "confirmatory_code",
   "first",
   "last",
   "suffix",
   "birthdate",
   "sex",
   "initials"
)
for (var in reg_vars) {
   art_var <- var %>% as.symbol()
   tx_var  <- glue("txreg_{var}") %>% as.symbol()

   nhsss$harp_pmtct$official$new_child %<>%
      mutate(
         !!art_var := if_else(
            condition = is.na(!!art_var),
            true      = !!tx_var,
            false     = !!art_var,
            missing   = !!art_var
         )
      )
}

# remove tx registry variables
nhsss$harp_pmtct$official$new_child %<>%
   select(
      -starts_with("txreg"),
   ) %>%
   zap_labels()

##  Flag data for validation ---------------------------------------------------

update <- input(
   prompt  = "Run `child.initial` validations?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
update <- substr(toupper(update), 1, 1)

nhsss$harp_pmtct$child.initial$check <- list()
if (update == "1") {
   # initialize checking layer
   # non-negotiable variables
   vars <- c(
      "birthdate",
      "year",
      "month",
      "first",
      "last",
      "sex",
      "CENTRAL_ID"
   )
   .log_info("Checking if non-negotiable variables are missing.")
   for (var in vars) {
      var                                       <- as.symbol(var)
      nhsss$harp_pmtct$child.initial$check[[var]] <- nhsss$harp_pmtct$official$new_child %>%
         filter(
            !(birth_status %in% c("aborted", "in womb", "stillbirth")),
            is.na(!!var)
         )
   }
}

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
gdrive_validation(nhsss$harp_pmtct, "child.initial", nhsss$harp_pmtct$coverage$ym)

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
