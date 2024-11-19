##  Filter Initial Data & Remove Already Reported ------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
.log_info("Generating `harp_pmtct`.`initial`.")
newpreg_dx <- nhsss$harp_pmtct$harp$dx %>%
   filter(
      year == nhsss$harp_pmtct$coverage$yr,
      pregnant == 1,
      month >= as.numeric(month(nhsss$harp_pmtct$coverage$min)),
      month <= as.numeric(nhsss$harp_pmtct$coverage$mo)
   ) %>%
   anti_join(
      y  = nhsss$harp_pmtct$official$old_mother %>% select(CENTRAL_ID),
      by = "CENTRAL_ID"
   ) %>%
   select(
      CENTRAL_ID,
      PATIENT_ID,
      idnum,
      dxlab_standard,
      report_date       = visit_date,
      confirmatory_code = labcode2,
      first             = firstname,
      middle,
      last,
      suffix            = name_suffix,
      initials          = pxcode,
      uic,
      birthdate         = bdate,
      sex,
      occupation        = curr_work,
      transmit,
      sexhow,
      confirm_date
   ) %>%
   mutate(
      info_source = "registry",
      mot         = case_when(
         transmit == "SEX" & sexhow == "HETEROSEXUAL" ~ "hetero",
         transmit == "IVDU" ~ "idu"
      )
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
   select(-dxlab_standard, -transmit, -sexhow)

newpreg_tx <- nhsss$harp_pmtct$forms$form_art_bc %>%
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
   inner_join(
      y  = nhsss$harp_pmtct$harp$tx_reg %>%
         select(-PATIENT_ID, -REC_ID),
      by = "CENTRAL_ID"
   ) %>%
   anti_join(
      y  = nhsss$harp_pmtct$official$old_mother %>% select(CENTRAL_ID),
      by = "CENTRAL_ID"
   ) %>%
   select(
      CENTRAL_ID,
      PATIENT_ID,
      art_id,
      FACI_ID     = SERVICE_FACI,
      report_date = VISIT_DATE,
      confirmatory_code,
      first,
      middle,
      last,
      suffix,
      initials,
      uic,
      birthdate,
      sex
   ) %>%
   mutate(
      info_source = "hub"
   )

newmom_pmtct <- nhsss$harp_pmtct$forms$form_pmtct %>%
   filter(
      RECORD_DATE >= nhsss$harp_pmtct$coverage$min,
      RECORD_DATE <= nhsss$harp_pmtct$coverage$min
   ) %>%
   select(
      SERVICE_FACI,
      RECORD_DATE,
      starts_with("MOM")
   ) %>%
   rename_all(
      ~case_when(
         . == "MOM_ID" ~ "REC_ID",
         . == "MOM_PXID" ~ "PATIENT_ID",
         TRUE ~ stri_replace_first_fixed(., "MOM_", "")
      )
   ) %>%
   select(-OB_SCORE) %>%
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
      initials   = paste0(
         if_else(
            condition = !is.na(FIRST),
            true      = str_left(FIRST, 1),
            false     = ""
         ),
         if_else(
            condition = !is.na(MIDDLE),
            true      = str_left(MIDDLE, 1),
            false     = ""
         ),
         if_else(
            condition = !is.na(LAST),
            true      = str_left(LAST, 1),
            false     = ""
         )
      ),
      SEX = "FEMALE"
   ) %>%
   anti_join(
      y  = nhsss$harp_pmtct$official$old_mother %>% select(CENTRAL_ID),
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
      FACI_ID     = SERVICE_FACI,
      report_date = RECORD_DATE,
      confirmatory_code,
      first,
      middle,
      last,
      suffix,
      initials,
      uic,
      birthdate,
      sex,
      pxcode      = PATIENT_CODE
   ) %>%
   mutate(
      info_source = "pmtct"
   )

newpreg <- bind_rows(newpreg_dx, newpreg_tx, newmom_pmtct) %>%
   group_by(CENTRAL_ID) %>%
   mutate(
      srcs   = paste(collapse = ", ", info_source),
      idnum  = suppress_warnings(min(idnum, na.rm = TRUE), "returning [\\-]*Inf"),
      art_id = suppress_warnings(min(art_id, na.rm = TRUE), "returning [\\-]*Inf"),
   ) %>%
   ungroup() %>%
   mutate_at(
      .vars = vars(idnum, art_id),
      ~if_else(is.infinite(.), as.numeric(NA), as.numeric(.), as.numeric(.))
   ) %>%
   mutate(
      info_source = if_else(
         srcs == "registry, hub",
         "both",
         info_source,
         info_source
      ),
      year        = as.numeric(nhsss$harp_pmtct$coverage$yr),
      month       = as.numeric(nhsss$harp_pmtct$coverage$mo),
   ) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE)

max_id <- max(nhsss$harp_pmtct$official$old_mother$pmtct_mom_id)

nhsss$harp_pmtct$official$new_mother <- nhsss$harp_pmtct$official$old_mother %>%
   mutate_at(
      .vars = vars(idnum, pmtct_mom_id),
      ~as.numeric(.)
   ) %>%
   bind_rows(
      newpreg %>%
         mutate(
            pmtct_mom_id = max_id + row_number()
         )
   )

.log_info("Updating data using dx registry information.")
nhsss$harp_pmtct$official$new_mother %<>%
   select(-starts_with("labcode2")) %>%
   left_join(
      y  = nhsss$harp_pmtct$harp$dx %>%
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
      by = "CENTRAL_ID"
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

   nhsss$harp_pmtct$official$new_mother %<>%
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
nhsss$harp_pmtct$official$new_mother %<>%
   select(
      -use_dxreg,
      -starts_with("dxreg")
   )

.log_info("Updating data using tx registry information.")
nhsss$harp_pmtct$official$new_mother %<>%
   select(-starts_with("labcode2")) %>%
   left_join(
      y  = nhsss$harp_pmtct$harp$tx_reg %>%
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
      by = "CENTRAL_ID"
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

   nhsss$harp_pmtct$official$new_mother %<>%
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
nhsss$harp_pmtct$official$new_mother %<>%
   select(
      -starts_with("txreg"),
      -srcs,
   ) %>%
   zap_labels()

##  Flag data for validation ---------------------------------------------------

update <- input(
   prompt  = "Run `mom.initial` validations?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
update <- substr(toupper(update), 1, 1)

nhsss$harp_pmtct$mom.initial$check <- list()
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
      "mot",
      "class",
      "initials",
      "CENTRAL_ID"
   )
   .log_info("Checking if non-negotiable variables are missing.")
   for (var in vars) {
      var                                       <- as.symbol(var)
      nhsss$harp_pmtct$mom.initial$check[[var]] <- nhsss$harp_pmtct$official$new_mother %>%
         filter(
            is.na(!!var)
         )
   }

   # special
   nhsss$harp_pmtct$mom.initial$check[["male mother"]] <- nhsss$harp_pmtct$official$new_mother %>%
      filter(
         sex == "MALE"
      )
}

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
gdrive_validation(nhsss$harp_pmtct, "mom.initial", nhsss$harp_pmtct$coverage$ym)

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
