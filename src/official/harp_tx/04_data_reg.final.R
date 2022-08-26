##  Append w/ old ART Registry -------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

.log_info("Appending with the previous registry.")
nhsss$harp_tx$official$new_reg <- nhsss$harp_tx$reg.converted$data %>%
   bind_rows(
      nhsss$harp_tx$official$old_reg %>%
         mutate(artstart_stage = as.integer(artstart_stage))
   ) %>%
   arrange(art_id) %>%
   mutate(
      drop_notyet     = 0,
      drop_duplicates = 0,
      drop_notart     = 0,
   ) %>%
   zap_labels()

##  Tag data to be reported later on and duplicates for dropping ---------------

.log_info("Tagging duplicates and postponed reports.")
for (drop_var in c("drop_notyet", "drop_duplicates", "drop_notart"))
   if (drop_var %in% names(nhsss$harp_tx$corr))
      for (i in seq_len(nrow(nhsss$harp_tx$corr[[drop_var]]))) {
         record_id <- nhsss$harp_tx$corr[[drop_var]][i,]$REC_ID
         drop_var  <- as.symbol(drop_var)

         # tag based on record id
         nhsss$harp_tx$official$new_reg %<>%
            mutate(
               !!drop_var := if_else(
                  condition = REC_ID == record_id,
                  true      = 1,
                  false     = !!drop_var,
                  missing   = !!drop_var
               )
            )
      }

##  Subsets for documentation --------------------------------------------------

.log_info("Generating subsets based on tags.")
nhsss$harp_tx$official$dropped_notyet <- nhsss$harp_tx$official$new_reg %>%
   filter(drop_notyet == 1)

nhsss$harp_tx$official$dropped_duplicates <- nhsss$harp_tx$official$new_reg %>%
   filter(drop_duplicates == 1)

nhsss$harp_tx$official$dropped_notart <- nhsss$harp_tx$official$new_reg %>%
   filter(drop_notart == 1)

##  Drop using taggings --------------------------------------------------------

.log_info("Dropping tagged data.")
nhsss$harp_tx$official$new_reg %<>%
   mutate(
      drop = drop_duplicates + drop_notyet + drop_notart,
   ) %>%
   filter(drop == 0) %>%
   select(-drop, -drop_duplicates, -drop_notyet, -drop_notart)

##  Merge w/ Dx Registry -------------------------------------------------------

.log_info("Matching w/ HARP Dx Registry dataset for `idnum`.")
.log_info("Getting latest dx registry dataset.")
nhsss$harp_dx$official$new <- ohasis$get_data("harp_dx", ohasis$yr, ohasis$mo) %>%
   read_dta(
      col_select = c(
         PATIENT_ID,
         labcode,
         labcode2,
         idnum,
         uic,
         firstname,
         middle,
         last,
         name_suffix,
         bdate,
         sex,
         pxcode,
         philhealth
      )
   ) %>%
   # convert Stata string missing data to NAs
   mutate_if(
      .predicate = is.character,
      ~if_else(. == '', NA_character_, .)
   ) %>%
   left_join(
      y  = nhsss$harp_tx$forms$id_registry,
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
      labcode2   = if_else(is.na(labcode2), labcode, labcode2)
   )

.log_info("Updating data using dx registry information.")
nhsss$harp_tx$official$new_reg %<>%
   select(-starts_with("labcode2")) %>%
   left_join(
      y  = nhsss$harp_dx$official$new %>%
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
            dxreg_philhealth_no     = philhealth
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
   "initials",
   "philhealth_no"
)
for (var in reg_vars) {
   art_var <- var %>% as.symbol()
   dx_var  <- glue("dxreg_{var}") %>% as.symbol()

   nhsss$harp_tx$official$new_reg %<>%
      mutate(
         !!art_var := if_else(
            condition = is.na(!!art_var) & use_dxreg == 1,
            true      = !!dx_var,
            false     = !!art_var,
            missing   = !!art_var
         )
      )
}

# remove dx registry variables
nhsss$harp_tx$official$new_reg %<>%
   select(
      -use_dxreg,
      -starts_with("dxreg")
   ) %>%
   left_join(
      y  = nhsss$harp_dx$official$new %>%
         select(idnum, labcode2) %>%
         mutate(
            labcode2 = case_when(
               idnum == 6978 ~ "R11-06-3387",
               idnum == 56460 ~ "D18-09-15962",
               TRUE ~ labcode2
            )
         ),
      by = "idnum"
   ) %>%
   mutate(
      # finalize age data
      age_dta           = if_else(
         condition = !is.na(birthdate),
         true      = floor((artstart_date - birthdate) / 365.25) %>% as.numeric(),
         false     = as.numeric(NA)
      ),
      age               = if_else(
         condition = is.na(age),
         true      = age_dta,
         false     = age
      ),
      confirmatory_code = case_when(
         is.na(confirmatory_code) & !is.na(uic) ~ glue("*{uic}"),
         is.na(confirmatory_code) & !is.na(px_code) ~ glue("*{px_code}"),
         TRUE ~ confirmatory_code
      ),
      confirmatory_code = if_else(
         condition = !is.na(idnum),
         true      = labcode2,
         false     = as.character(confirmatory_code),
         missing   = as.character(confirmatory_code)
      ),
      newonart          = if_else(
         condition = year(artstart_date) == as.numeric(ohasis$yr) &
            month(artstart_date) == as.numeric(ohasis$mo),
         true      = 1,
         false     = 0,
         missing   = 0
      ),
   ) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE)

##  Flag data for validation ---------------------------------------------------

update <- input(
   prompt  = "Run `reg.final` validations?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
update <- substr(toupper(update), 1, 1)

nhsss$harp_tx$reg.final$check <- list()
if (update == "1") {
   # initialize checking layer

   # select these variables always
   view_vars <- c(
      "CENTRAL_ID",
      "PATIENT_ID",
      "art_id",
      "year",
      "month",
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
      "artstart_hub",
      "artstart_date"
   )

   # dates
   vars <- c(
      "birthdate",
      "artstart_date"
   )
   .log_info("Checking dates.")
   for (var in vars) {
      var                                  <- as.symbol(var)
      nhsss$harp_tx$reg.final$check[[var]] <- nhsss$harp_tx$official$new_reg %>%
         filter(
            is.na(!!var) |
               !!var >= ohasis$next_date |
               !!var <= -25567
         ) %>%
         select(
            any_of(view_vars),
            !!var
         ) %>%
         arrange(artstart_hub)
   }

   # non-negotiable variables
   vars <- c(
      "age",
      "year",
      "month",
      "first",
      "last",
      "sex",
      "CENTRAL_ID"
   )
   .log_info("Checking if non-negotiable variables are missing.")
   for (var in vars) {
      var                                  <- as.symbol(var)
      nhsss$harp_tx$reg.final$check[[var]] <- nhsss$harp_tx$official$new_reg %>%
         filter(
            is.na(!!var)
         ) %>%
         select(
            any_of(view_vars),
            !!var
         ) %>%
         arrange(artstart_hub)
   }

   # special checks
   .log_info("Checking for mismatch age.")
   nhsss$harp_tx$reg.final$check[["mismatch_age"]] <- nhsss$harp_tx$official$new_reg %>%
      filter(
         age != age_dta
      ) %>%
      select(
         any_of(view_vars),
         age,
         age_dta,
      ) %>%
      arrange(artstart_hub)

   .log_info("Checking for mismatch birthdate.")
   nhsss$harp_tx$reg.final$check[["uic_bdate"]] <- nhsss$harp_tx$official$new_reg %>%
      mutate(
         uic_bdate = if_else(
            condition = stri_length(uic) == 14,
            true      = StrRight(uic, 8),
            false     = NA_character_
         ),
         uic_bdate = if_else(
            condition = StrIsNumeric(uic_bdate),
            true      = paste(sep = "-", StrRight(uic_bdate, 4), StrLeft(uic_bdate, 2), substr(uic_bdate, 3, 4)),
            false     = NA_character_
         ),
         uic_bdate = as.Date(uic_bdate)
      ) %>%
      filter(
         uic_bdate != birthdate | !is.na(uic_bdate) & is.na(birthdate)
      ) %>%
      select(
         any_of(view_vars),
         birthdate,
         uic_bdate,
      ) %>%
      arrange(artstart_hub)

   .log_info("Checking for TAT (confirmatory to enrollment).")
   nhsss$harp_tx$reg.final$check[["tat_confirm_enroll"]] <- nhsss$harp_tx$official$new_reg %>%
      left_join(
         y  = nhsss$harp_dx$official$new %>%
            select(
               CENTRAL_ID,
               confirm_date
            ),
         by = "CENTRAL_ID"
      ) %>%
      mutate(
         tat = abs(as.numeric(difftime(confirm_date, artstart_date, units = "days")) / 365.25),
         tat = floor(tat)
      ) %>%
      filter(
         year >= as.numeric(ohasis$yr) - 1,
         tat >= 2
      ) %>%
      select(
         any_of(view_vars),
         confirm_date,
         tat
      ) %>%
      arrange(artstart_hub)

   # range-median
   vars <- c(
      "age",
      "year",
      "month",
      "artstart_date"
   )
   .log_info("Checking range-median of data.")
   nhsss$harp_tx$reg.final$check$tabstat <- data.frame()
   for (var in vars) {
      var <- as.symbol(var)
      df  <- nhsss$harp_tx$official$new_reg

      nhsss$harp_tx$reg.final$check$tabstat <- df %>%
         summarise(
            VARIABLE = as.character(var),
            MIN      = suppress_warnings(min(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            MEDIAN   = suppress_warnings(median(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            MAX      = suppress_warnings(max(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            NAs      = sum(if_else(is.na(!!var), 1, 0, 0))
         ) %>%
         mutate_all(~as.character(.)) %>%
         bind_rows(nhsss$harp_tx$reg.final$check$tabstat)
   }
}

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
data_name <- "reg.final"
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
