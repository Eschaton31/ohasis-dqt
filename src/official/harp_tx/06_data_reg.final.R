##  Append w/ old ART Registry -------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

.log_info("Appending with the previous registry.")
nhsss$harp_tx$official$new_reg <- nhsss$harp_tx$official$old_reg %>%
   mutate(artstart_stage = as.integer(artstart_stage)) %>%
   bind_rows(nhsss$harp_tx$reg.converted$data) %>%
   arrange(art_id) %>%
   mutate(
      drop_notyet     = 0,
      drop_duplicates = 0,
   )

##  Tag data to be reported later on and duplicates for dropping ---------------

.log_info("Tagging duplicates and postponed reports.")
for (drop_var in c("drop_notyet", "drop_duplicates"))
   if (drop_var %in% names(nhsss$harp_tx$corr))
      for (i in seq_len(nrow(nhsss$harp_tx$corr[[drop_var]]))) {
         record_id <- nhsss$harp_tx$corr[[drop_var]][i, "REC_ID"]

         # tag based on record id
         nhsss$harp_tx$official$new_reg %<>%
            mutate(
               !!drop_var := if_else(
                  condition = REC_ID == record_id,
                  true      = 1,
                  false     = !!drop_var
               )
            )
      }

##  Subsets for documentation --------------------------------------------------

.log_info("Generating subsets based on tags.")
nhsss$harp_tx$official$dropped_notyet <- nhsss$harp_tx$official$new_reg %>%
   filter(drop_notyet == 1)

nhsss$harp_tx$official$dropped_duplicates <- nhsss$harp_tx$official$new_reg %>%
   filter(drop_duplicates == 1)

##  Drop using taggings --------------------------------------------------------

.log_info("Dropping tagged data.")
nhsss$harp_tx$official$new_reg %<>%
   mutate(
      drop = drop_duplicates + drop_notyet,
   ) %>%
   filter(drop == 0) %>%
   select(-drop, -drop_duplicates, -drop_notyet)

##  Merge w/ Dx Registry -------------------------------------------------------

.log_info("Matching w/ HARP Dx Registry dataset for `idnum`.")
lw_conn      <- ohasis$conn("lw")
oh_id_schema <- dbplyr::in_schema("ohasis_warehouse", "id_registry")

.log_info("Getting latest dx registry dataset.")
if (!("harp_dx" %in% names(nhsss)))
   nhsss$harp_dx$official$new <- ohasis$get_data("harp_dx", ohasis$yr, ohasis$mo) %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      ) %>%
      select(-CENTRAL_ID) %>%
      left_join(
         y  = tbl(lw_conn, oh_id_schema) %>%
            select(CENTRAL_ID, PATIENT_ID) %>%
            collect(),
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

dbDisconnect(lw_conn)

.log_info("Updating data using dx registry information.")
nhsss$harp_tx$official$new_reg %<>%
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
         false     = idnum %>% as.integer()
      ),
      confirmatory_code = if_else(
         condition = use_dxreg == 1,
         true      = dxreg_confirmatory_code,
         false     = confirmatory_code
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
   )

##  Flag data for validation ---------------------------------------------------

update <- input(
   prompt  = "Run `reg.final` validations?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
update <- substr(toupper(update), 1, 1)
if (update == "1") {
   # initialize checking layer
   nhsss$harp_tx$reg.final$check <- list()

   # select these variables always
   view_vars <- c(
      "CENTRAL_ID",
      "PATIENT_ID",
      "art_id",
      "year",
      "month",
      "confirmatory_code",
      "uic",
      "patient_code",
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
               !!var <= as.Date("1900-01-01")
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
            MIN      = min(!!var, na.rm = TRUE),
            MEDIAN   = median(!!var, na.rm = TRUE),
            MAX      = max(!!var, na.rm = TRUE),
            NAs      = sum(if_else(is.na(!!var), 1, 0, 0))
         ) %>%
         mutate_all(~as.character(.)) %>%
         bind_rows(nhsss$harp_tx$reg.final$check$tabstat)
   }
}

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
data_name <- "reg.final"
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
