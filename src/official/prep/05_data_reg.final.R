##  Append w/ old ART Registry -------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

.log_info("Appending with the previous registry.")
nhsss$prep$official$new_reg <- nhsss$prep$reg.converted$data %>%
   bind_rows(nhsss$prep$official$old_reg) %>%
   arrange(prep_id) %>%
   mutate(
      drop_notyet     = 0,
      drop_duplicates = 0,
      drop_notart     = 0,
   )

##  Tag data to be reported later on and duplicates for dropping ---------------

.log_info("Tagging duplicates and postponed reports.")
for (drop_var in c("drop_notyet", "drop_duplicates"))
   if (drop_var %in% names(nhsss$prep$corr))
      for (i in seq_len(nrow(nhsss$prep$corr[[drop_var]]))) {
         record_id <- nhsss$prep$corr[[drop_var]][i,]$REC_ID
         drop_var  <- as.symbol(drop_var)

         # tag based on record id
         nhsss$prep$official$new_reg %<>%
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
nhsss$prep$official$dropped_notyet <- nhsss$prep$official$new_reg %>%
   filter(drop_notyet == 1)

nhsss$prep$official$dropped_duplicates <- nhsss$prep$official$new_reg %>%
   filter(drop_duplicates == 1)

##  Drop using taggings --------------------------------------------------------

.log_info("Dropping tagged data.")
nhsss$prep$official$new_reg %<>%
   mutate(
      drop = drop_duplicates + drop_notyet + drop_notart,
   ) %>%
   filter(drop == 0) %>%
   select(-drop, -drop_duplicates, -drop_notyet, -drop_notart)

##  Final updates --------------------------------------------------------------

nhsss$prep$official$new_reg %<>%
   mutate(
      # finalize age data
      age_dta           = if_else(
         condition = !is.na(birthdate),
         true      = floor((prep_first_screen - birthdate) / 365.25) %>% as.numeric(),
         false     = as.numeric(NA)
      ),
      age               = if_else(
         condition = is.na(age),
         true      = age_dta,
         false     = age
      ),
   ) %>%
   distinct_all()

##  Flag data for validation ---------------------------------------------------

update <- input(
   prompt  = "Run `reg.final` validations?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
update <- substr(toupper(update), 1, 1)

nhsss$prep$reg.final$check <- list()
if (update == "1") {
   # initialize checking layer

   # select these variables always
   view_vars <- c(
      "CENTRAL_ID",
      "PATIENT_ID",
      "prep_id",
      "year",
      "month",
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
      "prep_first_faci",
      "prep_first_screen"
   )

   # dates
   vars <- c(
      "birthdate",
      "prep_first_screen"
   )
   .log_info("Checking dates.")
   for (var in vars) {
      var                               <- as.symbol(var)
      nhsss$prep$reg.final$check[[var]] <- nhsss$prep$official$new_reg %>%
         filter(
            is.na(!!var) |
               !!var >= ohasis$next_date |
               !!var <= -25567
         ) %>%
         select(
            any_of(view_vars),
            !!var
         ) %>%
         arrange(prep_first_faci)
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
      var                               <- as.symbol(var)
      nhsss$prep$reg.final$check[[var]] <- nhsss$prep$official$new_reg %>%
         filter(
            is.na(!!var)
         ) %>%
         select(
            any_of(view_vars),
            !!var
         ) %>%
         arrange(prep_first_faci)
   }

   # special checks
   .log_info("Checking for mismatch age.")
   nhsss$prep$reg.final$check[["mismatch_age"]] <- nhsss$prep$official$new_reg %>%
      filter(
         age != age_dta
      ) %>%
      select(
         any_of(view_vars),
         age,
         age_dta,
      ) %>%
      arrange(prep_first_faci)

   .log_info("Checking for mismatch birthdate.")
   nhsss$prep$reg.final$check[["uic_bdate"]] <- nhsss$prep$official$new_reg %>%
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
      arrange(prep_first_faci)

   # range-median
   vars <- c(
      "age",
      "year",
      "month",
      "prep_first_screen"
   )
   .log_info("Checking range-median of data.")
   nhsss$prep$reg.final$check$tabstat <- data.frame()
   for (var in vars) {
      var <- as.symbol(var)
      df  <- nhsss$prep$official$new_reg

      nhsss$prep$reg.final$check$tabstat <- df %>%
         summarise(
            VARIABLE = as.character(var),
            MIN      = suppress_warnings(min(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            MEDIAN   = suppress_warnings(median(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            MAX      = suppress_warnings(max(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            NAs      = sum(if_else(is.na(!!var), 1, 0, 0))
         ) %>%
         mutate_all(~as.character(.)) %>%
         bind_rows(nhsss$prep$reg.final$check$tabstat)
   }
}

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
gdrive_validation(nhsss$prep, "reg.final", ohasis$ym)

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
