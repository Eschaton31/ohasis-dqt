##  Append w/ old ART Registry -------------------------------------------------

append_data <- function(old, new) {
   data <- new %>%
      bind_rows(
         old %>%
            mutate(artstart_stage = as.integer(artstart_stage))
      ) %>%
      arrange(art_id) %>%
      mutate(
         drop_notyet     = 0,
         drop_duplicates = 0,
         drop_notart     = 0,
      ) %>%
      zap_labels()

   return(data)
}

##  Tag data to be reported later on and duplicates for dropping ---------------

tag_fordrop <- function() {
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
}

##  Subsets for documentation --------------------------------------------------

subset_drops <- function() {
   local(envir = nhsss$harp_tx, {
      official$dropped_notyet <- official$new_reg %>%
         filter(drop_notyet == 1)

      official$dropped_duplicates <- official$new_reg %>%
         filter(drop_duplicates == 1)

      official$dropped_notart <- official$new_reg %>%
         filter(drop_notart == 1)
   })
}

##  Drop using taggings --------------------------------------------------------

remove_drops <- function(data) {
   data %<>%
      mutate(
         drop = drop_duplicates + drop_notyet + drop_notart,
      ) %>%
      filter(drop == 0) %>%
      select(-drop, -drop_duplicates, -drop_notyet, -drop_notart)

   return(data)
}

##  Merge w/ Dx Registry -------------------------------------------------------

merge_dx <- function(data) {
   dx <- ohasis$get_data("harp_dx", ohasis$yr, ohasis$mo) %>%
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
            philhealth,
            confirm_date
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

   data %<>%
      select(-starts_with("labcode2"), -starts_with("dxreg")) %>%
      left_join(
         y  = dx %>%
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
            condition = !is.na(dxreg_confirmatory_code),
            # condition = is.na(idnum) & !is.na(dxreg_idnum),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         idnum             = if_else(
            condition = use_dxreg == 1,
            true      = dxreg_idnum %>% as.integer(),
            # false     = idnum %>% as.integer(),
            # missing   = idnum %>% as.integer()
            false     = NA_integer_,
            missing   = NA_integer_
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

      data %<>%
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
   data %<>%
      select(
         -use_dxreg,
         -starts_with("dxreg")
      ) %>%
      left_join(
         y  = dx %>%
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
         baseline_cd4      = labelled(
            baseline_cd4,
            c(
               "8888_Other"         = 8888,
               "1_500+ cells/μL"    = 1,
               "2_350-499 cells/μL" = 2,
               "3_200-349 cells/μL" = 3,
               "4_50-199 cells/μL"  = 4,
               "5_below 50"         = 5
            )
         ),
      ) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE)

   return(data)
}


##  Flag data for validation ---------------------------------------------------

get_checks <- function(data) {
   check  <- list()
   update <- input(
      prompt  = "Run `reg.final` validations?",
      options = c("1" = "yes", "2" = "no"),
      default = "1"
   )
   update <- substr(toupper(update), 1, 1)
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
      date_vars <- c(
         "birthdate",
         "artstart_date"
      )
      check     <- check_dates(data, check, view_vars, date_vars)

      # non-negotiable variables
      nonnegotiables <- c(
         "age",
         "year",
         "month",
         "first",
         "last",
         "sex",
         "CENTRAL_ID"
      )
      check          <- check_nonnegotiables(data, check, view_vars, nonnegotiables)

      # special checks
      .log_info("Checking for mismatch age.")
      check[["mismatch_age"]] <- data %>%
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
      check[["uic_bdate"]] <- data %>%
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
      check[["tat_confirm_enroll"]] <- data %>%
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
      .log_info("Checking range-median of data.")
      tabstat <- c(
         "age",
         "year",
         "month",
         "artstart_date"
      )
      check   <- check_tabstat(data, check, tabstat)
   }

   return(check)
}

##  Actual flow ----------------------------------------------------------------

.init <- function() {
   p <- parent.env(environment())
   local(envir = p, {
      old  <- .GlobalEnv$nhsss$harp_tx$official$old_reg
      new  <- read_rds(file.path(wd, "reg.converted.RDS"))
      data <- append_data(old, new)
      rm(new, old)

      .GlobalEnv$nhsss$harp_tx$official$new_reg <- data

      tag_fordrop()
      subset_drops()

      data %<>%
         remove_drops() %>%
         merge_dx()

      write_rds(data, file.path(wd, "reg.final.RDS"))

      check <- get_checks(data)
   })

   local(envir = .GlobalEnv, flow_validation(nhsss$harp_tx, "reg.final", ohasis$ym))
}