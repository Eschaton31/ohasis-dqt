##  Append w/ old ART Registry -------------------------------------------------

append_data <- function(old, new) {
   data <- new %>%
      bind_rows(old) %>%
      arrange(prep_id) %>%
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
   for (drop_var in c("drop_notyet", "drop_duplicates"))
      if (drop_var %in% names(nhsss$prep$corr))
         for (i in seq_len(nrow(nhsss$prep$corr[[drop_var]]))) {
            record_id <- nhsss$prep$corr[[drop_var]][i,]$REC_ID
            drop_var  <- as.symbol(drop_var)

            # tag based on record id
            data %<>%
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
   local(envir = nhsss$prep, {
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
      select(-drop, -drop_duplicates, -drop_notyet, -drop_notart) %>%
      mutate(
         # finalize age data
         age_dta = if_else(
            condition = !is.na(birthdate),
            true      = floor((prep_first_screen - birthdate) / 365.25) %>% as.numeric(),
            false     = as.numeric(NA)
         ),
         age     = if_else(
            condition = is.na(age),
            true      = age_dta,
            false     = age
         ),
      ) %>%
      distinct_all()

   return(data)
}

##  Merge w/ Dx Registry -------------------------------------------------------

merge_dx <- function(data) {
   dx <- hs_data("harp_dx", "reg", ohasis$yr, ohasis$mo) %>%
      read_dta(
         col_select = c(
            PATIENT_ID,
            idnum
         )
      ) %>%
      get_cid(nhsss$prep$forms$id_registry, PATIENT_ID) %>%
      select(-PATIENT_ID)

   if (!("idnum" %in% names(data)))
      data %<>% mutate(idnum = NA_integer_)

   # get idnum
   data %<>%
      left_join(
         y  = dx %>%
            rename(dxreg_idnum = idnum),
         by = "CENTRAL_ID"
      ) %>%
      mutate(
         # tag clients for updating w/ dx registry data
         use_dxreg = if_else(
            condition = !is.na(dxreg_idnum),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         idnum     = if_else(
            condition = use_dxreg == 1,
            true      = as.integer(dxreg_idnum),
            false     = NA_integer_,
            missing   = NA_integer_
         ),
      )

   # remove dx registry variables
   data %<>%
      select(
         -use_dxreg,
         -starts_with("dxreg")
      ) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      relocate(idnum, .after = prep_id)

   return(data)
}

##  Merge w/ Tx Registry -------------------------------------------------------

merge_tx <- function(data) {
   tx <- hs_data("harp_tx", "reg", ohasis$yr, ohasis$mo) %>%
      read_dta(
         col_select = c(
            PATIENT_ID,
            art_id
         )
      ) %>%
      get_cid(nhsss$prep$forms$id_registry, PATIENT_ID) %>%
      select(-PATIENT_ID)

   if (!("art_id" %in% names(data)))
      data %<>% mutate(art_id = NA_integer_)

   # get art_id
   data %<>%
      left_join(
         y  = tx %>%
            rename(txreg_art_id = art_id),
         by = "CENTRAL_ID"
      ) %>%
      mutate(
         # tag clients for updating w/ tx registry data
         use_txreg = if_else(
            condition = !is.na(txreg_art_id),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         art_id    = if_else(
            condition = use_txreg == 1,
            true      = as.integer(txreg_art_id),
            false     = NA_integer_,
            missing   = NA_integer_
         ),
      )

   # remove tx registry variables
   data %<>%
      select(
         -use_txreg,
         -starts_with("txreg")
      ) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      relocate(art_id, .after = idnum)

   return(data)
}

##  Merge w/ Death Registry ----------------------------------------------------

merge_dead <- function(data) {
   dead <- ohasis$get_data("harp_dead", ohasis$yr, ohasis$mo) %>%
      read_dta(
         col_select = c(
            PATIENT_ID,
            mort_id
         )
      ) %>%
      get_cid(nhsss$prep$forms$id_registry, PATIENT_ID) %>%
      select(-PATIENT_ID)

   if (!("mort_id" %in% names(data)))
      data %<>% mutate(mort_id = NA_integer_)

   # get mort_id
   data %<>%
      left_join(
         y  = dead %>%
            rename(deadreg_mort_id = mort_id),
         by = "CENTRAL_ID"
      ) %>%
      mutate(
         # tag clients for updating w/ dead registry data
         use_deadreg = if_else(
            condition = !is.na(deadreg_mort_id),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         mort_id     = if_else(
            condition = use_deadreg == 1,
            true      = as.integer(deadreg_mort_id),
            false     = NA_integer_,
            missing   = NA_integer_
         ),
      )

   # remove dead registry variables
   data %<>%
      select(
         -use_deadreg,
         -starts_with("deadreg")
      ) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      relocate(mort_id, .after = art_id)

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
      date_vars <- c(
         "birthdate",
         "prep_first_screen"
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
         arrange(prep_first_faci)

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
         arrange(prep_first_faci)

      # range-median
      tabstat <- c(
         "age",
         "year",
         "month",
         "prep_first_screen"
      )
      check   <- check_tabstat(data, check, tabstat)
   }

   return(check)
}

##  Actual flow ----------------------------------------------------------------

.init <- function() {
   p <- parent.env(environment())
   local(envir = p, {
      old  <- .GlobalEnv$nhsss$prep$official$old_reg
      new  <- read_rds(file.path(wd, "reg.converted.RDS"))
      data <- append_data(old, new)
      rm(new, old)

      .GlobalEnv$nhsss$prep$official$new_reg <- data

      tag_fordrop()
      subset_drops()
      data <- .GlobalEnv$nhsss$prep$official$new_reg
      data %<>%
         remove_drops() %>%
         merge_dx() %>%
         merge_tx() %>%
         merge_dead()

      .GlobalEnv$nhsss$prep$official$new_reg <- data

      write_rds(data, file.path(wd, "reg.final.RDS"))

      check <- get_checks(data)
   })

   local(envir = .GlobalEnv, flow_validation(nhsss$prep, "reg.final", ohasis$ym))
}
