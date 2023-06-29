##  Append w/ old Registry -----------------------------------------------------

append_data <- function(old, new) {
   data <- new %>%
      mutate(living_children = as.character(living_children)) %>%
      # keep only validated
      filter(is_valid == "1_Yes") %>%
      select(
         -is_valid,
         -MORT_FACI,
      ) %>%
      bind_rows(old %>% select(-matches("report_date"))) %>%
      arrange(mort_id) %>%
      mutate(
         drop_notyet     = 0,
         drop_duplicates = 0,
      ) %>%
      relocate(idnum, .after = mort_id)

   return(data)
}

##  Merge w/ Dx Registry -------------------------------------------------------

merge_dx <- function(data) {
   dx <- hs_data("harp_dx", "reg", ohasis$yr, ohasis$mo) %>%
      read_dta(
         col_select = c(
            REC_ID,
            PATIENT_ID,
            labcode,
            labcode2,
            idnum,
            uic,
            firstname,
            middle,
            last,
            name_suffix,
            name,
            bdate,
            sex,
            pxcode,
            philhealth,
            region,
            province,
            muncity
         )
      ) %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      ) %>%
      left_join(
         y  = nhsss$harp_dead$forms$id_registry,
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
      left_join(
         y  = dx %>%
            select(
               CENTRAL_ID,
               dxreg_confirmatory_code = labcode2,
               dxreg_idnum             = idnum,
               dxreg_uic               = uic,
               dxreg_fname             = firstname,
               dxreg_mname             = middle,
               dxreg_lname             = last,
               dxreg_sname             = name_suffix,
               dxreg_fullname          = name,
               dxreg_birthdate         = bdate,
               dxreg_sex               = sex,
               dxreg_initials          = pxcode,
               dxreg_philhealth        = philhealth,
               dxreg_region            = region,
               dxreg_province          = province,
               dxreg_muncity           = muncity
            ),
         by = "CENTRAL_ID"
      ) %>%
      mutate(
         # tag clients for updating w/ dx registry data
         use_dxreg      = if_else(
            condition = !is.na(dxreg_confirmatory_code),
            # condition = is.na(idnum) & !is.na(dxreg_idnum),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         idnum          = if_else(
            condition = use_dxreg == 1,
            true      = dxreg_idnum %>% as.integer(),
            false     = idnum %>% as.integer(),
            missing   = idnum %>% as.integer()
         ),
         saccl_lab_code = if_else(
            condition = !is.na(dxreg_confirmatory_code),
            true      = dxreg_confirmatory_code,
            false     = saccl_lab_code,
            missing   = saccl_lab_code
         ),
         region         = if_else(
            condition = use_dxreg == 1 & muncity == "UNKNOWN",
            true      = dxreg_region,
            false     = region,
            missing   = region
         ),
         province       = if_else(
            condition = use_dxreg == 1 & muncity == "UNKNOWN",
            true      = dxreg_province,
            false     = province,
            missing   = province
         ),
         muncity        = if_else(
            condition = use_dxreg == 1 & muncity == "UNKNOWN",
            true      = dxreg_muncity,
            false     = muncity,
            missing   = muncity
         ),
         final_region   = if_else(
            condition = dxreg_muncity == "UNKNOWN" & (muncity != "UNKNOWN" & !is.na(muncity)),
            true      = region,
            false     = dxreg_region,
            missing   = "UNKNOWN"
         ),
         final_province = if_else(
            condition = dxreg_muncity == "UNKNOWN" & (muncity != "UNKNOWN" & !is.na(muncity)),
            true      = province,
            false     = dxreg_province,
            missing   = "UNKNOWN"
         ),
         final_muncity  = if_else(
            condition = dxreg_muncity == "UNKNOWN" & (muncity != "UNKNOWN" & !is.na(muncity)),
            true      = muncity,
            false     = dxreg_muncity,
            missing   = "UNKNOWN"
         ),
         final_region   = if_else(
            condition = final_muncity == "UNKNOWN" | is.na(final_muncity),
            true      = mort_region,
            false     = final_region,
            missing   = final_region
         ),
         final_province = if_else(
            condition = final_muncity == "UNKNOWN" | is.na(final_muncity),
            true      = mort_province,
            false     = final_province,
            missing   = final_province
         ),
         final_muncity  = if_else(
            condition = final_muncity == "UNKNOWN" | is.na(final_muncity),
            true      = mort_muncity,
            false     = final_muncity,
            missing   = final_muncity
         ),
         final_region   = if_else(
            condition = is.na(final_muncity),
            true      = region,
            false     = final_region,
            missing   = final_region
         ),
         final_province = if_else(
            condition = is.na(final_muncity),
            true      = province,
            false     = final_province,
            missing   = final_province
         ),
         final_muncity  = if_else(
            condition = is.na(final_muncity),
            true      = muncity,
            false     = final_muncity,
            missing   = final_muncity
         ),
         final_region   = if_else(
            condition = is.na(final_muncity),
            true      = dxreg_region,
            false     = final_region,
            missing   = final_region
         ),
         final_province = if_else(
            condition = is.na(final_muncity),
            true      = dxreg_province,
            false     = final_province,
            missing   = final_province
         ),
         final_muncity  = if_else(
            condition = is.na(final_muncity),
            true      = dxreg_muncity,
            false     = final_muncity,
            missing   = final_muncity
         ),
      ) %>%
      # additional process to ensure final_region
      mutate(

         final_region   = if_else(
            condition = final_muncity == "UNKNOWN" & mort_muncity != "UNKNOWN",
            true      = mort_region,
            false     = final_region,
            missing   = final_region
         ),
         final_province = if_else(
            condition = final_muncity == "UNKNOWN" & mort_muncity != "UNKNOWN",
            true      = mort_province,
            false     = final_province,
            missing   = final_province
         ),
         final_muncity  = if_else(
            condition = final_muncity == "UNKNOWN" & mort_muncity != "UNKNOWN",
            true      = mort_muncity,
            false     = final_muncity,
            missing   = final_muncity
         ),
         final_region   = if_else(
            condition = is.na(final_muncity) & mort_muncity != "UNKNOWN",
            true      = mort_region,
            false     = final_region,
            missing   = final_region
         ),
         final_province = if_else(
            condition = is.na(final_muncity) & mort_muncity != "UNKNOWN",
            true      = mort_province,
            false     = final_province,
            missing   = final_province
         ),
         final_muncity  = if_else(
            condition = is.na(final_muncity) & mort_muncity != "UNKNOWN",
            true      = mort_muncity,
            false     = final_muncity,
            missing   = final_muncity
         ),
         final_region   = if_else(
            condition = final_muncity == "UNKNOWN" & muncity != "UNKNOWN",
            true      = region,
            false     = final_region,
            missing   = final_region
         ),
         final_province = if_else(
            condition = final_muncity == "UNKNOWN" & muncity != "UNKNOWN",
            true      = province,
            false     = final_province,
            missing   = final_province
         ),
         final_muncity  = if_else(
            condition = final_muncity == "UNKNOWN" & muncity != "UNKNOWN",
            true      = muncity,
            false     = final_muncity,
            missing   = final_muncity
         ),
      )

   # update these variables if missing in art reg
   reg_vars <- c(
      "uic",
      "fname",
      "mname",
      "sname",
      "birthdate",
      "sex",
      "philhealth",
      "fullname"
   )
   for (var in reg_vars) {
      mort_var <- var %>% as.symbol()
      dx_var   <- glue("dxreg_{var}") %>% as.symbol()

      data %<>%
         mutate(
            !!mort_var := if_else(
               condition = is.na(!!mort_var) & use_dxreg == 1,
               true      = !!dx_var,
               false     = !!mort_var,
               missing   = !!mort_var
            )
         )
   }

   # remove dx registry variables
   data %<>%
      select(
         -use_dxreg,
         -starts_with("dxreg"),
         -starts_with("labcode2")
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
         age_dta        = case_when(
            !is.na(birthdate) & !is.na(date_of_death) ~ floor((date_of_death - birthdate) / 365.25) %>% as.numeric(),
            !is.na(birthdate) & is.na(date_of_death) ~ floor((report_date - birthdate) / 365.25) %>% as.numeric(),
            TRUE ~ as.numeric(NA)
         ),
         age            = if_else(
            condition = is.na(age),
            true      = age_dta,
            false     = age
         ),
         saccl_lab_code = case_when(
            is.na(saccl_lab_code) & !is.na(uic) ~ glue("*{uic}"),
            TRUE ~ saccl_lab_code
         ),
         saccl_lab_code = if_else(
            condition = !is.na(idnum),
            true      = labcode2,
            false     = as.character(saccl_lab_code),
            missing   = as.character(saccl_lab_code)
         ),
      ) %>%
      distinct_all()

   return(data)
}


##  Tag data to be reported later on and duplicates for dropping ---------------

tag_fordrop <- function() {
   for (drop_var in c("drop_notyet", "drop_duplicates"))
      if (drop_var %in% names(nhsss$harp_dead$corr))
         for (i in seq_len(nrow(nhsss$harp_dead$corr[[drop_var]]))) {
            record_id <- nhsss$harp_dead$corr[[drop_var]][i,]$REC_ID
            drop_var  <- as.symbol(drop_var)

            # tag based on record id
            nhsss$harp_dead$official$new %<>%
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
   local(envir = nhsss$harp_dead, {
      official$dropped_notyet <- official$new %>%
         filter(drop_notyet == 1)

      official$dropped_duplicates <- official$new %>%
         filter(drop_duplicates == 1)
   })
}

##  Drop using taggings --------------------------------------------------------

generate_final <- function(data) {
   data %<>%
      mutate(
         drop = drop_duplicates + drop_notyet,
      ) %>%
      filter(drop == 0) %>%
      select(-drop, -drop_duplicates, -drop_notyet) %>%
      select(
         -any_of(
            c(
               "transmit",
               "sexhow",
               "labcode2",
               "interval_mort",
               "interval_reg",
               "drop_tag",
               "age_dta",
               "motcat4",
               "motcat"
            )
         )
      )

   return(data)
}

##  Actual flow ----------------------------------------------------------------

.init <- function() {
   p <- parent.env(environment())
   local(envir = p, {
      old <- .GlobalEnv$nhsss$harp_dead$official$old
      new <- read_rds(file.path(wd, "converted.RDS"))

      .GlobalEnv$nhsss$harp_dead$official$new <- append_data(old, new) %>%
         merge_dx()
      rm(new, old)

      tag_fordrop()
      subset_drops()
      .GlobalEnv$nhsss$harp_dead$official$new %<>%
         generate_final()

      write_rds(.GlobalEnv$nhsss$harp_dead$official$new, file.path(wd, "final.RDS"))
   }

   )
}
