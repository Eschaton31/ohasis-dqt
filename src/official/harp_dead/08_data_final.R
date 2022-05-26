##  Append w/ old Registry -----------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

.log_info("Appending with the previous registry.")
nhsss$harp_dead$official$new <- nhsss$harp_dead$converted$data %>%
   mutate(living_children = as.character(living_children)) %>%
   # keep only validated
   filter(is_valid == "1_Yes") %>%
   select(
      -is_valid,
      -MORT_FACI,
   ) %>%
   bind_rows(nhsss$harp_dead$official$old %>% select(-matches("report_date"))) %>%
   arrange(mort_id) %>%
   mutate(
      drop_notyet     = 0,
      drop_duplicates = 0,
   ) %>%
   relocate(idnum, .after = mort_id)

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
nhsss$harp_dead$official$new %<>%
   left_join(
      y  = nhsss$harp_dx$official$new %>%
         select(
            CENTRAL_ID,
            dxreg_saccl_lab_code = labcode2,
            dxreg_idnum          = idnum,
            dxreg_uic            = uic,
            dxreg_fname          = firstname,
            dxreg_mname          = middle,
            dxreg_lname          = last,
            dxreg_sname          = name_suffix,
            dxreg_fullname       = name,
            dxreg_birthdate      = bdate,
            dxreg_sex            = sex,
            dxreg_initials       = pxcode,
            dxreg_philhealth     = philhealth,
            dxreg_region         = region,
            dxreg_province       = province,
            dxreg_muncity        = muncity
         ),
      by = "CENTRAL_ID"
   ) %>%
   mutate(
      # tag clients for updating w/ dx registry data
      use_dxreg      = if_else(
         condition = is.na(idnum) & !is.na(dxreg_idnum),
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
         condition = !is.na(dxreg_saccl_lab_code),
         true      = dxreg_saccl_lab_code,
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

   nhsss$harp_dead$official$new %<>%
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
nhsss$harp_dead$official$new %<>%
   select(
      -use_dxreg,
      -starts_with("dxreg"),
      -starts_with("labcode2")
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

##  Tag data to be reported later on and duplicates for dropping ---------------

.log_info("Tagging duplicates and postponed reports.")
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

##  Subsets for documentation --------------------------------------------------

.log_info("Generating subsets based on tags.")
nhsss$harp_dead$official$dropped_notyet <- nhsss$harp_dead$official$new %>%
   filter(drop_notyet == 1)

nhsss$harp_dead$official$dropped_duplicates <- nhsss$harp_dead$official$new %>%
   filter(drop_duplicates == 1)

##  Drop using taggings --------------------------------------------------------

.log_info("Finalizing dataset.")
nhsss$harp_dead$official$new %<>%
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

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
