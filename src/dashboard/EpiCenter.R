EpiCenter <- R6Class(
   "EpiCenter",
   public  = list(
      yr               = NULL,
      mo               = NULL,
      min              = NULL,
      max              = NULL,
      refs             = list(),
      dx               = tibble(),
      tx               = tibble(),
      linelist         = tibble(),

      initialize       = function(yr = NULL, mo = NULL) {
         self$mo <- ifelse(!is.null(mo), mo, input(prompt = "What is the reporting month?", max.char = 2))
         self$yr <- ifelse(!is.null(yr), yr, input(prompt = "What is the reporting year?", max.char = 4))
         self$mo <- self$mo %>% stri_pad_left(width = 2, pad = "0")
         self$yr <- self$yr %>% stri_pad_left(width = 4, pad = "0")

         self$min <- start_ym(self$yr, self$mo)
         self$max <- end_ym(self$yr, self$mo)
      },

      fetchRefs        = function() {
         self$refs      <- psgc_aem(.GlobalEnv$ohasis$ref_addr)
         self$refs$faci <- .GlobalEnv$ohasis$ref_faci %>%
            select(-contains("PSGC")) %>%
            harp_addr_to_id(
               ohasis$ref_addr,
               c(
                  FACI_PSGC_REG  = "FACI_NHSSS_REG",
                  FACI_PSGC_PROV = "FACI_NHSSS_PROV",
                  FACI_PSGC_MUNC = "FACI_NHSSS_MUNC"
               ),
               aem_sub_ntl = FALSE,
               add_ph      = TRUE
            ) %>%
            left_join(
               y  = self$refs$addr %>%
                  select(
                     FACI_PSGC_REG  = PSGC_REG,
                     FACI_PSGC_PROV = PSGC_PROV,
                     FACI_PSGC_MUNC = PSGC_MUNC,
                     FACI_PSGC_AEM  = PSGC_AEM,
                     FACI_NAME_AEM  = NAME_AEM,
                  ),
               by = join_by(FACI_PSGC_REG, FACI_PSGC_PROV, FACI_PSGC_MUNC)
            )
      },

      fetchDx          = function() {
         self$dx <- hs_data("harp_dx", "reg", self$yr, self$mo) %>%
            read_dta(
               col_select = any_of(c(
                  "CENTRAL_ID",
                  "idnum",
                  "labcode",
                  "labcode2",
                  "year",
                  "month",
                  "sex",
                  "transmit",
                  "sexhow",
                  "pregnant",
                  "sex",
                  "self_identity",
                  "bdate",
                  "age",
                  "region",
                  "province",
                  "muncity",
                  "dx_region",
                  "dx_province",
                  "dx_muncity",
                  "dxlab_standard",
                  "confirm_date",
                  "rhivda_done",
                  "art_id",
                  "confirmlab",
                  "class",
                  "class2022",
                  "blood_extract_date",
                  "specimen_receipt_date",
                  "test_date",
                  "ahd",
                  "t0_date",
                  "visit_date",
                  "baseline_cd4",
                  "baseline_cd4_result",
                  "description_symptoms",
                  "clinicalpicture",
                  "tbpatient1",
                  "who_staging"
               ))
            ) %>%
            mutate_if(
               .predicate = is.character,
               ~na_if(str_squish(.), "")
            ) %>%
            mutate(
               dxlab_standard = case_when(
                  idnum %in% c(166980, 166981, 166982, 166983) ~ "MANDAUE SHC",
                  TRUE ~ dxlab_standard
               ),
               confirmlab     = case_when(
                  confirmlab == "RITM" ~ "RIT",
                  TRUE ~ confirmlab
               ),
               confirm_branch = NA_character_,
               who_staging    = as.integer(StrLeft(who_staging, 1)),
            ) %>%
            left_join(
               y  = hs_data("harp_vl", "naive_dx", self$yr, self$mo) %>%
                  read_dta(
                     col_select = c(
                        idnum,
                        vl_naive,
                     )
                  ),
               by = join_by(idnum)
            )

         if ("labcode2" %in% names(self$dx)) {
            self$dx %<>%
               mutate(
                  labcode = labcode2
               ) %>%
               select(-labcode2)
         }
      },

      fetchTx          = function() {
         self$tx <- hs_data("harp_tx", "outcome", self$yr, self$mo) %>%
            read_dta(
               col_select = any_of(c(
                  "CENTRAL_ID",
                  "art_id",
                  "idnum",
                  "sacclcode",
                  "sex",
                  "sexhow",
                  "transmit",
                  "sex",
                  "birthdate",
                  "age",
                  "cur_age",
                  "curr_age",
                  "artstart_date",
                  "hub",
                  "realhub",
                  "realhub_branch",
                  "baseline_vl",
                  "vlp12m",
                  "vl_result",
                  "everonart",
                  "onart",
                  "outcome",
                  "latest_regimen",
                  "latest_ffupdate",
                  "latest_nextpickup",
                  "baseline_cd4",
                  "baseline_cd4_result",
                  "description_symptoms",
                  "clinicalpicture",
                  "tbpatient1",
                  "who_staging"
               ))
            ) %>%
            mutate_if(
               .predicate = is.character,
               ~na_if(str_squish(.), "")
            ) %>%
            mutate(
               final_hub    = if (self$yr >= 2022) realhub else toupper(hub),
               final_branch = if (self$yr >= 2022) realhub_branch else NA_character_,
               baseline_vl  = if (self$yr == 2020) if_else(baseline_vl == 0, NA, baseline_vl, baseline_vl) else baseline_vl,
            ) %>%
            select(
               -any_of(c(
                  "realhub",
                  "realhub_branch",
                  "hub"
               ))
            ) %>%
            left_join(
               y  = hs_data("harp_vl", "naive_tx", self$yr, self$mo) %>%
                  read_dta(
                     col_select = c(
                        art_id,
                        vl_naive,
                     )
                  ),
               by = join_by(art_id)
            )

         if (self$yr >= 2022) {
            self$tx %<>%
               left_join(
                  y  = hs_data("harp_tx", "reg", self$yr, self$mo) %>%
                     read_dta(
                        col_select = any_of(c(
                           "art_id",
                           "idnum",
                           "birthdate",
                           "confirmatory_code",
                           "px_code",
                           "uic",
                           "PATIENT_ID"
                        ))
                     ) %>%
                     mutate(
                        labcode = case_when(
                           art_id == 95031 & is.na(idnum) ~ "*BAT301-19",
                           confirmatory_code == "" & uic != "" ~ str_c("*", uic),
                           confirmatory_code == "" & px_code != "" ~ str_c("*", px_code),
                           art_id == 43460 & is.na(idnum) ~ "JEJO0111221993_1",
                           art_id == 82604 & is.na(idnum) ~ "JEJO0111221993_2",
                           TRUE ~ confirmatory_code
                        ),
                        labcode = coalesce(labcode, PATIENT_ID)
                     ) %>%
                     select(
                        art_id,
                        birthdate,
                        labcode
                     ),
                  by = join_by(art_id)
               )
         } else {
            self$tx %<>%
               rename(
                  labcode = sacclcode
               )
         }

         self$tx %<>%
            rename_all(
               ~case_when(
                  . == "birthdate" ~ "bdate",
                  TRUE ~ .
               )
            )

         private$corrTx()
      },

      createLinelist   = function() {
         vars_dx <- names(self$dx)
         vars_dx <- vars_dx[vars_dx != "idnum"]

         self$linelist <- self$dx %>%
            left_join(
               y  = self$tx %>%
                  select(-any_of(vars_dx)),
               by = join_by(idnum)
            ) %>%
            left_join(
               y  = hs_data("harp_dead", "reg", self$yr, self$mo) %>%
                  read_dta(col_select = c(idnum, date_of_death)) %>%
                  mutate(
                     mort = 1
                  ),
               by = join_by(idnum)
            ) %>%
            bind_rows(filter(self$tx, is.na(idnum))) %>%
            rename(
               dx_age = age
            ) %>%
            mutate(
               row_id  = row_number(),
               .before = 1
            ) %>%
            mutate(
               harp_date        = end_ym(year, month),
               end_date         = end_ym(self$yr, self$mo),

               dx               = !is.na(idnum),
               mortality        = case_when(
                  mort == 1 ~ TRUE,
                  outcome == "dead" ~ TRUE,
                  TRUE ~ FALSE
               ),
               ffupdif          = if_else(
                  condition = outcome %in% c("alive on arv", "trans out", "lost to follow up") &
                     mortality &
                     !is.na(date_of_death),
                  true      = interval(latest_ffupdate, date_of_death) / days(1),
                  false     = as.numeric(NA)
               ),
               mortality        = case_when(
                  outcome %in% c("alive on arv", "trans out") & (is.na(ffupdif) | ffupdif < 0) ~ FALSE,
                  outcome %in% c("alive on arv", "trans out") & ffupdif == 0 ~ TRUE,
                  outcome == "lost to follow up" & ffupdif < 0 ~ FALSE,
                  TRUE ~ mortality
               ),
               # mortality            = coalesce(outcome == "dead" |
               #                                    (!is.na(mort) & is.na(outcome)), FALSE),
               plhiv            = !mortality,
               plhiv            = (!mortality & is.na(outcome)) | outcome != "dead",
               everonart        = !is.na(artstart_date),

               outcome_30       = if (self$yr >= 2022) outcome else hiv_tx_outcome(outcome, latest_nextpickup, end_date, 30, "days"),
               outcome_90       = if (self$yr >= 2022) hiv_tx_outcome(outcome, latest_nextpickup, end_date, 3, "months") else outcome,
               onart_30         = outcome_30 == "alive on arv",
               onart_90         = outcome_90 == "alive on arv",

               artlen           = floor(interval(artstart_date, end_date) / months(1)),
               artlen_c         = private$tat(artstart_date, end_date, "on ARVs"),
               artestablish_90  = artlen >= 3,
               artestablish_180 = artlen >= 6,

               artduration_c    = private$tat(artstart_date, latest_nextpickup, "on ARVs"),
               ltfulen          = floor(interval(latest_nextpickup, end_date) / months(1)),
               ltfulen_c        = private$tat(latest_nextpickup, end_date, "LTFU"),

               vl_naive         = vl_naive == 1,
               vl_tested_30     = onart_30 & is.na(baseline_vl) & !is.na(vlp12m),
               vl_tested_90     = onart_90 & is.na(baseline_vl) & !is.na(vlp12m),
               vl_eligible_30   = vl_tested_30 | artlen >= 3,
               vl_eligible_90   = vl_tested_90 | artlen >= 3,
               vl_suppress_30   = vl_tested_30 & vl_result < 1000,
               vl_suppress_90   = vl_tested_90 & vl_result < 1000,
               vl_undetected_30 = vl_tested_30 & vl_result < 50,
               vl_undetected_90 = vl_tested_90 & vl_result < 50,

               tld              = str_detect(latest_regimen, "TDF/3TC/DTG"),
               lte              = str_detect(latest_regimen, "TDF/3TC/EFV"),

               # report age
               rep_age          = calc_age(bdate, self$max),
               rep_age          = coalesce(rep_age, (as.numeric(self$yr) - year) + dx_age, curr_age),
               rep_age_c        = gen_agegrp(rep_age, "harp"),

               dx_age_c         = gen_agegrp(dx_age, "harp"),

               # sex
               sex              = toupper(sex),
               sex              = case_when(
                  StrLeft(sex, 1) == "M" ~ "Male",
                  StrLeft(sex, 1) == "F" ~ "Female",
                  TRUE ~ "(no data)"
               ),

               # mode of transmission
               mot              = case_when(
                  transmit == "SEX" & sexhow == "BISEXUAL" ~ "Sex w/ Males & Females",
                  transmit == "SEX" & sexhow == "HETEROSEXUAL" ~ "Male-Female Sex",
                  transmit == "SEX" & sexhow == "HOMOSEXUAL" ~ "Male-Male Sex",
                  transmit == "IVDU" ~ "Sharing of infected needles",
                  transmit == "PERINATAL" ~ "Mother-to-child",
                  transmit == "TRANSFUSION" ~ "Blood transfusion",
                  transmit == "OTHERS" ~ "Others",
                  transmit == "UNKNOWN" ~ "(no data)",
                  TRUE ~ transmit
               ),

               msm              = sex == "Male" & sexhow %in% c("HOMOSEXUAL", "BISEXUAL"),
               tgw              = sex == "Male" & self_identity %in% c("FEMALE", "OTHERS"),
               kap_type         = case_when(
                  transmit == "IVDU" ~ "PWID",
                  msm ~ "MSM",
                  sex == "Male" ~ "Other Males",
                  sex == "Female" & pregnant == 1 ~ "Pregnant WLHIV",
                  sex == "Female" ~ "Other Females",
                  TRUE ~ "Other"
               ),

               confirm_type     = case_when(
                  !is.na(idnum) & coalesce(rhivda_done, 0) == 0 ~ "SACCL",
                  !is.na(idnum) & coalesce(rhivda_done, 0) == 1 ~ "CrCL",
               ),

               reactive_date    = as.Date(coalesce(blood_extract_date, specimen_receipt_date, test_date, t0_date, visit_date, confirm_date)),
               tat_confirm_c    = private$tat(reactive_date, confirm_date, "LTFU"),
               tat_artstart     = floor(interval(reactive_date, artstart_date) / days(1)),
               tat_artstart_c   = private$tat(reactive_date, artstart_date, "initiation", add_days = TRUE),

               enroll_type      = case_when(
                  StrLeft(tat_artstart_c, 1) %in% c("a", "b", "c") ~ "a) Rapid initiation (RAI)",
                  StrLeft(tat_artstart_c, 1) %in% c("d", "e") ~ "b) Same month initiation",
                  StrLeft(tat_artstart_c, 1) %in% c("f", "g", "h") ~ "c) Same year initiation",
                  StrLeft(tat_artstart_c, 1) %in% c("i", "j", "k", "l") ~ "d) Late initiation",
               ),

               nodata_hiv_stage = if_else(
                  is.na(ahd) &
                     is.na(baseline_cd4) &
                     ((coalesce(description_symptoms, "") == "" & clinicalpicture == 1) | is.na(clinicalpicture)) &
                     is.na(tbpatient1) &
                     is.na(who_staging) &
                     class2022 == "HIV",
                  1,
                  0,
                  0
               ),
               class2022_c      = case_when(
                  nodata_hiv_stage == 1 ~ "No info to classify",
                  StrLeft(who_staging, 1) %in% c(3, 4) ~ "Stage III/IV",
                  dx_age >= 5 & StrLeft(baseline_cd4, 1) %in% c("4", "5") ~ "CD4 <200",
                  dx_age < 5 ~ "<5 y.o.",
                  class2022 == "AIDS" & tbpatient1 == 1 ~ "TB Patient",
                  class == "AIDS" ~ "Symptomatic",
                  class2022 == "AIDS" & coalesce(description_symptoms, "") != "" ~ "Symptomatic",
                  class2022 == "AIDS" ~ "Other Criteria",
                  class2022 == "HIV" ~ "Non-AHD",
               ),

               pickup_months    = floor(interval(latest_ffupdate, latest_nextpickup) / months(1)),
               pickup_months_c  = private$tat(latest_ffupdate, latest_nextpickup, "of meds"),
            ) %>%
            mutate_at(
               .vars = vars(class, class2022),
               ~case_when(
                  . == "AIDS" ~ "AHD",
                  . == "HIV" ~ "Non-AHD",
               )
            ) %>%
            private$regLine() %>%
            harp_addr_to_id(
               ohasis$ref_addr,
               c(
                  PERM_PSGC_REG  = "region",
                  PERM_PSGC_PROV = "province",
                  PERM_PSGC_MUNC = "muncity"
               ),
               aem_sub_ntl = FALSE,
               add_ph      = TRUE
            ) %>%
            left_join(
               y  = self$refs$addr %>%
                  select(
                     PERM_PSGC_REG  = PSGC_REG,
                     PERM_PSGC_PROV = PSGC_PROV,
                     PERM_PSGC_MUNC = PSGC_MUNC,
                     PERM_PSGC_AEM  = PSGC_AEM,
                     PERM_NAME_REG  = NAME_REG,
                     PERM_NAME_PROV = NAME_PROV,
                     PERM_NAME_MUNC = NAME_MUNC,
                     PERM_NAME_AEM  = NAME_AEM,
                  ),
               by = join_by(PERM_PSGC_REG, PERM_PSGC_PROV, PERM_PSGC_MUNC)
            ) %>%
            relocate(starts_with("PERM_NAME"), .after = PERM_PSGC_MUNC) %>%
            dxlab_to_id(
               c("DX_FACI", "DX_SUB_FACI"),
               c("dx_region", "dx_province", "dx_muncity", "dxlab_standard"),
               ohasis$ref_faci
            ) %>%
            faci_code_to_id(
               ohasis$ref_faci_code,
               c(CONFIRM_FACI = "confirmlab", CONFIRM_SUB_FACI = "confirm_branch")
            ) %>%
            faci_code_to_id(
               ohasis$ref_faci_code,
               c(ART_FACI = "final_hub", ART_SUB_FACI = "final_branch")
            ) %>%
            mutate_at(
               .vars = vars(DX_FACI, DX_SUB_FACI, CONFIRM_FACI, CONFIRM_SUB_FACI, ART_FACI, ART_SUB_FACI),
               ~replace_na(., "")
            ) %>%
            mutate_if(
               .predicate = is.logical,
               ~coalesce(., FALSE)
            ) %>%
            mutate_at(
               .vars = vars(ends_with("_NAME_REG"), ends_with("_NAME_PROV"), ends_with("_NAME_MUNC"), ends_with("_NAME_AEM")),
               ~coalesce(na_if(., "Overseas"), "Unknown")
            ) %>%
            private$convertFacility(
               "DX_FACI",
               "DX_SUB_FACI",
               "DX_HUB",
               c("DX_PSGC_REG", "DX_PSGC_PROV", "DX_PSGC_MUNC", "DX_PSGC_AEM"),
               c("DX_NAME_REG", "DX_NAME_PROV", "DX_NAME_MUNC", "DX_NAME_AEM")
            ) %>%
            private$convertFacility(
               "CONFIRM_FACI",
               "CONFIRM_SUB_FACI",
               "CONFIRM_HUB",
               c("CONFIRM_PSGC_REG", "CONFIRM_PSGC_PROV", "CONFIRM_PSGC_MUNC", "CONFIRM_PSGC_AEM"),
               c("CONFIRM_NAME_REG", "CONFIRM_NAME_PROV", "CONFIRM_NAME_MUNC", "CONFIRM_NAME_AEM")
            ) %>%
            private$convertFacility(
               "ART_FACI",
               "ART_SUB_FACI",
               "ART_HUB",
               c("ART_PSGC_REG", "ART_PSGC_PROV", "ART_PSGC_MUNC", "ART_PSGC_AEM"),
               c("ART_NAME_REG", "ART_NAME_PROV", "ART_NAME_MUNC", "ART_NAME_AEM")
            ) %>%
            select(
               -any_of(c(
                  "year",
                  "month",
                  "pregnant",
                  "self_identity",
                  "transmit",
                  "sexhow",
                  "confirmlab",
                  "confirm_branch",
                  "rhivda_done",
                  "rhivda_done",
                  "region",
                  "province",
                  "muncity",
                  "dx_region",
                  "dx_province",
                  "dx_muncity",
                  "dxlab_standard",
                  "blood_extract_date",
                  "specimen_receipt_date",
                  "test_date",
                  "t0_date",
                  "visit_date",
                  "latest_ffupdate",
                  "latest_nextpickup",
                  "onart",
                  "baseline_vl",
                  "vlp12m",
                  "vl_result",
                  "outcome",
                  "final_hub",
                  "final_branch",
                  "ffupdif",
                  "description_symptoms",
                  "clinicalpicture",
                  "tbpatient1",
                  "who_staging",
                  "nodata_hiv_stage"
               ))
            )
      },

      uploadLinelist   = function() {
         private$upload(stri_c("harp_", self$yr, self$mo), self$linelist, "row_id")
      },

      uploadEstimates  = function() {
         data <- self$refs$aem %>%
            mutate(
               report_date = end_ym(report_yr, 12)
            ) %>%
            mutate_at(
               .vars = vars(PSGC_REG, PSGC_PROV, PSGC_MUNC),
               ~coalesce(if_else(. != "", str_c("PH", .), ., .), "")
            ) %>%
            left_join(
               y  = self$refs$addr %>%
                  select(
                     PSGC_REG,
                     PSGC_PROV,
                     PSGC_MUNC,
                     NAME_REG,
                     NAME_PROV,
                     NAME_AEM,
                  ),
               by = join_by(PSGC_REG, PSGC_PROV, PSGC_MUNC)
            )
         private$upload("estimates", data, c("PSGC_REG", "PSGC_PROV", "PSGC_MUNC", "PSGC_AEM", "report_yr"))
      },

      uploadFacilities = function() {
         data <- self$refs$faci %>%
            rename(
               NAME_REG  = FACI_NAME_REG,
               NAME_PROV = FACI_NAME_PROV,
               NAME_MUNC = FACI_NAME_MUNC,
               NAME_AEM  = FACI_NAME_AEM,
            )
         private$upload("facilities", data, c("FACI_ID", "SUB_FACI_ID"))
      },

      uploadAddress    = function() {
         private$upload("address", self$refs$addr, c("PSGC_REG", "PSGC_PROV", "PSGC_MUNC"))
      }
   ),
   private = list(
      convertFacility = function(data, id, subid, name, psgc, names) {
         data %<>%
            mutate_at(
               .vars = vars(all_of(subid)),
               ~case_when(
                  . == "130023_001" ~ "130023_001",
                  StrLeft(., 6) %in% c("130001", "130605", "040200", "130797") ~ .,
                  TRUE ~ ""
               )
            ) %>%
            left_join(
               y  = self$refs$faci %>%
                  select(
                     FACI_ID,
                     SUB_FACI_ID,
                     FACI_NAME,
                     starts_with("FACI_PSGC"),
                     starts_with("FACI_NAME"),
                  ) %>%
                  select(-FACI_NAME_CLEAN) %>%
                  rename_all(
                     ~case_when(
                        . == "FACI_ID" ~ id,
                        . == "SUB_FACI_ID" ~ subid,
                        . == "FACI_NAME" ~ name,
                        . == "FACI_PSGC_REG" ~ psgc[[1]],
                        . == "FACI_PSGC_PROV" ~ psgc[[2]],
                        . == "FACI_PSGC_MUNC" ~ psgc[[3]],
                        . == "FACI_PSGC_AEM" ~ psgc[[4]],
                        . == "FACI_NAME_REG" ~ names[[1]],
                        . == "FACI_NAME_PROV" ~ names[[2]],
                        . == "FACI_NAME_MUNC" ~ names[[3]],
                        . == "FACI_NAME_AEM" ~ names[[4]],
                        TRUE ~ .
                     )
                  ),
               by = c(id, subid)
            )

         return(data)
      },
      tat             = function(start, end, suffix, add_days = FALSE) {
         months <- floor(interval(start, end) / months(1))
         days   <- floor(interval(start, end) / days(1))

         if (add_days) {
            cat <- case_when(
               days < 0 ~ "a) Before",
               days == 0 ~ "b) Same day",
               days < 8 ~ "c) 1-7 days",
               days < 15 ~ "d) 8-14 days",
               months < 2 ~ "e) 1 month",
               months < 4 ~ "f) 2-3 months",
               months < 7 ~ "g) 4-6 months",
               months < 13 ~ "h) 7-12 months",
               months < 37 ~ "i) 2-3 years",
               months < 61 ~ "j) 4-5 years",
               months < 121 ~ "k) 6-10 years",
               months >= 121 ~ "l) 10+ years",
            )
         } else {
            cat <- case_when(
               months < 2 ~ "a) 1 month",
               months < 4 ~ "b) 2-3 months",
               months < 7 ~ "c) 4-6 months",
               months < 13 ~ "d) 7-12 months",
               months < 37 ~ "e) 2-3 years",
               months < 61 ~ "f) 4-5 years",
               months < 121 ~ "g) 6-10 years",
               months >= 121 ~ "h) 10+ years",
            )
         }

         return(stri_c(cat, " ", suffix))
      },
      upload          = function(table, data, ids) {
         conn        <- ohasis$conn("lw")
         db          <- "dashboard"
         table_space <- Id(schema = db, table = table)
         if (dbExistsTable(conn, table_space)) {
            dbRemoveTable(conn, table_space)
         }
         data %<>%
            mutate_at(
               .vars = vars(contains("PSGC")),
               ~str_replace(., "^PH", "")
            )
         ohasis$upsert(conn, db, table, data, ids)
         dbDisconnect(conn)
      },
      regLine         = function(data) {
         data %<>%
            mutate(
               regimen       = toupper(str_squish(latest_regimen)),
               r_abc         = if_else(str_detect(regimen, "ABC") & !str_detect(regimen, "ABCSYR"), "ABC", NA_character_),
               r_abcsyr      = if_else(str_detect(regimen, "ABCSYR"), "ABCsyr", NA_character_),
               r_azt_3tc     = if_else(str_detect(regimen, "AZT/3TC"), "AZT/3TC", NA_character_),
               r_azt         = if_else(str_detect(regimen, "AZT") &
                                          !str_detect(regimen, "AZT/3TC") &
                                          !str_detect(regimen, "AZTSYR"), "AZT", NA_character_),
               r_aztsyr      = if_else(str_detect(regimen, "AZTSYR"), "AZTsyr", NA_character_),
               r_tdf         = if_else(str_detect(regimen, "TDF") &
                                          !str_detect(regimen, "TDF/3TC") &
                                          !str_detect(regimen, "TDF100MG"), "TDF", NA_character_),
               r_tdf_3tc     = if_else(str_detect(regimen, "TDF/3TC") &
                                          !str_detect(regimen, "TDF/3TC/EFV") &
                                          !str_detect(regimen, "TDF/3TC/DTG"), "TDF/3TC", NA_character_),
               r_tdf_3tc_efv = if_else(str_detect(regimen, "TDF/3TC/EFV"), "TDF/3TC/EFV", NA_character_),
               r_tdf_3tc_dtg = if_else(str_detect(regimen, "TDF/3TC/DTG"), "TDF/3TC/DTG", NA_character_),
               r_tdf100      = if_else(str_detect(regimen, "TDF100MG"), "TDF100mg", NA_character_),
               r_xtc         = case_when(
                  str_detect(regimen, "3TC") &
                     !str_detect(regimen, "/3TC") &
                     !str_detect(regimen, "3TCSYR") ~ "3TC",
                  str_detect(regimen, "D4T/3TC") ~ "D4T/3TC",
                  TRUE ~ NA_character_
               ),
               r_xtcsyr      = if_else(str_detect(regimen, "3TCSYR"), "3TCsyr", NA_character_),
               r_nvp         = if_else(str_detect(regimen, "NVP") & !str_detect(regimen, "NVPSYR"), "NVP", NA_character_),
               r_nvpsyr      = if_else(str_detect(regimen, "NVPSYR"), "NVPsyr", NA_character_),
               r_efv         = if_else(str_detect(regimen, "EFV") &
                                          !str_detect(regimen, "/EFV") &
                                          !str_detect(regimen, "EFV50MG") &
                                          !str_detect(regimen, "EFV200MG") &
                                          !str_detect(regimen, "EFVSYR"), "EFV", NA_character_),
               r_efv50       = if_else(str_detect(regimen, "EFV50MG"), "EFV50mg", NA_character_),
               r_efv200      = if_else(str_detect(regimen, "EFV200MG"), "EFV200mg", NA_character_),
               r_efvsyr      = if_else(str_detect(regimen, "EFVSYR"), "EFVsyr", NA_character_),
               r_dtg         = if_else(str_detect(regimen, "DTG") & !str_detect(regimen, "/DTG"), "DTG", NA_character_),
               r_lpvr        = if_else((str_detect(regimen, "LPV/R") | str_detect(regimen, "LPVR")) & (!str_detect(regimen, "RSYR") & !str_detect(regimen, "R PEDIA")), "LPV/r", NA_character_),
               r_lpvr_pedia  = if_else(str_detect(regimen, "LPV") & (str_detect(regimen, "RSYR") | str_detect(regimen, "R PEDIA")), "LPV/rsyr", NA_character_),
               r_ril         = if_else(str_detect(regimen, "RIL"), "RIL", NA_character_),
               r_ral         = if_else(str_detect(regimen, "RAL"), "RAL", NA_character_),
               r_ftc         = if_else(str_detect(regimen, "FTC"), "FTC", NA_character_),
               r_idv         = if_else(str_detect(regimen, "IDV"), "IDV", NA_character_)
            ) %>%
            unite(
               col   = "regimen",
               sep   = "+",
               na.rm = T,
               starts_with("r_", ignore.case = FALSE)
            ) %>%
            mutate(
               # reg disagg
               reg_line = case_when(
                  regimen == "AZT/3TC+NVP" ~ 1,
                  regimen == "AZT+3TC+NVP" ~ 1,
                  regimen == "AZT/3TC+EFV" ~ 1,
                  regimen == "AZT+3TC+EFV" ~ 1,
                  regimen == "TDF/3TC/EFV" ~ 1,
                  regimen == "TDF/3TC+EFV" ~ 1,
                  regimen == "TDF+3TC+EFV" ~ 1,
                  regimen == "TDF/3TC+NVP" ~ 1,
                  regimen == "TDF+3TC+NVP" ~ 1,
                  regimen == "ABC+3TC+NVP" ~ 1,
                  regimen == "ABC+3TC+EFV" ~ 1,
                  regimen == "ABC+3TC+DTG" ~ 1,
                  regimen == "ABC+3TC+RTV" ~ 1,
                  regimen == "TDF+3TC+RTV" ~ 1,
                  regimen == "TDF/3TC+RTV" ~ 1,
                  regimen == "AZT/3TC+RTV" ~ 1,
                  regimen == "AZT+3TC+RTV" ~ 1,
                  regimen == "TDF/3TC/DTG" ~ 1,
                  regimen == "TDF/3TC+DTG" ~ 1,
                  regimen == "TDF+3TC+DTG" ~ 1,
                  regimen == "AZT/3TC+LPV/r" ~ 2,
                  regimen == "AZT+3TC+LPV/r" ~ 2,
                  regimen == "TDF/3TC+LPV/r" ~ 2,
                  regimen == "TDF+3TC+LPV/r" ~ 2,
                  regimen == "ABC+3TC+LPV/r" ~ 2,
                  regimen == "AZT+3TC+DTG" ~ 2,
                  regimen == "AZT/3TC+DTG" ~ 2,
                  regimen == "ABC+3TC+LPV/r" ~ 2,
                  regimen == "AZT/3TC+NVPsyr" ~ 2,
                  !stri_detect_fixed(regimen, "syr") & !stri_detect_fixed(regimen, "pedia") ~ 3,
                  stri_detect_fixed(regimen, "syr") | stri_detect_fixed(regimen, "pedia") ~ 4
               )
            )

         return(data)
      },

      corrTx          = function() {
         self$tx %<>%
            mutate(
               artstart_date = case_when(
                  art_id == 110703 & artstart_date == "2001-05-27" ~ as.Date("2024-05-21"),
                  TRUE ~ artstart_date
               ),
            )
      }
   )
)

try <- EpiCenter$new(2024, 8)
try$fetchRefs()
try$fetchDx()
try$fetchTx()
try$createLinelist()
try$uploadLinelist()
# try$uploadEstimates()
# try$uploadFacilities()
# try$uploadAddress()

# try$dx %>%
#    distinct(PERM_PSGC_REG)

migrate <- try$linelist %>%
   mutate(
      PERM_NAME_REG = stri_c("Resident: ", PERM_NAME_REG),
      DX_NAME_REG   = stri_c("Diagnosed: ", DX_NAME_REG),
   ) %>%
   select(
      source      = PERM_NAME_REG,
      destination = DX_NAME_REG
   ) %>%
   group_by(source, destination) %>%
   summarise(
      value = n()
   ) %>%
   ungroup() %>%
   bind_rows(
      try$linelist %>%
         mutate(
            DX_NAME_REG  = stri_c("Diagnosed: ", DX_NAME_REG),
            ART_NAME_REG = stri_c("Treat: ", ART_NAME_REG),
         ) %>%
         select(
            source      = DX_NAME_REG,
            destination = ART_NAME_REG
         ) %>%
         group_by(source, destination) %>%
         summarise(
            value = n()
         )
   )
