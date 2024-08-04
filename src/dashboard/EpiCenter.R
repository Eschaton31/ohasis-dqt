EpiCenter <- R6Class(
   "EpiCenter",
   public  = list(
      yr              = NULL,
      mo              = NULL,
      min             = NULL,
      max             = NULL,
      refs            = list(),
      dx              = tibble(),
      tx              = tibble(),
      linelist        = tibble(),

      initialize      = function(yr = NULL, mo = NULL) {
         self$mo <- ifelse(!is.null(mo), mo, input(prompt = "What is the reporting month?", max.char = 2))
         self$yr <- ifelse(!is.null(yr), yr, input(prompt = "What is the reporting year?", max.char = 4))
         self$mo <- self$mo %>% stri_pad_left(width = 2, pad = "0")
         self$yr <- self$yr %>% stri_pad_left(width = 4, pad = "0")

         self$min <- start_ym(self$yr, self$mo)
         self$max <- end_ym(self$yr, self$mo)
      },

      fetchRefs       = function() {
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

      fetchDx         = function() {
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
                  "mort_id",
                  "confirmlab",
                  "class",
                  "class2022",
                  "blood_extract_date",
                  "specimen_receipt_date",
                  "test_date",
                  "t0_date",
                  "visit_date"
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
            )

         if ("labcode2" %in% names(self$dx)) {
            self$dx %<>%
               mutate(
                  labcode = labcode2
               ) %>%
               select(-labcode2)
         }
      },

      fetchTx         = function() {
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
                  "latest_nextpickup",
                  "mort_id"
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
      },

      createLinelist  = function() {
         vars_dx <- names(self$dx)
         vars_dx <- vars_dx[vars_dx != "idnum"]

         self$linelist <- self$dx %>%
            left_join(
               y  = self$tx %>%
                  select(-any_of(vars_dx)),
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
               harp_date            = end_ym(year, month),
               end_date             = end_ym(self$yr, self$mo),

               dx                   = !is.na(idnum),
               mortality            = outcome == "dead" | !is.na(mort_id),
               plhiv                = !mortality,
               everonart            = !is.na(artstart_date),
               outcome_30           = hiv_tx_outcome(outcome, latest_nextpickup, end_date, 30, "days"),
               outcome_90           = hiv_tx_outcome(outcome, latest_nextpickup, end_date, 3, "months"),
               onart_30             = outcome_30 == "alive on arv",
               onart_90             = outcome_90 == "alive on arv",

               artlen               = floor(interval(artstart_date, end_date) / months(1)),
               artlen_c             = private$tat(artlen, "on ARVs"),
               artestablish_90      = artlen <= 3,
               artestablish_180     = artlen <= 6,

               ltfulen              = floor(interval(artstart_date, latest_nextpickup) / months(1)),
               ltfulen_c            = private$tat(ltfulen, "LTFU"),

               vl_tested_30         = onart_30 & is.na(baseline_vl) & !is.na(vlp12m),
               vl_tested_90         = onart_90 & is.na(baseline_vl) & !is.na(vlp12m),
               vl_eligible_30       = vl_tested_30 | artlen >= 3,
               vl_eligible_90       = vl_tested_90 | artlen >= 3,
               vl_suppress_30       = vl_tested_30 & vl_result <= 1000,
               vl_suppress_90       = vl_tested_90 & vl_result <= 1000,
               vl_undetected_30     = vl_tested_30 & vl_result <= 50,
               vl_undetected_90     = vl_tested_90 & vl_result <= 50,

               tld                  = str_detect(latest_regimen, "TDF/3TC/DTG"),
               lte                  = str_detect(latest_regimen, "TDF/3TC/EFV"),

               # report age
               rep_age              = calc_age(bdate, self$max),
               rep_age              = coalesce(rep_age, (as.numeric(self$yr) - year) + dx_age, curr_age),
               reg_age_c            = gen_agegrp(rep_age, "harp"),

               dx_age_c             = gen_agegrp(dx_age, "harp"),

               # sex
               sex                  = toupper(sex),
               sex                  = case_when(
                  StrLeft(sex, 1) == "M" ~ "Male",
                  StrLeft(sex, 1) == "F" ~ "Female",
                  TRUE ~ "(no data)"
               ),

               # mode of transmission
               mot                  = case_when(
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

               msm                  = sex == "Male" & sexhow %in% c("HOMOSEXUAL", "BISEXUAL"),
               tgw                  = sex == "Male" & self_identity %in% c("FEMALE", "OTHERS"),
               kap_type             = case_when(
                  transmit == "IVDU" ~ "PWID",
                  msm ~ "MSM",
                  sex == "Male" ~ "Other Males",
                  sex == "Female" & pregnant == 1 ~ "Pregnant WLHIV",
                  sex == "Female" ~ "Other Females",
                  TRUE ~ "Other"
               ),

               confirm_type         = case_when(
                  is.na(idnum) & coalesce(rhivda_done, 0) == 0 ~ "SACCL",
                  is.na(idnum) & coalesce(rhivda_done, 0) == 1 ~ "CrCL",
               ),

               reactive_date        = as.Date(coalesce(blood_extract_date, specimen_receipt_date, test_date, t0_date, visit_date, confirm_date)),
               tat_reactive_confirm = floor(interval(reactive_date, confirm_date) / days(1)),
            ) %>%
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
                  "latest_nextpickup",
                  "onart",
                  "baseline_vl",
                  "vlp12m",
                  "vl_result",
                  "outcome",
                  "final_hub",
                  "final_branch"
               ))
            )
      },

      uploadLinelist  = function() {
         conn <- ohasis$conn("lw")
         ohasis$upsert(conn, "dashboard", stri_c("harp_", self$yr, self$mo), self$linelist, "row_id")
         dbDisconnect(conn)
      },

      uploadEstimates = function() {
         conn <- ohasis$conn("lw")
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
         ohasis$upsert(conn, "dashboard", "estimates", data, c("PSGC_REG", "PSGC_PROV", "PSGC_MUNC", "PSGC_AEM", "report_yr"))
         dbDisconnect(conn)
      }
   ),
   private = list(
      convertFacility = function(data, id, subid, name, psgc, names) {
         data %<>%
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
      tat             = function(months, suffix) {
         return(stri_c(case_when(
            months < 2 ~ "1) 1 mo.",
            months < 4 ~ "2) 2-3 mos.",
            months < 6 ~ "3) 4-5 mos.",
            months < 13 ~ "4) 6-12 mos.",
            months < 37 ~ "5) 2-3 yrs.",
            months < 61 ~ "6) 4-5 yrs.",
            months < 121 ~ "7) 6-10 yrs.",
            months >= 121 ~ "8) 10+ yrs.",
         ), " ", suffix))
      }
   )
)

try <- EpiCenter$new(2024, 1)
try$fetchRefs()
# try$fetchDx()
# try$fetchTx()
# try$createLinelist()
# try$uploadLinelist()
try$uploadEstimates()

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

con <- ohasis$conn("lw")
table
