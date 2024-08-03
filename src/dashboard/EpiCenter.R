EpiCenter <- R6Class(
   "EpiCenter",
   public  = list(
      yr             = NULL,
      mo             = NULL,
      min            = NULL,
      max            = NULL,
      refs           = list(),
      dx             = tibble(),
      tx             = tibble(),
      linelist       = tibble(),
      upload         = list(),

      initialize     = function(yr = NULL, mo = NULL) {
         self$mo <- ifelse(!is.null(mo), mo, input(prompt = "What is the reporting month?", max.char = 2))
         self$yr <- ifelse(!is.null(yr), yr, input(prompt = "What is the reporting year?", max.char = 4))
         self$mo <- self$mo %>% stri_pad_left(width = 2, pad = "0")
         self$yr <- self$yr %>% stri_pad_left(width = 4, pad = "0")

         self$min <- start_ym(self$yr, self$mo)
         self$max <- end_ym(self$yr, self$mo)
      },

      fetchRefs      = function() {
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

      fetchDx        = function() {
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
                  "class2022"
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

      fetchTx        = function() {
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
                  "mort_id",
                  "art_reg",
                  "art_reg1"
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
               art_reg1     = if (self$yr >= 2022) art_reg else art_reg1
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

      createLinelist = function() {
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
               # report age
               rep_age      = calc_age(bdate, self$max),
               rep_age      = coalesce(rep_age, (as.numeric(self$yr) - year) + dx_age, curr_age),
               reg_age_c    = gen_agegrp(rep_age, "harp"),

               dx_age_c     = gen_agegrp(dx_age, "harp"),

               # sex
               sex          = toupper(sex),
               sex          = case_when(
                  StrLeft(sex, 1) == "M" ~ "Male",
                  StrLeft(sex, 1) == "F" ~ "Female",
                  TRUE ~ "(no data)"
               ),

               # mode of transmission
               mot          = case_when(
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

               msm          = if_else(sex == "Male" & sexhow %in% c("HOMOSEXUAL", "BISEXUAL"), "Yes", NA_character_),
               tgw          = if_else(sex == "Male" & self_identity %in% c("FEMALE", "OTHERS"), "Yes", NA_character_),
               kap_type     = case_when(
                  transmit == "IVDU" ~ "PWID",
                  msm == 1 ~ "MSM",
                  sex == "Male" ~ "Other Males",
                  sex == "Female" & pregnant == 1 ~ "Pregnant WLHIV",
                  sex == "Female" ~ "Other Females",
                  TRUE ~ "Other"
               ),

               confirm_type = case_when(
                  is.na(idnum) & coalesce(rhivda_done, 0) == 0 ~ "SACCL",
                  is.na(idnum) & coalesce(rhivda_done, 0) == 1 ~ "CrCL",
               ),
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
                  "region",
                  "province",
                  "muncity",
                  "dx_region",
                  "dx_province",
                  "dx_muncity",
                  "dxlab_standard"
               ))
            )
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
      }
   )
)

try <- EpiCenter$new(2024, 6)
try$fetchRefs()
try$fetchDx()
try$fetchTx()
try$createLinelist()

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
