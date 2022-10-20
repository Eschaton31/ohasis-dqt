##
local(envir = epictr, {
   data$linelist <- list()
   for (yr in c(2020, 2021, 2022)) {
      ref_yr                  <- as.character(yr)
      data$linelist[[ref_yr]] <- harp$dx[[ref_yr]] %>%
         select(-starts_with("PSGC")) %>%
         mutate(
            harp_report_date = as.Date(paste(sep = "-", year, stri_pad_left(month, 2, "0"), "01"))
         ) %>%
         bind_rows(
            harp$tx[[ref_yr]] %>%
               mutate(
                  everonart = 1,
                  region    = "UNKNOWN",
                  province  = "UNKNOWN",
                  muncity   = "UNKNOWN",
                  labcode   = if (yr == 2022) as.character(confirmatory_code) else sacclcode,
                  age       = if (yr == 2022) curr_age else age,
               ) %>%
               select(
                  labcode,
                  sex,
                  cur_age_tx = age,
                  idnum,
                  region,
                  province,
                  muncity,
                  final_hub,
                  final_branch,
                  outcome,
                  everonart,
                  onart,
                  baseline_vl,
                  vlp12m
               )
         ) %>%
         mutate(labcode = str_replace_all(labcode, "[^[:alnum:]]", "")) %>%
         distinct(idnum, labcode, .keep_all = TRUE) %>%
         rename(dx_age = age) %>%
         mutate(
            report_yr     = yr,
            row_id        = paste(sep = "-", idnum, labcode),

            # tx age
            cur_age_tx    = floor(cur_age_tx),
            cur_age_tx_c  = case_when(
               cur_age >= 0 & cur_age < 15 ~ "<15",
               cur_age >= 15 & cur_age < 25 ~ "15-24",
               cur_age >= 25 & cur_age < 35 ~ "25-34",
               cur_age >= 35 & cur_age < 50 ~ "35-49",
               cur_age >= 50 & cur_age < 1000 ~ "50+",
               TRUE ~ "(no data)"
            ),
            cur_age_tx_c  = case_when(
               cur_age >= 0 & cur_age < 15 ~ "<15",
               cur_age >= 15 & cur_age < 25 ~ "15-24",
               cur_age >= 25 & cur_age < 35 ~ "25-34",
               cur_age >= 35 & cur_age < 50 ~ "35-49",
               cur_age >= 50 & cur_age < 1000 ~ "50+",
               TRUE ~ "(no data)"
            ),

            # dx age
            dx_age        = floor(cur_age),
            dx_age_c      = case_when(
               dx_age >= 0 & dx_age < 15 ~ "<15",
               dx_age >= 15 & dx_age < 25 ~ "15-24",
               dx_age >= 25 & dx_age < 35 ~ "25-34",
               dx_age >= 35 & dx_age < 50 ~ "35-49",
               dx_age >= 50 & dx_age < 1000 ~ "50+",
               TRUE ~ "(no data)"
            ),
            dx_age_c      = case_when(
               dx_age >= 0 & dx_age < 15 ~ "<15",
               dx_age >= 15 & dx_age < 25 ~ "15-24",
               dx_age >= 25 & dx_age < 35 ~ "25-34",
               dx_age >= 35 & dx_age < 50 ~ "35-49",
               dx_age >= 50 & dx_age < 1000 ~ "50+",
               TRUE ~ "(no data)"
            ),

            #sex
            sex           = case_when(
               StrLeft(toupper(sex), 1) == "M" ~ "Male",
               StrLeft(toupper(sex), 1) == "F" ~ "Female",
               TRUE ~ "(no data)"
            ),

            # MOT
            mot           = case_when(
               transmit == "SEX" ~ sexhow,
               TRUE ~ transmit
            ),

            msm           = if_else(sex == "Male" & sexhow %in% c("HOMOSEXUAL", "BISEXUAL"), 1, 0, 0),
            tgw           = if_else(sex == "Male" & self_identity %in% c("FEMALE", "OTHERS"), 1, 0, 0),
            kap_type      = case_when(
               transmit == "IVDU" ~ "PWID",
               msm == 1 ~ "MSM",
               sex == "Male" ~ "Other Males",
               sex == "Female" & pregnant == 1 ~ "Pregnant WLHIV",
               sex == "Female" ~ "Other Females",
               TRUE ~ "Other"
            ),

            confirm_type  = case_when(
               is.na(rhivda_done) ~ "SACCL",
               rhivda_done == 0 ~ "SACCL",
               rhivda_done == 1 ~ "CrCL",
            ),

            plhiv         = if_else(dead != 1 | is.na(dead), 1, 0, 0),
            evertx        = if_else(everonart == 1, 1, 0, 0),
            ontx          = if_else(onart == 1, 1, 0, 0),
            ontx_1mo      = if (yr == 2022) {
               if_else(onart == 1 & latest_nextpickup >= as.Date(paste0(yr, "-12-01")), 1, 0, 0)
            } else {
               if_else(onart == 1 & latest_nextpickup >= as.Date(paste0(yr, "-09-01")), 1, 0, 0)
            },
            txestablish   = if (yr == 2022) {
               if_else(onart == 1 & artstart_date <= as.Date(paste0(yr, "-03-31")), 1, 0, 0)
            } else {
               if_else(onart == 1 & artstart_date <= as.Date(paste0(yr, "-06-30")), 1, 0, 0)
            },
            vltested      = if_else(onart == 1 & is.na(baseline_vl) & !is.na(vlp12m), 1, 0, 0),
            vlsuppress    = if_else(onart == 1 & is.na(baseline_vl) & vlp12m == 1, 1, 0, 0),
            vlsuppress_50 = if_else(onart == 1 &
                                       is.na(baseline_vl) &
                                       vlp12m == 1 &
                                       vl_result < 50, 1, 0, 0),
         ) %>%
         # perm address
         left_join(
            y = ref_addr %>%
               select(
                  region        = NHSSS_REG,
                  province      = NHSSS_PROV,
                  muncity       = NHSSS_MUNC,
                  RES_PSGC_REG  = PSGC_REG,
                  RES_PSGC_PROV = PSGC_PROV,
                  RES_PSGC_MUNC = PSGC_MUNC,
               ),
         ) %>%
         left_join(
            y          = estimates$class %>%
               select(
                  aem_class_res = aem_class,
                  RES_PSGC_REG  = PSGC_REG,
                  RES_PSGC_PROV = PSGC_PROV,
                  RES_PSGC_MUNC = PSGC_MUNC,
               ),
            na_matches = "never"
         ) %>%
         mutate(
            RES_PSGC_AEM = if_else(aem_class_res %in% c("a", "ncr", "cebu city", "cebu province"), RES_PSGC_MUNC, RES_PSGC_PROV, RES_PSGC_PROV),
         ) %>%
         # dx address
         left_join(
            y = ref_addr %>%
               select(
                  dx_region    = NHSSS_REG,
                  dx_province  = NHSSS_PROV,
                  dx_muncity   = NHSSS_MUNC,
                  DX_PSGC_REG  = PSGC_REG,
                  DX_PSGC_PROV = PSGC_PROV,
                  DX_PSGC_MUNC = PSGC_MUNC,
               ),
         ) %>%
         left_join(
            y          = estimates$class %>%
               select(
                  aem_class_dx = aem_class,
                  DX_PSGC_REG  = PSGC_REG,
                  DX_PSGC_PROV = PSGC_PROV,
                  DX_PSGC_MUNC = PSGC_MUNC
               ),
            na_matches = "never"
         ) %>%
         mutate(
            DX_PSGC_AEM  = if_else(aem_class_dx %in% c("a", "ncr", "cebu city", "cebu province"), DX_PSGC_MUNC, DX_PSGC_PROV, DX_PSGC_PROV),
            final_branch = case_when(
               final_hub == "HASH" & is.na(final_branch) ~ "HASH-QC",
               final_hub == "TLY" & is.na(final_branch) ~ "TLY-ANGLO",
               final_hub == "SHP" & is.na(final_branch) ~ "SHIP-MAKATI",
               TRUE ~ final_branch
            ),
         ) %>%
         left_join(
            y  = ohasis$ref_faci_code %>%
               mutate(
                  FACI_CODE     = case_when(
                     stri_detect_regex(SUB_FACI_CODE, "^HASH") ~ "HASH",
                     stri_detect_regex(SUB_FACI_CODE, "^SAIL") ~ "SAIL",
                     stri_detect_regex(SUB_FACI_CODE, "^TLY") ~ "TLY",
                     TRUE ~ FACI_CODE
                  ),
                  SUB_FACI_CODE = if_else(
                     condition = nchar(SUB_FACI_CODE) == 3,
                     true      = NA_character_,
                     false     = SUB_FACI_CODE
                  ),
                  SUB_FACI_CODE = case_when(
                     FACI_CODE == "HASH" & is.na(SUB_FACI_CODE) ~ "HASH-QC",
                     FACI_CODE == "TLY" & is.na(SUB_FACI_CODE) ~ "TLY-ANGLO",
                     FACI_CODE == "SHP" & is.na(SUB_FACI_CODE) ~ "SHIP-MAKATI",
                     TRUE ~ SUB_FACI_CODE
                  ),
               ) %>%
               select(
                  TX_FACI_ID   = FACI_ID,
                  TX_FACI_NAME = FACI_NAME,
                  final_hub    = FACI_CODE,
                  final_branch = SUB_FACI_CODE,
                  TX_PSGC_REG  = FACI_PSGC_REG,
                  TX_PSGC_PROV = FACI_PSGC_PROV,
                  TX_PSGC_MUNC = FACI_PSGC_MUNC,
               ) %>%
               distinct_all() %>%
               left_join(
                  y          = estimates$class %>%
                     select(
                        aem_class_tx = aem_class,
                        TX_PSGC_REG  = PSGC_REG,
                        TX_PSGC_PROV = PSGC_PROV,
                        TX_PSGC_MUNC = PSGC_MUNC
                     ),
                  na_matches = "never"
               ),
            by = c("final_hub", "final_branch")
         ) %>%
         mutate(
            TX_PSGC_AEM = if_else(aem_class_tx %in% c("a", "ncr", "cebu city", "cebu province"), TX_PSGC_MUNC, TX_PSGC_PROV, TX_PSGC_PROV),
         ) %>%
         select(
            report_yr,
            harp_report_date,
            row_id,
            sex,
            mot,
            msm,
            tgw,
            kap_type,
            gender_identity,
            confirm_type,
            plhiv,
            evertx,
            ontx,
            ontx_1mo,
            txestablish,
            vltested,
            vlsuppress,
            vlsuppress_50,
            dx_age,
            dx_age_c,
            cur_age_tx,
            cur_age_tx_c,
            RES_PSGC_REG,
            RES_PSGC_PROV,
            RES_PSGC_MUNC,
            RES_PSGC_AEM,
            aem_class_res,
            DX_PSGC_REG,
            DX_PSGC_PROV,
            DX_PSGC_MUNC,
            DX_PSGC_AEM,
            dxlab_standard,
            aem_class_dx,
            TX_PSGC_REG,
            TX_PSGC_PROV,
            TX_PSGC_MUNC,
            TX_PSGC_AEM,
            TX_FACI_ID,
            TX_FACI_NAME,
            aem_class_tx,
            final_hub,
            final_branch,
            outcome,
            everonart,
            onart,
            baseline_vl,
            vlp12m
         ) %>%
         left_join(
            y          = ref_addr %>%
               select(
                  RES_PSGC_REG  = PSGC_REG,
                  RES_PSGC_PROV = PSGC_PROV,
                  RES_PSGC_MUNC = PSGC_MUNC,
                  RES_NAME_REG  = NAME_REG,
                  RES_NAME_PROV = NAME_PROV,
                  RES_NAME_MUNC = NAME_MUNC
               ) %>%
               mutate_at(
                  .vars = vars(starts_with("PSGC")),
                  ~if_else(. != "", paste0("PH", .), "")
               ),
            na_matches = "never"
         ) %>%
         distinct_all() %>%
         left_join(
            y          = ref_addr %>%
               select(
                  TX_PSGC_REG  = PSGC_REG,
                  TX_PSGC_PROV = PSGC_PROV,
                  TX_PSGC_MUNC = PSGC_MUNC,
                  TX_NAME_REG  = NAME_REG,
                  TX_NAME_PROV = NAME_PROV,
                  TX_NAME_MUNC = NAME_MUNC
               ) %>%
               mutate_at(
                  .vars = vars(starts_with("PSGC")),
                  ~if_else(. != "", paste0("PH", .), "")
               ),
            na_matches = "never"
         ) %>%
         distinct_all() %>%
         left_join(
            y          = ref_addr %>%
               select(
                  DX_PSGC_REG  = PSGC_REG,
                  DX_PSGC_PROV = PSGC_PROV,
                  DX_PSGC_MUNC = PSGC_MUNC,
                  DX_NAME_REG  = NAME_REG,
                  DX_NAME_PROV = NAME_PROV,
                  DX_NAME_MUNC = NAME_MUNC
               ) %>%
               mutate_at(
                  .vars = vars(starts_with("PSGC")),
                  ~if_else(. != "", paste0("PH", .), "")
               ),
            na_matches = "never"
         ) %>%
         distinct_all() %>%
         mutate(
            report_yr    = as.Date(paste(sep = "-", report_yr, "01-01")),
            RES_NAME_AEM = if_else(aem_class_res %in% c("a", "ncr", "cebu city", "cebu province"), RES_NAME_MUNC, RES_NAME_PROV, RES_NAME_PROV),
            DX_NAME_AEM  = if_else(aem_class_dx %in% c("a", "ncr", "cebu city", "cebu province"), DX_NAME_MUNC, DX_NAME_PROV, DX_NAME_PROV),
            TX_NAME_AEM  = if_else(aem_class_tx %in% c("a", "ncr", "cebu city", "cebu province"), TX_NAME_MUNC, TX_NAME_PROV, TX_NAME_PROV),
         )
   }
})
