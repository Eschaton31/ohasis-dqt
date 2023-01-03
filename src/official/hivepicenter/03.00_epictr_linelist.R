##
local(envir = epictr, {
   data$linelist <- list()
   for (yr in c(2020, 2021, 2022)) {
      ref_yr     <- as.character(yr)
      ref_mo     <- ifelse(yr == params$yr, params$mo, '12')
      ref_artmin <- paste(sep = "-", ref_yr, ref_mo, "01")
      ref_artmax <- ref_artmin %>%
         as.Date() %>%
         ceiling_date(unit = "month") %m-%
         days(1) %>%
         as.character()

      data$linelist[[ref_yr]] <- harp$dx[[ref_yr]] %>%
         select(-starts_with("PSGC")) %>%
         mutate(
            harp_report_date = as.Date(paste(sep = "-", year, stri_pad_left(month, 2, "0"), "01")),
         ) %>%
         bind_rows(
            harp$tx[[ref_yr]] %>%
               mutate(
                  everonart = 1,
                  region    = "UNKNOWN",
                  province  = "UNKNOWN",
                  muncity   = "UNKNOWN",
                  labcode   = if (yr >= 2022) as.character(confirmatory_code) else sacclcode,
                  age       = if (yr >= 2022) curr_age else age,
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
                  vlp12m,
                  art_reg1,
                  line,
                  artstart_date,
                  latest_ffupdate,
                  latest_nextpickup,
                  vl_naive,
                  tidyselect::any_of(
                     c(
                        "tx_reg",
                        "tx_prov",
                        "tx_munc",
                        "real_reg",
                        "real_prov",
                        "real_munc"
                     )
                  )
               )
         ) %>%
         mutate(labcode = str_replace_all(labcode, "[^[:alnum:]]", "")) %>%
         distinct(idnum, labcode, .keep_all = TRUE) %>%
         rename(dx_age = age) %>%
         mutate(
            report_yr             = yr,
            row_id                = paste(sep = "-", idnum, labcode),

            # tat
            tat_test_confirm      = case_when(
               !is.na(specimen_receipt_date) ~ specimen_receipt_date,
               !is.na(visit_date) ~ visit_date,
            ),
            tat_test_confirm      = interval(tat_test_confirm, confirm_date) / days(1),
            tat_confirm_art       = interval(confirm_date, artstart_date) / days(1),

            # tx age
            cur_age_tx            = floor(cur_age_tx),
            cur_age_tx_c          = case_when(
               cur_age >= 0 & cur_age < 15 ~ "<15",
               cur_age >= 15 & cur_age < 25 ~ "15-24",
               cur_age >= 25 & cur_age < 35 ~ "25-34",
               cur_age >= 35 & cur_age < 50 ~ "35-49",
               cur_age >= 50 & cur_age < 1000 ~ "50+",
               TRUE ~ "(no data)"
            ),
            cur_age_tx_c          = case_when(
               cur_age >= 0 & cur_age < 15 ~ "<15",
               cur_age >= 15 & cur_age < 25 ~ "15-24",
               cur_age >= 25 & cur_age < 35 ~ "25-34",
               cur_age >= 35 & cur_age < 50 ~ "35-49",
               cur_age >= 50 & cur_age < 1000 ~ "50+",
               TRUE ~ "(no data)"
            ),

            # dx age
            dx_age                = floor(cur_age),
            dx_age_c              = case_when(
               dx_age >= 0 & dx_age < 15 ~ "<15",
               dx_age >= 15 & dx_age < 25 ~ "15-24",
               dx_age >= 25 & dx_age < 35 ~ "25-34",
               dx_age >= 35 & dx_age < 50 ~ "35-49",
               dx_age >= 50 & dx_age < 1000 ~ "50+",
               TRUE ~ "(no data)"
            ),
            dx_age_c              = case_when(
               dx_age >= 0 & dx_age < 15 ~ "<15",
               dx_age >= 15 & dx_age < 25 ~ "15-24",
               dx_age >= 25 & dx_age < 35 ~ "25-34",
               dx_age >= 35 & dx_age < 50 ~ "35-49",
               dx_age >= 50 & dx_age < 1000 ~ "50+",
               TRUE ~ "(no data)"
            ),

            #sex
            sex                   = case_when(
               StrLeft(toupper(sex), 1) == "M" ~ "Male",
               StrLeft(toupper(sex), 1) == "F" ~ "Female",
               TRUE ~ "(no data)"
            ),

            # MOT
            mot                   = case_when(
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

            msm                   = if_else(sex == "Male" & sexhow %in% c("HOMOSEXUAL", "BISEXUAL"), 1, 0, 0),
            tgw                   = if_else(sex == "Male" & self_identity %in% c("FEMALE", "OTHERS"), 1, 0, 0),
            kap_type              = case_when(
               transmit == "IVDU" ~ "PWID",
               msm == 1 ~ "MSM",
               sex == "Male" ~ "Other Males",
               sex == "Female" & pregnant == 1 ~ "Pregnant WLHIV",
               sex == "Female" ~ "Other Females",
               TRUE ~ "Other"
            ),

            confirm_type          = case_when(
               is.na(rhivda_done) ~ "SACCL",
               rhivda_done == 0 ~ "SACCL",
               rhivda_done == 1 ~ "CrCL",
            ),

            txlen_days            = floor(interval(artstart_date, as.Date(ref_artmax)) / days(1)),
            txlen_months          = floor(interval(artstart_date, as.Date(ref_artmax)) / months(1)),

            dx                    = if_else(!is.na(idnum), 1, 0, 0),
            dx_plhiv              = if_else(!is.na(idnum) &
                                               (dead != 1 | is.na(dead)) &
                                               (is.na(outcome) | outcome != "dead"), 1, 0, 0),
            plhiv                 = if_else((dead != 1 | is.na(dead)) & (is.na(outcome) | outcome != "dead"), 1, 0, 0),
            evertx                = if_else(everonart == 1, 1, 0, 0),
            evertx_plhiv          = if_else(evertx == 1 & plhiv == 1, 1, 0, 0),
            ontx                  = if_else(onart == 1, 1, 0, 0),
            ontx_1mo              = if_else(ontx == 1 & latest_nextpickup >= as.Date(ref_artmin), 1, 0, 0),

            # tag baseline data
            # baseline_vl           = if_else(
            #    condition = floor(interval(artstart_date, vl_date) / days(1)) <= 92,
            #    true      = 1,
            #    false     = 0,
            #    missing  = 0
            # ),
            baseline_vl           = if_else(
               condition = baseline_vl == 1,
               true      = 1,
               false     = 0,
               missing   = 0
            ),
            baseline_vl1mo        = if_else(
               condition = floor(interval(artstart_date, vl_date) / months(1)) < 6,
               true      = 1,
               false     = 0,
               missing   = 0
            ),


            txestablish           = if_else(ontx == 1 & txlen_days > 92, 1, 0, 0),
            txestablish_1mo       = if_else(ontx_1mo == 1 & txlen_months >= 6, 1, 0, 0),
            vltested              = if_else(ontx == 1 &
                                               baseline_vl == 0 &
                                               !is.na(vlp12m), 1, 0, 0),
            vltested_1mo          = if_else(ontx_1mo == 1 &
                                               baseline_vl1mo == 0 &
                                               !is.na(vlp12m), 1, 0, 0),
            vlsuppress            = if_else(ontx == 1 &
                                               baseline_vl == 0 &
                                               vlp12m == 1, 1, 0, 0),
            vlsuppress_50         = if_else(ontx_1mo == 1 &
                                               baseline_vl1mo == 0 &
                                               vlp12m == 1 &
                                               vl_result < 50, 1, 0, 0),

            # special tagging
            mmd_months            = floor(interval(latest_ffupdate, latest_nextpickup) / months(1)),
            mmd                   = case_when(
               mmd_months <= 1 ~ "(1) 1 mo. of ARVs",
               mmd_months %in% seq(2, 3) ~ "(2) 2-3 mos. of ARVs",
               mmd_months %in% seq(4, 5) ~ "(3) 4-5 mos. of ARVs",
               mmd_months %in% seq(6, 12) ~ "(4) 6-12 mos. of ARVs",
               mmd_months > 12 ~ "(5) 12+ mos. worth of ARVs",
               TRUE ~ "(no data)"
            ),
            tle                   = if_else(
               condition = stri_detect_fixed(art_reg1, "tdf") &
                  stri_detect_fixed(art_reg1, "3tc") &
                  stri_detect_fixed(art_reg1, "efv"),
               true      = 1,
               false     = 0,
               missing   = 0
            ),
            tld                   = if_else(
               condition = stri_detect_fixed(art_reg1, "tdf") &
                  stri_detect_fixed(art_reg1, "3tc") &
                  stri_detect_fixed(art_reg1, "dtg"),
               true      = 1,
               false     = 0,
               missing   = 0
            ),

            txlen                 = case_when(
               txlen_months <= 1 ~ "(1) 1 mo. on ARVs",
               txlen_months %in% seq(2, 3) ~ "(2) 2-3 mos. on ARVs",
               txlen_months %in% seq(4, 5) ~ "(3) 4-5 mos. on ARVs",
               txlen_months %in% seq(6, 12) ~ "(4) 6-12 mos. on ARVs",
               txlen_months %in% seq(13, 36) ~ "(5) 2-3 yrs. on ARVs",
               txlen_months %in% seq(37, 60) ~ "(6) 4-5 yrs. on ARVs",
               txlen_months %in% seq(61, 120) ~ "(7) 6-10 yrs. on ARVs",
               txlen_months > 120 ~ "(8) 10+ yrs. on ARVs",
               TRUE ~ "(no data)"
            ),

            startpickuplen_months = floor(interval(artstart_date, latest_nextpickup) / months(1)),
            startpickuplen        = case_when(
               startpickuplen_months <= 1 ~ "(1) 1 mo. on ARVs",
               startpickuplen_months %in% seq(2, 3) ~ "(2) 2-3 mos. on ARVs",
               startpickuplen_months %in% seq(4, 5) ~ "(3) 4-5 mos. on ARVs",
               startpickuplen_months %in% seq(6, 12) ~ "(4) 6-12 mos. on ARVs",
               startpickuplen_months %in% seq(13, 36) ~ "(5) 2-3 yrs. on ARVs",
               startpickuplen_months %in% seq(37, 60) ~ "(6) 4-5 yrs. on ARVs",
               startpickuplen_months %in% seq(61, 120) ~ "(7) 6-10 yrs. on ARVs",
               startpickuplen_months > 120 ~ "(8) 10+ yrs. on ARVs",
               TRUE ~ "(no data)"
            ),

            pickup                = if_else(is.na(latest_nextpickup), latest_ffupdate + 30, latest_nextpickup, latest_nextpickup),
            ltfu_months           = floor(interval(pickup, as.Date(ref_artmax)) / months(1)),
            ltfulen               = case_when(
               ltfu_months <= 1 ~ "(1) 1 mo. LTFU",
               ltfu_months %in% seq(2, 3) ~ "(2) 2-3 mos. LTFU",
               ltfu_months %in% seq(4, 5) ~ "(3) 4-5 mos. LTFU",
               ltfu_months %in% seq(6, 12) ~ "(4) 6-12 mos. LTFU",
               ltfu_months %in% seq(13, 36) ~ "(5) 2-3 yrs. LTFU",
               ltfu_months %in% seq(37, 60) ~ "(6) 4-5 yrs. LTFU",
               ltfu_months %in% seq(61, 120) ~ "(7) 6-10 yrs. LTFU",
               ltfu_months > 120 ~ "(8) 10+ yrs. LTFU",
               TRUE ~ "(no data)"
            ),

            ltfuestablish         = if_else(ontx == 0 & startpickuplen_months >= 6, 1, 0, 0),
            ltfuestablish_1mo     = if_else(ontx_1mo == 0 & startpickuplen_months >= 6, 1, 0, 0),
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
                  RES_PSGC_MUNC = PSGC_MUNC
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
         group_by(RES_PSGC_REG, RES_PSGC_PROV) %>%
         mutate(
            muncity_aem = case_when(
               !is.na(aem_class_res) ~ muncity,
               paste(collapse = "", aem_class_res) == "" ~ province,
               paste(collapse = "", aem_class_res) != "" ~ paste0(province, "_ROTP"),
               TRUE ~ muncity
            )
         ) %>%
         ungroup() %>%
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
               # is.na(final_branch) ~ "",
               TRUE ~ final_branch
            ),
         ) %>%
         group_by(DX_PSGC_REG, DX_PSGC_PROV) %>%
         mutate(
            dx_muncity_aem = case_when(
               !is.na(aem_class_dx) ~ dx_muncity,
               paste(collapse = "", aem_class_dx) == "" ~ dx_province,
               paste(collapse = "", aem_class_dx) != "" ~ paste0(dx_province, "_ROTP"),
               TRUE ~ dx_muncity
            )
         ) %>%
         ungroup() %>%
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
         )

      # mo <- ifelse(yr == 2022, "10", "12")
      # data$linelist[[ref_yr]] %>%
      #    write_dta(glue(r"(E:/{format(Sys.time(), "%Y%m%d")}_harp_stirup_{yr}-{mo}.dta)"))
      data$linelist[[ref_yr]] %<>%
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
            dx,
            dx_plhiv,
            dead,
            mort,
            plhiv,
            evertx,
            evertx_plhiv,
            ontx,
            ontx_1mo,
            txestablish,
            txestablish_1mo,
            vltested,
            vltested_1mo,
            vlsuppress,
            vlsuppress_50,
            dx_age,
            dx_age_c,
            cur_age_tx,
            cur_age_tx_c,
            region,
            province,
            muncity,
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
            vlp12m,
            art_reg1,
            line,
            mmd,
            mmd_months,
            tld,
            tle,
            class,
            class2022,
            tat_test_confirm,
            tat_confirm_art,
            artstart_date,
            confirm_date,
            txlen,
            txlen_months,
            ltfulen,
            ltfu_months,
            ltfuestablish,
            ltfuestablish_1mo,
            startpickuplen,
            startpickuplen_months,
            vl_naive
         ) %>%
         left_join(
            y          = ref_addr %>%
               select(
                  RES_PSGC_REG  = PSGC_REG,
                  RES_PSGC_PROV = PSGC_PROV,
                  RES_PSGC_MUNC = PSGC_MUNC,
                  RES_NAME_REG  = NAME_REG,
                  RES_NAME_PROV = NAME_PROV,
                  RES_NAME_MUNC = NAME_MUNC,
                  RES_ISO3166   = ISO
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
                  TX_NAME_MUNC = NAME_MUNC,
                  TXS_ISO3166  = ISO
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
                  DX_NAME_MUNC = NAME_MUNC,
                  DX_ISO3166   = ISO
               ),
            na_matches = "never"
         ) %>%
         distinct_all() %>%
         mutate(
            report_yr    = as.Date(paste(sep = "-", report_yr, "01-01")),
            RES_NAME_AEM = if_else(aem_class_res %in% c("a", "ncr", "cebu city", "cebu province"), RES_NAME_MUNC, RES_NAME_PROV, RES_NAME_PROV),
            DX_NAME_AEM  = if_else(aem_class_dx %in% c("a", "ncr", "cebu city", "cebu province"), DX_NAME_MUNC, DX_NAME_PROV, DX_NAME_PROV),
            TX_NAME_AEM  = if_else(aem_class_tx %in% c("a", "ncr", "cebu city", "cebu province"), TX_NAME_MUNC, TX_NAME_PROV, TX_NAME_PROV),
         ) %>%
         mutate_at(
            .vars = vars(contains("PSGC")),
            ~if_else(. != "" & nchar(.) == 9, paste0("PH", .), ., "")
         )
   }
})
