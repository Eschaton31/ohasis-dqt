convert_to_psgc <- function(harp, params) {

   linelist <- lapply(params$periods, function(end_period) {
      ref_date <- str_split(end_period, "-")[[1]]
      ref_yr   <- ref_date[[1]]
      ref_mo   <- ref_date[[2]]

      data <- harp$dx[[ref_yr]] %>%
         mutate(
            labcode          = str_replace_all(labcode, "[^[:alnum:]]", ""),
            harp_report_date = as.Date(str_c(sep = "-", year, stri_pad_left(month, 2, "0"), "01")),
         ) %>%
         rename(
            dx_age     = age,
            cur_age_tx = cur_age,
         ) %>%
         bind_rows(
            harp$tx[[ref_yr]] %>%
               mutate(
                  everonart = 1,
                  labcode   = if (as.numeric(ref_yr) >= 2022) as.character(confirmatory_code) else sacclcode,
                  age       = if (as.numeric(ref_yr) >= 2022) curr_age else age,
               ) %>%
               select(
                  labcode,
                  sex,
                  bdate      = birthdate,
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
                  latest_regimen,
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
         distinct(idnum, labcode, .keep_all = TRUE) %>%
         mutate_at(
            .vars = vars(region, province, muncity, dx_region, dx_province, dx_muncity),
            ~coalesce(., "UNKNOWN")
         ) %>%
         mutate(
            report_yr             = as.numeric(ref_yr),
            row_id                = str_c(sep = "-", idnum, labcode),

            # tat
            tat_test_confirm      = if (as.numeric(ref_yr) >= 2022) reactive_date else case_when(
               !is.na(specimen_receipt_date) ~ specimen_receipt_date,
               !is.na(visit_date) ~ visit_date,
            ),
            tat_test_confirm      = interval(tat_test_confirm, confirm_date) / days(1),
            tat_confirm_art       = interval(confirm_date, artstart_date) / days(1),

            # tx age
            cur_age_tx_c          = floor(cur_age_tx),

            # dx age
            dx_age_c              = floor(dx_age),

            # report age
            rep_age               = if_else(
               is.na(bdate),
               as.integer((as.numeric(yr) - year) + dx_age),
               calc_age(bdate, end_period)
            ),
            rep_age_c             = floor(rep_age),

            # sex
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

            mortality             = if_else(dead == 1 | outcome == "dead", 1, 0, 0),
            dx                    = if_else(!is.na(idnum), 1, 0, 0),
            dx_plhiv              = if_else(!is.na(idnum) & dead == 0, 1, 0, 0),
            plhiv                 = if_else(mortality == 0, 1, 0, 0),
            evertx                = if_else(everonart == 1, 1, 0, 0),
            evertx_plhiv          = if_else(everonart == 1 & mortality == 0, 1, 0, 0),
            outcome_new           = hiv_tx_outcome(outcome, latest_nextpickup, end_period, 30),
            ontx                  = if_else(onart == 1, 1, 0, 0),
            ontx_1mo              = if_else(outcome_new == "alive on arv", 1, 0, 0),

            # tag baseline data
            baseline_vl           = if_else(
               condition = baseline_vl == 1,
               true      = 1,
               false     = 0,
               missing   = 0
            ),

            txestablish           = if_else(ontx == 1 & txlen_days > 92, 1, 0, 0),
            txestablish_1mo       = if_else(ontx_1mo == 1 & txlen_days > 92, 1, 0, 0),
            vltested              = if_else(ontx == 1 &
                                               baseline_vl == 0 &
                                               !is.na(vlp12m), 1, 0, 0),
            vltested_1mo          = if_else(ontx_1mo == 1 &
                                               baseline_vl == 0 &
                                               !is.na(vlp12m), 1, 0, 0),
            vlsuppress            = if_else(ontx == 1 &
                                               baseline_vl == 0 &
                                               (vlp12m == 1 | (vlp12m == 0 & vl_result < 1000)), 1, 0, 0),
            vlsuppress_50         = if_else(ontx_1mo == 1 &
                                               baseline_vl == 0 &
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
               condition = stri_detect_fixed(latest_regimen, "TDF") &
                  stri_detect_fixed(latest_regimen, "3TC") &
                  stri_detect_fixed(latest_regimen, "EFV"),
               true      = 1,
               false     = 0,
               missing   = 0
            ),
            tld                   = if_else(
               condition = stri_detect_fixed(latest_regimen, "TDF") &
                  stri_detect_fixed(latest_regimen, "3TC") &
                  stri_detect_fixed(latest_regimen, "DTG"),
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

            pickup                = coalesce(latest_nextpickup, latest_ffupdate %m+% days(30)),
            startpickuplen_months = floor(interval(artstart_date, pickup) / months(1)),
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

            ltfu_months           = floor(interval(pickup, end_period) / months(1)),
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
         mutate_at(
            .vars = vars(cur_age_tx_c, dx_age_c, rep_age_c),
            ~case_when(
               . < 1 ~ "<01",
               . >= 1 & . < 10 ~ "01-09",
               . >= 10 & . < 15 ~ "10-14",
               . >= 15 & . < 18 ~ "15-17",
               . >= 18 & . < 20 ~ "18-19",
               . >= 20 & . < 25 ~ "20-24",
               . >= 25 & . < 30 ~ "25-29",
               . >= 30 & . < 35 ~ "30-34",
               . >= 35 & . < 40 ~ "35-39",
               . >= 40 & . < 45 ~ "40-44",
               . >= 45 & . < 50 ~ "45-49",
               . >= 50 & . < 1000 ~ "50+",
               TRUE ~ "(no data)"
            )
         ) %>%
         # perm address
         harp_addr_to_id(
            ohasis$ref_addr,
            c(
               RES_PSGC_REG  = "region",
               RES_PSGC_PROV = "province",
               RES_PSGC_MUNC = "muncity"
            ),
            aem_sub_ntl = TRUE
         ) %>%
         left_join(
            y  = params$refs$addr %>%
               select(
                  RES_PSGC_REG  = PSGC_REG,
                  RES_PSGC_PROV = PSGC_PROV,
                  RES_PSGC_MUNC = PSGC_MUNC,
                  RES_PSGC_AEM  = PSGC_AEM,
                  RES_NAME_AEM  = NAME_AEM,
                  RES_NAME_REG  = NAME_REG,
                  RES_NAME_PROV = NAME_PROV,
                  RES_NAME_MUNC = NAME_MUNC,
               ),
            by = join_by(RES_PSGC_REG, RES_PSGC_PROV, RES_PSGC_MUNC)
         ) %>%
         # dx address
         harp_addr_to_id(
            ohasis$ref_addr,
            c(
               DX_PSGC_REG  = "dx_region",
               DX_PSGC_PROV = "dx_province",
               DX_PSGC_MUNC = "dx_muncity"
            ),
            aem_sub_ntl = TRUE
         ) %>%
         left_join(
            y  = params$refs$addr %>%
               select(
                  DX_PSGC_REG  = PSGC_REG,
                  DX_PSGC_PROV = PSGC_PROV,
                  DX_PSGC_MUNC = PSGC_MUNC,
                  DX_PSGC_AEM  = PSGC_AEM,
                  DX_NAME_AEM  = NAME_AEM,
                  DX_NAME_REG  = NAME_REG,
                  DX_NAME_PROV = NAME_PROV,
                  DX_NAME_MUNC = NAME_MUNC,
               ),
            by = join_by(DX_PSGC_REG, DX_PSGC_PROV, DX_PSGC_MUNC)
         ) %>%
         faci_code_to_id(
            ohasis$ref_faci_code,
            c(TX_FACI = "final_hub", TX_SUB_FACI = "final_brunch")
         ) %>%
         mutate(
            TX_FACI_2     = TX_FACI,
            TX_SUB_FACI_2 = TX_SUB_FACI,
         ) %>%
         ohasis$get_faci(
            list(TX_FACI_NAME = c("TX_FACI", "TX_SUB_FACI")),
            "name"
         ) %>%
         ohasis$get_faci(
            list(TX_DROP_FACI = c("TX_FACI_2", "TX_SUB_FACI_2")),
            "nhsss",
            c("tx_region", "tx_province", "tx_muncity")
         ) %>%
         select(-TX_DROP_FACI) %>%
         # tx address
         harp_addr_to_id(
            ohasis$ref_addr,
            c(
               TX_PSGC_REG  = "tx_region",
               TX_PSGC_PROV = "tx_province",
               TX_PSGC_MUNC = "tx_muncity"
            ),
            aem_sub_ntl = TRUE
         ) %>%
         left_join(
            y  = params$refs$addr %>%
               select(
                  TX_PSGC_REG  = PSGC_REG,
                  TX_PSGC_PROV = PSGC_PROV,
                  TX_PSGC_MUNC = PSGC_MUNC,
                  TX_PSGC_AEM  = PSGC_AEM,
                  TX_NAME_AEM  = NAME_AEM,
                  TX_NAME_REG  = NAME_REG,
                  TX_NAME_PROV = NAME_PROV,
                  TX_NAME_MUNC = NAME_MUNC,
               ),
            by = join_by(TX_PSGC_REG, TX_PSGC_PROV, TX_PSGC_MUNC)
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
            rep_age_c,
            dx_age_c,
            cur_age_tx_c,
            RES_PSGC_REG,
            RES_PSGC_PROV,
            RES_PSGC_AEM,
            RES_NAME_REG,
            RES_NAME_PROV,
            RES_NAME_AEM,
            DX_PSGC_REG,
            DX_PSGC_PROV,
            DX_PSGC_AEM,
            DX_NAME_REG,
            DX_NAME_PROV,
            DX_NAME_AEM,
            TX_PSGC_REG,
            TX_PSGC_PROV,
            TX_PSGC_AEM,
            TX_NAME_REG,
            TX_NAME_PROV,
            TX_NAME_AEM,
            TX_FACI_NAME,
            outcome,
            outcome_new,
            mmd,
            tld,
            tle,
            class,
            class2022,
            tat_test_confirm,
            tat_confirm_art,
            artstart_date,
            confirm_date,
            txlen,
            ltfulen,
            ltfu_months,
            ltfuestablish,
            ltfuestablish_1mo,
            startpickuplen,
            startpickuplen_months,
            vl_naive
         ) %>%
         # left_join(
         #    y          = ref_addr %>%
         #       select(
         #          RES_PSGC_REG  = PSGC_REG,
         #          RES_PSGC_PROV = PSGC_PROV,
         #          RES_PSGC_MUNC = PSGC_MUNC,
         #          RES_NAME_REG  = NAME_REG,
         #          RES_NAME_PROV = NAME_PROV,
         #          RES_NAME_MUNC = NAME_MUNC,
         #          RES_ISO3166   = ISO
         #       ),
         #    na_matches = "never"
         # ) %>%
         # distinct_all() %>%
         # left_join(
         #    y          = ref_addr %>%
         #       select(
         #          TX_PSGC_REG  = PSGC_REG,
         #          TX_PSGC_PROV = PSGC_PROV,
         #          TX_PSGC_MUNC = PSGC_MUNC,
         #          TX_NAME_REG  = NAME_REG,
         #          TX_NAME_PROV = NAME_PROV,
         #          TX_NAME_MUNC = NAME_MUNC,
         #          TXS_ISO3166  = ISO
         #       ),
         #    na_matches = "never"
         # ) %>%
         # distinct_all() %>%
         # left_join(
         #    y          = ref_addr %>%
         #       select(
         #          DX_PSGC_REG  = PSGC_REG,
         #          DX_PSGC_PROV = PSGC_PROV,
         #          DX_PSGC_MUNC = PSGC_MUNC,
         #          DX_NAME_REG  = NAME_REG,
         #          DX_NAME_PROV = NAME_PROV,
         #          DX_NAME_MUNC = NAME_MUNC,
         #          DX_ISO3166   = ISO
         #       ),
         #    na_matches = "never"
         # ) %>%
         distinct_all() %>%
         mutate(
            report_yr = as.Date(paste(sep = "-", report_yr, "01-01")),
            # RES_NAME_AEM = if_else(aem_class_res %in% c("a", "ncr", "cebu city", "cebu province"), RES_NAME_MUNC, RES_NAME_PROV, RES_NAME_PROV),
            # DX_NAME_AEM  = if_else(aem_class_dx %in% c("a", "ncr", "cebu city", "cebu province"), DX_NAME_MUNC, DX_NAME_PROV, DX_NAME_PROV),
            # TX_NAME_AEM  = if_else(aem_class_tx %in% c("a", "ncr", "cebu city", "cebu province"), TX_NAME_MUNC, TX_NAME_PROV, TX_NAME_PROV),
         ) %>%
         mutate_at(
            .vars = vars(contains("PSGC")),
            ~if_else(. != "" & nchar(.) == 9, paste0("PH", .), ., "")
         )

   })
}