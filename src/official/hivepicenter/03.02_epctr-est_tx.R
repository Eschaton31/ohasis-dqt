##
local(envir = epictr, {
   data[["est_tx"]] <- list()
   for (yr in c(2020, 2021, 2022)) {
      ref_yr                     <- as.character(yr)
      data[["est_tx"]][[ref_yr]] <- harp$dx[[ref_yr]] %>%
         select(-starts_with("PSGC")) %>%
         bind_rows(
            harp$tx[[ref_yr]] %>%
               mutate(
                  everonart = 1,
                  region    = "UNKNOWN",
                  province  = "UNKNOWN",
                  muncity   = "UNKNOWN",
                  labcode   = if (yr == 2022) as.character(confirmatory_code) else sacclcode
               ) %>%
               select(
                  labcode,
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
         filter(everonart == 1) %>%
         left_join(
            y = ref_addr %>%
               select(
                  region   = NHSSS_REG,
                  province = NHSSS_PROV,
                  muncity  = NHSSS_MUNC,
                  PSGC_REG,
                  PSGC_PROV,
                  PSGC_MUNC
               ),
         ) %>%
         left_join(
            y = estimates$class %>%
               select(
                  aem_class,
                  PSGC_REG,
                  PSGC_PROV,
                  PSGC_MUNC
               )
         ) %>%
         mutate(
            PSGC_AEM = if_else(aem_class %in% c("a", "ncr", "cebu city", "cebu province"), PSGC_MUNC, PSGC_PROV, PSGC_PROV),
         ) %>%
         group_by(
            # region, province, muncity,
            PSGC_REG,
            PSGC_PROV,
            PSGC_AEM
         ) %>%
         summarise(
            evertx        = sum(if_else(everonart == 1, 1, 0, 0)),
            ontx          = sum(if_else(onart == 1, 1, 0, 0)),
            ontx_1mo      = if (yr == 2022) {
               sum(if_else(onart == 1 & latest_nextpickup >= as.Date(paste0(yr, "-12-01")), 1, 0, 0))
            } else {
               sum(if_else(onart == 1 & latest_nextpickup >= as.Date(paste0(yr, "-09-01")), 1, 0, 0))
            },
            txestablish   = if (yr == 2022) {
               sum(if_else(onart == 1 & artstart_date <= as.Date(paste0(yr, "-03-31")), 1, 0, 0))
            } else {
               sum(if_else(onart == 1 & artstart_date <= as.Date(paste0(yr, "-06-30")), 1, 0, 0))
            },
            vltested      = sum(if_else(onart == 1 & is.na(baseline_vl) & !is.na(vlp12m), 1, 0, 0)),
            vlsuppress    = sum(if_else(onart == 1 & is.na(baseline_vl) & vlp12m == 1, 1, 0, 0)),
            vlsuppress_50 = sum(if_else(onart == 1 &
                                           is.na(baseline_vl) &
                                           vlp12m == 1 &
                                           vl_result < 50, 1, 0, 0)),
         ) %>%
         ungroup() %>%
         mutate_at(
            .vars = vars(starts_with("PSGC")),
            ~if_else(. != "", paste0("PH", .), "")
         ) %>%
         mutate(
            report_yr = yr
         )
   }
   data[["est_tx"]] <- bind_rows(data[["est_tx"]])

   rm(ref_yr, yr)
})
