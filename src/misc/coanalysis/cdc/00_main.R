coa         <- list()
coa$ss      <- "1dmdbw_vyK3xr8V8nlJnp1LBL5ufW6zLtf6ccf7NwsqE"
coa$harp$dx <- hs_data("harp_full", "reg", 2023, 4) %>%
   read_dta()

coa$harp$dx %>%
   filter(
      year >= 2011
   ) %>%
   mutate(
      no_data_ahd = if_else(
         is.na(ahd) &
            is.na(baseline_cd4) &
            description_symptoms == "" &
            is.na(tbpatient1) &
            is.na(who_staging) &
            class2022 == "HIV",
         1,
         0,
         0
      )
   ) %>%
   tab(class, class2022, no_data_ahd)

coa$data$dx <- coa$harp$dx %>%
   mutate(
      dxlab_standard       = case_when(
         REC_ID %in% c('20230418092538_0700100056', '20230418093537_0700100056', '20230418094552_0700100056', '20230418101649_0700100056') ~ "MANDAUE SHC",
         TRUE ~ dxlab_standard
      ),

      rhivda_done          = coalesce(rhivda_done, 0),
      confirm_type         = case_when(
         rhivda_done == 0 ~ "NRL",
         rhivda_done == 1 ~ "CrCL",
      ),

      reactive_date        = coalesce(blood_extract_date, test_date, t0_date, visit_date, confirm_date) %>% as.Date(),

      hiv_test_date        = case_when(
         !is.na(specimen_receipt_date) ~ specimen_receipt_date,
         !is.na(visit_date) ~ visit_date,
      ),

      tat_reactive_confirm = interval(reactive_date, confirm_date) / days(1),
      tat_test_confirm     = interval(hiv_test_date, confirm_date) / days(1),
      tat_reactive_enroll  = interval(reactive_date, artstart_date) / days(1),
      tat_confirm_enroll   = interval(confirm_date, artstart_date) / days(1),

      qr                   = case_when(
         month %in% c(1, 2, 3) ~ "1st",
         month %in% c(4, 5, 6) ~ "2nd",
         month %in% c(7, 8, 9) ~ "3rd",
         month %in% c(10, 11, 12) ~ "4th",
      ),
      report_date          = as.Date(stri_c(sep = "-", year, stri_pad_left(month, 2, "0"), "01")),

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

      msm                  = if_else(sex == "MALE" & sexhow %in% c("HOMOSEXUAL", "BISEXUAL"), 1, 0, 0),
      tgw                  = if_else(sex == "MALE" & self_identity %in% c("FEMALE", "OTHERS"), 1, 0, 0),
      kap_type             = case_when(
         transmit == "IVDU" ~ "PWID",
         msm == 1 ~ "MSM",
         ocw == 1 & nationalit == "FILIPINO" ~ "OFW",
         sex == "MALE" ~ "Other Males",
         sex == "FEMALE" & pregnant == 1 ~ "Pregnant WLHIV",
         sex == "FEMALE" ~ "Other Females",
         TRUE ~ "Other"
      ),
   ) %>%
   mutate_at(
      .vars = vars(starts_with("tat_")),
      ~as.integer(.)
   ) %>%
   dxlab_to_id(
      c("HARPDX_FACI", "HARPDX_SUB_FACI"),
      c("dx_region", "dx_province", "dx_muncity", "dxlab_standard"),
      ohasis$ref_faci
   ) %>%
   ohasis$get_faci(
      list(hts_tst_pos_faci = c("HARPDX_FACI", "HARPDX_SUB_FACI")),
      "name",
      c("hts_tst_pos_reg", "hts_tst_pos_prov", "hts_tst_pos_munc")
   )

coa$out$faci$tat <- coa$data$dx %>%
   filter(year >= 2017) %>%
   group_by(
      hts_tst_pos_reg, hts_tst_pos_prov, hts_tst_pos_munc, hts_tst_pos_faci, year, qr
   ) %>%
   summarise(
      react_confirm_all   = median(tat_reactive_confirm, na.rm = TRUE),
      test_confirm_all    = median(tat_test_confirm, na.rm = TRUE),
      react_enroll_all    = median(tat_reactive_enroll, na.rm = TRUE),
      test_enroll_all     = median(tat_confirm_enroll, na.rm = TRUE),
      react_confirm_crcl  = median(if_else(rhivda_done == 1, tat_reactive_confirm, NA_integer_, NA_integer_), na.rm = TRUE),
      test_confirm_crcl   = median(if_else(rhivda_done == 1, tat_test_confirm, NA_integer_, NA_integer_), na.rm = TRUE),
      react_enroll_crcl   = median(if_else(rhivda_done == 1, tat_reactive_enroll, NA_integer_, NA_integer_), na.rm = TRUE),
      test_enroll_crcl    = median(if_else(rhivda_done == 1, tat_confirm_enroll, NA_integer_, NA_integer_), na.rm = TRUE),
      react_confirm_saccl = median(if_else(rhivda_done == 0, tat_reactive_confirm, NA_integer_, NA_integer_), na.rm = TRUE),
      test_confirm_saccl  = median(if_else(rhivda_done == 0, tat_test_confirm, NA_integer_, NA_integer_), na.rm = TRUE),
      react_enroll_saccl  = median(if_else(rhivda_done == 0, tat_reactive_enroll, NA_integer_, NA_integer_), na.rm = TRUE),
      test_enroll_saccl   = median(if_else(rhivda_done == 0, tat_confirm_enroll, NA_integer_, NA_integer_), na.rm = TRUE),
   ) %>%
   ungroup() %>%
   mutate_if(
      .predicate = is.numeric,
      ~as.integer(floor(.))
   )

coa$out$faci$enroll <- coa$data$dx %>%
   filter(year >= 2022) %>%
   group_by(
      hts_tst_pos_reg, hts_tst_pos_prov, hts_tst_pos_munc, hts_tst_pos_faci, year, qr, confirm_type
   ) %>%
   summarise(
      dx     = n(),
      enroll = sum(if_else(!is.na(everonart), 1, 0, 0))
   ) %>%
   ungroup() %>%
   mutate_if(
      .predicate = is.numeric,
      ~as.integer(floor(.))
   ) %>%
   mutate(
      enroll_rate = enroll / dx
   )

coa$out$region$tat <- coa$out$faci$tat %>%
   group_by(hts_tst_pos_reg, year, qr) %>%
   summarise_at(
      .vars = vars(ends_with("_all"), ends_with("_crcl"), ends_with("_saccl")),
      list(
         ~min(., na.rm = TRUE),
         ~median(., na.rm = TRUE),
         ~max(., na.rm = TRUE)
      )
   ) %>%
   select(any_of(starts_with(names(coa$out$faci$tat)))) %>%
   mutate_if(
      .predicate = is.numeric,
      ~as.integer(floor(.))
   )


coa$out$region$enroll <- coa$data$dx %>%
   filter(year >= 2022) %>%
   group_by(hts_tst_pos_reg, year, qr, confirm_type) %>%
   summarise(
      dx     = n(),
      enroll = sum(if_else(!is.na(everonart), 1, 0, 0))
   ) %>%
   ungroup() %>%
   mutate_if(
      .predicate = is.numeric,
      ~as.integer(floor(.))
   ) %>%
   mutate(
      enroll_rate = enroll / dx
   )


coa$out$faci$kap_tat <- coa$data$dx %>%
   filter(year >= 2022) %>%
   group_by(hts_tst_pos_reg, hts_tst_pos_prov, hts_tst_pos_munc, hts_tst_pos_faci, kap_type) %>%
   summarise(
      react_confirm_all   = median(tat_reactive_confirm, na.rm = TRUE),
      test_confirm_all    = median(tat_test_confirm, na.rm = TRUE),
      react_enroll_all    = median(tat_reactive_enroll, na.rm = TRUE),
      test_enroll_all     = median(tat_confirm_enroll, na.rm = TRUE),
      react_confirm_crcl  = median(if_else(rhivda_done == 1, tat_reactive_confirm, NA_integer_, NA_integer_), na.rm = TRUE),
      test_confirm_crcl   = median(if_else(rhivda_done == 1, tat_test_confirm, NA_integer_, NA_integer_), na.rm = TRUE),
      react_enroll_crcl   = median(if_else(rhivda_done == 1, tat_reactive_enroll, NA_integer_, NA_integer_), na.rm = TRUE),
      test_enroll_crcl    = median(if_else(rhivda_done == 1, tat_confirm_enroll, NA_integer_, NA_integer_), na.rm = TRUE),
      react_confirm_saccl = median(if_else(rhivda_done == 0, tat_reactive_confirm, NA_integer_, NA_integer_), na.rm = TRUE),
      test_confirm_saccl  = median(if_else(rhivda_done == 0, tat_test_confirm, NA_integer_, NA_integer_), na.rm = TRUE),
      react_enroll_saccl  = median(if_else(rhivda_done == 0, tat_reactive_enroll, NA_integer_, NA_integer_), na.rm = TRUE),
      test_enroll_saccl   = median(if_else(rhivda_done == 0, tat_confirm_enroll, NA_integer_, NA_integer_), na.rm = TRUE),
   ) %>%
   ungroup() %>%
   mutate_if(
      .predicate = is.numeric,
      ~as.integer(floor(.))
   )

coa$out$region$kap_tat <- coa$out$faci$kap_tat %>%
   group_by(hts_tst_pos_reg, kap_type) %>%
   summarise_at(
      .vars = vars(ends_with("_all"), ends_with("_crcl"), ends_with("_saccl")),
      list(
         ~min(., na.rm = TRUE),
         ~median(., na.rm = TRUE),
         ~max(., na.rm = TRUE)
      )
   ) %>%
   select(any_of(starts_with(names(coa$out$faci$kap_tat)))) %>%
   mutate_if(
      .predicate = is.numeric,
      ~as.integer(floor(.))
   )

coa$out$region$kap_enroll <- coa$data$dx %>%
   filter(year >= 2022) %>%
   group_by(hts_tst_pos_reg, kap_type, year, qr, confirm_type) %>%
   summarise(
      dx     = n(),
      enroll = sum(if_else(!is.na(everonart), 1, 0, 0))
   ) %>%
   ungroup() %>%
   mutate_if(
      .predicate = is.numeric,
      ~as.integer(floor(.))
   ) %>%
   mutate(
      enroll_rate = enroll / dx
   )

coa$out$natl$tat <- coa$data$dx %>%
   filter(year >= 2017) %>%
   group_by(year, qr) %>%
   summarise(
      react_confirm_all   = median(tat_reactive_confirm, na.rm = TRUE),
      test_confirm_all    = median(tat_test_confirm, na.rm = TRUE),
      react_enroll_all    = median(tat_reactive_enroll, na.rm = TRUE),
      test_enroll_all     = median(tat_confirm_enroll, na.rm = TRUE),
      react_confirm_crcl  = median(if_else(rhivda_done == 1, tat_reactive_confirm, NA_integer_, NA_integer_), na.rm = TRUE),
      test_confirm_crcl   = median(if_else(rhivda_done == 1, tat_test_confirm, NA_integer_, NA_integer_), na.rm = TRUE),
      react_enroll_crcl   = median(if_else(rhivda_done == 1, tat_reactive_enroll, NA_integer_, NA_integer_), na.rm = TRUE),
      test_enroll_crcl    = median(if_else(rhivda_done == 1, tat_confirm_enroll, NA_integer_, NA_integer_), na.rm = TRUE),
      react_confirm_saccl = median(if_else(rhivda_done == 0, tat_reactive_confirm, NA_integer_, NA_integer_), na.rm = TRUE),
      test_confirm_saccl  = median(if_else(rhivda_done == 0, tat_test_confirm, NA_integer_, NA_integer_), na.rm = TRUE),
      react_enroll_saccl  = median(if_else(rhivda_done == 0, tat_reactive_enroll, NA_integer_, NA_integer_), na.rm = TRUE),
      test_enroll_saccl   = median(if_else(rhivda_done == 0, tat_confirm_enroll, NA_integer_, NA_integer_), na.rm = TRUE),
   ) %>%
   ungroup() %>%
   mutate_if(
      .predicate = is.numeric,
      ~as.integer(floor(.))
   )

coa$out$natl$tat_kp <- coa$data$dx %>%
   filter(year >= 2017) %>%
   group_by(year, qr, kap_type) %>%
   summarise(
      react_confirm_all   = median(tat_reactive_confirm, na.rm = TRUE),
      test_confirm_all    = median(tat_test_confirm, na.rm = TRUE),
      react_enroll_all    = median(tat_reactive_enroll, na.rm = TRUE),
      test_enroll_all     = median(tat_confirm_enroll, na.rm = TRUE),
      react_confirm_crcl  = median(if_else(rhivda_done == 1, tat_reactive_confirm, NA_integer_, NA_integer_), na.rm = TRUE),
      test_confirm_crcl   = median(if_else(rhivda_done == 1, tat_test_confirm, NA_integer_, NA_integer_), na.rm = TRUE),
      react_enroll_crcl   = median(if_else(rhivda_done == 1, tat_reactive_enroll, NA_integer_, NA_integer_), na.rm = TRUE),
      test_enroll_crcl    = median(if_else(rhivda_done == 1, tat_confirm_enroll, NA_integer_, NA_integer_), na.rm = TRUE),
      react_confirm_saccl = median(if_else(rhivda_done == 0, tat_reactive_confirm, NA_integer_, NA_integer_), na.rm = TRUE),
      test_confirm_saccl  = median(if_else(rhivda_done == 0, tat_test_confirm, NA_integer_, NA_integer_), na.rm = TRUE),
      react_enroll_saccl  = median(if_else(rhivda_done == 0, tat_reactive_enroll, NA_integer_, NA_integer_), na.rm = TRUE),
      test_enroll_saccl   = median(if_else(rhivda_done == 0, tat_confirm_enroll, NA_integer_, NA_integer_), na.rm = TRUE),
   ) %>%
   ungroup() %>%
   mutate_if(
      .predicate = is.numeric,
      ~as.integer(floor(.))
   )


coa$out$faci$tat %>%
   write_sheet(coa$ss, "Bene (TAT-Facility)")
coa$out$region$tat %>%
   write_sheet(coa$ss, "Bene (TAT-Region)")

coa$out$faci$enroll %>%
   write_sheet(coa$ss, "Bene (Enrollment Rate-Facility)")
coa$out$region$enroll %>%
   write_sheet(coa$ss, "Bene (Enrollment Rate-Region)")

coa$out$region$kap_tat %>%
   write_sheet(coa$ss, "Bene (TAT-KP)-20-23")
coa$out$region$kap_enroll %>%
   write_sheet(coa$ss, "Bene (Enrollment Rate-KP)")

coa$out$natl$tat %>%
   write_sheet(coa$ss, "Bene (TAT-National)")
coa$out$natl$tat %>%
   write_sheet(coa$ss, "Bene (TAT-National)")
coa$out$natl$tat_kp %>%
   write_sheet(coa$ss, "Bene (TAT-National_KP)")

lw_conn <- ohasis$conn("lw")
ohasis$upsert(lw_conn, "nhsss_coanalysis", "tat_amongdx_byfaci", coa$out$faci$tat, c("hts_tst_pos_reg", "hts_tst_pos_prov", "hts_tst_pos_munc", "hts_tst_pos_faci"))
ohasis$upsert(
   lw_conn,
   "nhsss_coanalysis",
   "tat_amongdx_byfaci",
   coa$data$dx %>%
      select(
         idnum,

      ),
   c("hts_tst_pos_reg", "hts_tst_pos_prov", "hts_tst_pos_munc", "hts_tst_pos_faci")
)
dbDisconnect(lw_conn)

invisible(lapply(c("Bene (TAT-Facility)", "Bene (TAT-Region)"), function(sheet) range_autofit(coa$ss, sheet)))