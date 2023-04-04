dr       <- new.env()
dr$ss    <- as_id("1qiiIjUbRpvzVSiSlQpbBf9MXjXqu_cbQVtM32I-Szyc")
dr$sites <- read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw") %>%
   distinct(FACI_ID, .keep_all = TRUE) %>%
   mutate(site_pepfar = coalesce(site_epic_2022, site_icap_2023)) %>%
   filter(site_pepfar == 1)

pepfar_fy <- function(date) {
   year  <- year(date)
   month <- month(date)
   # Qr    <- case_when(
   #    month %in% seq(1, 3) ~ stri_c("Q2 ", year),
   #    month %in% seq(4, 6) ~ stri_c("Q3 ", year),
   #    month %in% seq(7, 9) ~ stri_c("Q4 ", year),
   #    month %in% seq(10, 12) ~ stri_c("Q1 ", year + 1),
   # )
   Qr    <- case_when(
      month %in% seq(1, 3) ~ stri_c(year, "02"),
      month %in% seq(4, 6) ~ stri_c(year, "03"),
      month %in% seq(7, 9) ~ stri_c(year, "04"),
      month %in% seq(10, 12) ~ stri_c(year + 1, "01"),
   )
   Qr    <- as.integer(Qr)
   return(Qr)
}

sort_list <- function(list, desc = FALSE) {
   sorted <- sort(names(list), decreasing = desc)
   list   <- setNames(lapply(sorted, FUN = function(n) list[[n]]), sorted)

   return(list)
}

conn         <- ohasis$conn("lw")
dbname       <- "ohasis_warehouse"
dr$oh$id_reg <- dbTable(conn, dbname, "id_registry", c("PATIENT_ID", "CENTRAL_ID"))
dr$oh$prep   <- dbTable(conn, dbname, "form_prep")
dr$oh$hts    <- dbTable(conn, dbname, "form_hts")
dr$oh$a      <- dbTable(conn, dbname, "form_a")
dr$oh$cfbs   <- dbTable(conn, dbname, "form_cfbs")
dbDisconnect(conn)

##  PrEP -----------------------------------------------------------------------

dr$harp$prep <- read_dta(hs_data("prep", "outcome", 2022, 12))
prep_risk    <- dr$harp$prep %>%
   select(
      prep_id,
      contains("risk", ignore.case = FALSE)
   ) %>%
   select(-ends_with("screen")) %>%
   pivot_longer(
      cols = contains("risk", ignore.case = FALSE)
   ) %>%
   group_by(prep_id) %>%
   summarise(
      risks = stri_c(collapse = ", ", unique(sort(value)))
   ) %>%
   ungroup()

dr$data$prep_new <- dr$harp$prep %>%
   left_join(y = prep_risk, by = join_by(prep_id)) %>%
   get_cid(dr$oh$id_reg, PATIENT_ID) %>%
   mutate(
      # sex
      sex      = coalesce(StrLeft(sex, 1), "(no data)"),

      # KAP
      msm      = case_when(
         sex == "M" & stri_detect_fixed(prep_risk_sexwithm, "yes") ~ 1,
         sex == "M" & stri_detect_fixed(hts_risk_sexwithm, "yes") ~ 1,
         sex == "M" & kp_msm == 1 ~ 1,
         TRUE ~ 0
      ),
      tgw      = if_else(
         condition = sex == "MALE" & self_identity %in% c("FEMALE", "OTHERS"),
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      hetero   = case_when(
         sex == "M" &
            !grepl("yes-", stri_c(hts_risk_sexwithm, prep_risk_sexwithm)) &
            grepl("yes-", stri_c(hts_risk_sexwithm, prep_risk_sexwithm)) ~ 1,
         sex == "F" &
            grepl("yes-", stri_c(hts_risk_sexwithm, prep_risk_sexwithm)) &
            !grepl("yes-", stri_c(hts_risk_sexwithm, prep_risk_sexwithm)) ~ 1,
         TRUE ~ 0
      ),
      pwid     = case_when(
         str_detect(prep_risk_injectdrug, "yes") ~ 1,
         str_detect(hts_risk_injectdrug, "yes") ~ 1,
         kp_pwid == 1 ~ 1,
         TRUE ~ 0
      ),
      unknown  = if_else(
         condition = risks == "(no data)",
         true      = 1,
         false     = 0,
         missing   = 0
      ),

      # kap
      sex      = case_when(
         sex == "M" ~ "Male",
         sex == "F" ~ "Female",
         TRUE ~ "(no data)"
      ),
      kap_type = case_when(
         msm == 1 & tgw == 0 ~ "MSM",
         msm == 1 & tgw == 1 ~ "MSM-TGW",
         hetero == 0 & pwid == 1 ~ "PWID",
         sex == "Male" ~ "Other Males",
         sex == "Female" ~ "Other Females",
         TRUE ~ "Other"
      ),

      Age_Band = gen_agegrp(curr_age, "5yr"),
      prep_new = pepfar_fy(prepstart_date)
   ) %>%
   filter(prep_new >= 202101) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(PREP_FACI = "faci", PREP_SUB_FACI = "branch")
   ) %>%
   mutate_at(
      .vars = vars(PREP_FACI),
      ~if_else(. == "130000", NA_character_, ., .),
   ) %>%
   distinct(prep_id, .keep_all = TRUE) %>%
   inner_join(
      y  = dr$sites %>%
         select(FACI_ID),
      by = join_by(PREP_FACI == FACI_ID)
   ) %>%
   ohasis$get_faci(
      list(prep_new_faci = c("PREP_FACI", "PREP_SUB_FACI")),
      "name",
      c("prep_new_reg", "prep_new_prov", "prep_new_munc")
   )

dr$data$prep_ct <- dr$data$prep_new %>%
   select(-Age_Band) %>%
   left_join(
      y        = dr$oh$prep %>%
         get_cid(dr$oh$id_reg, PATIENT_ID) %>%
         filter(!is.na(MEDICINE_SUMMARY), RECORD_DATE >= "2021-01-01" & RECORD_DATE <= "2022-12-31") %>%
         mutate(
            prep_ct         = pepfar_fy(RECORD_DATE),

            use_record_faci = if_else(
               condition = is.na(SERVICE_FACI),
               true      = 1,
               false     = 0
            ),
            SERVICE_FACI    = if_else(
               condition = use_record_faci == 1,
               true      = FACI_ID,
               false     = SERVICE_FACI
            ),

            curr_age        = case_when(
               !is.na(BIRTHDATE) ~ calc_age(BIRTHDATE, RECORD_DATE),
               TRUE ~ as.integer(AGE)
            ),
            Age_Band        = gen_agegrp(curr_age, "5yr"),
         ) %>%
         filter(prep_ct >= 202101) %>%
         inner_join(
            y  = dr$sites %>%
               select(FACI_ID),
            by = join_by(SERVICE_FACI == FACI_ID)
         ) %>%
         ohasis$get_faci(
            list(prep_ct_faci = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
            "name",
            c("prep_ct_reg", "prep_ct_prov", "prep_ct_munc")
         ),
      by       = join_by(CENTRAL_ID),
      multiple = "all"
   ) %>%
   filter(prep_ct > prep_new) %>%
   arrange(CENTRAL_ID, RECORD_DATE) %>%
   distinct(CENTRAL_ID, prep_ct, .keep_all = TRUE)

##  HTS ------------------------------------------------------------------------

dr$harp$dx <- read_dta(hs_data("harp_dx", "reg", 2022, 12))

dr$data$hts_tst_pos <- dr$harp$dx %>%
   mutate(
      modality    = if_else(modality == "", "FBT", modality),
      ref_report  = as.Date(paste(sep = "-", year, stri_pad_left(month, 2, "0"), "01")),
      hts_tst_pos = pepfar_fy(ref_report)
   ) %>%
   filter(hts_tst_pos > 201901) %>%
   mutate_at(
      .vars = vars(dx_region, dx_province, dx_muncity, dxlab_standard),
      ~str_squish(.)
   ) %>%
   left_join(
      y  = read_sheet(as_id("1WiUiB7n5qkvyeARwGV1l1ipuCknDT8wZ6Pt7662J2ms"), "Sheet1") %>%
         rename(
            DX_FACI     = FACI_ID,
            DX_SUB_FACI = SUB_FACI_ID
         ),
      by = join_by(dx_region, dx_province, dx_muncity, dxlab_standard)
   ) %>%
   left_join(
      y  = ohasis$ref_faci %>%
         filter(!is.na(FACI_NAME_CLEAN)) %>%
         select(
            NHSSS_FACI     = FACI_ID,
            NHSSS_SUB_FACI = SUB_FACI_ID,
            dxlab_standard = FACI_NAME_CLEAN,
            dx_region      = FACI_NHSSS_REG,
            dx_province    = FACI_NHSSS_PROV,
            dx_muncity     = FACI_NHSSS_MUNC,
         ) %>%
         arrange(desc(NHSSS_SUB_FACI), dx_region, dx_province, dx_muncity, dxlab_standard) %>%
         distinct(dx_region, dx_province, dx_muncity, dxlab_standard, .keep_all = TRUE) %>%
         mutate_all(~str_squish(.)),
      by = join_by(dx_region, dx_province, dx_muncity, dxlab_standard)
   ) %>%
   mutate(
      HARPDX_FACI     = if_else(DX_FACI == "", NHSSS_FACI, DX_FACI),
      HARPDX_SUB_FACI = if_else(DX_SUB_FACI == "", NHSSS_SUB_FACI, DX_SUB_FACI),
   ) %>%
   inner_join(
      y  = dr$sites %>%
         select(FACI_ID),
      by = join_by(HARPDX_FACI == FACI_ID)
   ) %>%
   ohasis$get_faci(
      list(hts_tst_pos_faci = c("HARPDX_FACI", "HARPDX_SUB_FACI")),
      "name",
      c("hts_tst_pos_reg", "hts_tst_pos_prov", "hts_tst_pos_munc")
   )

reach <- dr$data$hts_tst_pos %>%
   mutate(
      reach_clinical = if_else(modality == "FBT", 1, reach_clinical, reach_clinical)
   ) %>%
   select(
      idnum,
      starts_with("reach")
   ) %>%
   filter(if_any(starts_with("reach"), ~!is.na(.))) %>%
   pivot_longer(starts_with("reach")) %>%
   filter(!is.na(value)) %>%
   arrange(idnum, name) %>%
   mutate(name = stri_replace_all_fixed(name, "reach_", "")) %>%
   group_by(idnum) %>%
   summarise(reach = stri_c(collapse = ", ", name)) %>%
   ungroup()

dr$data$hts_tst_pos %<>%
   left_join(
      y = reach
   )

##  HTS_TST --------------------------------------------------------------------

dr$data$hts_tst <- process_hts(dr$oh$hts, dr$oh$a, dr$oh$cfbs) %>%
   mutate(
      hts_tst = pepfar_fy(hts_date)
   ) %>%
   filter(
      hts_tst > 201901
   ) %>%
   get_cid(dr$oh$id_reg, PATIENT_ID) %>%
   mutate_if(
      .predicate = is.POSIXct,
      ~null_dates(., "POSIXct")
   ) %>%
   mutate_if(
      .predicate = is.Date,
      ~null_dates(., "Date")
   ) %>%
   mutate(
      hts_priority = case_when(
         CONFIRM_RESULT %in% c(1, 2, 3) ~ 1,
         hts_result != "(no data)" & src %in% c("hts2021", "a2017") ~ 2,
         hts_result != "(no data)" & hts_modality == "FBT" ~ 3,
         hts_result != "(no data)" & hts_modality == "CBS" ~ 4,
         hts_result != "(no data)" & hts_modality == "FBS" ~ 5,
         hts_result != "(no data)" & hts_modality == "ST" ~ 6,
         TRUE ~ 9999
      )
   ) %>%
   left_join(
      y  = dr$harp$dx %>%
         get_cid(dr$oh$id_reg, PATIENT_ID) %>%
         select(
            CENTRAL_ID,
            idnum,
            transmit,
            sexhow,
            confirm_date,
            ref_report,
            HARPDX_BIRTHDATE  = bdate,
            HARPDX_SEX        = sex,
            HARPDX_SELF_IDENT = self_identity,
            HARPDX_FACI,
            HARPDX_SUB_FACI
         ),
      by = join_by(CENTRAL_ID)
   )

risk <- dr$data$hts_tst %>%
   select(
      REC_ID,
      contains("risk", ignore.case = FALSE)
   ) %>%
   pivot_longer(
      cols = contains("risk", ignore.case = FALSE)
   ) %>%
   group_by(REC_ID) %>%
   summarise(
      risks = stri_c(collapse = ", ", unique(sort(value)))
   )

dr$data$hts_tst %<>%
   left_join(y = risk, by = join_by(REC_ID)) %>%
   mutate(
      # tag if central to be used
      use_harpdx        = if_else(
         condition = !is.na(idnum),
         true      = 1,
         false     = 0,
         missing   = 0
      ),

      # old confirmed
      old_dx            = case_when(
         confirm_date >= hts_date ~ 0,
         confirm_date < hts_date ~ 1,
         TRUE ~ 0
      ),

      # tag those without form faci
      use_record_faci   = if_else(
         condition = is.na(SERVICE_FACI),
         true      = 1,
         false     = 0
      ),

      # tag which test to be used
      FINAL_FACI        = if_else(
         condition = use_record_faci == 1,
         true      = FACI_ID,
         false     = SERVICE_FACI
      ),
      FINAL_SUB_FACI    = case_when(
         use_record_faci == 1 & FACI_ID == "130000" ~ SPECIMEN_SUB_SOURCE,
         !(SERVICE_FACI %in% c("130001", "130605", "040200")) ~ NA_character_,
         nchar(SERVICE_SUB_FACI) == 6 ~ NA_character_,
         TRUE ~ SERVICE_SUB_FACI
      ),

      HTS_TST_RESULT    = case_when(
         hts_result == "R" ~ "Reactive",
         hts_result == "NR" ~ "Non-reactive",
         hts_result == "IND" ~ "Indeterminate",
         is.na(hts_result) ~ "(no data)",
         TRUE ~ hts_result
      ),

      FINAL_TEST_RESULT = case_when(
         old_dx == 1 & !is.na(idnum) ~ "Confirmed: Known Pos",
         old_dx == 0 & !is.na(idnum) ~ "Confirmed: Positive",
         CONFIRM_RESULT == 1 ~ "Confirmed: Positive",
         CONFIRM_RESULT == 2 ~ "Confirmed: Negative",
         CONFIRM_RESULT == 3 ~ "Confirmed: Indeterminate",
         hts_modality == "FBT" ~ paste0("Tested: ", HTS_TST_RESULT),
         hts_modality == "FBS" ~ paste0("Tested: ", HTS_TST_RESULT),
         hts_modality == "CBS" ~ paste0("CBS: ", HTS_TST_RESULT),
         hts_modality == "ST" ~ paste0("Self-Testing: ", HTS_TST_RESULT),
      ),
   )


confirm_data <- dr$data$hts_tst %>%
   mutate(
      FINAL_CONFIRM_DATE = case_when(
         old_dx == 0 & hts_priority == 1 ~ as.Date(coalesce(DATE_CONFIRM, T3_DATE, T2_DATE, T1_DATE)),
         old_dx == 1 ~ confirm_date,
         TRUE ~ NA_Date_
      )
   ) %>%
   filter(!is.na(FINAL_CONFIRM_DATE)) %>%
   select(CENTRAL_ID, FINAL_CONFIRM_DATE) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE)

reach <- dr$data$hts_tst_pos %>%
   mutate(
      reach_clinical = if_else(modality == "FBT", 1, reach_clinical, reach_clinical)
   ) %>%
   select(
      CENTRAL_ID,
      starts_with("reach")
   ) %>%
   filter(if_any(starts_with("reach"), ~!is.na(.))) %>%
   pivot_longer(starts_with("reach")) %>%
   filter(!is.na(value)) %>%
   arrange(CENTRAL_ID, name) %>%
   mutate(name = stri_replace_all_fixed(name, "reach_", "")) %>%
   group_by(CENTRAL_ID) %>%
   summarise(reach = stri_c(collapse = ", ", name)) %>%
   ungroup()

dr$data$hts_tst %<>%
   left_join(y = confirm_data, by = join_by(CENTRAL_ID)) %>%
   filter(HTS_TST_RESULT != "(no data)") %>%
   left_join(
      y = reach
   ) %>%
   arrange(hts_date) %>%
   distinct(CENTRAL_ID, hts_tst, .keep_all = TRUE) %>%
   ohasis$get_faci(
      list(`Site/Organization` = c("FINAL_FACI", "FINAL_SUB_FACI")),
      "nhsss",
      c("hts_reg", "hts_prov", "hts_munc")
   )

##  TX -------------------------------------------------------------------------

dr$harp$tx <- list()
for (yr in seq(2018, 2022)) {
   for (mo in c(3, 6, 9, 12)) {
      date <- as.Date(str_c(sep = "-", yr, stri_pad_left(mo, 2, "0"), "01"))
      max  <- ceiling_date(date, unit = "months") %m-% days(1)
      qr   <- pepfar_fy(date)

      if (qr >= 201901) {
         dr$harp$tx[[as.character(qr)]] <- hs_data("harp_tx", "outcome", yr, mo) %>%
            read_dta(col_select = c(any_of(c("art_id", "age", "curr_age", "vl_date", "sacclcode", "baseline_vl", "vlp12m", "hub", "branch")), idnum, latest_nextpickup, outcome, artstart_date)) %>%
            mutate(
               fy        = qr,
               outcome28 = hiv_tx_outcome(outcome, latest_nextpickup, max, 28), onart28 = if_else(
                  condition = outcome28 == "alive on arv",
                  true      = 1,
                  false     = 0,
                  missing   = 0
               ),
               tx_curr   = if_else(outcome28 == "alive on arv", fy, NA_integer_, NA_integer_),
               branch    = if ("branch" %in% names(.)) branch else NA_character_,
            ) %>%
            mutate_at(
               .vars = vars(any_of(c("hub", "branch"))),
               ~toupper(str_squish(.))
            ) %>%
            faci_code_to_id(
               ohasis$ref_faci_code,
               c(TX_FACI = "hub", TX_SUB_FACI = "branch")
            ) %>%
            inner_join(
               y  = dr$sites %>%
                  select(FACI_ID),
               by = join_by(TX_FACI == FACI_ID)
            ) %>%
            ohasis$get_faci(
               list(tx_faci = c("TX_FACI", "TX_SUB_FACI")),
               "name",
               c("tx_reg", "tx_prov", "tx_munc")
            )

         if (qr >= 202001) {
            dr$harp$tx[[as.character(qr)]] %<>%
               mutate(
                  vl_date       = if (qr == 202001) as.Date(vl_date, origin = "1960-01-01") else vl_date,
                  vl_elig       = if_else(
                     onart28 == 1 & interval(artstart_date, vl_date) / days(1) <= 92,
                     fy,
                     NA_integer_,
                     NA_integer_
                  ),
                  vl_tested     = if_else(
                     onart28 == 1 &
                        coalesce(baseline_vl, 0) == 0 &
                        !is.na(vlp12m),
                     fy,
                     NA_integer_,
                     NA_integer_
                  ),
                  vl_suppressed = if_else(
                     onart28 == 1 &
                        coalesce(baseline_vl, 0) == 0 &
                        vlp12m == 1,
                     fy,
                     NA_integer_,
                     NA_integer_
                  )
               )
         }

         if (yr >= 2022) {
            dr$harp$tx[[as.character(qr)]] %<>%
               left_join(
                  y  = hs_data("harp_tx", "reg", yr, mo) %>%
                     read_dta(col_select = c(art_id, confirmatory_code, sex)) %>%
                     mutate(
                        sacclcode = if_else(!is.na(confirmatory_code), str_replace_all(confirmatory_code, "[^[:alnum:]]", ""), NA_character_)
                     ) %>%
                     select(-confirmatory_code),
                  by = join_by(art_id)
               ) %>%
               mutate(age = curr_age)
         }


         dr$harp$tx[[as.character(qr)]] %<>%
            left_join(
               y = hs_data("harp_dx", "reg", 2022, 12) %>%
                  read_dta(col_select = c(idnum, sex, transmit, sexhow, pregnant)) %>%
                  mutate(
                     mot = case_when(
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
                  ) %>%
                  select(idnum, mot)
            ) %>%
            mutate(
               mot      = coalesce(mot, "(no data)"),
               Age_Band = gen_agegrp(age, "5yr"),
            )
      }
   }
}

dr$harp$tx <- sort_list(dr$harp$tx, TRUE)
for (i in seq_len(length(dr$harp$tx) - 1)) {
   dr$harp$tx[[i]] %<>%
      left_join(
         y  = dr$harp$tx[[i + 1]] %>%
            select(
               matches("art_id"),
               sacclcode,
               onart28_prev = onart28
            ),
         by = if ("art_id" %in% names(dr$harp$tx[[i + 1]])) join_by(art_id) else join_by(sacclcode)
      ) %>%
      mutate(
         tx_rtt = if_else(onart28_prev == 0 & onart28 == 1, fy, NA_integer_, NA_integer_),
         tx_ml  = if_else(onart28_prev == 1 & onart28 == 0, fy, NA_integer_, NA_integer_),
      )
}

dr$data$tx_curr <- bind_rows(dr$harp$tx) %>%
   filter(!is.na(tx_curr))

dr$data$tx_rtt <- bind_rows(dr$harp$tx) %>%
   filter(!is.na(tx_rtt))

dr$data$tx_ml <- bind_rows(dr$harp$tx) %>%
   filter(!is.na(tx_ml))

dr$data$tx_new <- dr$harp$tx$`202301` %>%
   mutate(
      tx_new = pepfar_fy(artstart_date)
   ) %>%
   filter(tx_new >= 201901)

dr$data$vl_elig <- bind_rows(dr$harp$tx) %>%
   filter(!is.na(vl_elig))

dr$data$vl_tested <- bind_rows(dr$harp$tx) %>%
   filter(!is.na(vl_tested))

dr$data$vl_suppressed <- bind_rows(dr$harp$tx) %>%
   filter(!is.na(vl_suppressed))

##  Aggregate ------------------------------------------------------------------

agg_prep <- function(data, ind_name, ...) {
   var <- as.name(ind_name)
   agg <- data[[ind_name]] %>%
      mutate(qr = stri_c(StrLeft({{var}}, 4), "-", "Q", StrRight({{var}}, 1))) %>%
      group_by(qr, ...) %>%
      summarise(total = n()) %>%
      ungroup() %>%
      mutate(ind = toupper(ind_name))
}

dr$flat$natl$prep <- agg_prep(dr$data, "prep_ct", Age_Band, kap_type) %>%
   mutate(scope = "natl") %>%
   bind_rows(
      agg_prep(dr$data, "prep_ct", Age_Band, kap_type, prep_ct_reg, prep_ct_faci) %>%
         mutate(
            scope    = case_when(
               prep_ct_reg == "Region VII (Central Visayas)" ~ "reg7",
               prep_ct_reg == "Region VI (Western Visayas)" ~ "reg6",
            ),
            facility = prep_ct_faci
         ) %>%
         select(-starts_with("prep_ct"))
   ) %>%
   bind_rows(
      agg_prep(dr$data, "prep_new", Age_Band, kap_type) %>%
         mutate(scope = "natl")
   ) %>%
   bind_rows(
      agg_prep(dr$data, "prep_new", Age_Band, kap_type, prep_reg, prep_new_faci) %>%
         mutate(
            scope    = case_when(
               prep_reg == "7" ~ "reg7",
               prep_reg == "6" ~ "reg6",
            ),
            facility = prep_new_faci
         ) %>%
         select(-prep_reg, -prep_new_faci)
   ) %>%
   filter(!is.na(scope)) %>%
   arrange(ind, scope, qr) %>%
   pivot_wider(
      id_cols     = c(ind, Age_Band, kap_type, scope, facility),
      names_from  = qr,
      values_from = total,
   ) %>%
   write_sheet(dr$ss, "PrEP")

dr$flat$natl$hts <- agg_prep(dr$data, "hts_tst_pos", dx_region) %>%
   mutate(scope = "natl") %>%
   bind_rows(
      agg_prep(dr$data, "hts_tst_pos", dx_region, modality, reach)
   ) %>%
   mutate(
      scope    = case_when(
         is.na(scope) & dx_region == "7" ~ "reg7",
         is.na(scope) & dx_region == "6" ~ "reg6",
         TRUE ~ scope
      ),
      modality = coalesce(modality, "all")
   ) %>%
   filter(!is.na(scope)) %>%
   arrange(scope, qr) %>%
   pivot_wider(
      id_cols     = c(ind, scope, dx_region, modality, reach),
      names_from  = qr,
      values_from = total,
   ) %>%
   write_sheet(dr$ss, "HTS")

dr$flat$natl$tst <- agg_prep(dr$data, "hts_tst", hts_reg) %>%
   mutate(scope = "natl") %>%
   bind_rows(
      agg_prep(dr$data, "hts_tst", hts_reg, hts_modality, reach)
   ) %>%
   mutate(
      scope        = case_when(
         is.na(scope) & dx_region == "7" ~ "reg7",
         is.na(scope) & dx_region == "6" ~ "reg6",
         TRUE ~ scope
      ),
      hts_modality = coalesce(hts_modality, "all")
   ) %>%
   filter(!is.na(scope)) %>%
   arrange(scope, qr) %>%
   pivot_wider(
      id_cols     = c(ind, scope, hts_reg, modality, reach),
      names_from  = qr,
      values_from = total,
   ) %>%
   write_sheet(dr$ss, "HTS")

dr$flat$natl$tx <- agg_prep(dr$data, "tx_curr", tx_reg, Age_Band, mot) %>%
   mutate(scope = "natl") %>%
   bind_rows(
      agg_prep(dr$data, "tx_new", tx_reg, Age_Band, mot) %>%
         mutate(scope = "natl")
   ) %>%
   bind_rows(
      agg_prep(dr$data, "tx_rtt", tx_reg, Age_Band, mot) %>%
         mutate(scope = "natl")
   ) %>%
   bind_rows(
      agg_prep(dr$data, "tx_ml", tx_reg, Age_Band, mot) %>%
         mutate(scope = "natl")
   ) %>%
   filter(!is.na(scope)) %>%
   arrange(ind, scope, qr) %>%
   pivot_wider(
      id_cols     = c(ind, scope, tx_reg, Age_Band, mot),
      names_from  = qr,
      values_from = total,
   ) %>%
   write_sheet(dr$ss, "TX")

dr$flat$natl$vl <- agg_prep(dr$data, "vl_elig", tx_reg, Age_Band, mot) %>%
   mutate(scope = "natl") %>%
   bind_rows(
      agg_prep(dr$data, "vl_tested", tx_reg, Age_Band, mot) %>%
         mutate(scope = "natl")
   ) %>%
   bind_rows(
      agg_prep(dr$data, "vl_suppressed", tx_reg, Age_Band, mot) %>%
         mutate(scope = "natl")
   ) %>%
   filter(!is.na(scope)) %>%
   arrange(ind, scope, qr) %>%
   pivot_wider(
      id_cols     = c(ind, scope, tx_reg, Age_Band, mot),
      names_from  = qr,
      values_from = total,
   ) %>%
   write_sheet(dr$ss, "VL")