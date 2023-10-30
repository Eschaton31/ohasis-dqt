faci_type <- read_sheet("1aOqYjx5wbc403xy-64YHJU6NzhEBRUu6Ldg59yDEUMw", "Sheet1", range = "A:D", col_types = "c")
tx        <- list()
for (yr in c("2021", "2022", "2023")) {
   for (mo in c("03", "06", "09", "12")) {
      if (yr == "2023" & mo %in% c("09", "12"))
         mo <- "08"

      ym       <- stri_c(yr, mo)
      tx[[ym]] <- read_dta(hs_data("harp_tx", "outcome", yr, mo), col_select = any_of(c("art_id", "idnum", "confirmatory_code", "sacclcode", "realhub", "realhub_branch", "hub", "outcome", "latest_nextpickup", "latest_regimen", "vl_result", "vl_date", "baseline_vl", "vlp12m"))) %>%
         mutate(
            realhub        = if ("realhub" %in% names(.) & ym != "202203") realhub else toupper(hub),
            realhub_branch = if ("realhub_branch" %in% names(.)) realhub_branch else NA_character_,

            outcome30      = hiv_tx_outcome(outcome, latest_nextpickup, end_ym(yr, mo), 30, "days"),
            onart30        = if_else(outcome30 == "alive on arv", 1, 0, 0),
            tld            = if_else(str_detect(toupper(latest_regimen), "TDF") &
                                        str_detect(toupper(latest_regimen), "3TC") &
                                        str_detect(toupper(latest_regimen), "DTG"), 1, 0, 0),
         ) %>%
         faci_code_to_id(
            ohasis$ref_faci_code,
            list(FACI_ID = "realhub", SUB_FACI_ID = "realhub_branch")
         )
   }
}
tx_reg        <- read_dta(hs_data("harp_tx", "reg", yr, mo), col_select = any_of(c("art_id", "confirmatory_code", "artstart_date", "artstart_hub", "artstart_branch", "artstart_regimen", "REC_ID")))
prep_reg      <- read_dta(hs_data("prep", "prepstart", yr, mo), col_select = any_of(c("prep_id", "prepstart_date", "prepstart_faci", "prepstart_branch", "prepstart_regimen", "REC_ID")))
lw_conn       <- ohasis$conn("lw")
start_regimen <- dbxSelect(lw_conn, "SELECT REC_ID, MEDICINE_SUMMARY FROM ohasis_warehouse.form_art_bc WHERE REC_ID IN (?)", params = list(tx_reg$REC_ID))
dbDisconnect(lw_conn)

sites_tx <- tx$`202308` %>%
   distinct(FACI_ID, SUB_FACI_ID)

tx_sum <- tx
tx_sum <- lapply(tx_sum, function(data) {
   sum <- data %>%
      mutate(
         `Total PLHIV on TLD` = if_else(onart30 == 1 & tld == 1, 1, 0, 0)
      ) %>%
      rename(
         `Total PLHIV on ART` = onart30,
      ) %>%
      group_by(FACI_ID, SUB_FACI_ID) %>%
      summarise_at(
         .vars = vars(`Total PLHIV on ART`, `Total PLHIV on TLD`),
         ~as.integer(sum(.))
      ) %>%
      ungroup()

   return(sum)
})
tx_sum %<>%
   bind_rows(.id = "ym") %>%
   mutate(
      Year    = StrLeft(ym, 4),
      Quarter = as.numeric(StrRight(ym, 2)),
      Quarter = case_when(
         Quarter <= 3 ~ "Q1",
         Quarter <= 6 ~ "Q2",
         Quarter <= 9 ~ "Q3",
         Quarter <= 12 ~ "Q4",
      )
   )


iit <- list()
for (ym in c("2022-09", "2022-12", "2023-03", "2023-06", "2023-09")) {
   split   <- strsplit(ym, "-")[[1]]
   curr_yr <- split[1]
   curr_mo <- split[2]

   if (ym == "2023-09")
      curr_mo <- "08"

   date    <- paste(sep = "-", ym, "01")
   max     <- date %>%
      as.Date() %m-%
      months(3) %>%
      as.character()
   split   <- strsplit(max, "-")[[1]]
   prev_yr <- split[1]
   prev_mo <- split[2]

   iit[[ym]] <- read_dta(hs_data("harp_tx", "outcome", curr_yr, curr_mo), col_select = any_of(c("art_id", "realhub", "realhub_branch", "outcome", "latest_nextpickup"))) %>%
      mutate(
         curr_outcome30 = hiv_tx_outcome(outcome, latest_nextpickup, end_ym(curr_yr, curr_mo), 30, "days"),
         curr_onart30   = if_else(curr_outcome30 == "alive on arv", 1, 0, 0),
      ) %>%
      left_join(
         y  = read_dta(hs_data("harp_tx", "outcome", prev_yr, prev_mo), col_select = any_of(c("art_id", "outcome", "latest_nextpickup"))) %>%
            mutate(
               prev_outcome30 = hiv_tx_outcome(outcome, latest_nextpickup, end_ym(prev_yr, prev_mo), 30, "days"),
               prev_onart30   = if_else(prev_outcome30 == "alive on arv", 1, 0, 0),
            ) %>%
            select(art_id, prev_outcome30, prev_onart30),
         by = join_by(art_id)
      ) %>%
      mutate(
         TX_CURR = if_else(curr_onart30 == 1, 1, 0, 0),
         TX_RTT  = if_else(curr_onart30 == 1 & prev_onart30 == 0, 1, 0, 0),
         TX_ML   = if_else(curr_onart30 == 0 & prev_onart30 == 1, 1, 0, 0),
      ) %>%
      faci_code_to_id(
         ohasis$ref_faci_code,
         list(FACI_ID = "realhub", SUB_FACI_ID = "realhub_branch")
      )
}
iit_sum <- iit
iit_sum <- lapply(iit_sum, function(data) {
   sum <- data %>%
      group_by(FACI_ID, SUB_FACI_ID) %>%
      summarise_at(
         .vars = vars(TX_CURR, TX_ML, TX_RTT),
         ~as.integer(sum(.))
      ) %>%
      ungroup() %>%
      pivot_longer(
         cols      = starts_with("TX_"),
         values_to = "Clients",
         names_to  = "Indicator"
      )

   return(sum)
})
iit_sum %<>%
   bind_rows(.id = "ym") %>%
   mutate(
      Year    = StrLeft(ym, 4),
      Quarter = as.numeric(StrRight(ym, 2)),
      Quarter = case_when(
         Quarter <= 3 ~ "Q1",
         Quarter <= 6 ~ "Q2",
         Quarter <= 9 ~ "Q3",
         Quarter <= 12 ~ "Q4",
      )
   )

cqi <- list()
dx  <- read_dta(hs_data("harp_full", "reg", 2023, 08)) %>%
   mutate(
      harp_report_date = as.Date(paste(sep = "-", year, stri_pad_left(month, 2, "0"), "01")),
   ) %>%
   dxlab_to_id(
      c("HARP_FACI", "HARP_SUB_FACI"),
      c("dx_region", "dx_province", "dx_muncity", "dxlab_standard"),
      ohasis$ref_faci
   ) %>%
   left_join(
      y  = sites_tx %>%
         distinct(HARP_FACI = FACI_ID) %>%
         mutate(`Treatment site?` = TRUE),
      by = join_by(HARP_FACI)
   ) %>%
   left_join(
      y  = faci_type %>%
         select(HARP_FACI, FINAL_FACI_TYPE, FINAL_PUBPRIV) %>%
         distinct(HARP_FACI, .keep_all = TRUE),
      by = join_by(HARP_FACI)
   ) %>%
   ohasis$get_faci(
      list(`Diagnosing facility` = c("HARP_FACI", "HARP_SUB_FACI")),
      "name",
      c("Region of Diagnosis", "Province of Diagnosis", "City/Municipality of Diagnosis")
   ) %>%
   mutate(
      `Facility type` = case_when(
         FINAL_FACI_TYPE == "Hospital" & FINAL_PUBPRIV == "Public" ~ "Public hospital",
         FINAL_FACI_TYPE == "Hospital" & FINAL_PUBPRIV == "Private" ~ "Private hospital",
         TRUE ~ FINAL_FACI_TYPE
      )
   )

dx %>%
   rename(
      dxlab_type = `Facility type`,
      dxlab_name = `Diagnosing facility`
   ) %>%
   select(-contains(" "), -harp_report_date) %>%
   relocate(dxlab_name, dxlab_type, .after = dxlab_standard) %>%
   mutate(
      reactive_date = coalesce(blood_extract_date, test_date, t0_date, visit_date, confirm_date) %>% as.Date(),
   ) %>%
   write_dta("H:/20231012_harp_2023-08_wVL-reactive_date.dta")

# Pvt hosp
# pub hosp
# primary care
# private clinic/lab
# CBO


cqi$rHIVda <- dx %>%
   filter(harp_report_date >= "2020-10-01") %>%
   mutate(
      Quarter              = case_when(
         month %in% c(1, 2, 3) ~ "Q1",
         month %in% c(4, 5, 6) ~ "Q2",
         month %in% c(7, 8, 9) ~ "Q3",
         month %in% c(10, 11, 12) ~ "Q4",
      ),

      `Diagnosed in CrCL`  = if_else(rhivda_done == 1, 1, 0, 0),
      `Diagnosed in SACCL` = if_else(is.na(rhivda_done), 1, 0, 0),
      `Total diagnosed`    = 1
   ) %>%
   rename(
      `Confirm year` = year
   ) %>%
   group_by(`Region of Diagnosis`, `Diagnosing facility`, `Confirm year`, `Quarter`, `Treatment site?`, `Facility type`) %>%
   summarise_at(
      .vars = vars(`Diagnosed in CrCL`, `Diagnosed in SACCL`, `Total diagnosed`),
      ~as.integer(sum(.))
   ) %>%
   ungroup() %>%
   mutate(
      `% confirmed through rHIVda` = (`Diagnosed in CrCL` / `Total diagnosed`),
      `% confirmed through SACCL`  = (`Diagnosed in SACCL` / `Total diagnosed`),
   ) %>%
   arrange(`Region of Diagnosis`, `Diagnosing facility`, `Confirm year`, `Quarter`)

cqi$`AHD reporting` <- dx %>%
   filter(harp_report_date >= "2019-01-01") %>%
   mutate(
      Quarter                                                            = case_when(
         month %in% c(1, 2, 3) ~ "Q1",
         month %in% c(4, 5, 6) ~ "Q2",
         month %in% c(7, 8, 9) ~ "Q3",
         month %in% c(10, 11, 12) ~ "Q4",
      ),

      `Total Diagnosed`                                                  = if_else(class2022 == "AIDS", 1, 0, 0),
      `Number of diagnosed cases with WHO staging`                       = if_else(class2022 == "AIDS" & !is.na(who_staging), 1, 0, 0),
      `Number of diagnosed cases with baseline CD4`                      = if_else(class2022 == "AIDS" & !is.na(baseline_cd4), 1, 0, 0),
      `Number of diagnosed cases with TB`                                = if_else(class2022 == "AIDS" & !is.na(tbpatient1), 1, 0, 0),
      `Number of diagnosed cases with reported s/sx or clinical picture` = if_else(class2022 == "AIDS" & !is.na(clinicalpicture), 1, 0, 0),
      `Number of diagnosed cases with TB screening`                      = if_else(class2022 == "AIDS" & !is.na(tbpatient1), 1, 0, 0),
   ) %>%
   rename(
      `Confirm year` = year
   ) %>%
   group_by(`Region of Diagnosis`, `Diagnosing facility`, `Confirm year`, `Facility type`) %>%
   summarise_at(
      .vars = vars(
         `Total Diagnosed`,
         `Number of diagnosed cases with WHO staging`,
         `Number of diagnosed cases with baseline CD4`,
         `Number of diagnosed cases with TB`,
         `Number of diagnosed cases with reported s/sx or clinical picture`,
         `Number of diagnosed cases with TB screening`
      ),
      ~as.integer(sum(.))
   ) %>%
   ungroup() %>%
   arrange(`Region of Diagnosis`, `Diagnosing facility`, `Confirm year`)

cqi$AHD <- dx %>%
   filter(harp_report_date >= "2019-01-01") %>%
   mutate(
      Quarter                       = case_when(
         month %in% c(1, 2, 3) ~ "Q1",
         month %in% c(4, 5, 6) ~ "Q2",
         month %in% c(7, 8, 9) ~ "Q3",
         month %in% c(10, 11, 12) ~ "Q4",
      ),

      `Total Diagnosed`             = if_else(class2022 == "AIDS", 1, 0, 0),
      `Age at diagnosis`            = case_when(
         age < 15 ~ 1,
         age >= 15 & age < 25 ~ 2,
         TRUE ~ 3
      ),
      `Age at diagnosis`            = labelled(
         `Age at diagnosis`,
         c(
            "< 15"    = 1,
            "15 - 24" = 2,
            "25+"     = 3
         )
      ),
      `Diagnosed cases without AHD` = if_else(class2022 == "HIV", 1, 0, 0),
      `Diagnosed cases with AHD`    = if_else(class2022 == "AIDS", 1, 0, 0),
      `Total Diagnosed cases`       = 1
   ) %>%
   rename(
      `Confirm year` = year
   ) %>%
   group_by(`Region of Diagnosis`, `Diagnosing facility`, `Confirm year`, `Age at diagnosis`, `Treatment site?`, `Facility type`) %>%
   summarise_at(
      .vars = vars(`Diagnosed cases without AHD`, `Diagnosed cases with AHD`, `Total Diagnosed cases`),
      ~as.integer(sum(.))
   ) %>%
   ungroup() %>%
   mutate(
      `% AHD` = (`Diagnosed cases with AHD` / `Total Diagnosed cases`),
   ) %>%
   arrange(`Region of Diagnosis`, `Diagnosing facility`, `Confirm year`, `Age at diagnosis`)

cqi$TLD <- tx_reg %>%
   filter(artstart_date >= "2021-01-01") %>%
   left_join(
      y  = start_regimen %>%
         filter(!is.na(MEDICINE_SUMMARY)) %>%
         distinct(REC_ID, .keep_all = TRUE),
      by = join_by(REC_ID)
   ) %>%
   left_join(
      y  = tx$`202308` %>%
         select(art_id, latest_regimen, realhub, realhub_branch),
      by = join_by(art_id)
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~na_if(., "")
   ) %>%
   mutate(
      Year                            = as.character(year(artstart_date)),
      Quarter                         = month(artstart_date),
      Quarter                         = case_when(
         Quarter <= 3 ~ "Q1",
         Quarter <= 6 ~ "Q2",
         Quarter <= 9 ~ "Q3",
         Quarter <= 12 ~ "Q4",
      ),
      regimen                         = coalesce(MEDICINE_SUMMARY, artstart_regimen, latest_regimen),
      `Number of new enrolled on TLD` = if_else(str_detect(toupper(MEDICINE_SUMMARY), "TDF") &
                                                   str_detect(toupper(MEDICINE_SUMMARY), "3TC") &
                                                   str_detect(toupper(MEDICINE_SUMMARY), "DTG"), 1, 0, 0),

      final_hub                       = coalesce(artstart_hub, realhub),
      final_branch                    = coalesce(artstart_branch, realhub_branch),
      final_branch                    = if_else(!str_detect(final_branch, final_hub), NA_character_, final_branch, final_branch),
      `Number of new enrollees`       = 1,
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(FACI_ID = "final_hub", SUB_FACI_ID = "final_branch")
   ) %>%
   group_by(FACI_ID, SUB_FACI_ID, Year, Quarter) %>%
   summarise_at(
      .vars = vars(`Number of new enrollees`, `Number of new enrolled on TLD`),
      ~as.integer(sum(.))
   ) %>%
   ungroup() %>%
   full_join(
      y  = tx_sum,
      by = join_by(FACI_ID, SUB_FACI_ID, Year, Quarter)
   ) %>%
   left_join(
      y  = faci_type %>%
         select(FACI_ID = HARP_FACI, FINAL_FACI_TYPE, FINAL_PUBPRIV) %>%
         distinct(FACI_ID, .keep_all = TRUE),
      by = join_by(FACI_ID)
   ) %>%
   mutate(
      `Facility type` = case_when(
         FINAL_FACI_TYPE == "Hospital" & FINAL_PUBPRIV == "Public" ~ "Public hospital",
         FINAL_FACI_TYPE == "Hospital" & FINAL_PUBPRIV == "Private" ~ "Private hospital",
         TRUE ~ FINAL_FACI_TYPE
      )
   ) %>%
   ohasis$get_faci(
      list(`Treatment Facility` = c("FACI_ID", "SUB_FACI_ID")),
      "name",
      c("Region of Treatment Facility", "Province of Treatment Facility", "City/Municipality of Treatment Facility")
   ) %>%
   select(
      `Region of Treatment Facility`,
      `Treatment Facility`,
      `Facility type`,
      `Year`,
      `Quarter`,
      `Total PLHIV on ART`,
      `Total PLHIV on TLD`,
      `Number of new enrollees`,
      `Number of new enrolled on TLD`
   ) %>%
   arrange(`Region of Treatment Facility`, `Treatment Facility`, `Year`, `Quarter`)

cqi$IIT <- tx_reg %>%
   filter(artstart_date >= "2022-09-01") %>%
   left_join(
      y  = tx$`202308` %>%
         select(art_id, latest_regimen, realhub, realhub_branch),
      by = join_by(art_id)
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~na_if(., "")
   ) %>%
   mutate(
      Year         = as.character(year(artstart_date)),
      Quarter      = month(artstart_date),
      Quarter      = case_when(
         Quarter <= 3 ~ "Q1",
         Quarter <= 6 ~ "Q2",
         Quarter <= 9 ~ "Q3",
         Quarter <= 12 ~ "Q4",
      ),

      final_hub    = coalesce(artstart_hub, realhub),
      final_branch = coalesce(artstart_branch, realhub_branch),
      final_branch = if_else(!str_detect(final_branch, final_hub), NA_character_, final_branch, final_branch),
      TX_NEW       = 1,
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(FACI_ID = "final_hub", SUB_FACI_ID = "final_branch")
   ) %>%
   group_by(FACI_ID, SUB_FACI_ID, Year, Quarter) %>%
   summarise_at(
      .vars = vars(TX_NEW),
      ~as.integer(sum(.))
   ) %>%
   ungroup() %>%
   mutate(
      Indicator = "TX_NEW"
   ) %>%
   rename(
      Clients = TX_NEW
   ) %>%
   bind_rows(iit_sum) %>%
   left_join(
      y  = faci_type %>%
         select(FACI_ID = HARP_FACI, FINAL_FACI_TYPE, FINAL_PUBPRIV) %>%
         distinct(FACI_ID, .keep_all = TRUE),
      by = join_by(FACI_ID)
   ) %>%
   mutate(
      `Report date`   = stri_c(Year, "-", Quarter),
      `Facility type` = case_when(
         FINAL_FACI_TYPE == "Hospital" & FINAL_PUBPRIV == "Public" ~ "Public hospital",
         FINAL_FACI_TYPE == "Hospital" & FINAL_PUBPRIV == "Private" ~ "Private hospital",
         TRUE ~ FINAL_FACI_TYPE
      )
   ) %>%
   ohasis$get_faci(
      list(`Treatment Facility` = c("FACI_ID", "SUB_FACI_ID")),
      "name",
      c("Region of Treatment Facility", "Province of Treatment Facility", "City/Municipality of Treatment Facility")
   ) %>%
   select(
      `Region of Treatment Facility`,
      `Treatment Facility`,
      `Facility type`,
      `Indicator`,
      `Report date`,
      `Clients`
   ) %>%
   arrange(`Region of Treatment Facility`, `Treatment Facility`, `Report date`, `Indicator`) %>%
   pivot_wider(
      id_cols     = c(`Region of Treatment Facility`, `Treatment Facility`, `Facility type`, `Report date`),
      names_from  = `Indicator`,
      values_from = `Clients`
   )

vl_naive <- read_dta(hs_data("harp_vl", "naive_tx", 2023, 08)) %>%
   select(
      art_id,
      vl_naive,
      vl_date_first,
      vl_result_first,
      vl_date_last,
      vl_result_last
   )
cqi$VL   <- tx$`202308` %>%
   left_join(vl_naive %>% select(art_id, vl_naive), join_by(art_id)) %>%
   left_join(tx_reg %>% select(art_id, artstart_date), join_by(art_id)) %>%
   left_join(dx %>% select(idnum, class2022), join_by(idnum)) %>%
   mutate(
      `HIV status at diagnosis` = case_when(
         class2022 == "AIDS" ~ "AHD",
         class2022 == "HIV" ~ "non-AHD",
         TRUE ~ "(no data)"
      ),
      `Alive on ART`            = if_else(onart30 == 1, 0, 0),
      `Eligible for VL`         = if_else(
         onart30 == 1 & (interval(artstart_date, end_ym(2023, 8)) / days(1)) > 92,
         1,
         0,
         0
      ),
      `VL Naive`                = if_else(vl_naive == 1, 1, 0, 0),
      `Tested (w/in P12M)`      = if_else(is.na(baseline_vl) & !is.na(vlp12m), 1, 0, 0),
      `Suppressed (<1000)`      = if_else(is.na(baseline_vl) &
                                             !is.na(vlp12m) &
                                             vl_result < 1000, 1, 0, 0),
      `Suppressed (<50)`        = if_else(is.na(baseline_vl) & vlp12m == 1, 1, 0, 0),
   ) %>%
   left_join(
      y  = faci_type %>%
         select(FACI_ID = HARP_FACI, FINAL_FACI_TYPE, FINAL_PUBPRIV) %>%
         distinct(FACI_ID, .keep_all = TRUE),
      by = join_by(FACI_ID)
   ) %>%
   mutate(
      `Facility type` = case_when(
         FINAL_FACI_TYPE == "Hospital" & FINAL_PUBPRIV == "Public" ~ "Public hospital",
         FINAL_FACI_TYPE == "Hospital" & FINAL_PUBPRIV == "Private" ~ "Private hospital",
         TRUE ~ FINAL_FACI_TYPE
      )
   ) %>%
   ohasis$get_faci(
      list(`Treatment Facility` = c("FACI_ID", "SUB_FACI_ID")),
      "name",
      c("Region of Treatment Facility", "Province of Treatment Facility", "City/Municipality of Treatment Facility")
   ) %>%
   group_by(`Region of Treatment Facility`, `Treatment Facility`, `Facility type`, `HIV status at diagnosis`) %>%
   summarise_at(
      .vars = vars(`Alive on ART`, `Eligible for VL`, `VL Naive`, `Tested (w/in P12M)`, `Suppressed (<1000)`, `Suppressed (<50)`),
      ~as.integer(sum(.))
   ) %>%
   ungroup() %>%
   mutate(
      `Testing Coverage`         = `Tested (w/in P12M)` / `Eligible for VL`,
      `Suppression Rate (<1000)` = `Suppressed (<1000)` / `Tested (w/in P12M)`,
      `Suppression Rate (<50)`   = `Suppressed (<50)` / `Tested (w/in P12M)`,
   ) %>%
   select(
      `Region of Treatment Facility`,
      `Treatment Facility`,
      `Facility type`,
      `HIV status at diagnosis`,
      `Alive on ART`,
      `Eligible for VL`,
      `VL Naive`,
      `Tested (w/in P12M)`,
      `Testing Coverage`,
      `Suppressed (<1000)`,
      `Suppressed (<50)`,
      `Suppression Rate (<50)`,
      `Suppression Rate (<1000)`
   ) %>%
   arrange(`Region of Treatment Facility`, `Treatment Facility`, `HIV status at diagnosis`)


cqi$PrEP <- prep_reg %>%
   filter(prepstart_date >= "2022-09-01") %>%
   mutate_if(
      .predicate = is.character,
      ~na_if(., "")
   ) %>%
   mutate(
      Year     = as.character(year(prepstart_date)),
      Quarter  = month(prepstart_date),
      Quarter  = case_when(
         Quarter <= 3 ~ "Q1",
         Quarter <= 6 ~ "Q2",
         Quarter <= 9 ~ "Q3",
         Quarter <= 12 ~ "Q4",
      ),

      PREP_NEW = 1
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(FACI_ID = "prepstart_faci", SUB_FACI_ID = "prepstart_branch")
   ) %>%
   group_by(FACI_ID, SUB_FACI_ID, Year, Quarter) %>%
   summarise_at(
      .vars = vars(PREP_NEW),
      ~as.integer(sum(.))
   ) %>%
   ungroup() %>%
   left_join(
      y  = faci_type %>%
         select(FACI_ID = HARP_FACI, FINAL_FACI_TYPE, FINAL_PUBPRIV) %>%
         distinct(FACI_ID, .keep_all = TRUE),
      by = join_by(FACI_ID)
   ) %>%
   mutate(
      `Report date`   = stri_c(Year, "-", Quarter),
      `Facility type` = case_when(
         FINAL_FACI_TYPE == "Hospital" & FINAL_PUBPRIV == "Public" ~ "Public hospital",
         FINAL_FACI_TYPE == "Hospital" & FINAL_PUBPRIV == "Private" ~ "Private hospital",
         TRUE ~ FINAL_FACI_TYPE
      )
   ) %>%
   ohasis$get_faci(
      list(`PrEP Facility` = c("FACI_ID", "SUB_FACI_ID")),
      "name",
      c("Region of PrEP Facility", "Province of PrEP Facility", "City/Municipality of PrEP Facility")
   ) %>%
   mutate(
      `Region of PrEP Facility` = case_when(
         `PrEP Facility` == "HIV & AIDS Support House (HASH) (Region I)" ~ "Region I (Ilocos Region)",
         `PrEP Facility` == "HIV & AIDS Support House (HASH) (Region III)" ~ "Region III (Central Luzon)",
         `PrEP Facility` == "HIV & AIDS Support House (HASH) (Region VIII)" ~ "Region VIII (Eastern Visayas)",
         TRUE ~ `Region of PrEP Facility`
      )
   ) %>%
   select(`Region of PrEP Facility`, `PrEP Facility`, `Facility type`, `Report date`, `PREP_NEW`) %>%
   arrange(`Region of PrEP Facility`, `PrEP Facility`, `Report date`)
