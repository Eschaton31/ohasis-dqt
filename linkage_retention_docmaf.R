gf_sites <- read_excel("C:/Users/Administrator/Downloads/2023 Target Breakdown per sites.xlsx", range = "A6:C306", col_names = FALSE) %>%
   rename(
      reg  = 1,
      prov = 2,
      munc = 3
   ) %>%
   mutate(
      munc = str_extract(munc, "^[^\\(]*"),
      munc = str_squish(munc),
   ) %>%
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         PERM_PSGC_REG  = "reg",
         PERM_PSGC_PROV = "prov",
         PERM_PSGC_MUNC = "munc"
      ),
      aem_sub_ntl = FALSE
   ) %>%
   ohasis$get_addr(
      c(
         dx_region   = "PERM_PSGC_REG",
         dx_province = "PERM_PSGC_PROV",
         dx_muncity  = "PERM_PSGC_MUNC"
      ),
      "nhsss"
   )

pepfar_sites <- read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw") %>%
   distinct(FACI_ID, .keep_all = TRUE) %>%
   mutate(site_pepfar = coalesce(site_epic_2023, site_icap_2023)) %>%
   filter(site_pepfar == 1)

full <- hs_data("harp_full", "reg", 2023, 8) %>%
   read_dta() %>%
   mutate(
      overseas_addr       = case_when(
         muncity == "OUT OF COUNTRY" ~ 1,
         TRUE ~ 0
      ),

      end_date            = as.Date("2023-08-31"),

      reactive_date       = coalesce(blood_extract_date, specimen_receipt_date, test_date, t0_date, visit_date, confirm_date) %>% as.Date(),

      tat_reactive_enroll = interval(reactive_date, artstart_date) / days(1),
      tat_confirm_enroll  = interval(confirm_date, artstart_date) / days(1),

      muncity             = case_when(
         muncity == "PINAMUNGAHAN" & province == "CEBU" ~ "PINAMUNGAJAN",
         muncity == "SAN JUAN" & province == "BULACAN" ~ "MALOLOS",
         overseas_addr == 1 ~ "OVERSEAS",
         TRUE ~ muncity
      ),
      province            = case_when(
         muncity == "UNKNOWN" & province == "NCR" ~ "UNKNOWN",
         province == "COMPOSTELA VALLEY" ~ "DAVAO DE ORO",
         overseas_addr == 1 ~ "OVERSEAS",
         TRUE ~ province
      ),
      region              = case_when(
         overseas_addr == 1 ~ "OVERSEAS",
         TRUE ~ region
      ),

      mortality           = if_else(
         (dead != 1 | is.na(dead)) & (is.na(outcome) | outcome != "dead"),
         0,
         1,
         1
      ),


      dx                  = if_else(!is.na(idnum), 1, 0, 0),
      dx_plhiv            = if_else(!is.na(idnum) &
                                       (dead != 1 | is.na(dead)) &
                                       (is.na(outcome) | outcome != "dead"), 1, 0, 0),
      plhiv               = if_else((dead != 1 | is.na(dead)) & (is.na(outcome) | outcome != "dead"), 1, 0, 0),
      everonart           = if_else(everonart == 1, 1, 0, 0),
      everonart_plhiv     = if_else(everonart == 1 & outcome != "alive on arv", 1, 0, 0),
      ononart             = if_else(onart == 1, 1, 0, 0),


      baseline_vl_new     = if_else(
         condition = floor(interval(artstart_date, vl_date) / months(1)) < 6,
         true      = 1,
         false     = 0,
         missing   = 0
      ),

      vl_tested           = if_else(
         onart == 1 &
            (is.na(baseline_vl_new) | baseline_vl_new == 0) &
            !is.na(vlp12m),
         1,
         0,
         0
      ),
      vl_suppressed       = if_else(
         onart == 1 &
            (is.na(baseline_vl_new) | baseline_vl_new == 0) &
            vlp12m == 1,
         1,
         0,
         0
      ),
   ) %>%
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         PERM_PSGC_REG  = "region",
         PERM_PSGC_PROV = "province",
         PERM_PSGC_MUNC = "muncity"
      ),
      aem_sub_ntl = FALSE
   ) %>%
   ohasis$get_addr(
      c(
         Region              = "PERM_PSGC_REG",
         Province            = "PERM_PSGC_PROV",
         `City/Municipality` = "PERM_PSGC_MUNC"
      ),
      "name"
   ) %>%
   mutate(
      # KAP
      msm            = case_when(
         sex == "MALE" & sexhow %in% c("BISEXUAL", "HOMOSEXUAL") ~ 1,
         sex == "MALE" & sexhow == "HETEROSEXUAL" ~ 0,
         TRUE ~ 0
      ),
      tgw            = if_else(
         condition = msm == 1 & self_identity %in% c("FEMALE", "OTHERS"),
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      hetero         = if_else(
         condition = sexhow == "HETEROSEXUAL",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      pwid           = if_else(
         condition = transmit == "IVDU",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      unknown        = case_when(
         transmit == "UNKNOWN" ~ 1,
         is.na(transmit) ~ 1,
         TRUE ~ 0
      ),
      Key_Population = case_when(
         msm == 1 & tgw == 0 ~ "MSM",
         msm == 1 & tgw == 1 ~ "TGW",
         pwid == 1 ~ "PWID",
         transmit == "SEX" & sex == "MALE" ~ "Hetero Male",
         transmit == "SEX" & sex == "FEMALE" ~ "Hetero Female",
         unknown == 1 ~ "Unknown",
         TRUE ~ "Others"
      ),

      # Age
      age            = floor(age),
      Age_Band       = case_when(
         age >= 0 & age < 15 ~ '<15',
         age >= 15 & age < 25 ~ '15-24',
         age >= 25 & age < 35 ~ '25-34',
         age >= 35 & age < 50 ~ '35-49',
         age >= 50 & age < 1000 ~ '50+',
         TRUE ~ '(no data)'
      ),

      Sex            = case_when(
         sex == "MALE" ~ "Male",
         sex == "FEMALE" ~ "Female",
         TRUE ~ "(no data)"
      ),
   ) %>%
   dxlab_to_id(
      c("dx_faci_id", "dx_sub_faci_id"),
      c("dx_region", "dx_province", "dx_muncity", "dxlab_standard"),
      ohasis$ref_faci
   ) %>%
   mutate(
      rHIVda_Reporting_Facility = case_when(
         dx_faci_id == "010001" ~ "Yes",
         dx_faci_id == "010004" ~ "Yes",
         dx_faci_id == "010082" ~ "Yes",
         dx_faci_id == "020001" ~ "Yes",
         dx_faci_id == "030001" ~ "Yes",
         dx_faci_id == "030005" ~ "Yes",
         dx_faci_id == "030006" ~ "Yes",
         dx_faci_id == "030015" ~ "Yes",
         dx_faci_id == "030016" ~ "Yes",
         dx_faci_id == "030022" ~ "Yes",
         dx_faci_id == "040001" ~ "Yes",
         dx_faci_id == "040005" ~ "Yes",
         dx_faci_id == "040014" ~ "Yes",
         dx_faci_id == "040254" ~ "Yes",
         dx_faci_id == "050001" ~ "Yes",
         dx_faci_id == "060001" ~ "Yes",
         dx_faci_id == "060003" ~ "Yes",
         dx_faci_id == "060007" ~ "Yes",
         dx_faci_id == "060008" ~ "Yes",
         dx_faci_id == "060223" ~ "Yes",
         dx_faci_id == "070003" ~ "Yes",
         dx_faci_id == "070004" ~ "Yes",
         dx_faci_id == "070010" ~ "Yes",
         dx_faci_id == "070013" ~ "Yes",
         dx_faci_id == "070078" ~ "Yes",
         dx_faci_id == "080001" ~ "Yes",
         dx_faci_id == "090003" ~ "Yes",
         dx_faci_id == "100004" ~ "Yes",
         dx_faci_id == "100028" ~ "Yes",
         dx_faci_id == "110002" ~ "Yes",
         dx_faci_id == "110004" ~ "Yes",
         dx_faci_id == "110005" ~ "Yes",
         dx_faci_id == "120005" ~ "Yes",
         dx_faci_id == "130001" ~ "Yes",
         dx_faci_id == "130002" ~ "Yes",
         dx_faci_id == "130003" ~ "Yes",
         dx_faci_id == "130004" ~ "Yes",
         dx_faci_id == "130005" ~ "Yes",
         dx_faci_id == "130023" ~ "Yes",
         dx_faci_id == "130024" ~ "Yes",
         dx_faci_id == "130055" ~ "Yes",
         dx_faci_id == "130166" ~ "Yes",
         dx_faci_id == "130400" ~ "Yes",
         dx_faci_id == "130453" ~ "Yes",
         dx_faci_id == "140001" ~ "Yes",
         dx_faci_id == "160001" ~ "Yes",
         dx_faci_id == "160002" ~ "Yes",
         dx_faci_id == "160003" ~ "Yes",
         dx_faci_id == "170004" ~ "Yes",
         TRUE ~ "No"
      )
   ) %>%
   left_join(pepfar_sites %>% select(dx_faci_id = FACI_ID, site_pepfar), join_by(dx_faci_id)) %>%
   left_join(gf_sites %>%
                select(dx_region, dx_province, dx_muncity) %>%
                mutate(site_gf = 1), join_by(dx_region, dx_province, dx_muncity)) %>%
   mutate(
      PEPFAR_Supported_Facility = if_else(site_pepfar == 1, "Yes", "No", "No"),
      GF_Supported_Facility     = if_else(site_gf == 1 & (pubpriv == "PUBLIC" | dx_faci_id == "130257"), "Yes", "No", "No"),
   ) %>%
   ohasis$get_faci(
      list(`Site/Organization` = c("dx_faci_id", "dx_sub_faci_id")),
      "name",
      c("Site_Region", "Site_Province", "Site_City/Municipality")
   )

tx <- hs_data("harp_tx", "outcome", 2023, 8) %>%
   read_dta() %>%
   left_join(
      y  = full %>%
         select(
            idnum,
            Region,
            Province,
            `City/Municipality`,
            Sex,
            Key_Population
         ),
      by = join_by(idnum)
   ) %>%
   mutate_at(
      .vars = vars(Region, Province, `City/Municipality`, Key_Population),
      ~coalesce(., "Unknown")
   ) %>%
   mutate(
      age      = floor(curr_age),
      Age_Band = case_when(
         age >= 0 & age < 15 ~ '<15',
         age >= 15 & age < 25 ~ '15-24',
         age >= 25 & age < 35 ~ '25-34',
         age >= 35 & age < 50 ~ '35-49',
         age >= 50 & age < 1000 ~ '50+',
         TRUE ~ '(no data)'
      ),
      Sex      = case_when(
         Sex == "(no data)" & sex == "MALE" ~ "Male",
         Sex == "(no data)" & sex == "FEMALE" ~ "Female",
         is.na(Sex) & sex == "MALE" ~ "Male",
         is.na(Sex) & sex == "FEMALE" ~ "Female",
         TRUE ~ Sex
      ),
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(tx_faci_id = "realhub", tx_sub_faci_id = "realhub_branch")
   ) %>%
   mutate(
      rHIVda_Reporting_Facility = case_when(
         tx_faci_id == "010001" ~ "Yes",
         tx_faci_id == "010004" ~ "Yes",
         tx_faci_id == "010082" ~ "Yes",
         tx_faci_id == "020001" ~ "Yes",
         tx_faci_id == "030001" ~ "Yes",
         tx_faci_id == "030005" ~ "Yes",
         tx_faci_id == "030006" ~ "Yes",
         tx_faci_id == "030015" ~ "Yes",
         tx_faci_id == "030016" ~ "Yes",
         tx_faci_id == "030022" ~ "Yes",
         tx_faci_id == "040001" ~ "Yes",
         tx_faci_id == "040005" ~ "Yes",
         tx_faci_id == "040014" ~ "Yes",
         tx_faci_id == "040254" ~ "Yes",
         tx_faci_id == "050001" ~ "Yes",
         tx_faci_id == "060001" ~ "Yes",
         tx_faci_id == "060003" ~ "Yes",
         tx_faci_id == "060007" ~ "Yes",
         tx_faci_id == "060008" ~ "Yes",
         tx_faci_id == "060223" ~ "Yes",
         tx_faci_id == "070003" ~ "Yes",
         tx_faci_id == "070004" ~ "Yes",
         tx_faci_id == "070010" ~ "Yes",
         tx_faci_id == "070013" ~ "Yes",
         tx_faci_id == "070078" ~ "Yes",
         tx_faci_id == "080001" ~ "Yes",
         tx_faci_id == "090003" ~ "Yes",
         tx_faci_id == "100004" ~ "Yes",
         tx_faci_id == "100028" ~ "Yes",
         tx_faci_id == "110002" ~ "Yes",
         tx_faci_id == "110004" ~ "Yes",
         tx_faci_id == "110005" ~ "Yes",
         tx_faci_id == "120005" ~ "Yes",
         tx_faci_id == "130001" ~ "Yes",
         tx_faci_id == "130002" ~ "Yes",
         tx_faci_id == "130003" ~ "Yes",
         tx_faci_id == "130004" ~ "Yes",
         tx_faci_id == "130005" ~ "Yes",
         tx_faci_id == "130023" ~ "Yes",
         tx_faci_id == "130024" ~ "Yes",
         tx_faci_id == "130055" ~ "Yes",
         tx_faci_id == "130166" ~ "Yes",
         tx_faci_id == "130400" ~ "Yes",
         tx_faci_id == "130453" ~ "Yes",
         tx_faci_id == "140001" ~ "Yes",
         tx_faci_id == "160001" ~ "Yes",
         tx_faci_id == "160002" ~ "Yes",
         tx_faci_id == "160003" ~ "Yes",
         tx_faci_id == "170004" ~ "Yes",
         TRUE ~ "No"
      )
   ) %>%
   left_join(pepfar_sites %>% select(tx_faci_id = FACI_ID, site_pepfar), join_by(tx_faci_id)) %>%
   left_join(gf_sites %>%
                select(real_reg = dx_region, real_prov = dx_province, real_munc = dx_muncity) %>%
                mutate(site_gf = 1), join_by(real_reg, real_prov, real_munc)) %>%
   mutate(
      PEPFAR_Supported_Facility = if_else(site_pepfar == 1, "Yes", "No", "No"),
      GF_Supported_Facility     = if_else(site_gf == 1 | tx_faci_id == "130257", "Yes", "No", "No"),
   ) %>%
   ohasis$get_faci(
      list(`Site/Organization` = c("tx_faci_id", "tx_sub_faci_id")),
      "name",
      c("Site_Region", "Site_Province", "Site_City/Municipality")
   )

conn  <- ohasis$conn("lw")
oh_tx <- dbxSelect(conn, "SELECT REC_ID, CLIENT_TYPE from ohasis_warehouse.form_art_bc WHERE REC_ID IN (?)", params = list(tx$REC_ID))
dbDisconnect(conn)

tx %<>%
   left_join(oh_tx, join_by(REC_ID))

##  actual requests
req1 <- function(data, ...) {
   res <- data %>%
      filter(year >= 2022) %>%
      mutate(
         tat_reactive_art = interval(reactive_date, artstart_date) / days(1),
         tat_reactive_art = floor(tat_reactive_art),
         Disaggregation   = case_when(
            tat_reactive_art < 0 ~ "1) Same day",
            tat_reactive_art >= 1 & tat_reactive_art <= 7 ~ "2) 2 - 7 days",
            tat_reactive_art >= 8 & tat_reactive_art <= 14 ~ "3) More than 7 days",
            TRUE ~ "0) Not enrolled",
         ),
      ) %>%
      group_by(..., Disaggregation) %>%
      summarise(Value = n()) %>%
      ungroup() %>%
      mutate(
         Indicator = "Linkage-to-care"
      )

   return(res)
}

req2 <- function(data, ...) {
   res <- data %>%
      filter(year >= 2022, everonart == 1) %>%
      mutate(
         tat_reactive_art = interval(reactive_date, artstart_date) / days(1),
         tat_reactive_art = floor(tat_reactive_art),
      ) %>%
      group_by(...) %>%
      summarise(Value = median(tat_reactive_art, na.rm = TRUE)) %>%
      ungroup() %>%
      mutate(
         Value     = if_else(Value < 0, 0, Value, Value),
         Indicator = "Median time to initiation"
      )

   return(res)
}

req3 <- function(data, ...) {
   res <- data %>%
      filter(outcome == "alive on arv") %>%
      group_by(...) %>%
      summarise(Value = n()) %>%
      ungroup() %>%
      mutate(
         Indicator = "PLHIV on ART"
      )

   return(res)
}

req4 <- function(data, ...) {
   res <- data %>%
      mutate(
         Disaggregation = keep_code(CLIENT_TYPE),
         Disaggregation = case_when(
            Disaggregation == "1" ~ "Inpatient",
            Disaggregation == "2" ~ "Walk-in",
            Disaggregation == "4" ~ "Walk-in",
            Disaggregation == "5" ~ "Satellite",
            Disaggregation == "6" ~ "Transient",
            Disaggregation == "8" ~ "Courier",
            TRUE ~ "Not indicated",
         ),
      ) %>%
      group_by(..., Disaggregation) %>%
      summarise(Value = n()) %>%
      ungroup() %>%
      mutate(
         Indicator = "DSD approach"
      )

   return(res)
}

req6 <- function(data, ...) {
   res <- data %>%
      filter(outcome == "alive on arv") %>%
      mutate(
         Disaggregation = case_when(
            !is.na(vlp12m) & baseline_vl == 1 ~ "Baseline (P12M)",
            !is.na(vlp12m) & is.na(baseline_vl) ~ "Tested (P12M)",
            !is.na(vl_date) ~ "Tested (>12M)",
            TRUE ~ "VL Naive",
         ),
      ) %>%
      group_by(..., Disaggregation) %>%
      summarise(Value = n()) %>%
      ungroup() %>%
      mutate(
         Indicator = "VL testing coverage"
      )

   return(res)
}

req7 <- function(data, ...) {
   res <- data %>%
      filter(outcome == "alive on arv") %>%
      mutate(
         Disaggregation = case_when(
            !is.na(vlp12m) &
               is.na(baseline_vl) &
               vl_result < 50 ~ "Undetectable (<50)",
            !is.na(vlp12m) &
               is.na(baseline_vl) &
               vl_result < 1000 ~ "Suppressed (<1000)",
            !is.na(vlp12m) &
               is.na(baseline_vl) &
               vl_result >= 1000 ~ "Detected (1000+)",
            TRUE ~ "VL Naive",
         ),
      ) %>%
      group_by(..., Disaggregation) %>%
      summarise(Value = n()) %>%
      ungroup() %>%
      mutate(
         Indicator = "VL suppression"
      )

   return(res)
}

res      <- list()
res$req1 <- req1(full, Region, Province, `City/Municipality`, Sex, Age_Band, Key_Population, rHIVda_Reporting_Facility, PEPFAR_Supported_Facility, GF_Supported_Facility)
res$req2 <- req2(full, Region, Province, `City/Municipality`, Sex, Age_Band, Key_Population, rHIVda_Reporting_Facility, PEPFAR_Supported_Facility, GF_Supported_Facility)
res$req3 <- req3(tx, Region, Province, `City/Municipality`, Sex, Age_Band, Key_Population, rHIVda_Reporting_Facility, PEPFAR_Supported_Facility, GF_Supported_Facility)
res$req4 <- req4(tx, Region, Province, `City/Municipality`, Sex, Age_Band, Key_Population, rHIVda_Reporting_Facility, PEPFAR_Supported_Facility, GF_Supported_Facility)
res$req6 <- req6(tx, Region, Province, `City/Municipality`, Sex, Age_Band, Key_Population, rHIVda_Reporting_Facility, PEPFAR_Supported_Facility, GF_Supported_Facility)
res$req7 <- req7(tx, Region, Province, `City/Municipality`, Sex, Age_Band, Key_Population, rHIVda_Reporting_Facility, PEPFAR_Supported_Facility, GF_Supported_Facility)


site      <- list()
site$req1 <- req1(full, Site_Region, Site_Province, `Site_City/Municipality`, `Site/Organization`, Sex, Age_Band, Key_Population, rHIVda_Reporting_Facility, PEPFAR_Supported_Facility, GF_Supported_Facility)
site$req2 <- req2(full, Site_Region, Site_Province, `Site_City/Municipality`, `Site/Organization`, Sex, Age_Band, Key_Population, rHIVda_Reporting_Facility, PEPFAR_Supported_Facility, GF_Supported_Facility)
site$req3 <- req3(tx, Site_Region, Site_Province, `Site_City/Municipality`, `Site/Organization`, Sex, Age_Band, Key_Population, rHIVda_Reporting_Facility, PEPFAR_Supported_Facility, GF_Supported_Facility)
site$req4 <- req4(tx, Site_Region, Site_Province, `Site_City/Municipality`, `Site/Organization`, Sex, Age_Band, Key_Population, rHIVda_Reporting_Facility, PEPFAR_Supported_Facility, GF_Supported_Facility)
site$req6 <- req6(tx, Site_Region, Site_Province, `Site_City/Municipality`, `Site/Organization`, Sex, Age_Band, Key_Population, rHIVda_Reporting_Facility, PEPFAR_Supported_Facility, GF_Supported_Facility)
site$req7 <- req7(tx, Site_Region, Site_Province, `Site_City/Municipality`, `Site/Organization`, Sex, Age_Band, Key_Population, rHIVda_Reporting_Facility, PEPFAR_Supported_Facility, GF_Supported_Facility)

final_res  <- bind_rows(res) %>%
   relocate(Indicator, .before = Disaggregation)
final_site <- bind_rows(site) %>%
   relocate(Indicator, .before = Disaggregation)


write_flat_file(list("Residence" = final_res, "Sites" = final_site), "H:/20231130-doc_maffy-cqi.xlsx")
write_sheet(final_req, "1L4eZd9PaikSEXdBnaNEQ3Nl-eRy49OMUZtLJFglzpK0", "30nov2023")

sheet_data <- list("Residence" = final_res, "Sites" = final_site)