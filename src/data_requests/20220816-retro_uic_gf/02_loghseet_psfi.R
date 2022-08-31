##  Append testing data  -------------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

##  Download relevant form data ------------------------------------------------

for (file in dir_ls("H:/Data Requests/20220816_retro-uic")) {
   year                           <- stri_replace_all_regex(basename(file), "[^[:digit:]]", "")
   dr$data[[paste0("gf_", year)]] <- read_xlsx(file, col_types = "text", na = c("", "NULL")) %>%
      rename_all(
         ~case_when(
            . == "Region" ~ "site_region",
            . == "Province" ~ "site_province",
            . == "City" ~ "site_muncity",
            . == "Validated" ~ "gf_validated",
            . == "Validation Status" ~ "gf_validated",
            . == "Logsheet Type" ~ "ls_type",
            . == "Logsheet Subtype" ~ "ls_subtype",
            . == "Loghseet Subtype" ~ "ls_subtype",
            . == "Logsheet Kind" ~ "ls_kind",
            . == "Peer Navigator Name" ~ "provider_name",
            . == "Reach Date" ~ "reach_date",
            . == "Date of Reach" ~ "reach_date",
            . == "Type of Venue" ~ "venue_type",
            . == "Venue" ~ "venue",
            . == "Client UIC" ~ "uic",
            . == "UIC" ~ "uic",
            . == "Type of KAP" ~ "kap_type",
            . == "Date of Last Sex" ~ "date_last_sex_msm",
            . == "Oral" ~ "sextype_oral",
            . == "Anal Inserter" ~ "sextype_anal_insert",
            . == "Anal Receiver" ~ "sextype_anal_receive",
            . == "Vaginal" ~ "sextype_vaginal",
            . == "Number of Male Sex Partner" ~ "num_sex_partner_m",
            . == "No. Of Male Sex Partner" ~ "num_sex_partner_m",
            . == "Number of Female Sex Partner" ~ "num_sex_partner_f",
            . == "No. Of Female Sex Partner" ~ "num_sex_partner_f",
            . == "Stage of Condom Use" ~ "stage_condom_use",
            . == "Testing Facility" ~ "tested",
            . == "Date Tested" ~ "test_date",
            . == "Test 1" ~ "t1_complete",
            . == "Remarks" ~ "remarks",
            . == "Reactive" ~ "reactive",
            . == "Reactive (Y/N)" ~ "reactive",
            . == "Confirmed Positive" ~ "confirmed_positive",
            . == "Date Confirmatory" ~ "confirm_date",
            . == "Date of Confirmatory" ~ "confirm_date",
            . == "Date Link to Care" ~ "link2care_date",
            . == "Date of Link to Care" ~ "link2care_date",
            . == "Date of Enrollment" ~ "artstart_date",
            # . == "DateofEnrollment0" ~ "artstart_date",
            . == "Treatment Hub" ~ "tx_hub",
            . == "UIC Length" ~ "uic_len",
            . == "Birth Month" ~ "bmo",
            . == "Birth Day" ~ "bdy",
            . == "Birth Year" ~ "byr",
            . == "Birthday" ~ "birthdate",
            . == "Correct Birthday?" ~ "birthdate_is_right",
            . == "Age" ~ "age",
            . == "Duplicate Count" ~ "gf_dup_ct",
            . == "DOLS <= DOR?" ~ "dols_less_dor",
            . == "DOLS < 365" ~ "dols_p12m",
            . == "With Anal Sex" ~ "with_anal_sex",
            . == "DT>DR" ~ "dols_great_dor",
            . == "Venue of Last Injection" ~ "venue_last_inject",
            . == "Date Last Injected" ~ "date_last_inject",
            . == "Shared Injection" ~ "shared_inject",
            . == "Type of Test" ~ "tested",
            . == "Region" ~ "site_region",
            . == "Province" ~ "site_province",
            . == "City" ~ "site_muncity",
            . == "Validated" ~ "gf_validated",
            . == "ValidationStatus" ~ "gf_validated",
            . == "LogsheetType" ~ "ls_type",
            . == "LogsheetSubtype" ~ "ls_subtype",
            . == "LoghseetSubtype" ~ "ls_subtype",
            . == "LogsheetKind" ~ "ls_kind",
            . == "PeerNavigatorName" ~ "provider_name",
            . == "ReachDate" ~ "reach_date",
            . == "DateofReach" ~ "reach_date",
            . == "TypeofVenue" ~ "venue_type",
            . == "Venue" ~ "venue",
            . == "ClientUIC" ~ "uic",
            . == "UIC" ~ "uic",
            . == "TypeofKAP" ~ "kap_type",
            . == "DateofLastSex" ~ "date_last_sex_msm",
            . == "Oral" ~ "sextype_oral",
            . == "AnalInserter" ~ "sextype_anal_insert",
            . == "AnalReceiver" ~ "sextype_anal_receive",
            . == "Vaginal" ~ "sextype_vaginal",
            . == "NumberofMaleSexPartner" ~ "num_sex_partner_m",
            . == "No.OfMaleSexPartner" ~ "num_sex_partner_m",
            . == "NumberofFemaleSexPartner" ~ "num_sex_partner_f",
            . == "No.OfFemaleSexPartner" ~ "num_sex_partner_f",
            . == "StageofCondomUse" ~ "stage_condom_use",
            . == "TestingFacility" ~ "tested",
            . == "DateTested" ~ "test_date",
            . == "DateofTesting" ~ "test_date",
            . == "Test1" ~ "t1_complete",
            . == "Remarks" ~ "remarks",
            . == "Reactive" ~ "reactive",
            . == "Reactive(Y/N)" ~ "reactive",
            . == "ConfirmedPositive" ~ "confirmed_positive",
            . == "DateConfirmatory" ~ "confirm_date",
            . == "DateofConfirmatory" ~ "confirm_date",
            . == "DateLinktoCare" ~ "link2care_date",
            . == "DateofLinktoCare" ~ "link2care_date",
            . == "DateofEnrollment" ~ "artstart_date",
            . == "TreatmentHub" ~ "tx_hub",
            . == "UICLength" ~ "uic_len",
            . == "BirthMonth" ~ "bmo",
            . == "BirthDay" ~ "bdy",
            . == "BirthYear" ~ "byr",
            . == "Birthday" ~ "birthdate",
            . == "CorrectBirthday?" ~ "birthdate_is_right",
            . == "Age" ~ "age",
            . == "DuplicateCount" ~ "gf_dup_ct",
            . == "DOLS<=DOR?" ~ "dols_less_dor",
            . == "DOLS<365" ~ "dols_p12m",
            . == "WithAnalSex" ~ "with_anal_sex",
            . == "DT>DR" ~ "dols_great_dor",
            . == "VenueofLastInjection" ~ "venue_last_inject",
            . == "DateLastInjected" ~ "date_last_inject",
            . == "SharedInjection" ~ "shared_inject",
            . == "TypeofTest" ~ "tested",
            TRUE ~ .
         )
      ) %>%
      mutate_at(
         .vars = vars(
            matches("reach_date"),
            matches("date_last_sex_msm"),
            matches("test_date"),
            matches("confirm_date"),
            matches("link2care_date"),
            matches("artstart_date"),
            matches("date_last_inject")
         ),
         ~if_else(
            condition = StrIsNumeric(.),
            true      = excel_numeric_to_date(as.numeric(.)),
            false     = NA_Date_
         )
      ) %>%
      mutate(
         sheet         = year,
         ohasis_record = as.character(row_number()),
         ohasis_record = stri_pad_left(ohasis_record, max(nchar(ohasis_record)), "0"),
         ohasis_record = glue(r"({year}-{ohasis_record})"),
         .before       = 1
      )
}

##  Import excel data ----------------------------------------------------------

# assign to global
dr$for_match <- bind_rows(dr$data) %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(toupper(.))
   ) %>%
   distinct_all() %>%
   left_join(
      y  = dr$corr$logsheet_psfi$site_addr,
      by = c("site_region", "site_province", "site_muncity")
   )

dr$for_match <- ohasis$get_addr(
   dr$for_match,
   c(
      "SITE_REG"  = "oh_reg",
      "SITE_PROV" = "oh_prov",
      "SITE_MUNC" = "oh_munc"
   ),
   "name"
)

min_date <- min(dr$for_match$reach_date, na.rm = TRUE)
max_date <- max(dr$for_match$reach_date, na.rm = TRUE)
.log_info("{green('PSFI')} earliest reach: {red(min_date)}")
.log_info("{green('PSFI')} latest reach: {red(max_date)}")

.log_success("Done!")
rm(list = setdiff(ls(), currEnv))