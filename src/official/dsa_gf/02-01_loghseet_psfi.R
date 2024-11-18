##  Append testing data  -------------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

##  Download relevant form data ------------------------------------------------

logsheet_dir  <- file.path("archive", ohasis$ym, ohasis$output_title, "dsa_gf")
# logsheet_file <- file.path(logsheet_dir, "gf_logsheet_raw.xlsx")
logsheet_file <- "C:/Users/johnb/Downloads/Raw_Data_for Jul-Dec 2023_as of 01092024.xlsx"
check_dir(logsheet_dir)

.log_info("Checking if file is downloaded.")
if (!file.exists(logsheet_file)) {
   redownload <- "1"
} else {
   .log_warn("File already exists!")
   redownload <- input(
      prompt  = "Re-download the file?",
      options = c("1" = "yes", "2" = "no"),
      default = "2"
   )
}

# download if not exists
if (redownload == "1") {
   data <- input(prompt = "What is the drive link for the GF-provided raw data?")
   drive_download(data, logsheet_file, overwrite = TRUE)
}
##  Import excel data ----------------------------------------------------------

.log_info("Importing sheets.")
psfi <- list()
for (kp in c("MSM", "TGW", "PWID", "MSMTGW")) {
   sheet         <- tolower(kp)
   psfi[[sheet]] <- read_xlsx(logsheet_file, sheet = kp, col_types = "text", na = c("", "NULL")) %>%
      rename_all(
         ~case_when(
            . == "Region" ~ "site_region",
            . == "Province" ~ "site_province",
            . == "City" ~ "site_muncity",
            . == "Validated" ~ "gf_validated",
            . == "Validation Status" ~ "gf_validated",
            . == "Logsheet Type" ~ "ls_type",
            . == "Logsheet Subtype" ~ "ls_subtype",
            . == "Logsheet Kind" ~ "ls_kind",
            . == "Facility Type" ~ "ls_subtype",
            . == "Facility Name" ~ "ls_kind",
            . == "Peer Navigator Name" ~ "provider_name",
            . == "PN Name" ~ "provider_name",
            . == "Reach Date" ~ "reach_date",
            . == "Date of Reach" ~ "reach_date",
            . == "Type of Venue" ~ "venue_type",
            . == "Venue" ~ "venue",
            . == "Client UIC" ~ "uic",
            . == "UIC" ~ "uic",
            . == "Type of KAP" ~ "kap_type",
            . == "KAP" ~ "kap_type",
            . == "Sex" ~ "sex",
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
            . == "Date of L2C" ~ "link2care_date",
            . == "Date of Enrollment" ~ "artstart_date",
            . == "Date Enrolled" ~ "artstart_date",
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
            . == "Injection Venue" ~ "venue_last_inject",
            . == "Date Last Injected" ~ "date_last_inject",
            . == "Date of Last Injection" ~ "date_last_inject",
            . == "Shared Injection" ~ "shared_inject",
            . == "Shared Needles" ~ "shared_inject",
            . == "Type of Test" ~ "tested",
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
         # ~case_when(
         #    StrIsNumeric(.) ~ excel_numeric_to_date(as.numeric(.)),
         #    stri_detect_fixed(., "/") ~ as.Date(., "%m/%d/%Y"),
         #    stri_detect_fixed(., "-") ~ as.Date(., "%Y-%m-%d"),
         #    # TRUE ~ as.Date(.)
         # )
         ~if_else(StrIsNumeric(.), as.Date(excel_numeric_to_date(as.numeric(.))), as.Date(parse_date_time(., c("YmdHMS", "Ymd", "mdY", "mdy"))))
      ) %>%
      mutate(
         sheet         = kp,
         ohasis_record = as.character(row_number()),
         ohasis_record = stri_pad_left(ohasis_record, max(nchar(ohasis_record)), "0"),
         ohasis_record = glue(r"({kp}-{ohasis_record})"),
         .before       = 1
      )
}

# assign to global
gf$logsheet$psfi <- bind_rows(psfi) %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(toupper(.))
   ) %>%
   distinct_all() %>%
   filter(
      reach_date >= as.Date(gf$coverage$min),
      reach_date <= as.Date(gf$coverage$max)
   ) %>%
   left_join(
      y  = gf$corr$site_addr,
      by = c("site_region", "site_province", "site_muncity")
   )

gf$logsheet$psfi <- ohasis$get_addr(
   gf$logsheet$psfi,
   c(
      "SITE_REG"  = "oh_reg",
      "SITE_PROV" = "oh_prov",
      "SITE_MUNC" = "oh_munc"
   ),
   "name"
)

min_date <- min(gf$logsheet$psfi$reach_date, na.rm = TRUE)
max_date <- max(gf$logsheet$psfi$reach_date, na.rm = TRUE)
.log_info("{green('PSFI')} earliest reach: {red(min_date)}")
.log_info("{green('PSFI')} latest reach: {red(max_date)}")

.log_success("Done!")
rm(list = setdiff(ls(), currEnv))