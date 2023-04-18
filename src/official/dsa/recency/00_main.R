rt       <- new.env()
rt$ss    <- "1Zqku6Dsk6gykRd3wwcF0zeHv5iqh7_CA"
rt$wd    <- file.path("src", "official", "dsa", "recency")
rt$sites <- read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw") %>%
   distinct(FACI_ID, .keep_all = TRUE) %>%
   filter(site_rt_2023 == 1)

rt$download_forms <- function(wd) {
   forms <- list()

   log_info("Preparing select.")
   rt_where  <- read_file(file.path(wd, "rt.sql"))
   hts_where <- "REC_ID IN (SELECT REC_ID FROM ohasis_lake.px_hiv_testing WHERE RT_RESULT IS NOT NULL)"

   log_info("Downloading lake.")
   lw_conn    <- ohasis$conn("lw")
   dbname     <- "ohasis_warehouse"
   forms$hts  <- dbTable(lw_conn, dbname, "form_hts", raw_where = TRUE, where = hts_where)
   forms$a    <- dbTable(lw_conn, dbname, "form_a", raw_where = TRUE, where = hts_where)
   forms$cfbs <- dbTable(lw_conn, dbname, "form_cfbs", raw_where = TRUE, where = hts_where)
   dbDisconnect(lw_conn)

   log_info("Downloading RT.")
   db_conn  <- ohasis$conn("db")
   forms$rt <- tracked_select(db_conn, rt_where, "rt_data")
   dbDisconnect(db_conn)

   return(forms)
}

rt$rt_initial <- function(forms) {

   hts_data         <- process_hts(forms$hts, forms$a, forms$cfbs)
   hts_names_remove <- intersect(names(forms$rt), names(hts_data))
   hts_names_remove <- hts_names_remove[hts_names_remove != "REC_ID"]

   rt_raw <- forms$rt %>%
      left_join(
         y  = hts_data %>%
            select(-any_of(hts_names_remove)) %>%
            distinct(REC_ID, .keep_all = TRUE),
         by = join_by(REC_ID)
      ) %>%
      remove_pii() %>%
      mutate(
         FORM_ENCODED = if_else(!is.na(FORM_VERSION), '1_Yes', "0_No", "0_No"),
         .after       = RT_RESULT
      ) %>%
      mutate(
         CBS_VENUE      = toupper(str_squish(HIV_SERVICE_ADDR)),
         ONLINE_APP     = case_when(
            grepl("GRINDR", CBS_VENUE) ~ "GRINDR",
            grepl("GRNDR", CBS_VENUE) ~ "GRINDR",
            grepl("GRINDER", CBS_VENUE) ~ "GRINDR",
            grepl("TWITTER", CBS_VENUE) ~ "TWITTER",
            grepl("FACEBOOK", CBS_VENUE) ~ "FACEBOOK",
            grepl("MESSENGER", CBS_VENUE) ~ "FACEBOOK",
            grepl("\\bFB\\b", CBS_VENUE) ~ "FACEBOOK",
            grepl("\\bGR\\b", CBS_VENUE) ~ "GRINDR",
         ),
         REACH_ONLINE   = if_else(!is.na(ONLINE_APP), "1_Yes", REACH_ONLINE, REACH_ONLINE),
         REACH_CLINICAL = if_else(
            condition = if_all(starts_with("REACH_"), ~is.na(.)) & hts_modality == "FBT",
            true      = "1_Yes",
            false     = REACH_CLINICAL,
            missing   = REACH_CLINICAL
         )
      ) %>%
      rename(
         CREATED = CREATED_BY,
         UPDATED = UPDATED_BY,
      ) %>%
      generate_gender_identity(SEX, SELF_IDENT, SELF_IDENT_OTHER, gender_identity) %>%
      rename(
         HTS_PROVIDER_TYPE       = PROVIDER_TYPE,
         HTS_PROVIDER_TYPE_OTHER = PROVIDER_TYPE_OTHER,
      ) %>%
      mutate(
         RT_VL_RESULT = as.integer(stri_replace_all_fixed(RT_VL_RESULT, ",", "")),
         RITA_RESULT  = case_when(
            RT_VL_RESULT >= 1000 ~ "1_Recent",
            RT_VL_RESULT < 1000 ~ "2_Long-term",
         ),
         .after       = RT_VL_RESULT,
      ) %>%
      select(
         -any_of(
            c(
               "PRIME",
               "RECORD_DATE",
               "DISEASE",
               "BIRTHDATE",
               "HIV_SERVICE_TYPE",
               "GENDER_AFFIRM_THERAPY",
               "HIV_SERVICE_ADDR",
               "src",
               "MODULE",
               "MODALITY",
               "FACI_ID",
               "SUB_FACI_ID",
               "FORM_VERSION",
               "CONFIRMATORY_CODE",
               "DELETED_BY",
               "DELETED_AT",
               "SCREEN_AGREED",
               "EXPOSE_SEX_M_NOCONDOM",
               "EXPOSE_SEX_F_NOCONDOM",
               "EXPOSE_SEX_HIV",
               "AGE_FIRST_SEX",
               "NUM_F_PARTNER",
               "YR_LAST_F",
               "NUM_M_PARTNER",
               "YR_LAST_M",
               "AGE_FIRST_INJECT",
               "MED_CBS_REACTIVE",
               "MED_IS_PREGNANT",
               "FORMA_MSM",
               "FORMA_TGW",
               "FORMA_PWID",
               "FORMA_FSW",
               "FORMA_GENPOP",
               "SCREEN_REFER",
               "PARTNER_REFERRAL_FACI",
               "EXPOSE_SEX_EVER",
               "EXPOSE_CONDOMLESS_ANAL",
               "EXPOSE_CONDOMLESS_VAGINAL",
               "EXPOSE_M_SEX_ORAL_ANAL",
               "EXPOSE_NEEDLE_SHARE",
               "EXPOSE_ILLICIT_DRUGS",
               "EXPOSE_SEX_HIV_DATE",
               "EXPOSE_CONDOMLESS_ANAL_DATE",
               "EXPOSE_CONDOMLESS_VAGINAL_DATE",
               "EXPOSE_NEEDLE_SHARE_DATE",
               "EXPOSE_ILLICIT_DRUGS_DATE",
               "SERVICE_GIVEN_CONDOMS",
               "SERVICE_GIVEN_LUBES",
               "TEST_REFUSE_NO_TIME",
               "TEST_REFUSE_OTHER",
               "TEST_REFUSE_NO_CURE",
               "TEST_REFUSE_FEAR_RESULT",
               "TEST_REFUSE_FEAR_DISCLOSE",
               "TEST_REFUSE_FEAR_MSM",
               "CFBS_MSM",
               "CFBS_TGW",
               "CFBS_PWID",
               "CFBS_FSW",
               "CFBS_GENPOP"
            )
         )
      )

   return(rt_raw)
}

rt$forms        <- rt$download_forms(rt$wd)
rt$data$initial <- rt$rt_initial(rt$forms)
rt$data$final   <- rt$data$initial %>%
   ohasis$get_faci(
      list(HTS_FACI = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
      "name"
   ) %>%
   ohasis$get_faci(
      list(SPECIMEN_SOURCE_FACI = c("SPECIMEN_SOURCE", "SPECIMEN_SUB_SOURCE")),
      "name"
   ) %>%
   ohasis$get_faci(
      list(CONFIRM_LAB = c("CONFIRM_FACI", "CONFIRM_SUB_FACI")),
      "name"
   ) %>%
   ohasis$get_addr(
      c(
         PERM_REG  = "PERM_PSGC_REG",
         PERM_PROV = "PERM_PSGC_PROV",
         PERM_MUNC = "PERM_PSGC_MUNC"
      ),
      "name"
   ) %>%
   ohasis$get_addr(
      c(
         CURR_REG  = "CURR_PSGC_REG",
         CURR_PROV = "CURR_PSGC_PROV",
         CURR_MUNC = "CURR_PSGC_MUNC"
      ),
      "name"
   ) %>%
   ohasis$get_addr(
      c(
         BIRTH_REG  = "BIRTH_PSGC_REG",
         BIRTH_PROV = "BIRTH_PSGC_PROV",
         BIRTH_MUNC = "BIRTH_PSGC_MUNC"
      ),
      "name"
   ) %>%
   ohasis$get_addr(
      c(
         CBS_REG  = "HIV_SERVICE_PSGC_REG",
         CBS_PROV = "HIV_SERVICE_PSGC_PROV",
         CBS_MUNC = "HIV_SERVICE_PSGC_MUNC"
      ),
      "name"
   ) %>%
   ohasis$get_staff(c(CREATED_BY = "CREATED")) %>%
   ohasis$get_staff(c(UPDATED_BY = "UPDATED")) %>%
   ohasis$get_staff(c(HTS_PROVIDER = "SERVICE_BY")) %>%
   ohasis$get_staff(c(ANALYZED_BY = "SIGNATORY_1")) %>%
   ohasis$get_staff(c(REVIEWED_BY = "SIGNATORY_2")) %>%
   ohasis$get_staff(c(NOTED_BY = "SIGNATORY_3"))

oh_dir       <- file.path("O:/My Drive/Data Sharing/ICAP")
file_initial <- file.path(oh_dir, "RecencyTesting-PreProcess.xlsx")
file_final   <- file.path(oh_dir, "RecencyTesting-PostProcess.xlsx")
file_faci    <- file.path(oh_dir, "OHASIS-FacilityIDs.xlsx")
file_json    <- file.path(oh_dir, "DataStatus.json")
rt$data$json <- jsonlite::read_json(file_json)

rt$data$json$`RecencyTesting-PreProcess`  <- list(
   upload_date  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
   version_date = format(max(as.POSIXct(rt$data$initial$SNAPSHOT), na.rm = TRUE), "%Y-%m-%d %H:%M:%S")
)
rt$data$json$`RecencyTesting-PostProcess` <- list(
   upload_date  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
   version_date = format(max(as.POSIXct(rt$data$final$SNAPSHOT), na.rm = TRUE), "%Y-%m-%d %H:%M:%S")
)
rt$data$json$`OHASIS-FacilityIDs`         <- list(
   upload_date  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
   version_date = format(
      as.POSIXct(
         paste0(
            strsplit(ohasis$timestamp, "\\.")[[1]][1], "-",
            strsplit(ohasis$timestamp, "\\.")[[1]][2], "-",
            strsplit(ohasis$timestamp, "\\.")[[1]][3], " ",
            StrLeft(strsplit(ohasis$timestamp, "\\.")[[1]][4], 2), ":",
            substr(strsplit(ohasis$timestamp, "\\.")[[1]][4], 3, 4), ":",
            StrRight(strsplit(ohasis$timestamp, "\\.")[[1]][4], 2)
         )
      ),

      "%Y-%m-%d %H:%M:%S"
   )
)

write_xlsx(rt$data$initial, file_initial)
write_xlsx(rt$data$final, file_final)
write_xlsx(ohasis$ref_faci, file_faci)
jsonlite::write_json(rt$data$json, file_json, pretty = TRUE, auto_unbox = TRUE)
slackr_save(rt, file = "recency-testing", title = stri_c("Recency Testing Data as of ", ohasis$timestamp))


##  MER ------------------------------------------------------------------------
oh_ts <- format(
   as.POSIXct(
      paste0(
         strsplit(ohasis$timestamp, "\\.")[[1]][1], "-",
         strsplit(ohasis$timestamp, "\\.")[[1]][2], "-",
         strsplit(ohasis$timestamp, "\\.")[[1]][3], " ",
         StrLeft(strsplit(ohasis$timestamp, "\\.")[[1]][4], 2), ":",
         substr(strsplit(ohasis$timestamp, "\\.")[[1]][4], 3, 4), ":",
         StrRight(strsplit(ohasis$timestamp, "\\.")[[1]][4], 2)
      )
   ),
   "%Y-%m-%d %H:%M:%S"
)

rt                               <- new.env()
rt$data$json                     <- jsonlite::read_json(file_json)
rt$data$json$`Linelist-TX`       <- list(
   upload_date  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
   version_date = oh_ts,
   coverage     = pepfar$coverage[1:5]
)
rt$data$json$`Linelist-PrEP`     <- list(
   upload_date  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
   version_date = oh_ts,
   coverage     = pepfar$coverage[1:5]
)
rt$data$json$`Linelist-HTS-PREV` <- list(
   upload_date  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
   version_date = oh_ts,
   coverage     = pepfar$coverage[1:5]
)

pepfar$ip$ICAP$linelist$tx %>%
   remove_pii() %>%
   write_xlsx(file.path(oh_dir, glue("Linelist-TX.xlsx")))

pepfar$ip$ICAP$linelist$prep %>%
   remove_pii() %>%
   write_xlsx(file.path(oh_dir, glue("Linelist-PrEP.xlsx")))

pepfar$ip$ICAP$linelist$reach %>%
   remove_pii() %>%
   write_xlsx(file.path(oh_dir, glue("Linelist-HTS-PREV.xlsx")))

jsonlite::write_json(rt$data$json, file_json, pretty = TRUE, auto_unbox = TRUE)
