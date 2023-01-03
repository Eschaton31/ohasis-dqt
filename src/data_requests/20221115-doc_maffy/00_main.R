dr           <- list()
dr$`2022115` <- new.env()

# download forms
local(envir = dr$`2022115`, {
   forms   <- list()
   lw_conn <- ohasis$conn("lw")
   db_name <- "ohasis_warehouse"
   min     <- "2021-11-01"
   max     <- "2022-10-31"
   for (form in c("form_hts", "form_a", "form_cfbs")) {
      main_cols <- colnames(tbl(lw_conn, dbplyr::in_schema(db_name, form)))
      cols      <- ""
      cols      <- c(cols, main_cols[grepl("EXPOSE", main_cols)])
      cols      <- c(cols, main_cols[grepl("RISK", main_cols)])
      cols      <- c(cols, main_cols[grepl("PARTNER", main_cols)])
      cols      <- c(cols, main_cols[grepl("DATE", main_cols)])
      cols      <- c(cols, main_cols[grepl("AGREE", main_cols)])
      cols      <- c(cols, main_cols[grepl("RESULT", main_cols)])
      cols      <- c(cols, main_cols[grepl("FACI", main_cols)])
      cols      <- c(cols, main_cols[grepl("PSGC", main_cols)])
      cols      <- c(cols, main_cols[grepl("YR_LAST", main_cols)])
      cols      <- c(cols, main_cols[grepl("SOURCE", main_cols)])
      cols      <- unique(cols)
      cols      <- cols[cols != ""]
      cols      <- c("REC_ID", "PATIENT_ID", "SEX", "SELF_IDENT", "SELF_IDENT_OTHER", "AGE", "FORM_VERSION", "MODALITY", cols)

      if (form != "form_cfbs") {
         forms[[form]] <- dbTable(
            lw_conn,
            db_name,
            form,
            cols      = cols,
            raw_where = TRUE,
            where     = glue(r"(
((RECORD_DATE BETWEEN '{min}' AND '{max}') OR
(DATE(DATE_COLLECT) BETWEEN '{min}' AND '{max}') OR
(DATE(DATE_CONFIRM) BETWEEN '{min}' AND '{max}') OR
(DATE(T3_DATE) BETWEEN '{min}' AND '{max}') OR
(DATE(T2_DATE) BETWEEN '{min}' AND '{max}') OR
(DATE(T1_DATE) BETWEEN '{min}' AND '{max}') OR
(DATE(T0_DATE) BETWEEN '{min}' AND '{max}')))"),
         )
      } else
         forms[[form]] <- dbTable(
            lw_conn,
            db_name,
            form,
            cols      = cols,
            raw_where = TRUE,
            where     = glue(r"(
((RECORD_DATE BETWEEN '{min}' AND '{max}') OR
(DATE(TEST_DATE) BETWEEN '{min}' AND '{max}')))"),
         )
   }
   dbDisconnect(lw_conn)
   rm(min, max, lw_conn, db_name, form, cols, main_cols)
})

# standardize
local(envir = dr$`2022115`, {
   hts_raw <- process_hts(forms$form_hts, forms$form_a, forms$form_cfbs)
})

# get faci types
local(envir = dr$`2022115`, {
   db_conn   <- ohasis$conn("db")
   faci_type <- dbTable(db_conn, "ohasis_interim", "facility", cols = c("FACI_ID", "FACI_TYPE", "EDIT_NUM"))
   faci_serv <- dbTable(db_conn, "ohasis_interim", "facility_service", cols = c("FACI_ID", "SERVICE"))
   id_reg    <- dbTable(db_conn, "ohasis_interim", "registry", cols = c("PATIENT_ID", "CENTRAL_ID"))
   rm(db_conn)
})

local(envir = dr$`2022115`, {
   hts_final <- hts_raw %>%
      mutate(
         # tag those without form faci
         use_record_faci  = if_else(
            condition = is.na(SERVICE_FACI),
            true      = 1,
            false     = 0
         ),
         SERVICE_FACI     = case_when(
            use_record_faci == 1 & FACI_ID != "130000" ~ FACI_ID,
            use_record_faci == 1 & FACI_ID == "130000" ~ SPECIMEN_SOURCE,
            TRUE ~ SERVICE_FACI
         ),
         SERVICE_SUB_FACI = case_when(
            use_record_faci == 1 & FACI_ID == "130000" ~ SPECIMEN_SUB_SOURCE,
            !(SERVICE_FACI %in% c("130001", "130605")) ~ NA_character_,
            TRUE ~ SERVICE_SUB_FACI
         ),
      ) %>%
      left_join(
         y = faci_type %>%
            filter(!is.na(FACI_TYPE)) %>%
            mutate(
               FACI_TYPE = case_when(
                  FACI_TYPE == '10000' ~ 'Health Service Facility',
                  FACI_TYPE == '20000' ~ 'Private Entity',
                  FACI_TYPE == '21001' ~ 'NGO',
                  FACI_TYPE == '21002' ~ 'CBO',
                  FACI_TYPE == '22001' ~ 'Private Company',
                  FACI_TYPE == '30000' ~ 'Public Entity',
                  FACI_TYPE == '31000' ~ "Gov't - National",
                  FACI_TYPE == '32000' ~ "Gov't - Regional",
                  FACI_TYPE == '40000' ~ 'International Entity',
               )
            ) %>%
            arrange(FACI_ID, desc(EDIT_NUM)) %>%
            distinct(FACI_ID, .keep_all = TRUE) %>%
            rename(SERVICE_FACI = FACI_ID)
      ) %>%
      left_join(
         y = faci_serv %>%
            filter(SERVICE == "101103") %>%
            distinct(FACI_ID) %>%
            rename(SERVICE_FACI = FACI_ID) %>%
            mutate(CBS = 1)
      ) %>%
      ohasis$get_faci(
         list(hts_faci = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
         "name",
         c("hts_region", "hts_province", "hts_muncity")
      ) %>%
      ohasis$get_faci(
         list(confirm_faci = c("CONFIRM_FACI", "CONFIRM_SUB_FACI")),
         "name"
      ) %>%
      ohasis$get_addr(
         c(
            cbs_reg  = "HIV_SERVICE_PSGC_REG",
            cbs_prov = "HIV_SERVICE_PSGC_PROV",
            cbs_munc = "HIV_SERVICE_PSGC_MUNC"
         ),
         "name"
      ) %>%
      ohasis$get_addr(
         c(
            perm_reg  = "PERM_PSGC_REG",
            perm_prov = "PERM_PSGC_PROV",
            perm_munc = "PERM_PSGC_MUNC"
         ),
         "name"
      ) %>%
      ohasis$get_addr(
         c(
            curr_reg  = "CURR_PSGC_REG",
            curr_prov = "CURR_PSGC_PROV",
            curr_munc = "CURR_PSGC_MUNC"
         ),
         "name"
      ) %>%
      ohasis$get_addr(
         c(
            birth_reg  = "BIRTH_PSGC_REG",
            birth_prov = "BIRTH_PSGC_PROV",
            birth_munc = "BIRTH_PSGC_MUNC"
         ),
         "name"
      ) %>%
      get_cid(id_reg, PATIENT_ID) %>%
      mutate(
         hts_faci  = case_when(
            stri_detect_fixed(hts_faci, "HASH TL") ~ "HIV & AIDS Support House (HASH)",
            hts_faci == "GILEAD" ~ "HASH-GILEAD",
            TRUE ~ hts_faci
         ),
         faci_type = case_when(
            FACI_TYPE == "NGO" ~ "NGO",
            FACI_TYPE == "CBO" ~ "CBO",
            stri_detect_fixed(toupper(hts_faci), "OSPITAL") ~ "hospital",
            stri_detect_fixed(toupper(hts_faci), "MEDICAL CENTER") ~ "hospital",
            stri_detect_fixed(toupper(hts_faci), "MEDICAL CITY") ~ "hospital",
            stri_detect_fixed(toupper(hts_faci), "CITY HEALTH OFFICE") ~ "CHO",
            stri_detect_fixed(toupper(hts_faci), "RURAL HEALTH UNIT") ~ "RHU",
            stri_detect_fixed(toupper(hts_faci), "RHU") ~ "RHU",
            stri_detect_fixed(toupper(hts_faci), "SOCIAL HYGIENE CLINIC") ~ "SHC",
            stri_detect_fixed(toupper(hts_faci), "SHC") ~ "SHC",
            stri_detect_fixed(toupper(hts_faci), "REPRODUCTIVE HEALTH AND WELLNESS") ~ "SHC",
            stri_detect_fixed(toupper(hts_faci), "RHWC") ~ "SHC",
            stri_detect_fixed(toupper(hts_faci), "LOVEYOURSELF") ~ "LY",
            stri_detect_fixed(toupper(hts_faci), "HI-PRECISION DIAGNOSTICS") ~ "HPD",
            stri_detect_fixed(toupper(hts_faci), "HASH") ~ "CBO",
            stri_detect_fixed(toupper(hts_faci), "LAKAN") ~ "CBO",
            stri_detect_fixed(toupper(hts_faci), "MEDICAL CLINIC") ~ "Private Clinic",
            stri_detect_fixed(toupper(hts_faci), "(SHIP)") ~ "Private Clinic",
            stri_detect_fixed(toupper(hts_faci), "(SAIL)") ~ "CBO",
            stri_detect_fixed(toupper(hts_faci), "(FPOP)") ~ "CBO",
         )
      ) %>%
      mutate(
         hts_date = case_when(
            year(hts_date) < 2020 & year(T0_DATE) >= 2021 ~ as.Date(T0_DATE),
            year(hts_date) < 2020 & year(T1_DATE) >= 2021 ~ as.Date(T1_DATE),
            TRUE ~ hts_date
         ),
         AGE_DTA  = if_else(
            condition = !is.na(BIRTHDATE),
            true      = floor(interval(BIRTHDATE, hts_date) / years(1)),
            false     = as.numeric(NA)
         ),
         age      = case_when(
            !is.na(AGE) ~ as.integer(AGE),
            !is.na(AGE_DTA) ~ as.integer(AGE_DTA)
         )
      ) %>%
      select(
         -UPDATED_BY,
         -UPDATED_AT,
         -FACI_ID,
         -SUB_FACI_ID,
         -SERVICE_FACI,
         -SERVICE_SUB_FACI,
         -CONFIRM_FACI,
         -CONFIRM_SUB_FACI,
         -SPECIMEN_SOURCE,
         -SPECIMEN_SUB_SOURCE,
         -MODALITY,
         -EDIT_NUM,
         -CBS,
         -FACI_TYPE,
         -use_record_faci,
         -FORM_VERSION,
         -AGE,
         -AGE_DTA,
         -contains("PSGC", ignore.case = FALSE)
      ) %>%
      generate_gender_identity(SEX, SELF_IDENT, SELF_IDENT_OTHER, gender_identity) %>%
      rename_at(
         .vars = vars(SEX, SELF_IDENT, SELF_IDENT_OTHER),
         ~tolower(.)
      )
})

local(envir = dr$`2022115`, {
   hts <- hts_final %>%
      filter(
         !is.na(hts_faci),
         hts_date >= "2021-11-01",
         hts_date <= "2022-10-31",
         # age >= 15,
         # test_agreed == 1
      ) %>%
      arrange(hts_date) %>%
      group_by(CENTRAL_ID) %>%
      mutate(
         modes = paste0(collapse = ', ', unique(sort(hts_modality)))
      ) %>%
      ungroup() %>%
      distinct(CENTRAL_ID, .keep_all = TRUE)
})

dr$`2022115`$hts %>%
   format_stata() %>%
   write_dta("H:/20221229-hts_data.dta")