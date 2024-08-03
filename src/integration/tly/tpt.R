file <- "D:/Downloads/Documents/20240802_ly-tpt.ods"
tly  <- list(
   convert = list(
      addr  = read_sheet("1r8CVfX16oDSStwLfIQdExyA-X21QuGnKwZp1AGDNXdc", "addr", range = "A:F", col_types = "c"),
      staff = read_sheet("1r8CVfX16oDSStwLfIQdExyA-X21QuGnKwZp1AGDNXdc", "staff", range = "A:C", col_types = "c")
   )
)

##  processing -----------------------------------------------------------------

sheets     <- ods_sheets(file)
sheets     <- sheets[!(sheets %in% c("Template", "Dropdown"))]
tly$visits <- sapply(sheets, read_ods, path = file, col_types = cols(.default = "c"), skip = 1, .name_repair = "unique_quiet", USE.NAMES = TRUE) %>%
   sapply(rename_all, ~str_replace_all(., "\\n", " "), USE.NAMES = TRUE) %>%
   sapply(rename_all, str_squish, USE.NAMES = TRUE) %>%
   bind_rows(.id = "BRANCH") %>%
   select(
      BRANCH,
      RECORD_DATE       = `Date of Screening`,
      TB_IPT_START_DATE = `Date Start of Treatment`,
      PATIENT_CODE      = `Client Code`,
      UIC               = `Unique Identifier Code`,
      LAST              = `Last Name`,
      FIRST             = `First Name`,
      MIDDLE            = `Middle Name`,
      BIRTHDATE         = `Date of Birth`,
      AGE               = `Age`,
      SEX               = `Sex`,
      PERM_BRGY         = `Permanent Address (Barangay)`,
      PERM_MUNC         = `Permanent Address (City/ Municipality)`,
      TB_IPT_REGIMEN    = `Regimen`,
      TB_IPT_END_DATE   = `Outcome Date`,
      TB_IPT_STATUS     = `Status`,
      LAB_XPERT_RESULT  = `GeneXpert Result`,
      LAB_XPERT_DATE    = `GeneXpert Date`,
   ) %>%
   mutate_all(~na_if(., "Err:522")) %>%
   mutate(
      drop = if_all(c(RECORD_DATE, TB_IPT_START_DATE, PATIENT_CODE, FIRST, LAST, UIC, BIRTHDATE), is.na),
   ) %>%
   filter(!drop) %>%
   mutate(
      drop = PATIENT_CODE == "ARM24-0057"
   ) %>%
   filter(!drop) %>%
   mutate_at(
      .vars = vars(contains("DATE")),
      ~as.Date(parse_date_time(., c("mdy", "mdY")))
   ) %>%
   mutate(
      SEX           = case_when(
         SEX == "Male" ~ "1_Male",
         SEX == "Female" ~ "2_Female",
      ),
      TB_IPT_STATUS = case_when(
         is.na(TB_IPT_START_DATE) ~ "0_Not on IPT",
         RECORD_DATE == TB_IPT_START_DATE ~ '12_Started IPT',
         TB_IPT_STATUS == "On Treatment" ~ '11_Ongoing IPT',
         TB_IPT_STATUS == "Treatment Completed" ~ '13_Ended IPT',
         TB_IPT_STATUS == "Discontinued" ~ '13_Ended IPT',
         TB_IPT_STATUS == "Not Enrolled" ~ "0_Not on IPT",
         TRUE ~ "11_Ongoing IPT"
      ),
      CLINIC_NOTES  = glue(r"(Date of TB Screening = {RECORD_DATE})"),
      RECORD_DATE   = TB_IPT_START_DATE
   ) %>%
   left_join(
      y  = read_sheet("1r8CVfX16oDSStwLfIQdExyA-X21QuGnKwZp1AGDNXdc", "sites", range = "A:C", col_types = "c"),
      by = join_by(BRANCH)
   )

min <- min(tly$visits$RECORD_DATE, na.rm = TRUE)
max <- max(tly$visits$RECORD_DATE, na.rm = TRUE)

#  uploaded --------------------------------------------------------------------

db              <- "ohasis_warehouse"
lw_conn         <- ohasis$conn("lw")
tly$prev_upload <- QB$new(lw_conn)
tly$prev_upload$from("ohasis_warehouse.form_art_bc")
tly$prev_upload$where
tly$prev_upload %<>%
   mutate_if(
      .predicate = is.POSIXct,
      ~as.Date(.)
   )
dbDisconnect(lw_conn)

##  new data -------------------------------------------------------------------

tly$records <- tly$visits %>%
   filter(!is.na(RECORD_DATE)) %>%
   mutate(
      REC_ID       = NA_character_,
      PATIENT_ID    = NA_character_,
      TX_STATUS    = "0_Not on ART",
      CREATED_BY   = "1300000048",
      CREATED_AT   = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      FORM_VERSION = "ART Form (v2021)",
   )

TIMESTAMP <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
tly$records %<>%
   # retain only not uploaded and those with changes
   # anti_join(
   #    y  = tly$prev_upload,
   #    # by = join_by(!!!intersect(names(tly$records), names(tly$prev_upload))),
   #    by = join_by(REC_ID, VISIT_DATE, MEDICINE_SUMMARY),
   # ) %>%
   mutate(
      row_id     = row_number(),
      old_rec    = if_else(!is.na(REC_ID), 1, 0, 0),
      CREATED_BY = coalesce(CREATED_BY, "1300000048"),
      CREATED_AT = coalesce(as.character(CREATED_AT), TIMESTAMP),
      UPDATED_BY = if_else(old_rec == 1, "1300000048", NA_character_),
      UPDATED_AT = if_else(old_rec == 1, TIMESTAMP, NA_character_)
   ) %>%
   # relocate(any_of(names(tly$prev_upload)), .before = 1) %>%
   select(-old_rec)

tly$import <- tly$records %>%
   filter(!is.na(REC_ID)) %>%
   bind_rows(
      batch_rec_ids(tly$records %>% filter(is.na(REC_ID)), REC_ID, CREATED_BY, "row_id")
   )

tly$import %<>%
   filter(!is.na(PATIENT_ID)) %>%
   bind_rows(
      batch_px_ids(tly$import %>% filter(is.na(PATIENT_ID)), PATIENT_ID, FACI_ID, "row_id")
   )


tly$import %<>%
   mutate(
      UPDATED_BY = "1300000048",
      UPDATED_AT = TIMESTAMP
   )
##  table formats
tables           <- list()
tables$px_record <- list(
   name = "px_record",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = tly$import %>%
      mutate(
         SUB_FACI_ID = NA_character_,
         DISEASE     = "101000",
         MODULE      = 3
      ) %>%
      select(
         REC_ID,
         PATIENT_ID,
         FACI_ID,
         SUB_FACI_ID,
         RECORD_DATE,
         DISEASE,
         MODULE,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      )
)

tables$px_info <- list(
   name = "px_info",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = tly$import %>%
      select(
         REC_ID,
         PATIENT_ID,
         UIC,
         PATIENT_CODE,
         SEX,
         BIRTHDATE,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      ) %>%
      mutate_at(
         .vars = vars(SEX),
         ~keep_code(.)
      )
)

tables$px_name <- list(
   name = "px_name",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = tly$import %>%
      select(
         REC_ID,
         PATIENT_ID,
         FIRST,
         MIDDLE,
         LAST,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      )
)

tables$px_faci <- list(
   name = "px_faci",
   pk   = c("REC_ID", "SERVICE_TYPE"),
   data = tly$import %>%
      mutate(
         SERVICE_TYPE = "101201"
      ) %>%
      select(
         REC_ID,
         FACI_ID     ,
         SUB_FACI_ID ,
         SERVICE_TYPE,
         TX_STATUS,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      ) %>%
      mutate_at(
         .vars = vars(TX_STATUS),
         ~keep_code(.)
      )
)

tables$px_form <- list(
   name = "px_form",
   pk   = c("REC_ID", "FORM"),
   data = tly$import %>%
      mutate(
         FORM    = "ART Form",
         VERSION = "2021"
      ) %>%
      select(
         REC_ID,
         FORM,
         VERSION,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      )
)

tables$px_labs <- list(
   name = "px_labs",
   pk   = c("REC_ID", "LAB_TEST"),
   data = tly$import %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
         starts_with("LAB"),
      ) %>%
      mutate_at(
         .vars = vars(contains("_DATE")),
         ~as.character(.)
      ) %>%
      pivot_longer(
         cols      = starts_with("LAB"),
         names_to  = "LAB_DATA",
         values_to = "LAB_VALUE"
      ) %>%
      mutate(
         LAB_TEST = substr(LAB_DATA, 5, stri_locate_last_fixed(LAB_DATA, "_") - 1),
         PIECE    = substr(LAB_DATA, stri_locate_last_fixed(LAB_DATA, "_") + 1, 1000),
      ) %>%
      mutate(
         LAB_TEST = case_when(
            LAB_TEST == "HBSAG" ~ "1",
            LAB_TEST == "CREA" ~ "2",
            LAB_TEST == "SYPH" ~ "3",
            LAB_TEST == "VL" ~ "4",
            LAB_TEST == "CD4" ~ "5",
            LAB_TEST == "XRAY" ~ "6",
            LAB_TEST == "XPERT" ~ "7",
            LAB_TEST == "DSSM" ~ "8",
            LAB_TEST == "HIVDR" ~ "9",
            LAB_TEST == "HEMO" ~ "10",
            LAB_TEST == "HEMOG" ~ "10",
            TRUE ~ LAB_TEST
         )
      ) %>%
      distinct(REC_ID, CREATED_AT, CREATED_BY, LAB_TEST, PIECE, .keep_all = TRUE) %>%
      pivot_wider(
         id_cols      = c(REC_ID, CREATED_AT, CREATED_BY, LAB_TEST),
         names_from   = PIECE,
         values_from  = LAB_VALUE,
         names_prefix = "LAB_"
      ) %>%
      filter(!is.na(LAB_DATE) | !is.na(LAB_RESULT)) %>%
      arrange(REC_ID, LAB_TEST)
)

tables$px_tb <- list(
   name = "px_tb",
   pk   = "REC_ID",
   data = tly$import %>%
      mutate(
         TB_STATUS = "0_No active TB",
      ) %>%
      select(
         REC_ID,
         TB_STATUS,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      ) %>%
      mutate_at(
         .vars = vars( TB_STATUS),
         ~keep_code(.)
      )
)

tables$px_tb_ipt <- list(
   name = "px_tb_ipt",
   pk   = "REC_ID",
   data = tly$import %>%
      select(
         REC_ID,
         TB_IPT_STATUS,
         TB_IPT_REGIMEN,
         TB_IPT_START_DATE,
         TB_IPT_END_DATE,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      ) %>%
      mutate_at(
         .vars = vars(TB_IPT_STATUS),
         ~keep_code(.)
      )
)

db_conn <- ohasis$conn("db")
lapply(tables, function(ref, db_conn) {
   log_info("Uploading {green(ref$name)}.")
   table_space <- Id(schema = "ohasis_interim", table = ref$name)
   dbxUpsert(db_conn, table_space, ref$data, ref$pk)
   # dbExecute(db_conn, glue("DELETE FROM ohasis_interim.{ref$name} WHERE REC_ID IN (?)"), params = list(unique(ref$data$REC_ID)))
}, db_conn)
dbDisconnect(db_conn)
