##  Download encoding documentation --------------------------------------------

db_conn    <- ohasis$conn("db")
px_confirm <- dbTable(db_conn, "ohasis_interim", "px_confirm")
dbDisconnect(db_conn)

# special
ei <- encoded$data$records %>%
   mutate(
      CONFIRM_CODE = CONFIRMATORY_CODE
   ) %>%
   select(
      `Facility ID`   = TEST_FACI,
      `Facility Name` = FACI_ID,
      `Page ID`       = CONFIRMATORY_CODE,
      `Record ID`     = REC_ID,
      `Identifier`    = CONFIRM_CODE,
      `Issues`        = CLINIC_NOTES,
      `Validation`    = COUNSEL_NOTES,
      `Encoder`       = encoder
   )

ei      <- get_ei("2022.07")
ei      <- bind_rows(
   read_sheet("1oyje9a2JrW_bVF0HrdYgy3QmhMqY358NxDn17yz8O_8", "documentation") %>%
      mutate(encoder = "jsmanaois.pbsp@gmail.com"),
   read_sheet("1HMspM0t5woHL8EJ7Bv5M-nflTMWo7P9-ivX90sxXsrU", "documentation") %>%
      mutate(encoder = "tayagallen14.doh@gmail.com")
)
encoded <- ei %>%
   filter(
      # Form %in% c("Form A", "HTS Form") | (is.na(Form) & nchar(`Page ID`) == 12),
      !is.na(`Record ID`)
   ) %>%
   mutate(,
      `Encoder` = stri_replace_first_fixed(encoder, "2022.04_", "")
   ) %>%
   select(
      `Facility ID`,
      `Facility Name`,
      `Page ID`,
      `Record ID`,
      # `ID Type`,
      `Identifier`,
      `Issues`,
      `Validation`,
      `Encoder`
   )

##  Get pdf results data frame -------------------------------------------------

results <- nhsss$harp_dx$pdf_saccl$data %>%
   mutate(
      FILENAME_FORM = NA_character_,
      FINAL_RESULT  = case_when(
         FORM == "*Computer" ~ "Duplicate",
         REMARKS == "Duplicate" ~ "Duplicate",
         TRUE ~ FINAL_RESULT
      ),
      FINAL_RESULT  = case_when(
         FORM == "*Computer" ~ "Duplicate",
         REMARKS == "Duplicate" ~ "Duplicate",
         TRUE ~ FINAL_RESULT
      ),
      LABCODE       = case_when(
         FILENAME == 'SACCLHIV - D22-03-02610.pdf' ~ 'D22-03-02610',
         FILENAME == 'SACCLHIV - D22-03-02636D.pdf' ~ 'D22-03-02636',
         FILENAME == 'SACCLHIV - D22-03-02652D.pdf' ~ 'D22-03-02652',
         FILENAME == 'SACCLHIV - D22-03-03306.pdf' ~ 'D22-03-03306',
         FILENAME == 'SACCLHIV - D22-03-03316.pdf' ~ 'D22-03-03316',
         TRUE ~ LABCODE
      ),
      FULLNAME      = case_when(
         FILENAME == 'SACCLHIV - D22-03-02610.pdf' ~ 'FERANGCO, BERNABE L.',
         FILENAME == 'SACCLHIV - D22-03-02636D.pdf' ~ 'ESPIRITU, CEVIR N.',
         FILENAME == 'SACCLHIV - D22-03-02652D.pdf' ~ 'SUCGANG, DANDEE R.',
         FILENAME == 'SACCLHIV - D22-03-03306.pdf' ~ 'BUSTILLO, ALJON G.',
         FILENAME == 'SACCLHIV - D22-03-03316.pdf' ~ 'SERAD , JOEL B.',
         TRUE ~ FULLNAME
      )
   ) %>%
   distinct(LABCODE, .keep_all = TRUE) %>%
   select(
      FILENAME_PDF  = FILENAME,
      LABCODE,
      FULLNAME_PDF  = FULLNAME,
      BIRTHDATE_PDF = BDATE,
      FILENAME_FORM,
      FINAL_INTERPRETATION,
      FINAL_RESULT
   )

##  Match w/ encoding documentation --------------------------------------------

# match with pdf results
match <- results %>%
   # fuzzyjoin::stringdist_full_join(
   full_join(
      # inner_join(
      y  = encoded %>% mutate(only = 1),
      # by     = c("FULLNAME_PDF" = "Identifier"),
      by = c("LABCODE" = "Page ID"),
      # method = "osa"
   ) %>%
   mutate(
      LABCODE = if_else(is.na(LABCODE), `Record ID`, LABCODE)
   ) %>%
   distinct(LABCODE, .keep_all = TRUE) %>%
   anti_join(
      y  = px_confirm %>% select(LABCODE = CONFIRM_CODE),
      by = "LABCODE"
   )

nrow(encoded)
nrow(results)
nrow(match)

# copy to clipboard for gsheets
write_clip(
   match %>%
      mutate(
         `For Import` = NA_character_,
         `PATIENT_ID` = NA_character_,
      ) %>%
      select(
         `FILENAME_PDF`,
         `LABCODE`,
         `FULLNAME_PDF`,
         `BIRTHDATE_PDF`,
         `FILENAME_FORM`,
         `FINAL_INTERPRETATION`,
         `FINAL_RESULT`,
         `REC_ID` = `Record ID`,
         `PATIENT_ID`,
         `For Import`,
         `Identifier`
      )
)

##  Generate import dataframes -------------------------------------------------

TIMESTAMP <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
# import    <- nhsss$harp_dx$pdf_saccl$data %>%
import    <- nhsss$harp_dx$pdf_saccl$data %>%
   mutate_all(~as.character(.)) %>%
   inner_join(
      y  = nhsss$harp_dx$corr$pdf_results %>%
         select(
            LABCODE,
            REC_ID,
            PATIENT_ID
         ) %>%
         mutate_all(~as.character(.)),
      by = "LABCODE",
      na_matches = "never"
   ) %>%
   inner_join(
      y  = match %>% select(LABCODE),
      by = "LABCODE",
      na_matches = "never"
   ) %>%
   distinct(LABCODE, .keep_all = TRUE) %>%
   filter(!is.na(REC_ID), nchar(REC_ID) == 25)

px_confirm <- import %>%
   mutate(
      FACI_ID      = "130023",
      SUB_FACI_ID  = "130023_001",
      CONFIRM_TYPE = "1",
      CREATED_AT   = TIMESTAMP,
      CREATED_BY   = "1300000001",
      FINAL_RESULT = case_when(
         FORM == "*Computer" ~ "Duplicate",
         REMARKS == "Duplicate" ~ "Duplicate",
         TRUE ~ FINAL_RESULT
      ),
      REMARKS      = case_when(
         REMARKS == "Duplicate" ~ "Client already has a previous confirmatory record.",
         FINAL_RESULT == "Negative" ~ "Laboratory evidence suggests no presence of HIV Antibodies at the time of testing.",
         TRUE ~ REMARKS
      ),
      SIGNATORY_1  = if_else(SIGNATORY_1 == "NULL", NA_character_, SIGNATORY_1),
      DATE_CONFIRM = as.character(DATE_CONFIRM),
      DATE_CONFIRM = if_else(!is.na(DATE_CONFIRM), glue("{DATE_CONFIRM} 00:00:00"), NA_character_),
      DATE_RELEASE = as.character(DATE_RELEASE),
   ) %>%
   select(
      REC_ID,
      FACI_ID,
      SUB_FACI_ID,
      CONFIRM_TYPE,
      CONFIRM_CODE = LABCODE,
      SOURCE       = SOURCE_FACI,
      SUB_SOURCE   = SOURCE_SUB_FACI,
      FINAL_RESULT,
      REMARKS,
      SIGNATORY_1,
      SIGNATORY_2,
      SIGNATORY_3,
      DATE_CONFIRM,
      DATE_RELEASE,
      CREATED_AT,
      CREATED_BY
   ) %>%
   distinct(REC_ID, .keep_all = TRUE)

px_record <- import %>%
   mutate(
      UPDATED_AT = TIMESTAMP,
      UPDATED_BY = "1300000001",
   ) %>%
   select(
      REC_ID,
      PATIENT_ID,
      UPDATED_AT,
      UPDATED_BY
   ) %>%
   distinct(REC_ID, .keep_all = TRUE)

px_test_31 <- import %>%
   mutate(
      TEST_TYPE     = "31",
      TEST_NUM      = "1",
      FACI_ID       = "130023",
      SUB_FACI_ID   = "130023_001",
      CREATED_AT    = TIMESTAMP,
      CREATED_BY    = "1300000001",
      SPECIMEN_TYPE = case_when(
         SPECIMEN_TYPE == "SERUM" ~ "1"
      ),
      RESULT        = substr(FINAL_RESULT_31, 1, 1)
   ) %>%
   select(
      REC_ID,
      FACI_ID,
      SUB_FACI_ID,
      TEST_TYPE,
      TEST_NUM,
      DATE_PERFORM = T1_DATE,
      RESULT,
      CREATED_AT,
      CREATED_BY
   ) %>%
   distinct(REC_ID, .keep_all = TRUE)

px_test_hiv_31 <- import %>%
   mutate(
      TEST_TYPE     = "31",
      TEST_NUM      = "1",
      FACI_ID       = "130023",
      SUB_FACI_ID   = "130023_001",
      CREATED_AT    = TIMESTAMP,
      CREATED_BY    = "1300000001",
      SPECIMEN_TYPE = case_when(
         SPECIMEN_TYPE == "SERUM" ~ "1"
      ),
   ) %>%
   select(
      REC_ID,
      FACI_ID,
      SUB_FACI_ID,
      TEST_TYPE,
      TEST_NUM,
      SPECIMEN_TYPE,
      DATE_RECEIVE = SPECIMEN_RECEIPT_DATE,
      KIT_NAME     = KIT_31,
      LOT_NO       = T1_LOT_NO,
      FINAL_RESULT = FINAL_RESULT_31,
      CREATED_AT,
      CREATED_BY
   )

px_test_32 <- import %>%
   mutate(
      TEST_TYPE     = "32",
      TEST_NUM      = "1",
      FACI_ID       = "130023",
      SUB_FACI_ID   = "130023_001",
      CREATED_AT    = TIMESTAMP,
      CREATED_BY    = "1300000001",
      SPECIMEN_TYPE = case_when(
         SPECIMEN_TYPE == "SERUM" ~ "1"
      ),
      RESULT        = substr(FINAL_RESULT_32, 1, 1)
   ) %>%
   select(
      REC_ID,
      FACI_ID,
      SUB_FACI_ID,
      TEST_TYPE,
      TEST_NUM,
      DATE_PERFORM = T2_DATE,
      RESULT,
      CREATED_AT,
      CREATED_BY
   ) %>%
   distinct(REC_ID, .keep_all = TRUE)

px_test_hiv_32 <- import %>%
   mutate(
      TEST_TYPE     = "32",
      TEST_NUM      = "1",
      FACI_ID       = "130023",
      SUB_FACI_ID   = "130023_001",
      CREATED_AT    = TIMESTAMP,
      CREATED_BY    = "1300000001",
      SPECIMEN_TYPE = case_when(
         SPECIMEN_TYPE == "SERUM" ~ "1"
      ),
   ) %>%
   select(
      REC_ID,
      FACI_ID,
      SUB_FACI_ID,
      TEST_TYPE,
      TEST_NUM,
      SPECIMEN_TYPE,
      # DATE_RECEIVE = SPECIMEN_RECEIPT_DATE,
      KIT_NAME     = KIT_32,
      LOT_NO       = T2_LOT_NO,
      FINAL_RESULT = FINAL_RESULT_32,
      CREATED_AT,
      CREATED_BY
   )

px_test_33 <- import %>%
   mutate(
      TEST_TYPE     = "33",
      TEST_NUM      = "1",
      FACI_ID       = "130023",
      SUB_FACI_ID   = "130023_001",
      CREATED_AT    = TIMESTAMP,
      CREATED_BY    = "1300000001",
      SPECIMEN_TYPE = case_when(
         SPECIMEN_TYPE == "SERUM" ~ "1"
      ),
      RESULT        = substr(FINAL_RESULT_33, 1, 1)
   ) %>%
   select(
      REC_ID,
      FACI_ID,
      SUB_FACI_ID,
      TEST_TYPE,
      TEST_NUM,
      DATE_PERFORM = T3_DATE,
      RESULT,
      CREATED_AT,
      CREATED_BY
   ) %>%
   distinct(REC_ID, .keep_all = TRUE)

px_test_hiv_33 <- import %>%
   mutate(
      TEST_TYPE     = "33",
      TEST_NUM      = "1",
      FACI_ID       = "130023",
      SUB_FACI_ID   = "130023_001",
      CREATED_AT    = TIMESTAMP,
      CREATED_BY    = "1300000001",
      SPECIMEN_TYPE = case_when(
         SPECIMEN_TYPE == "SERUM" ~ "1"
      ),
   ) %>%
   select(
      REC_ID,
      FACI_ID,
      SUB_FACI_ID,
      TEST_TYPE,
      TEST_NUM,
      SPECIMEN_TYPE,
      # DATE_RECEIVE = SPECIMEN_RECEIPT_DATE,
      KIT_NAME     = KIT_33,
      LOT_NO       = T3_LOT_NO,
      FINAL_RESULT = FINAL_RESULT_33,
      CREATED_AT,
      CREATED_BY
   )

db_conn     <- ohasis$conn("db")
table_space <- Id(schema = "ohasis_interim", table = "px_confirm")
dbxUpsert(
   db_conn,
   table_space,
   px_confirm,
   "REC_ID"
)
table_space <- Id(schema = "ohasis_interim", table = "px_record")
dbxUpsert(
   db_conn,
   table_space,
   px_record,
   c("REC_ID", "PATIENT_ID")
)
table_space <- Id(schema = "ohasis_interim", table = "px_test_hiv")
dbxUpsert(
   db_conn,
   table_space,
   px_test_hiv_31,
   c("REC_ID", "TEST_TYPE", "TEST_NUM")
)
dbxUpsert(
   db_conn,
   table_space,
   px_test_hiv_32,
   c("REC_ID", "TEST_TYPE", "TEST_NUM")
)
dbxUpsert(
   db_conn,
   table_space,
   px_test_hiv_33,
   c("REC_ID", "TEST_TYPE", "TEST_NUM")
)
table_space <- Id(schema = "ohasis_interim", table = "px_test")
dbxUpsert(
   db_conn,
   table_space,
   px_test_31,
   c("REC_ID", "TEST_TYPE", "TEST_NUM")
)
dbxUpsert(
   db_conn,
   table_space,
   px_test_32,
   c("REC_ID", "TEST_TYPE", "TEST_NUM")
)
dbxUpsert(
   db_conn,
   table_space,
   px_test_33,
   c("REC_ID", "TEST_TYPE", "TEST_NUM")
)