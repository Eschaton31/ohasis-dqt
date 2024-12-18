##  Download encoding documentation --------------------------------------------

import             <- new.env()
import$encoding_ss <- as_id("18hh6GZzjnNBidMg9sxOworj2IhbwTUak")

local(envir = import, {

   download_confirm <- function() {
      db_conn    <- ohasis$conn("db")
      px_confirm <- dbTable2(db_conn, "ohasis_interim", "px_confirm", cols = c(REC_ID, CONFIRM_CODE))
      dbDisconnect(db_conn)

      return(px_confirm)
   }

   download_ei <- function() {
      encode_mo     <- as_id(drive_ls(import$encoding_ss, pattern = ohasis$ym)$id)
      encode_surv   <- as_id(drive_ls(encode_mo, pattern = "REGISTRY")$id)
      encode_sheets <- drive_ls(encode_surv, pattern = ohasis$ym)
      encode_data   <- lapply(seq_len(nrow(encode_sheets)), function(i) {
         ss   <- encode_sheets[i,]$id
         name <- encode_sheets[i,]$name
         data <- read_sheet(ss, "documentation") %>%
            mutate(
               ss      = ss,
               encoder = substr(name, 9, 1000)
            )
         return(data)
      })
      encode_data   <- bind_rows(encode_data) %>%
         filter(
            # Form %in% c("Form A", "HTS Form") | (is.na(Form) & nchar(`Page ID`) == 12),
            !is.na(`Record ID`)
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
            `Encoder`  = encoder,
            `Sheet ID` = ss
         )

      return(encode_data)
   }

   get_pdf_data <- function() {
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

      return(results)
   }

   match_encode_pdf <- function(encoded, results) {
      # match with pdf results
      match <- results %>%
         full_join(
            y          = encoded %>% mutate(only = 1),
            by         = c("LABCODE" = "Page ID"),
            na_matches = "never"
         ) %>%
         mutate(
            LABCODE = if_else(is.na(LABCODE), `Record ID`, LABCODE)
         ) %>%
         distinct(LABCODE, .keep_all = TRUE) %>%
         anti_join(
            y  = px_confirm %>% select(LABCODE = CONFIRM_CODE),
            by = "LABCODE"
         ) %>%
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

      .log_info("Encoded = {green(nrow(encoded))}")
      .log_info("PDFs = {green(nrow(results))}")
      .log_info("Matched = {green(nrow(match))}")
      .log_warn("PDF Only = {green(nrow(filter(match, is.na(FILENAME_PDF))))}")
      .log_warn("Encode Only = {green(nrow(filter(match, is.na(REC_ID))))}")
      .log_success("Total for import = {green(nrow(filter(match, !is.na(REC_ID), !is.na(FILENAME_PDF))))}")

      return(match)
   }

   prepare_import <- function(data, match) {
      TIMESTAMP <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

      import <- data %>%
         mutate_all(~str_squish(as.character(.))) %>%
         select(-FINAL_INTERPRETATION, -FINAL_RESULT) %>%
         inner_join(
            y          = match %>%
               filter(!is.na(REC_ID)),
            by         = "LABCODE",
            na_matches = "never"
         ) %>%
         distinct(LABCODE, .keep_all = TRUE) %>%
         filter(!is.na(REC_ID), nchar(REC_ID) == 25) %>%
         mutate(
            UPDATED_AT = TIMESTAMP,
            UPDATED_BY = Sys.getenv("OH_USER_ID"),
         )

      return(import)
   }

   generate_tables <- function(import) {
      tables            <- list()
      tables$px_confirm <- import %>%
         mutate(
            FACI_ID      = "130023",
            SUB_FACI_ID  = "130023_001",
            CONFIRM_TYPE = "1",
            CREATED_AT   = UPDATED_AT,
            CREATED_BY   = Sys.getenv("OH_USER_ID"),
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

      tables$px_test <- import %>%
         mutate(
            TEST_TYPE     = "31",
            TEST_NUM      = "1",
            FACI_ID       = "130023",
            SUB_FACI_ID   = "130023_001",
            CREATED_AT    = UPDATED_AT,
            CREATED_BY    = Sys.getenv("OH_USER_ID"),
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
         distinct(REC_ID, .keep_all = TRUE) %>%
         bind_rows(
            import %>%
               mutate(
                  TEST_TYPE     = "32",
                  TEST_NUM      = "1",
                  FACI_ID       = "130023",
                  SUB_FACI_ID   = "130023_001",
                  CREATED_AT    = UPDATED_AT,
                  CREATED_BY    = Sys.getenv("OH_USER_ID"),
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
         ) %>%
         bind_rows(
            import %>%
               mutate(
                  TEST_TYPE     = "33",
                  TEST_NUM      = "1",
                  FACI_ID       = "130023",
                  SUB_FACI_ID   = "130023_001",
                  CREATED_AT    = UPDATED_AT,
                  CREATED_BY    = Sys.getenv("OH_USER_ID"),
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
         )

      tables$px_test_hiv <- import %>%
         mutate(
            TEST_TYPE     = "31",
            TEST_NUM      = "1",
            FACI_ID       = "130023",
            SUB_FACI_ID   = "130023_001",
            CREATED_AT    = UPDATED_AT,
            CREATED_BY    = Sys.getenv("OH_USER_ID"),
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
         ) %>%
         bind_rows(
            import %>%
               mutate(
                  TEST_TYPE     = "32",
                  TEST_NUM      = "1",
                  FACI_ID       = "130023",
                  SUB_FACI_ID   = "130023_001",
                  CREATED_AT    = UPDATED_AT,
                  CREATED_BY    = Sys.getenv("OH_USER_ID"),
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
         ) %>%
         bind_rows(
            import %>%
               mutate(
                  TEST_TYPE     = "33",
                  TEST_NUM      = "1",
                  FACI_ID       = "130023",
                  SUB_FACI_ID   = "130023_001",
                  CREATED_AT    = UPDATED_AT,
                  CREATED_BY    = Sys.getenv("OH_USER_ID"),
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
         )

      return(tables)
   }

   import_data <- function(tables) {

      db_conn     <- ohasis$conn("db")
      table_space <- Id(schema = "ohasis_interim", table = "px_confirm")
      dbxUpsert(
         db_conn,
         table_space,
         tables[["px_confirm"]],
         "REC_ID"
      )
      table_space <- Id(schema = "ohasis_interim", table = "px_test_hiv")
      dbxUpsert(
         db_conn,
         table_space,
         tables[["px_test_hiv"]],
         c("REC_ID", "TEST_TYPE", "TEST_NUM")
      )
      table_space <- Id(schema = "ohasis_interim", table = "px_test")
      dbxUpsert(
         db_conn,
         table_space,
         tables[["px_test"]],
         c("REC_ID", "TEST_TYPE", "TEST_NUM")
      )
      dbDisconnect(db_conn)

      update_credentials(tables$px_confirm$REC_ID)
   }

   .init <- function() {
      p <- parent.env(environment())
      local(envir = p, {
         px_confirm <- download_confirm()
         encoded    <- download_ei()
         results    <- get_pdf_data()
         match      <- match_encode_pdf(encoded, results)

         for_import <- prepare_import(.GlobalEnv$nhsss$harp_dx$pdf_saccl$data, match)
         tables     <- generate_tables(for_import)
      })
   }

})
