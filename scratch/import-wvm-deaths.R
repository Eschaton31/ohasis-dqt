raw <- read_sheet("1eeWxxu6Ut7Qs9Cu_rIA9J-zVU6FZv8jmsYIMjaYZpYw", "clean-bene")

clean <- raw %>%
   rename(
      PATIENT_ID = CENTRAL_ID
   ) %>%
   mutate(
      row_id      = row_number(),
      RECORD_DATE = format(Sys.time(), "%Y-%m-%d"),
      FACI_ID     = '060007',
      SUB_FACI_ID = NA_character_,
      DISEASE     = '*',
      MODULE      = '4',
      SEX         = case_when(
         SEX == "M" ~ "1",
         SEX == "F" ~ "2",
      ),
      REC_ID      = NA_character_,
      CREATED_BY  = '1300000048',
      CREATED_AT  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
   )

clean %<>%
   filter(!is.na(PATIENT_ID)) %>%
   bind_rows(
      batch_px_ids(clean %>% filter(is.na(PATIENT_ID)), PATIENT_ID, FACI_ID, "row_id")
   )

clean %<>%
   filter(!is.na(REC_ID)) %>%
   bind_rows(
      batch_rec_ids(clean %>% filter(is.na(REC_ID)), REC_ID, CREATED_BY, "row_id")
   )


tables           <- list()
tables$px_record <- list(
   name = "px_record",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = clean %>%
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
      )
)

tables$px_info <- list(
   name = "px_info",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = clean %>%
      select(
         REC_ID,
         PATIENT_ID,
         CONFIRMATORY_CODE,
         UIC,
         PATIENT_CODE,
         PHILHEALTH_NO,
         SEX,
         BIRTHDATE,
         CREATED_BY,
         CREATED_AT,
      )
)

tables$px_name <- list(
   name = "px_name",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = clean %>%
      select(
         REC_ID,
         PATIENT_ID,
         FIRST,
         CREATED_BY,
         CREATED_AT,
      )
)

tables$px_addr <- list(
   name = "px_addr",
   pk   = c("REC_ID", "ADDR_TYPE"),
   data = clean %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         starts_with("CURR")
      ) %>%
      pivot_longer(
         cols      = c(
            ends_with("_REG"),
            ends_with("_PROV"),
            ends_with("_MUNC"),
            ends_with("_ADDR")
         ),
         names_to  = "ADDR_DATA",
         values_to = "ADDR_VALUE"
      ) %>%
      separate(
         col  = "ADDR_DATA",
         into = c("ADDR_TYPE", "PIECE")
      ) %>%
      mutate(
         ADDR_TYPE = case_when(
            ADDR_TYPE == "CURR" ~ "1",
            ADDR_TYPE == "PERM" ~ "2",
            ADDR_TYPE == "BIRTH" ~ "3",
            ADDR_TYPE == "DEATH" ~ "4",
            ADDR_TYPE == "SERVICE" ~ "5",
            TRUE ~ ADDR_TYPE
         ),
         PIECE     = case_when(
            PIECE == "ADDR" ~ "TEXT",
            TRUE ~ PIECE
         )
      ) %>%
      pivot_wider(
         id_cols      = c(REC_ID, CREATED_BY, CREATED_AT, ADDR_TYPE),
         names_from   = PIECE,
         values_from  = ADDR_VALUE,
         names_prefix = "ADDR_"
      ) %>%
      select(
         REC_ID,
         starts_with("ADDR_"),
         CREATED_BY,
         CREATED_AT,
      )
)

tables$px_faci <- list(
   name = "px_faci",
   pk   = c("REC_ID", "SERVICE_TYPE"),
   data = clean %>%
      mutate(
         SERVICE_TYPE = "*00001"
      ) %>%
      select(
         REC_ID,
         FACI_ID,
         SUB_FACI_ID,
         SERVICE_TYPE,
         CREATED_BY,
         CREATED_AT,
      )
)

tables$px_form <- list(
   name = "px_form",
   pk   = c("REC_ID", "FORM"),
   data = clean %>%
      mutate(
         FORM    = "Form D",
         VERSION = "2017"
      ) %>%
      select(
         REC_ID,
         FORM,
         VERSION,
         CREATED_BY,
         CREATED_AT,
      )
)

tables$px_remarks <- list(
   name = "px_remarks",
   pk   = c("REC_ID", "REMARK_TYPE"),
   data = clean %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         REPORT_NOTES,
      ) %>%
      pivot_longer(
         cols      = c(
            REPORT_NOTES,
         ),
         names_to  = "REMARK_TYPE",
         values_to = "REMARKS"
      ) %>%
      mutate(
         REMARK_TYPE = case_when(
            REMARK_TYPE == "CLINIC_NOTES" ~ "1",
            REMARK_TYPE == "COUNSEL_NOTES" ~ "2",
            REMARK_TYPE == "REPORT_NOTES" ~ "3",
            REMARK_TYPE == "SYMPTOMS" ~ "10",
            TRUE ~ REMARK_TYPE
         ),
      )
)

tables$px_death <- list(
   name = "px_death",
   pk   = c("REC_ID", "DEATH_INFO", "INFO_NUM"),
   data = clean %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         DEATH_DATE,
      ) %>%
      mutate(
         EB_VALIDATED = "1",
         REPORT_BY    = "3",
      ) %>%
      pivot_longer(
         cols      = c(
            DEATH_DATE,
            EB_VALIDATED,
            REPORT_BY
         ),
         names_to  = "INFO_NUM",
         values_to = "INFO_TEXT"
      ) %>%
      mutate(
         DEATH_INFO = "0",
         INFO_NUM   = case_when(
            INFO_NUM == "DEATH_DATE" ~ "1",
            INFO_NUM == "EB_VALIDATED" ~ "2",
            INFO_NUM == "REPORT_BY" ~ "3",
            TRUE ~ INFO_NUM
         ),
      )
)

db_conn <- connect("ohasis-live")
lapply(tables, function(ref, db_conn) {
   log_info("Uploading {green(ref$name)}.")
   table_space <- Id(schema = "ohasis_interim", table = ref$name)
   dbxUpsert(db_conn, table_space, ref$data, ref$pk)
   # dbExecute(db_conn, glue("DELETE FROM ohasis_interim.{ref$name} WHERE REC_ID IN (?)"), params = list(unique(ref$data$REC_ID)))
}, db_conn)
dbDisconnect(db_conn)


clean %>%
   format_stata() %>%
   write_dta("H:/20250325_cleaned-wvmc-mortality-import.dta")

clean %>%
   write_rds("H:/20250325_cleaned-wvmc-mortality-import.rds")

tables %>%
   write_rds("H:/20250325_tables-wvmc-mortality-import.rds")
