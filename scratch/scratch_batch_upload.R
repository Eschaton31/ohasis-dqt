con        <- ohasis$conn("lw")
ly_clients <- QB$new(con)$from("ohasis_lake.ly_clients")$whereNull("CENTRAL_ID")$get()
dbDisconnect(con)

new <- ly_clients %>%
   mutate(
      BIRTHDATE = as.character(BIRTHDATE),
      BIRTHDATE = if_else(
         str_length(UIC) == 14 & is.na(BIRTHDATE),
         stri_c(sep = "-", StrRight(UIC, 4), substr(UIC, 7, 8), substr(UIC, 9, 10)),
         BIRTHDATE,
         BIRTHDATE
      ),
      drop      = case_when(
         BRANCH == "SELF CARE" & is.na(BIRTHDATE) ~ 1,
         BRANCH == "SELF CARE" & is.na(FIRST) & is.na(LAST) ~ 1,
         TRUE ~ 0
      )
   ) %>%
   filter(drop == 0) %>%
   filter(if_all(c(FIRST, LAST, BIRTHDATE), ~!is.na(.))) %>%
   mutate(
      REC_ID      = NA_character_,
      PATIENT_ID  = NA_character_,
      CREATED_BY  = "1300000048",
      CREATED_AT  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      FACI_ID     = "130001",
      SUB_FACI_ID = NA_character_
   )

##  PrEPPY ---------------------------------------------------------------------

preppy <- read_rds("H:/20240801_ly-preppy.rds")
new <- preppy %>%
   select(-any_of("row_id")) %>%
   distinct() %>%
   mutate(
      row_id = row_number(),
      BIRTHDATE = as.character(BIRTHDATE_VALUE),
      BIRTHDATE = if_else(
         str_length(UIC) == 14 & is.na(BIRTHDATE),
         stri_c(sep = "-", StrRight(UIC, 4), substr(UIC, 7, 8), substr(UIC, 9, 10)),
         BIRTHDATE,
         BIRTHDATE
      ),
      drop      = case_when(
         BRANCH == "SELF CARE" & is.na(BIRTHDATE) ~ 1,
         BRANCH == "SELF CARE" & is.na(FIRST) & is.na(LAST) ~ 1,
         TRUE ~ 0
      )
   ) %>%
   filter(drop == 0) %>%
   filter(if_all(c(FIRST, LAST, BIRTHDATE), ~!is.na(.))) %>%
   mutate(
      REC_ID      = NA_character_,
      PATIENT_ID  = NA_character_,
      CREATED_BY  = "1300000048",
      CREATED_AT  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      FACI_ID     = "130001",
      SUB_FACI_ID = NA_character_
   )

new %<>%
   filter(!is.na(PATIENT_ID)) %>%
   bind_rows(
      batch_px_ids(new %>% filter(is.na(PATIENT_ID)), PATIENT_ID, FACI_ID, "row_id")
   )

new %<>%
   filter(!is.na(REC_ID)) %>%
   bind_rows(
      batch_rec_ids(new %>% filter(is.na(REC_ID)), REC_ID, CREATED_BY, "row_id")
   )

tables           <- list()
tables$px_record <- list(
   name = "px_record",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = new %>%
      mutate(
         RECORD_DATE = format(Sys.time(), "%Y-%m-%d"),
         DISEASE     = "101000",
         MODULE      = "0",
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
      )
)

tables$px_info <- list(
   name = "px_info",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = new %>%
      mutate(
         SEX = NA_character_,
         SEX = case_when(
            SEX == "1_Male" ~ "1",
            SEX == "MALE" ~ "1",
            SEX == "FEMALE" ~ "2",
         )
      ) %>%
      select(
         REC_ID,
         PATIENT_ID,
         # CONFIRMATORY_CODE,
         UIC,
         PATIENT_CODE = CLIENT_CODE,
         SEX,
         BIRTHDATE,
         CREATED_BY,
         CREATED_AT,
      )
)

tables$px_name <- list(
   name = "px_name",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = new %>%
      select(
         REC_ID,
         PATIENT_ID,
         FIRST,
         MIDDLE,
         LAST,
         CREATED_BY,
         CREATED_AT,
      )
)

tables$px_contact <- list(
   name = "px_contact",
   pk   = c("REC_ID", "CONTACT_TYPE"),
   data = new %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         CLIENT_MOBILE,
         CLIENT_EMAIL
      ) %>%
      pivot_longer(
         cols      = c(CLIENT_MOBILE, CLIENT_EMAIL),
         names_to  = "CONTACT_TYPE",
         values_to = "CONTACT"
      ) %>%
      mutate(
         CONTACT_TYPE = case_when(
            CONTACT_TYPE == "CLIENT_MOBILE" ~ "1",
            CONTACT_TYPE == "CLIENT_EMAIL" ~ "2",
            TRUE ~ CONTACT_TYPE
         )
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

con <- ohasis$conn("lw")
dbxUpsert(con, Id(schema = "ohasis_lake", table = "ly_clients"), new %>% select(row_id, CENTRAL_ID = PATIENT_ID), "row_id")
dbDisconnect(con)
