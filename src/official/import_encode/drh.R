drh <- read_sheet("1gaI9FzcE5xvBDEEeTbtEfenpO62HLNBpKKpe7JjlGkk", "Jun 2024", col_types = "c")

conn        <- ohasis$conn("lw")
form_art_bc <- QB$new(conn)$
   from("ohasis_warehouse.form_art_bc AS art")$
   leftJoin("ohasis_warehouse.id_registry AS reg", "art.PATIENT_ID", "=", "reg.PATIENT_ID")$
   whereBetween("art.VISIT_DATE", c("2024-06-01", "2024-06-30"))$
   whereNotNull("MEDICINE_SUMMARY")$
   where("FACI_ID", "110005")$
   select("VISIT_DATE")$
   selectRaw("COALESCE(reg.CENTRAL_ID, art.PATIENT_ID) AS CENTRAL_ID")$
   get()
dbDisconnect(conn)

import <- drh %>%
   filter(!is.na(CENTRAL_ID)) %>%
   mutate(
      DISP_DATE = as.Date(DISP_DATE)
   ) %>%
   anti_join(
      y  = form_art_bc,
      by = join_by(CENTRAL_ID, DISP_DATE == VISIT_DATE)
   ) %>%
   select(PATIENT_ID = CENTRAL_ID, RECORD_DATE = DISP_DATE, PATIENT_CODE, UIC) %>%
   distinct(PATIENT_ID, RECORD_DATE, .keep_all = TRUE) %>%
   mutate(
      row_id      = row_number(),
      FACI_ID     = "110005",
      SUB_FACI_ID = NA_character_,
      REC_ID      = NA_character_,
      CREATED_BY  = "1300000048",
      CREATED_AT  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      MODULE      = 3,
      DISEASE     = '101000',
   )

import %<>%
   filter(!is.na(REC_ID)) %>%
   bind_rows(
      batch_rec_ids(import %>% filter(is.na(REC_ID)), REC_ID, CREATED_BY, "row_id")
   )

tables             <- list()
tables$px_record   <- list(
   name = "px_record",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = import %>%
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
tables$px_info     <- list(
   name = "px_info",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = import %>%
      select(
         REC_ID,
         PATIENT_ID,
         UIC,
         PATIENT_CODE,
         CREATED_BY,
         CREATED_AT,
      )
)
tables$px_medicine <- list(
   name = "px_medicine",
   pk   = c("REC_ID", "MEDICINE", "DISP_NUM"),
   data = import %>%
      select(
         REC_ID,
         FACI_ID,
         SUB_FACI_ID,
         PATIENT_ID,
         DISP_DATE = RECORD_DATE
      ) %>%
      left_join(
         y  = drh %>%
            mutate(
               DISP_DATE = as.Date(DISP_DATE)
            ) %>%
            select(PATIENT_ID = CENTRAL_ID, DISP_DATE, DISP_TOTAL = TOTAL_DISP, MEDICINE, MEDICINE_LEFT),
         by = join_by(PATIENT_ID, DISP_DATE)
      ) %>%
      mutate(
         PER_DAY       = case_when(
            MEDICINE == "AZT/3TC" ~ 2,
            MEDICINE == "LPV/r" ~ 4,
            MEDICINE == "DTG" ~ 1,
            TRUE ~ 1
         ),
         MEDICINE      = case_when(
            MEDICINE == "TDF/3TC/DTG" ~ "2029",
            MEDICINE == "TDF/3TC/EFV" ~ "2015",
            MEDICINE == "AZT/3TC" ~ "2018",
            MEDICINE == "LPV/r" ~ "2009",
            MEDICINE == "DTG" ~ "2035",
            MEDICINE == "EFV" ~ "2004",
         ),
         UNIT_BASIS    = "2",

         NEXT_DATE     = DISP_DATE %m+% days(as.numeric(DISP_TOTAL) / PER_DAY),
         MEDICINE_LEFT = as.numeric(MEDICINE_LEFT),
      ) %>%
      group_by(REC_ID) %>%
      mutate(
         DISP_NUM = row_number(),
      ) %>%
      ungroup() %>%
      select(
         REC_ID,
         FACI_ID,
         SUB_FACI_ID,
         MEDICINE,
         DISP_NUM,
         UNIT_BASIS,
         PER_DAY,
         DISP_TOTAL,
         MEDICINE_LEFT,
         DISP_DATE,
         NEXT_DATE,
      )
)

db_conn <- ohasis$conn("db")
dbxDelete(
   db_conn,
   Id(schema = "ohasis_interim", table = "px_medicine"),
   import %>% select(REC_ID),
   batch_size = 1000
)
lapply(tables, function(ref, db_conn) {
   log_info("Uploading {green(ref$name)}.")
   table_space <- Id(schema = "ohasis_interim", table = ref$name)
   dbxUpsert(db_conn, table_space, ref$data, ref$pk)
   # dbExecute(db_conn, glue("DELETE FROM ohasis_interim.{ref$name} WHERE REC_ID IN (?)"), params = list(unique(ref$data$REC_ID)))
}, db_conn)
dbDisconnect(db_conn)

