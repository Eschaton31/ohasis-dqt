con <- ohasis$conn("lw")
art <- QB$new(con)$from("ohasis_warehouse.form_art_bc AS pii")$
   leftJoin("ohasis_warehouse.id_registry as id", "pii.PATIENT_ID", "=", "id.PATIENT_ID")$
   where("FACI_ID", "100004")$
   select("pii.FIRST", "pii.LAST", "pii.UIC", "pii.BIRTHDATE", "pii.CLIENT_MOBILE")$
   selectRaw("COALESCE(id.CENTRAL_ID, pii.PATIENT_ID) AS CENTRAL_ID")$
   distinct()$
   get() %>%
   mutate_if(
      .predicate = is.character,
      ~toupper(str_squish(.))
   )
dbDisconnect(con)


nmm_addr   <- read_excel("C:/Users/johnb/Downloads/NMM_Addr.xlsx", col_types = "text")
nmm_file   <- "C:/Users/johnb/Downloads/REFILL ARV APR-MAY 2024.ods"
nmm_sheets <- ods_sheets(nmm_file)
nmm        <- lapply(nmm_sheets, read_ods, path = nmm_file, col_types = cols(.default = "c")) %>%
   bind_rows() %>%
   select(
      RECORD_DATE          = 1,
      CAT                  = 2,
      FIRST                = 3,
      LAST                 = 4,
      ADDRESS              = 7,
      CLIENT_MOBILE        = 8,
      IPT_STATUS           = 9,
      PHIC_EXIST           = 10,
      ARV                  = 11,
      MEDICINE_LEFT        = 12,
      MEDICINE_MISSED      = 13,
      REMARKS              = 14,
      DISP_MONTHS          = 15,
      DATE_ESTIMATE_RETURN = 16,
      D_B                  = 17,
      DISP_IPT             = 18,
      DISP_CPT             = 19,
      DISP_APT             = 20,
      DISP_FLUCO           = 21,
      DISP_VALGAN          = 22,
      VACCINES             = 23,
      INJECTED_BY          = 24,
      # TB_TPT_OUTCOME       = 25,
   ) %>%
   mutate_all(
      ~toupper(str_squish(.))
   ) %>%
   mutate(
      RECORD_DATE       = as.Date(parse_date_time(RECORD_DATE, c("Ymd", "mdY"))),
      RECORD_DATE       = na_if(RECORD_DATE, as.Date("0001-11-20")),
      CLIENT_MOBILE     = str_replace_all(CLIENT_MOBILE, "[^[:digit:]]", ""),
      ADDRESS           = stri_trans_general(ADDRESS, "latin-ascii"),

      TB_TPT_OUTCOME    = NA_character_,
      TB_TPT_TX_OUTCOME = coalesce(IPT_STATUS, TB_TPT_OUTCOME),
      TB_STATUS         = case_when(
         str_detect(TB_TPT_TX_OUTCOME, "TB-") ~ "1",
         str_detect(TB_TPT_TX_OUTCOME, "IPT-") ~ "0",
         TB_TPT_TX_OUTCOME == "N/A" ~ NA_character_,
         TRUE ~ NA_character_
      ),

      TB_IPT_STATUS     = case_when(
         TB_TPT_TX_OUTCOME == "IPT-O" ~ "11",
         TB_TPT_TX_OUTCOME == "IPT-C" ~ "13",
         TRUE ~ NA_character_
      ),
      TB_IPT_OUTCOME    = case_when(
         TB_TPT_TX_OUTCOME == "IPT-C" ~ "1",
         TRUE ~ NA_character_
      ),

      TB_TX_STATUS      = case_when(
         TB_TPT_TX_OUTCOME == "TB-O" ~ "11",
         TB_TPT_TX_OUTCOME == "TB-C" ~ "13",
         TRUE ~ NA_character_
      ),
   ) %>%
   select(-IPT_STATUS, -TB_TPT_OUTCOME, -TB_TPT_TX_OUTCOME) %>%
   mutate(
      keep = case_when(
         ARV == "NONE" ~ 0,
         CAT == "PREP" ~ 0,
         DISP_MONTHS == "NONE" ~ 0,
         TRUE ~ 1
      )
   ) %>%
   filter(keep == 1) %>%
   left_join(nmm_addr) %>%
   left_join(art %>% distinct(FIRST, LAST, CENTRAL_ID))

## final import
ss        <- "16ci-cPFm8oym0mMse92nBC54fdRL9lOkSAiTh-t8alk"
ss        <- "1PlcXdGNiKJF9TQO3e-79hlghVU33s5FLNMhItk-XQs8"
nmm       <- read_sheet(ss, col_types = "c")
nmm_clean <- nmm %>%
   filter(is.na(CENTRAL_ID)) %>%
   select(-CENTRAL_ID) %>%
   left_join(
      y  = nmm %>%
         filter(!is.na(CENTRAL_ID)) %>%
         distinct(FIRST, LAST, CENTRAL_ID),
      by = join_by(FIRST, LAST)
   ) %>%
   bind_rows(nmm %>% filter(!is.na(CENTRAL_ID))) %>%
   relocate(CENTRAL_ID, .before = 1)

write_sheet(nmm_clean %>% filter(!is.na(CENTRAL_ID)), ss, "with_cid")
write_sheet(nmm_clean %>% filter(is.na(CENTRAL_ID)), ss, "no_cid")


min <- min(nmm_matched$RECORD_DATE, na.rm = TRUE)
max <- max(nmm_matched$RECORD_DATE, na.rm = TRUE)

conn        <- ohasis$conn("lw")
id_reg      <- QB$new(conn)$from("ohasis_warehouse.id_registry")$select(CENTRAL_ID, PATIENT_ID)$get()
form_art_bc <- QB$new(conn)$
   from("ohasis_warehouse.form_art_bc AS art")$
   leftJoin("ohasis_warehouse.id_registry AS reg", "art.PATIENT_ID", "=", "reg.PATIENT_ID")$
   whereBetween("art.VISIT_DATE", c(as.character(min), as.character(max)))$
   whereNotNull("MEDICINE_SUMMARY")$
   where("FACI_ID", "100004")$
   select("VISIT_DATE")$
   selectRaw("COALESCE(reg.CENTRAL_ID, art.PATIENT_ID) AS CENTRAL_ID")$
   get()
dbDisconnect(conn)

nmm_matched <- read_sheet(ss, "with_cid")
import      <- nmm_matched %>%
   filter(!is.na(RECORD_DATE)) %>%
   mutate(
      RECORD_DATE = as.Date(parse_date_time(RECORD_DATE, c("Ymd", "mdY"))),
      RECORD_DATE = na_if(RECORD_DATE, as.Date("0001-11-20")),
   ) %>%
   rename(PATIENT_ID = CENTRAL_ID) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   anti_join(
      y  = form_art_bc,
      by = join_by(CENTRAL_ID, RECORD_DATE == VISIT_DATE)
   ) %>%
   mutate(
      row_id      = row_number(),
      FACI_ID     = "100004",
      SUB_FACI_ID = NA_character_,
      REC_ID      = NA_character_,
      CREATED_BY  = "1300000048",
      CREATED_AT  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      MODULE      = 3,
      DISEASE     = '101000',
   )

nmm_matched <- read_sheet(ss, "no_cid", col_types = "c")
import      <- nmm_matched %>%
   filter(!is.na(RECORD_DATE)) %>%
   mutate(
      CENTRAL_ID  = NA_character_,
      PATIENT_ID  = NA_character_,
      RECORD_DATE = as.Date(parse_date_time(RECORD_DATE, c("Ymd", "mdY"))),
      RECORD_DATE = na_if(RECORD_DATE, as.Date("0001-11-20")),
   ) %>%
   anti_join(
      y  = form_art_bc,
      by = join_by(CENTRAL_ID, RECORD_DATE == VISIT_DATE)
   ) %>%
   mutate(
      row_id      = row_number(),
      FACI_ID     = "100004",
      SUB_FACI_ID = NA_character_,
      REC_ID      = NA_character_,
      CREATED_BY  = "1300000048",
      CREATED_AT  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      MODULE      = 3,
      DISEASE     = '101000',
   ) %>%
   filter(!is.na(RECORD_DATE)) %>%
   mutate(
      keep = case_when(
         ARV == "NONE" ~ 0,
         CAT == "PREP" ~ 0,
         DISP_MONTHS == "NONE" ~ 0,
         is.na(UIC) ~ 0,
         TRUE ~ 1
      )
   ) %>%
   filter(keep == 1)

import %<>%
   filter(!is.na(PATIENT_ID)) %>%
   bind_rows(
      batch_px_ids(import %>% filter(is.na(PATIENT_ID)), PATIENT_ID, FACI_ID, "row_id")
   )

import %<>%
   filter(!is.na(REC_ID)) %>%
   bind_rows(
      batch_rec_ids(import %>% filter(is.na(REC_ID)), REC_ID, CREATED_BY, "row_id")
   )

tables           <- list()
tables$px_record <- list(
   name = "px_record",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = import %>%
      select(
         REC_ID,
         PATIENT_ID,
         # PATIENT_ID = CENTRAL_ID,
         FACI_ID,
         SUB_FACI_ID,
         RECORD_DATE,
         DISEASE,
         MODULE,
         CREATED_BY,
         CREATED_AT,
      )
)

tables$px_name <- list(
   name = "px_name",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = import %>%
      select(
         REC_ID,
         PATIENT_ID,
         # PATIENT_ID = CENTRAL_ID,
         FIRST,
         LAST,
         CREATED_BY,
         CREATED_AT,
      )
)

tables$px_info <- list(
   name = "px_info",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = import %>%
      select(
         REC_ID,
         PATIENT_ID,
         # PATIENT_ID = CENTRAL_ID,
         CONFIRMATORY_CODE,
         UIC,
         SEX,
         BIRTHDATE,
         CREATED_BY,
         CREATED_AT,
      ) %>%
      mutate(
         BIRTHDATE = as.Date(parse_date_time(BIRTHDATE, c("Ymd", "mdY"))),
         SEX       = case_when(
            SEX == "M" ~ "1",
            SEX == "F" ~ "2",
         )
      )
)

tables$px_form <- list(
   name = "px_form",
   pk   = c("REC_ID", "FORM"),
   data = import %>%
      mutate(
         FORM    = 'ART Form',
         VERSION = 2021,
      ) %>%
      select(
         REC_ID,
         FORM,
         VERSION,
         CREATED_BY,
         CREATED_AT,
      )
)

tables$px_tb <- list(
   name = "px_tb",
   pk   = "REC_ID",
   data = import %>%
      select(
         REC_ID,
         TB_STATUS,
         CREATED_BY,
         CREATED_AT,
      )
)

tables$px_tb_active <- list(
   name = "px_tb_active",
   pk   = "REC_ID",
   data = import %>%
      select(
         REC_ID,
         TB_TX_STATUS,
         CREATED_BY,
         CREATED_AT,
      )
)

tables$px_tb_ipt <- list(
   name = "px_tb_ipt",
   pk   = "REC_ID",
   data = import %>%
      select(
         REC_ID,
         TB_IPT_STATUS,
         TB_IPT_OUTCOME,
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
         DISP_DATE = RECORD_DATE,
         MEDICINE  = ARV,
         DISP_MONTHS,
         MEDICINE_LEFT,
         MEDICINE_MISSED
      ) %>%
      mutate(
         MEDICINE = str_replace_all(MEDICINE, "LP/R", "LPVR"),
         MEDICINE = case_when(
            MEDICINE == "AZT+ABC+EFV" ~ "AZT/ABC/EFV",
            MEDICINE == "N/A" ~ NA_character_,
            TRUE ~ MEDICINE
         )
      ) %>%
      filter(!is.na(MEDICINE)) %>%
      separate_longer_delim(
         cols  = MEDICINE,
         delim = "/"
      ) %>%
      mutate(
         PER_DAY       = case_when(
            MEDICINE == "TLD" ~ 1,
            MEDICINE == "LTE" ~ 1,
            MEDICINE == "AZT+3TC" ~ 2,
            MEDICINE == "AZT" ~ 2,
            MEDICINE == "LPVR" ~ 4,
            MEDICINE == "LPVR (B)" ~ 4,
            MEDICINE == "DTG" ~ 1,
            MEDICINE == "DTG (B)" ~ 1,
            MEDICINE == "3TC" ~ 2,
            MEDICINE == "3TC SYR" ~ 2,
            MEDICINE == "ABC" ~ 2,
            MEDICINE == "EFV" ~ 1,
            MEDICINE == "EFV 200" ~ 1,
            MEDICINE == "EFV 600" ~ 1,
            MEDICINE == "TDF" ~ 1,
            MEDICINE == "TDF+3TC" ~ 1,
            MEDICINE == "3TC+TDF" ~ 1,
            TRUE ~ 1
         ),
         MEDICINE      = case_when(
            MEDICINE == "TLD" ~ "2029",
            MEDICINE == "LTE" ~ "2015",
            MEDICINE == "AZT+3TC" ~ "2018",
            MEDICINE == "AZT" ~ "2019",
            MEDICINE == "LPVR" ~ "2009",
            MEDICINE == "LPVR (B)" ~ "2010",
            MEDICINE == "DTG" ~ "2035",
            MEDICINE == "DTG (B)" ~ "2035",
            MEDICINE == "3TC" ~ "2026",
            MEDICINE == "3TC SYR" ~ "2008",
            MEDICINE == "ABC" ~ "2002",
            MEDICINE == "EFV" ~ "2004",
            MEDICINE == "EFV 200" ~ "2005",
            MEDICINE == "EFV 600" ~ "2004",
            MEDICINE == "TDF" ~ "2034",
            MEDICINE == "TDF+3TC" ~ "2016",
            MEDICINE == "3TC+TDF" ~ "2016",
            TRUE ~ MEDICINE
         ),
         UNIT_BASIS    = "2",

         DISP_TOTAL    = case_when(
            str_detect(DISP_MONTHS, "MONTH") ~ parse_number(DISP_MONTHS) * 30,
            str_detect(DISP_MONTHS, "DAYS") ~ parse_number(DISP_MONTHS)
         ),
         DISP_TOTAL    = DISP_TOTAL * PER_DAY,

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

