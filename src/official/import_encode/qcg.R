ss     <- "1qBuif01DTlO-6HiFJB2qln2L5NYcIc_XopYmSyzxToE"
ml     <- read_sheet(ss, "masterlist", col_types = "c")
refill <- read_sheet(ss, "refill", col_types = "c")

try <- refill %>%
   pivot_longer(
      cols      = 3:14,
      names_to  = "month",
      values_to = "date",
   ) %>%
   separate_longer_delim(
      date,
      " "
   ) %>%
   filter(date != "TRANSFER") %>%
   group_by(px_code, name, date) %>%
   summarise(
      bottles = n()
   ) %>%
   ungroup() %>%
   mutate(
      date2 = as.Date(parse_date_time(stri_c(date, "/2024"), "mdY"))
   )

write_sheet(try, ss, "visits")

##  read visits ----------------------------------------------------------------

visits      <- read_sheet(ss, "visits", col_types = "c")
conn        <- ohasis$conn("lw")
form_art_bc <- QB$new(conn)$
   from("ohasis_warehouse.form_art_bc AS art")$
   leftJoin("ohasis_warehouse.id_registry AS reg", "art.PATIENT_ID", "=", "reg.PATIENT_ID")$
   whereBetween("art.VISIT_DATE", c("2023-12-01", "2024-10-31"))$
   whereNotNull("MEDICINE_SUMMARY")$
   where("FACI_ID", "130019")$
   select("VISIT_DATE")$
   selectRaw("COALESCE(reg.CENTRAL_ID, art.PATIENT_ID) AS CENTRAL_ID")$
   get()
dbDisconnect(conn)

import <- visits %>%
   filter(!is.na(CENTRAL_ID)) %>%
   filter(!is.na(patient_code)) %>%
   separate_wider_delim(
      cols    = cd4_2023,
      delim   = "extraction date",
      names   = c("cd41_result", "cd41_date"),
      too_few = "align_start"
   ) %>%
   separate_wider_delim(
      cols    = cd4_2024,
      delim   = "extraction date",
      names   = c("cd42_result", "cd42_date"),
      too_few = "align_start"
   ) %>%
   separate_wider_delim(
      cols    = vl_2023,
      delim   = "extraction date",
      names   = c("vl1_result", "vl1_date"),
      too_few = "align_start"
   ) %>%
   separate_wider_delim(
      cols    = vl_2024,
      delim   = "extraction date",
      names   = c("vl2_result", "vl2_date"),
      too_few = "align_start"
   ) %>%
   mutate_at(
      .vars = vars(vl1_date, vl2_date, cd41_date, cd42_date),
      ~as.Date(parse_date_time(., c("mdY", "mdy")))
   ) %>%
   mutate(
      LAB_VIRAL_DATE   = coalesce(vl2_date, vl1_date),
      LAB_VIRAL_RESULT = coalesce(vl2_result, vl1_result),
      LAB_CD4_DATE     = coalesce(cd42_date, cd41_date),
      LAB_CD4_RESULT   = coalesce(cd42_result, cd41_result),

      DISP_DATE        = as.Date(date2),
      NEXT_DATE        = DISP_DATE %m+% months(as.numeric(bottles)),
      DISP_TOTAL       = as.numeric(bottles) * 30,
      stage            = case_when(
         stage == "I" ~ "1",
         stage == "II" ~ "2",
         stage == "III" ~ "3",
         stage == "IV" ~ "4",
      )
   ) %>%
   select(
      CENTRAL_ID,
      RECORD_DATE       = date2,
      PATIENT_CODE      = patient_code,
      FIRST             = first,
      LAST              = last,
      PHILHEALTH_NO     = philhealth_num,
      CONFIRMATORY_CODE = confirmatory_code,
      UIC               = uic,
      CLIENT_MOBILE     = mobile,
      BIRTHDATE         = birthdate,
      CURR_REG          = region,
      CURR_PROV         = province,
      CURR_MUNC         = muncity,
      WHO_CLASS         = stage,
      LAB_VIRAL_DATE,
      LAB_VIRAL_RESULT,
      LAB_CD4_DATE,
      LAB_CD4_RESULT,
      DISP_DATE,
      NEXT_DATE,
      MEDICINE_SUMMARY  = arv_2024,
      DISP_TOTAL
   ) %>%
   anti_join(
      y  = form_art_bc,
      by = join_by(CENTRAL_ID, DISP_DATE == VISIT_DATE)
   ) %>%
   mutate(
      row_id      = row_number(),
      PATIENT_ID  = CENTRAL_ID,
      FACI_ID     = "130019",
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
         CONFIRMATORY_CODE,
         UIC,
         PATIENT_CODE,
         BIRTHDATE,
         PHILHEALTH_NO,
         CREATED_BY,
         CREATED_AT,
      )
)
tables$px_name     <- list(
   name = "px_name",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = import %>%
      select(
         REC_ID,
         PATIENT_ID,
         FIRST,
         LAST,
         CREATED_BY,
         CREATED_AT,
      )
)
tables$px_contact  <- list(
   name = "px_contact",
   pk   = c("REC_ID", "CONTACT_TYPE"),
   data = import %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         CLIENT_MOBILE,
      ) %>%
      pivot_longer(
         cols      = c(CLIENT_MOBILE),
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
tables$px_addr     <- list(
   name = "px_addr",
   pk   = c("REC_ID", "ADDR_TYPE"),
   data = import %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         CURR_REG,
         CURR_PROV,
         CURR_MUNC,
      ) %>%
      pivot_longer(
         cols      = c(
            ends_with("_REG"),
            ends_with("_PROV"),
            ends_with("_MUNC"),
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
         names_prefix = "NAME_"
      ) %>%
      harp_addr_to_id(
         ohasis$ref_addr,
         c(
            ADDR_REG  = "NAME_REG",
            ADDR_PROV = "NAME_PROV",
            ADDR_MUNC = "NAME_MUNC"
         )
      ) %>%
      select(
         REC_ID,
         starts_with("ADDR_"),
         CREATED_BY,
         CREATED_AT,
      )
)
tables$px_staging <- list(
   name = "px_staging",
   pk   = "REC_ID",
   data = import %>%
      select(
         REC_ID,
         WHO_CLASS,
         CREATED_BY,
         CREATED_AT,
      )
)
tables$px_labs <- list(
   name = "px_labs",
   pk   = c("REC_ID", "LAB_TEST"),
   data = import %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
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
            LAB_TEST == "VIRAL" ~ "4",
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
tables$px_medicine <- list(
   name = "px_medicine",
   pk   = c("REC_ID", "MEDICINE", "DISP_NUM"),
   data = import %>%
      select(
         REC_ID,
         FACI_ID,
         SUB_FACI_ID,
         PATIENT_ID,
         DISP_DATE,
         NEXT_DATE,
         MEDICINE = MEDICINE_SUMMARY,
         DISP_TOTAL
      ) %>%
      separate_longer_delim(
         cols  = MEDICINE,
         delim = "+"
      ) %>%
      mutate(
         PER_DAY       = case_when(
            MEDICINE == "AZT/3TC" ~ 2,
            MEDICINE == "LPV/r" ~ 4,
            MEDICINE == "DTG" ~ 1,
            MEDICINE == "TDF/3TC/EV" ~ 1,
            MEDICINE == "NVP" ~ 2,
            MEDICINE == "ABC" ~ 2,
            MEDICINE == "3TC" ~ 2,
            TRUE ~ 1
         ),
         MEDICINE      = case_when(
            MEDICINE == "TDF/3TC/DTG" ~ "2029",
            MEDICINE == "TDF/3TC/EFV" ~ "2015",
            MEDICINE == "AZT/3TC" ~ "2018",
            MEDICINE == "LPV/r" ~ "2009",
            MEDICINE == "DTG" ~ "2035",
            MEDICINE == "EFV" ~ "2004",
            MEDICINE == "TDF/3TC/EV" ~ "2015",
            MEDICINE == "NVP" ~ "2011",
            MEDICINE == "ABC" ~ "2000",
            MEDICINE == "3TC" ~ "2023",
            TRUE ~ MEDICINE
         ),
         UNIT_BASIS    = "2",

         NEXT_DATE     = DISP_DATE %m+% days(as.numeric(DISP_TOTAL) / PER_DAY),
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

