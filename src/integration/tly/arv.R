##  inputs ---------------------------------------------------------------------

file <- "D:/20240416_tly-arv_disp.rds"
mo   <- "03"
yr   <- "2024"

##  processing -----------------------------------------------------------------

tly <- read_rds(file)
# min <- as.Date(stri_c(sep = "-", stri_pad_left(yr, 4, "0"), stri_pad_left(mo, 2, "0"), "01"))
# max <- min %m+% months(1) %m-% days(1)
min <- min(tly$visits$DISP_DATE, na.rm = TRUE)
max <- max(tly$visits$DISP_DATE, na.rm = TRUE)

#  uploaded --------------------------------------------------------------------

db              <- "ohasis_warehouse"
lw_conn         <- ohasis$conn("lw")
tly$prev_upload <- dbTable(
   lw_conn,
   db,
   "form_art_bc",
   raw_where = TRUE,
   where     = glue(r"(
         (VISIT_DATE BETWEEN '{min}' AND '{max}') AND
            CREATED_BY = '1300000048'
   )")
)
tly$prev_upload %<>%
   mutate_if(
      .predicate = is.POSIXct,
      ~as.Date(.)
   )
dbDisconnect(lw_conn)

##  new data -------------------------------------------------------------------

tly$records <- tly$visits %>%
   filter(DISP_DATE %within% interval(min, max)) %>%
   distinct(CLIENT_CODE, DISP_DATE, .keep_all = TRUE) %>%
   mutate(
      DISP_DATE = as.Date(DISP_DATE),
   ) %>%
   rename(
      PATIENT_CODE     = CLIENT_CODE,
      VISIT_DATE       = DISP_DATE,
      SERVICE_FACI     = FACI_ID,
      SERVICE_SUB_FACI = SUB_FACI_ID,
      CLINIC_NOTES     = REMARKS,
      LATEST_NEXT_DATE = NEXT_PICKUP,
   ) %>%
   mutate(
      RECORD_DATE    = VISIT_DATE,
      FORM_VERSION   = "ART Form (v2021)",
      LAB_CD4_RESULT = as.character(LAB_CD4_RESULT)
   ) %>%
   mutate_at(
      .vars = vars(CURR_REG, CURR_PROV, CURR_MUNC),
      ~coalesce(., "UNKNOWN")
   ) %>%
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         ADDR_REG  = "CURR_REG",
         ADDR_PROV = "CURR_PROV",
         ADDR_MUNC = "CURR_MUNC"
      )
   ) %>%
   # get records id if existing
   left_join(
      y  = tly$prev_upload %>%
         select(
            REC_ID,
            CREATED_BY,
            CREATED_AT,
            PATIENT_CODE,
            VISIT_DATE
         ),
      by = join_by(PATIENT_CODE, VISIT_DATE)
   ) %>%
   mutate(BIRTHDATE = as.Date(BIRTHDATE)) %>%
   select(-PROPH_FLUCANO)

TIMESTAMP <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
tly$records %<>%
   # retain only not uploaded and those with changes
   filter(!is.na(PATIENT_ID), !is.na(MEDICINE_SUMMARY)) %>%
   anti_join(
      y  = tly$prev_upload,
      # by = join_by(!!!intersect(names(tly$records), names(tly$prev_upload))),
      by = join_by(REC_ID, VISIT_DATE, MEDICINE_SUMMARY),
   ) %>%
   mutate(
      old_rec    = if_else(!is.na(REC_ID), 1, 0, 0),
      CREATED_BY = coalesce(CREATED_BY, "1300000048"),
      CREATED_AT = coalesce(as.character(CREATED_AT), TIMESTAMP),
      UPDATED_BY = if_else(old_rec == 1, "1300000048", NA_character_),
      UPDATED_AT = if_else(old_rec == 1, TIMESTAMP, NA_character_)
   ) %>%
   relocate(any_of(names(tly$prev_upload)), .before = 1) %>%
   select(-old_rec)

tly$import <- tly$records %>%
   filter(!is.na(REC_ID)) %>%
   bind_rows(
      batch_rec_ids(tly$records %>% filter(is.na(REC_ID)), REC_ID, CREATED_BY, c("PATIENT_ID", "VISIT_DATE"))
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
         FACI_ID     = "130001",
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
         CONFIRMATORY_CODE,
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

tables$px_contact <- list(
   name = "px_contact",
   pk   = c("REC_ID", "CONTACT_TYPE"),
   data = tly$import %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
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

tables$px_addr <- list(
   name = "px_addr",
   pk   = c("REC_ID", "ADDR_TYPE"),
   data = tly$import %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
         CURR_REG,
         CURR_PROV,
         CURR_MUNC,
         CURR_ADDR
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
         id_cols      = c(REC_ID, CREATED_BY, CREATED_AT, UPDATED_BY, UPDATED_AT, ADDR_TYPE),
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
         ADDR_TEXT = NAME_TEXT,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT
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
         FACI_ID     = DISP_FACI,
         SUB_FACI_ID = DISP_SUB_FACI,
         SERVICE_TYPE,
         VISIT_TYPE,
         CLIENT_TYPE,
         TX_STATUS,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      ) %>%
      mutate_at(
         .vars = vars(VISIT_TYPE, CLIENT_TYPE, TX_STATUS),
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

tables$px_profile <- list(
   name = "px_profile",
   pk   = "REC_ID",
   data = tly$import %>%
      mutate(
         AGE = calc_age(BIRTHDATE, VISIT_DATE)
      ) %>%
      select(
         REC_ID,
         AGE,
         SELF_IDENT,
         SELF_IDENT_OTHER,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      ) %>%
      mutate_at(
         .vars = vars(SELF_IDENT),
         ~keep_code(.)
      )
)

tables$px_staging <- list(
   name = "px_staging",
   pk   = "REC_ID",
   data = tly$import %>%
      mutate(
         AGE = calc_age(BIRTHDATE, VISIT_DATE)
      ) %>%
      select(
         REC_ID,
         WHO_CLASS,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      ) %>%
      mutate_at(
         .vars = vars(WHO_CLASS),
         ~keep_code(.)
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
         TB_STATUS = case_when(
            !is.na(TB_IPT_STATUS) ~ "0_No active TB",
            TRUE ~ TB_STATUS
         )
      ) %>%
      select(
         REC_ID,
         TB_SCREEN,
         TB_STATUS,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      ) %>%
      mutate_at(
         .vars = vars(TB_SCREEN, TB_STATUS),
         ~keep_code(.)
      )
)

tables$px_tb_ipt <- list(
   name = "px_tb_ipt",
   pk   = "REC_ID",
   data = tly$import %>%
      mutate(
         TB_STATUS = case_when(
            !is.na(TB_IPT_STATUS) ~ "0_No active TB",
            TRUE ~ TB_STATUS
         )
      ) %>%
      select(
         REC_ID,
         TB_IPT_STATUS,
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

tables$px_prophylaxis <- list(
   name = "px_prophylaxis",
   pk   = c("REC_ID", "PROPHYLAXIS"),
   data = tly$import %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
         starts_with("PROPH_"),
      ) %>%
      pivot_longer(
         cols      = starts_with("PROPH_"),
         names_to  = "PROPHYLAXIS",
         values_to = "IS_PROPH"
      ) %>%
      mutate(
         PROPHYLAXIS = case_when(
            PROPHYLAXIS == "PROPH_COTRI" ~ "2",
            PROPHYLAXIS == "PROPH_AZITHRO" ~ "3",
            PROPHYLAXIS == "PROPH_FLUCA" ~ "4",
            TRUE ~ PROPHYLAXIS
         ),
         IS_PROPH    = keep_code(IS_PROPH)
      )
)

tables$px_medicine <- list(
   name = "px_medicine",
   pk   = c("REC_ID", "MEDICINE", "DISP_NUM"),
   data = tly$import %>%
      select(ROW_LINK, REC_ID) %>%
      inner_join(tly$disp, join_by(ROW_LINK)) %>%
      mutate(
         UNIT_BASIS = "2",
      ) %>%
      group_by(ROW_LINK) %>%
      mutate(
         DISP_NUM   = row_number(),
         DISP_TOTAL = TYPICAL_PER_DAY * DISP_TOTAL
      ) %>%
      ungroup() %>%
      select(
         REC_ID,
         FACI_ID,
         SUB_FACI_ID,
         MEDICINE,
         DISP_NUM,
         UNIT_BASIS,
         PER_DAY   = TYPICAL_PER_DAY,
         DISP_TOTAL,
         MEDICINE_LEFT,
         MEDICINE_MISSED,
         DISP_DATE,
         NEXT_DATE = NEXT_PICKUP,
      )
)

db_conn <- ohasis$conn("db")
dbxDelete(
   db_conn,
   Id(schema = "ohasis_interim", table = "px_medicine"),
   tly$import %>% select(REC_ID),
   batch_size = 1000
)
lapply(tables, function(ref, db_conn) {
   log_info("Uploading {green(ref$name)}.")
   table_space <- Id(schema = "ohasis_interim", table = ref$name)
   dbxUpsert(db_conn, table_space, ref$data, ref$pk)
   # dbExecute(db_conn, glue("DELETE FROM ohasis_interim.{ref$name} WHERE REC_ID IN (?)"), params = list(unique(ref$data$REC_ID)))
}, db_conn)
dbDisconnect(db_conn)
