##  inputs ---------------------------------------------------------------------

file <- "H:/20231005_tly-arv_disp.rds"
mo   <- "08"
yr   <- "2023"

##  processing -----------------------------------------------------------------

tly <- read_rds("H:/20231005_tly-arv_disp.rds")
min <- as.Date(stri_c(sep = "-", stri_pad_left(yr, 4, "0"), stri_pad_left(mo, 2, "0"), "01"))
max <- min %m+% months(1) %m-% days(1)

#  uploaded --------------------------------------------------------------------

db              <- "ohasis_warehouse"
lw_conn         <- ohasis$conn("lw")
tly$prev_upload <- dbTable(
   lw_conn,
   db,
   "form_art_bc",
   raw_where = TRUE,
   where     = glue(r"(
         (DATE(CREATED_AT) BETWEEN '{min}' AND '{max}') AND LEFT(CREATED_BY, 6) = '130000' AND FACI_ID = '130001'
   )")
)
dbDisconnect(lw_conn)

##  new data -------------------------------------------------------------------

tly$records <- tly$visits %>%
   filter(DISP_DATE %within% interval(min, max)) %>%
   distinct(CLIENT_CODE, DISP_DATE, .keep_all = TRUE) %>%
   mutate(
      REC_ID       = NA_character_,
      DISP_DATE    = as.Date(DISP_DATE),
      RECORD_DATE  = DISP_DATE,

      REC_ID       = NA_character_,
      CREATED_BY   = "1300000000",
      CREATED_AT   = stri_c(as.character(DISP_DATE), " 00:00:00"),
      UPDATED_BY   = "1300000000",
      UPDATED_AT   = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),

      MODULE       = "3",
      DISEASE      = "101000",
      SERVICE_TYPE = "101201",

      VERSION      = "2021",
      FORM         = "Form ART"
   ) %>%
   mutate_at(
      .vars = vars(
         SEX,
         SELF_IDENT,
         TX_STATUS,
         VISIT_TYPE,
         WHO_CLASS,
         TB_SCREEN,
         TB_IPT_STATUS,
         CLIENT_TYPE
      ),
      ~keep_code(.)
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
   )

##  table formats
tables           <- list()
tables$px_record <- list(
   name = "px_record",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = tly$records %>%
      filter(EXIST_INFO == 0 |
                EXIST_CONFIRM == 0 |
                coalesce(EXIST_TEST, 0) == 0) %>%
      mutate(
         FACI_ID     = "130000",
         SUB_FACI_ID = NA_character_,
         DISEASE     = "101000"
      ) %>%
      select(
         REC_ID,
         PATIENT_ID,
         FACI_ID,
         SUB_FACI_ID,
         RECORD_DATE = DATE_RECEIVE,
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
   data = tly$records %>%
      filter(EXIST_INFO == 0) %>%
      select(
         REC_ID,
         PATIENT_ID,
         CONFIRMATORY_CODE,
         PATIENT_CODE,
         SEX,
         BIRTHDATE,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      )
)

tables$px_name <- list(
   name = "px_name",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = tly$records %>%
      filter(EXIST_INFO == 0) %>%
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
