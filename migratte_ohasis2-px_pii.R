##  db data --------------------------------------------------------------------
conn        <- ohasis$conn("lw")
db          <- "ohasis_interim"
data        <- list()
tbls        <- c("px_record", "px_name", "px_info", "px_addr", "px_contact", "registry")
data        <- lapply(tbls, dbTable, conn = conn, dbname = db)
names(data) <- tbls
data$px_contact %<>%
   mutate(
      CONTACT_TYPE = case_when(
         CONTACT_TYPE == "1" ~ "MOBILE",
         CONTACT_TYPE == "2" ~ "EMAIL",
         TRUE ~ as.character(CONTACT_TYPE)
      )
   ) %>%
   pivot_wider(
      id_cols     = REC_ID,
      names_from  = CONTACT_TYPE,
      values_from = CONTACT,
      names_glue  = 'CLIENT_{CONTACT_TYPE}_{.value}'
   ) %>%
   rename_at(
      .vars = vars(ends_with('_CONTACT')),
      ~stri_replace_all_fixed(., '_CONTACT', '')
   ) %>%
   select(
      REC_ID,
      starts_with("CLIENT_") & !matches("\\d")
   )

data$px_addr %<>%
   rename(
      REG  = ADDR_REG,
      PROV = ADDR_PROV,
      MUNC = ADDR_MUNC,
      ADDR = ADDR_TEXT
   ) %>%
   mutate(
      ADDR_TYPE = case_when(
         ADDR_TYPE == 1 ~ "CURR",
         ADDR_TYPE == 2 ~ "PERM",
         ADDR_TYPE == 3 ~ "BIRTH",
         ADDR_TYPE == 4 ~ "DEATH",
         ADDR_TYPE == 5 ~ "SERVICE",
         TRUE ~ as.character(ADDR_TYPE)
      )
   ) %>%
   pivot_wider(
      id_cols     = REC_ID,
      names_from  = ADDR_TYPE,
      values_from = c(REG, PROV, MUNC, ADDR),
      names_glue  = "{ADDR_TYPE}_{.value}"
   ) %>%
   select(
      REC_ID,
      starts_with("CURR_"),
      starts_with("PERM_"),
      starts_with("BIRTH_"),
      starts_with("DEATH_"),
      starts_with("SERVICE_")
   )

dbDisconnect(conn)

#  harp data  ------------------------------------------------------------------
harp      <- list()
harp$dx   <- read_dta(hs_data("harp_dx", "reg", 2022, 11),
                      col_select = c(PATIENT_ID, REC_ID, idnum)) %>%
   mutate(dx = 1) %>%
   left_join(
      y  = data$registry %>%
         select(PATIENT_ID, CENTRAL_ID),
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   )
harp$tx   <- read_dta(hs_data("harp_tx", "reg", 2022, 11),
                      col_select = c(PATIENT_ID, REC_ID, art_id)) %>%
   mutate(tx = 1) %>%
   left_join(
      y  = data$registry %>%
         select(PATIENT_ID, CENTRAL_ID),
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   )
harp$prep <- read_dta(hs_data("prep", "reg", 2022, 11),
                      col_select = c(PATIENT_ID, REC_ID, prep_id)) %>%
   mutate(prep = 1) %>%
   left_join(
      y  = data$registry %>%
         select(PATIENT_ID, CENTRAL_ID),
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   )

#  per pid unique --------------------------------------------------------------

pii_ref <- data$px_record %>%
   filter(is.na(DELETED_BY)) %>%
   left_join(
      y  = data$px_name %>%
         select(
            REC_ID,
            PATIENT_ID,
            FIRST,
            MIDDLE,
            LAST,
            SUFFIX
         ),
      by = join_by(REC_ID, PATIENT_ID)
   ) %>%
   left_join(
      y  = data$px_info %>%
         select(
            REC_ID,
            PATIENT_ID,
            CONFIRMATORY_CODE,
            UIC,
            PHILHEALTH_NO,
            SEX,
            BIRTHDATE,
            PATIENT_CODE,
            PHILSYS_ID
         ),
      by = join_by(REC_ID, PATIENT_ID)
   ) %>%
   left_join(
      y  = data$px_contact %>%
         select(
            REC_ID,
            CLIENT_MOBILE,
            CLIENT_EMAIL
         ),
      by = join_by(REC_ID)
   ) %>%
   left_join(
      y  = data$px_addr,
      by = join_by(REC_ID)
   ) %>%
   arrange(desc(UPDATED_AT), desc(CREATED_AT))

pid_registry <- pii_ref %>%
   select(-REC_ID, -DELETED_AT) %>%
   mutate_if(
      .predicate = is.character,
      ~clean_pii(.)
   ) %>%
   mutate(
      BIRTHDATE = as.character(BIRTHDATE),
      SEX       = as.character(SEX),
      SNAPSHOT  = coalesce(UPDATED_AT, CREATED_AT)
   ) %>%
   pivot_longer(
      cols = c(FIRST, MIDDLE, LAST, SUFFIX, UIC, CONFIRMATORY_CODE, PATIENT_CODE, BIRTHDATE, PHILSYS_ID, PHILHEALTH_NO, CLIENT_EMAIL, CLIENT_MOBILE, SEX, PERM_REG, PERM_PROV, PERM_MUNC, CURR_REG, CURR_PROV, CURR_MUNC)
   ) %>%
   mutate(
      sort = if_else(!is.na(value), 1, 9999, 9999)
   ) %>%
   arrange(sort, desc(SNAPSHOT)) %>%
   distinct(PATIENT_ID, FACI_ID, SUB_FACI_ID, name, .keep_all = TRUE) %>%
   pivot_wider(
      id_cols     = c(PATIENT_ID, FACI_ID, SUB_FACI_ID),
      names_from  = name,
      values_from = value
   )

lw_conn     <- ohasis$conn("lw")
table_space <- Id(schema = "ohasis_interim", table = "px_pii")
dbCreateTable(lw_conn, table_space, pid_registry)
dbxUpsert(lw_conn, table_space, pid_registry, "PATIENT_ID")
dbExecute(lw_conn, "ALTER TABLE ohasis_interim.px_pii ADD FULLTEXT(FIRST, MIDDLE, LAST, SUFFIX, UIC, CONFIRMATORY_CODE, PATIENT_CODE, PHILSYS_ID, PHILHEALTH_NO, CLIENT_EMAIL, CLIENT_MOBILE)")
dbDisconnect(lw_conn)

