##  db data --------------------------------------------------------------------
conn <- ohasis$conn("db")
db   <- "ohasis_interim"
data <- list()
tbls <- c("px_record", "px_name", "px_info", "px_contact", "id_registry")
for (tbl in tbls) {
   data[[tbl]] <- dbTable(conn, db, tbl)
}
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
            FIRST,
            MIDDLE,
            LAST,
            SUFFIX
         ),
      by = "REC_ID"
   ) %>%
   left_join(
      y  = data$px_info %>%
         select(
            REC_ID,
            CONFIRMATORY_CODE,
            UIC,
            PHILHEALTH_NO,
            SEX,
            BIRTHDATE,
            PATIENT_CODE,
            PHILSYS_ID
         ),
      by = "REC_ID"
   ) %>%
   left_join(
      y  = data$px_contact %>%
         select(
            REC_ID,
            MOBILE = CLIENT_MOBILE,
            EMAIL  = CLIENT_EMAIL
         ),
      by = "REC_ID"
   ) %>%
   arrange(desc(UPDATED_AT), desc(CREATED_AT))

cols <- c(
      "FIRST",
      "MIDDLE",
      "LAST",
      "SUFFIX",
      "CONFIRMATORY_CODE",
      "UIC",
      "PHILHEALTH_NO",
      "SEX",
      "BIRTHDATE",
      "PATIENT_CODE",
      "PHILSYS_ID"
)

clean <- list()
for (col in cols) {
   .log_info("Getting latest data for {green(col)}.")
   col_name          <- as.name(col)
   clean$vars[[col]] <- pii_ref %>%
      select(
         PATIENT_ID,
         FACI_ID,
         !!col_name
      ) %>%
      filter(!is.na(!!col_name)) %>%
      distinct(PATIENT_ID, FACI_ID, .keep_all = TRUE) %>%
      rename(
         DATA = 3
      ) %>%
      mutate(
         VAR = col
      ) %>%
      mutate_all(~as.character(.))
}



.log_info("Consolidating variables.")
pid_registry <- bind_rows(clean$vars) %>%
   pivot_wider(
      id_cols     = c(PATIENT_ID, FACI_ID),
      names_from  = VAR,
      values_from = DATA
   )