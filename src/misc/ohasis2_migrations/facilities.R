db_conn <- ohasis$conn("db")
old     <- dbxSelect(db_conn, "SELECT * FROM ohasis_interim.facility")
dbDisconnect(db_conn)

new <- old %>%
   arrange(desc(EDIT_NUM)) %>%
   distinct(FACI_ID, .keep_all = TRUE) %>%
   distinct(
      FACI_ID,
      FACI_NAME,
      FACI_NAME_HARP = FACI_NAME_CLEAN,
      FACI_NAME_ALT  = ALT_FACI_NAME,
      FACI_CODE,
      PUBPRIV,
      LONG,
      LAT,
      EMAIL,
      MOBILE,
      LANDLINE,
      REG,
      PROV,
      MUNC,
      ADDRESS,
      LOGO,
      UPDATED_BY     = CREATED_BY,
      UPDATED_AT     = CREATED_AT,
   ) %>%
   left_join(
      y  = curr %>%
         arrange(EDIT_NUM) %>%
         distinct(FACI_ID, .keep_all = TRUE) %>%
         select(FACI_ID, CREATED_BY, CREATED_AT),
      by = join_by(FACI_ID)
   ) %>%
   relocate(CREATED_BY, CREATED_AT, .before = UPDATED_BY) %>%
   mutate(
      LOGO = basename(LOGO),
      DELETED_BY = NA_character_,
      DELETED_AT = NA_POSIXct_
   )

lw_conn     <- ohasis$conn("lw")
db_name     <- "ohasis_interim"
tbl_name    <- "facilities"
table_space <- Id(schema = db_name, table = tbl_name)
if (dbExistsTable(lw_conn, table_space)) {
   dbRemoveTable(lw_conn, table_space)
}
dbCreateTable(lw_conn, table_space, new)
dbExecute(lw_conn, stri_c("ALTER TABLE ohasis_interim.facilities ADD PRIMARY KEY (FACI_ID);"))
ohasis$upsert(lw_conn, db_name, tbl_name, new, "FACI_ID")
dbDisconnect(lw_conn)
