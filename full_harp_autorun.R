ohasis <- DB$new("2025", "02", "finalizing harp", "2")

harp_dx$steps$`01_load_reqs`$.init(harp_dx, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "2", update_visits = "1", update_harp = "1", harp_reprocess = "1")
harp_dx$steps$`02_data_hts_tst_pos`$.init(harp_dx, run_checks = "2", upload = "1", exclude_drops = "1", save = "1")

ohasis$data_factory("warehouse", "form_d", "refresh", TRUE)
harp_dead$steps$`01_load_reqs`$.init(harp_dead, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "2", update_harp = "1", harp_reprocess = "1")
harp_dead$steps$`02_data_mortality`$.init(harp_dead, run_checks = "2", upload = "1", exclude_drops = "1", save = "1")

harp_tx$steps$`01_load_reqs`$.init(harp_tx, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "2", update_visits = "1", update_harp = "1", harp_reprocess = "1")
harp_tx$steps$`02_data_tx_new`$.init(harp_tx, run_checks = "2", upload = "1", exclude_drops = "1")
harp_tx$steps$`03_data_tx_curr`$.init(harp_tx, run_checks = "2", upload = "1", save = "1")

prep$steps$`01_load_reqs`$.init(prep, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "2", update_visits = "2", update_init = "2", update_link = "2", update_harp = "1", harp_reprocess = "1")
prep$steps$`02_data_prep_offer`$.init(prep, run_checks = "2", upload = "1", exclude_drops = "1")
prep$steps$`03_data_prep_curr`$.init(prep, run_checks = "2", upload = "1", save = "1")


con        <- ohasis$conn("lw")
ly_clients <- QB$new(con)$from("ohasis_lake.ly_clients")$whereNull("CENTRAL_ID")$get()
dbDisconnect(con)

upload_ids <- QB$new(`oh-lw`)$from("nhsss_validations.ohasis-national-splink")$
   where("Bene", "Y")$
   where("Gab", "Y")$
   where("Lala", "Y")$
   get()

registry <- QB$new(`oh-live`)$from("ohasis_interim.registry")$get()

ts           <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
dedup_upload <- upload_ids %>%
   select(
      CENTRAL_ID = right_cid,
      PATIENT_ID = left_cid
   )
bind_pid     <- dedup_upload %>%
   select(
      CENTRAL_ID = 1,
      PATIENT_ID = 2,
   ) %>%
   left_join(
      y  = registry %>%
         select(PATIENT_ID, CREATED_BY, CREATED_AT) %>%
         mutate_all(as.character),
      by = join_by(PATIENT_ID)
   ) %>%
   mutate(old = if_else(!is.na(CREATED_AT), 1, 0, 0)) %>%
   mutate(
      REPORT_DATE = NA_Date_,
      IDNUM       = NA_character_,
      REMARKS     = NA_character_,
      PRIME       = NA_integer_,
      CREATED_BY  = if_else(old == 0, Sys.getenv("OH_USER_ID"), CREATED_BY, CREATED_BY),
      CREATED_AT  = if_else(old == 0, ts, CREATED_AT, CREATED_AT),
      UPDATED_BY  = if_else(old == 1, Sys.getenv("OH_USER_ID"), NA_character_, NA_character_),
      UPDATED_AT  = if_else(old == 1, ts, NA_character_, NA_character_),
      DELETED_BY  = NA_character_,
      DELETED_AT  = NA_character_
   ) %>%
   select(names(registry))

bind_cid <- dedup_upload %>%
   select(
      CENTRAL_ID = 1,
   ) %>%
   distinct_all() %>%
   mutate(
      PATIENT_ID = CENTRAL_ID
   ) %>%
   left_join(
      y  = registry %>%
         select(PATIENT_ID, CREATED_BY, CREATED_AT) %>%
         mutate_all(as.character),
      by = join_by(PATIENT_ID)
   ) %>%
   mutate(old = if_else(!is.na(CREATED_AT), 1, 0, 0)) %>%
   mutate(
      REPORT_DATE = NA_Date_,
      IDNUM       = NA_character_,
      REMARKS     = NA_character_,
      PRIME       = NA_integer_,
      CREATED_BY  = if_else(old == 0, Sys.getenv("OH_USER_ID"), CREATED_BY, CREATED_BY),
      CREATED_AT  = if_else(old == 0, ts, CREATED_AT, CREATED_AT),
      UPDATED_BY  = if_else(old == 1, Sys.getenv("OH_USER_ID"), NA_character_, NA_character_),
      UPDATED_AT  = if_else(old == 1, ts, NA_character_, NA_character_),
      DELETED_BY  = NA_character_,
      DELETED_AT  = NA_character_
   ) %>%
   filter(old == 0) %>%
   select(names(registry))

# updated old cids
new_reg <- registry %>%
   mutate_all(as.character) %>%
   left_join(
      y  = dedup_upload %>%
         select(NEW_CID = 1, CENTRAL_ID = 2),
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate(
      CENTRAL_ID = coalesce(NEW_CID, CENTRAL_ID),
      UPDATED_BY = if_else(!is.na(NEW_CID), Sys.getenv("OH_USER_ID"), UPDATED_BY, UPDATED_BY),
      UPDATED_AT = if_else(!is.na(NEW_CID), ts, UPDATED_AT, UPDATED_AT),
   ) %>%
   select(-NEW_CID)

# final new data
new_reg <- bind_pid %>%
   mutate_all(as.character) %>%
   bind_rows(bind_cid %>% mutate_all(as.character)) %>%
   bind_rows(new_reg %>% mutate_all(as.character)) %>%
   distinct(PATIENT_ID, .keep_all = TRUE)

upload <- new_reg %>% anti_join(registry %>% mutate_all(as.character))

conn <- connect("ohasis-live")
dbxUpsert(conn, Id(schema = "ohasis_interim", table = "registry"), upload, where_cols = "PATIENT_ID")
dbDisconnect(conn)

new <- ly_clients %>%
   # filter(if_all(c(UIC, FIRST, LAST), ~!is.na(.))) %>%
   filter(if_all(c(BIRTHDATE, FIRST, LAST), ~!is.na(.))) %>%
   # select(
   #    row_id,
   #    FIRST,
   #    LAST,
   #    UIC,
   #    PATIENT_CODE = CLIENT_CODE
   # ) %>%
   # inner_join(pii_unique) %>%
   distinct(row_id, .keep_all = TRUE) %>%
   mutate(
      BIRTHDATE = if_else(
         is.na(BIRTHDATE) & nchar(UIC) == 14,
         stri_c(sep = "-", StrRight(UIC, 4), substr(UIC, 7, 8), substr(UIC, 9, 10)),
         as.character(BIRTHDATE),
         as.character(BIRTHDATE)
      ),
   ) %>%
   filter(!is.na(BIRTHDATE)) %>%
   mutate(
      FACI_ID = "130001"
   ) %>%
   rename(
      PATIENT_CODE = CLIENT_CODE
   )

created <- oh_batch_newpx(new, "row_id")

con <- ohasis$conn("lw")
dbxUpsert(con, Id(schema = "ohasis_lake", table = "ly_clients"), created %>% select(row_id, CENTRAL_ID = PATIENT_ID), "row_id")
dbDisconnect(con)





