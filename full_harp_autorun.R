ohasis <- DB$new("2024", "10", "initial dedup", "2")

harp_dx$steps$`01_load_reqs`$.init(harp_dx, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "1", update_visits = "1", update_harp = "1", harp_reprocess = "1")
harp_dx$steps$`02_data_hts_tst_pos`$.init(harp_dx, run_checks = "1", upload = "1", exclude_drops = "1", save = "1")

ohasis$data_factory("warehouse", "form_d", "refresh", TRUE)
harp_dead$steps$`01_load_reqs`$.init(harp_dead, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "2", update_harp = "1", harp_reprocess = "1")
harp_dead$steps$`02_data_mortality`$.init(harp_dead, run_checks = "2", upload = "1", exclude_drops = "1", save = "1")

harp_tx$steps$`01_load_reqs`$.init(harp_tx, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "1", update_visits = "1", update_harp = "1", harp_reprocess = "1")
harp_tx$steps$`02_data_tx_new`$.init(harp_tx, run_checks = "1", upload = "1", exclude_drops = "1")
harp_tx$steps$`03_data_tx_curr`$.init(harp_tx, run_checks = "1", upload = "1", save = "1")

prep$steps$`01_load_reqs`$.init(prep, end_date = end_ym(ohasis$yr, ohasis$mo), dl_corr = "1", dl_forms = "1", update_lw = "1", update_visits = "1", update_init = "2", update_link = "2", update_harp = "1", harp_reprocess = "1")
prep$steps$`02_data_prep_offer`$.init(prep, run_checks = "2", upload = "1", exclude_drops = "1")
prep$steps$`03_data_prep_curr`$.init(prep, run_checks = "2", upload = "1", save = "1")


con        <- ohasis$conn("lw")
ly_clients <- QB$new(con)$from("ohasis_lake.ly_clients")$whereNull("CENTRAL_ID")$get()
dbDisconnect(con)


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





