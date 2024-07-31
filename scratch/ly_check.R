lw_conn <- ohasis$conn("lw")
min     <- "2023-10-01"
max     <- "2024-03-31"

forms <- QB$new(lw_conn)
forms$where(function(query = QB$new(lw_conn)) {
   query$whereBetween('RECORD_DATE', c(min, max), "or")
   query$whereBetween('DATE_CONFIRM', c(min, max), "or")
   query$whereBetween('T0_DATE', c(min, max), "or")
   query$whereBetween('T1_DATE', c(min, max), "or")
   query$whereBetween('T2_DATE', c(min, max), "or")
   query$whereBetween('T3_DATE', c(min, max), "or")
   query$whereNested
})
forms$where(function(query = QB$new(lw_conn)) {
   query$whereIn('FACI_ID', ly_sites$FACI_ID, boolean = "or")
   query$whereIn('SERVICE_FACI', ly_sites$FACI_ID, boolean = "or")
   query$whereNested
})

forms$from("ohasis_warehouse.form_hts")
hts <- forms$get()

forms$from("ohasis_warehouse.form_a")
a <- forms$get()

cfbs <- QB$new(lw_conn)$
   from("ohasis_warehouse.form_cfbs")$
   limit(0)$
   get()

testing <- process_hts(hts, a, cfbs)

id_reg <- QB$new(lw_conn)$
   from("ohasis_warehouse.id_registry")$
   select(PATIENT_ID, CENTRAL_ID)$
   get()

dbDisconnect(lw_conn)

ly_hts <- testing %>%
   get_cid(id_reg, PATIENT_ID) %>%
   convert_hts("name")

prep <- read_dta(hs_data("prep", "outcome", 2024, 4)) %>%
   get_cid(id_reg, PATIENT_ID)


ly_review <- ly_hts %>%
   arrange(hts_date) %>%
   # filter(coalesce(hts_result, "NR") == "NR") %>%
   # distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   # left_join(
   #    y = prep %>%
   #       filter(!is.na(prepstart_date)) %>%
   #       distinct(CENTRAL_ID) %>%
   #       mutate(prep_started = 1),
   #    by = join_by(CENTRAL_ID)
   # ) %>%
   # group_by(HTS_FACI) %>%
   # summarise(
   #    nr = n(),
   #    prep_new = sum(prep_started, na.rm = TRUE)
   # ) %>%
   # ungroup() %>%
   # mutate(
   #    `%` = (prep_new / nr)*100
   # ) %>%
   filter(
      str_detect(HTS_FACI, "Anglo") |
      str_detect(HTS_FACI, "Victoria") |
      str_detect(HTS_FACI, "Lily")
   ) %>%
   remove_pii()

