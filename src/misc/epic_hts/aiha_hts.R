con   <- ohasis$conn("lw")
forms <- QB$new(con)
forms$where("FACI_ID", "=", "990005", "or")
forms$where("SERVICE_FACI", "-", "990005", "or")
forms$from("ohasis_warehouse.form_hts")
form_hts <- forms$get()
forms$from("ohasis_warehouse.form_a")
form_a <- forms$get()
forms$from("ohasis_warehouse.form_cfbs")
form_cfbs <- forms$get()
forms$from("ohasis_warehouse.id_registry")
forms$select(CENTRAL_ID, PATIENT_ID)
id_reg <- forms$get()
dbDisconnect(con)

prep_start <- read_dta(hs_data("prep", "prepstart", 2023, 12)) %>%
   select(-any_of("CENTRAL_ID")) %>%
   get_cid(id_reg, PATIENT_ID)

tx <- read_dta(hs_data("harp_tx", "reg", 2023, 12)) %>%
   select(-any_of("CENTRAL_ID")) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   left_join(
      y  = read_dta(hs_data("harp_tx", "outcome", 2023, 12), col_select = c(art_id, realhub, realhub_branch, real_reg, real_prov, real_munc, outcome)),
      by = join_by(art_id)
   )

dx <- read_dta(hs_data("harp_dx", "reg", 2023, 12)) %>%
   select(-any_of("CENTRAL_ID")) %>%
   get_cid(id_reg, PATIENT_ID)

hts_all <- process_hts(form_hts, form_a, form_cfbs) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   mutate(
      # tag those without form faci
      use_record_faci  = if_else(
         condition = is.na(SERVICE_FACI),
         true      = 1,
         false     = 0
      ),
      SERVICE_FACI     = case_when(
         use_record_faci == 1 & FACI_ID != "130000" ~ FACI_ID,
         use_record_faci == 1 & FACI_ID == "130000" ~ SPECIMEN_SOURCE,
         TRUE ~ SERVICE_FACI
      ),
      SERVICE_SUB_FACI = case_when(
         use_record_faci == 1 & FACI_ID == "130000" ~ SPECIMEN_SUB_SOURCE,
         !(SERVICE_FACI %in% c("130001", "130605", "130023")) ~ NA_character_,
         TRUE ~ SERVICE_SUB_FACI
      ),
   ) %>%
   # inner_join(
   #    y  = sites %>%
   #       filter(site_icap_2024 == 1) %>%
   #       distinct(FACI_ID),
   #    by = join_by(SERVICE_FACI == FACI_ID)
   # ) %>%
   convert_hts("nhsss") %>%
   mutate_at(
      .vars = vars(HTS_REG, HTS_PROV, HTS_MUNC, CBS_REG, CBS_PROV, CBS_MUNC),
      ~coalesce(na_if(., "OVERSEAS"), "UNKNOWN")
   ) %>%
   mutate(
      use_addr  = case_when(
         hts_modality %in% c("ST", "CBS", "FBS", "ST") & if_any(c(CBS_REG, CBS_PROV, CBS_MUNC), ~. != "UNKNOWN") ~ "cbs",
         hts_modality %in% c("ST", "CBS", "FBS", "ST") & if_all(c(CBS_REG, CBS_PROV, CBS_MUNC), ~. == "UNKNOWN") ~ "hts",
         if_any(c(HTS_REG, HTS_PROV, HTS_MUNC), ~. != "UNKNOWN") ~ "hts",
         CBS_PROV != "UNKNOWN" & CBS_MUNC == "UNKNOWN" ~ "cbs",
         HTS_PROV != "UNKNOWN" & HTS_MUNC == "UNKNOWN" ~ "hts",
         CBS_REG != "UNKNOWN" & CBS_PROV == "UNKNOWN" ~ "cbs",
         HTS_REG != "UNKNOWN" & HTS_PROV == "UNKNOWN" ~ "hts",
         if_all(c(HTS_REG, HTS_PROV, HTS_MUNC), ~. == "UNKNOWN") ~ "res",
      ),
      TEST_REG  = case_when(
         use_addr == "cbs" ~ CBS_REG,
         use_addr == "hts" ~ HTS_REG,
      ),
      TEST_PROV = case_when(
         use_addr == "cbs" ~ CBS_PROV,
         use_addr == "hts" ~ HTS_PROV,
      ),
      TEST_MUNC = case_when(
         use_addr == "cbs" ~ CBS_MUNC,
         use_addr == "hts" ~ HTS_MUNC,
      ),
   )

file <- "H:/20240319_hts-EpiC_ever.dta"
hts_all %>%
   remove_pii() %>%
   left_join(
      y  = prep_start %>%
         select(CENTRAL_ID, prepstart_date) %>%
         distinct(CENTRAL_ID, .keep_all = TRUE),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = tx %>%
         select(CENTRAL_ID, artstart_date, outcome) %>%
         distinct(CENTRAL_ID, .keep_all = TRUE),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = dx %>%
         select(CENTRAL_ID, confirm_date) %>%
         distinct(CENTRAL_ID, .keep_all = TRUE),
      by = join_by(CENTRAL_ID)
   ) %>%
   select(
      -any_of(c(
         "CREATED_BY",
         "UPDATED_BY",
         "CLINIC_NOTES",
         "COUNSEL_NOTES",
         "use_curr",
         "AGE_DTA",
         "risks",
         "idnum",
         "male",
         "female",
         "SELF_IDENT_OTHER_SIEVE",
         "VL_ERROR",
         "VL_DROP"
      ))
   ) %>%
   format_stata() %>%
   write_dta(file)
compress_stata(file)