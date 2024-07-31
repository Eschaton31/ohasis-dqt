rit <- read_excel("C:/Users/johnb/Downloads/ARG PATIENT1.1.xlsx", "GEN INFO") %>%
   rename(
      PATIENT_CODE      = 1,
      HOSPITAL_NO       = 2,
      BIRTHDATE         = 3,
      CONFIRMATORY_CODE = 4,
      UIC               = 5,
      RESIDENCE         = 6
   ) %>%
   mutate(
      PATIENT_CODE = str_replace_all(PATIENT_CODE, "[^[:alnum:]]", ""),
      HOSPITAL_NO  = str_replace_all(HOSPITAL_NO, "/", " / "),
      BIRTHDATE    = as.Date(BIRTHDATE)
   ) %>%
   mutate_if(is.character, ~str_squish(toupper(.))) %>%
   mutate(
      pii_row = row_number(),
      .before = 1
   )

visits_old <- read_excel("C:/Users/johnb/Downloads/ARG PATIENT1.1.xlsx", "OLD DATABASE") %>%
   rename(
      PATIENT_CODE  = 1,
      DATE_VISIT    = 2,
      FOLLOWUP_TYPE = 3,
      REMARKS       = 4,
      HISTORY       = 5,
   ) %>%
   mutate(
      REMARKS    = str_replace_all(REMARKS, "\\s", " "),
      REMARKS    = stri_trans_general(str_squish(stri_trans_toupper(REMARKS)), "latin-ascii"),
      REMARKS    = na_if(REMARKS, ""),

      VISIT_TYPE = NA_character_,
      VISIT_TYPE = if_else(str_detect(REMARKS, "\\bARV\\b"), "ART", VISIT_TYPE, VISIT_TYPE),
      VISIT_TYPE = if_else(str_detect(REMARKS, "REFILL"), "ART", VISIT_TYPE, VISIT_TYPE),
      VISIT_TYPE = if_else(REMARKS == "ARV REFILL", "ART", VISIT_TYPE, VISIT_TYPE),
   ) %>%
   mutate_if(is.character, ~str_squish(toupper(.)))


visits_new <- read_excel("C:/Users/johnb/Downloads/ARG PATIENT1.1.xlsx", "NEW DATABASE") %>%
   rename(
      PATIENT_CODE   = 1,
      REMARKS        = 2,
      FORM_COMPLETED = 3,
      DATE_FILLED    = 4,
      ENCODER_1      = 5,
      ENCODED_1      = 6,
      ENCODER_2      = 7,
      ENCODED_2      = 8
   ) %>%
   mutate(
      DATE_VISIT = coalesce(ENCODED_1, ENCODED_2, DATE_FILLED),

      REMARKS    = str_replace_all(REMARKS, "\\s", " "),
      REMARKS    = stri_trans_general(str_squish(stri_trans_toupper(REMARKS)), "latin-ascii"),
      REMARKS    = na_if(REMARKS, ""),

      VISIT_TYPE = NA_character_,
      VISIT_TYPE = if_else(str_detect(REMARKS, "\\bARV\\b"), "ART", VISIT_TYPE, VISIT_TYPE),
      VISIT_TYPE = if_else(str_detect(REMARKS, "REFILL"), "ART", VISIT_TYPE, VISIT_TYPE),
      VISIT_TYPE = if_else(REMARKS == "ARV REFILL", "ART", VISIT_TYPE, VISIT_TYPE),
   ) %>%
   mutate_if(is.character, ~str_squish(toupper(.)))

art_visits <- visits_new %>%
   filter(VISIT_TYPE == "ART") %>%
   bind_rows(
      visits_old %>%
         filter(VISIT_TYPE == "ART")
   )

latest_art <- art_visits %>%
   mutate(
      not_inf = !is.infinite(DATE_VISIT)
   ) %>%
   filter(not_inf) %>%
   group_by(PATIENT_CODE) %>%
   summarise(
      latest_dispdate = max(DATE_VISIT, na.rm = TRUE)
   ) %>%
   ungroup()

latest_visit <- visits_new %>%
   bind_rows(visits_old) %>%
   mutate(
      not_inf = !is.infinite(DATE_VISIT)
   ) %>%
   filter(not_inf) %>%
   group_by(PATIENT_CODE) %>%
   summarise(
      latest_ffupdate = max(DATE_VISIT, na.rm = TRUE)
   ) %>%
   ungroup()

latest_data <- latest_visit %>%
   full_join(latest_art) %>%
   full_join(rit, join_by(PATIENT_CODE)) %>%
   mutate_at(
      .vars = vars(latest_ffupdate, latest_dispdate),
      ~if_else(is.infinite(.), NA_Date_, ., .) %>% as.Date(.)
   )

rit %>% write_sheet("1me32u3P-SbP9KzHTJoGa2XqfpE2O2DHQ-X-UJM02bH4", "rit_ml")
latest_data %>% write_sheet("1me32u3P-SbP9KzHTJoGa2XqfpE2O2DHQ-X-UJM02bH4", "rit_latest")

con <- ohasis$conn("lw")
art <- QB$new(con)$from("ohasis_warehouse.form_art_bc AS pii")$
   leftJoin("ohasis_warehouse.id_registry as id", "pii.PATIENT_ID", "=", "id.PATIENT_ID")$
   where("FACI_ID", "130003")$
   select("pii.FIRST", "pii.LAST", "pii.UIC", "pii.BIRTHDATE", "pii.CONFIRMATORY_CODE", "pii.PATIENT_CODE")$
   selectRaw("COALESCE(id.CENTRAL_ID, pii.PATIENT_ID) AS CENTRAL_ID")$
   distinct()$
   get() %>%
   mutate_if(
      .predicate = is.character,
      ~toupper(str_squish(.))
   )
dbDisconnect(con)

art_rit <- art %>%
   mutate_if(
      .predicate = is.character,
      ~clean_pii(.)
   ) %>%
   distinct(PATIENT_CODE, BIRTHDATE, .keep_all = TRUE) %>%
   mutate(
      PXCODE_SIEVE = str_replace_all(PATIENT_CODE, "[^[:alnum:]]", ""),
      PXCODE_BDATE = str_c(PXCODE_SIEVE, "|", BIRTHDATE)
   )


art_rit %>% write_sheet("1me32u3P-SbP9KzHTJoGa2XqfpE2O2DHQ-X-UJM02bH4", "ohasis_rit_unique")