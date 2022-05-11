data <- limiter %>%
   inner_join(
      y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_consent")),
      by = "REC_ID"
   ) %>%
   mutate(
      SIGNATURE_NAME = if_else(!is.na(SIGNATURE), '1_Yes', NA_character_),
      SIGNATURE_ESIG = if_else(!is.na(ESIG), '1_Yes', NA_character_),
      VERBAL_CONSENT = if_else(VERBAL_CONSENT == 1, '1_Yes', NA_character_)
   ) %>%
   select(
      REC_ID,
      VERBAL_CONSENT,
      SIGNATURE_ESIG,
      SIGNATURE_NAME
   )