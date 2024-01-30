remove_pii <- function(data) {
   data %<>%
      select(
         -any_of(
            c(
               "UIC",
               "uic",
               "FIRST",
               "first",
               "firstname",
               "MIDDLE",
               "middle",
               "LAST",
               "last",
               "SUFFIX",
               "suffix",
               "name_suffix",
               "PATIENT_CODE",
               "px_code",
               "PHILHEALTH_NO",
               "philhealth_no",
               "PHILHEALTH",
               "philhealth",
               "PHILSYS_ID",
               "philsys_id",
               "PERM_ADDR",
               "CURR_ADDR",
               "BIRTH_ADDR",
               "CLIENT_MOBILE",
               "client_mobile",
               "mobile",
               "CLIENT_EMAIL",
               "client_email",
               "email",
               "name",
               "STANDARD_FIRST"
            )
         )
      )
   return(data)
}

clean_pii <- function(pii_col) {
   clean <- str_squish(stri_trans_toupper(pii_col))
   clean <- case_when(
      str_detect(clean, "^[^[:alnum:]]$") ~ NA_character_,
      str_detect(clean, "^AWAITING") ~ NA_character_,
      str_detect(clean, "^PENDING") ~ NA_character_,
      str_detect(clean, "^NOT\\b") ~ NA_character_,
      str_detect(clean, "^NO\\b") ~ NA_character_,
      !str_detect(clean, "[^-]") ~ NA_character_,
      clean == "" ~ NA_character_,
      clean == "NONE" ~ NA_character_,
      clean == "N/A" ~ NA_character_,
      clean == "NA" ~ NA_character_,
      clean == "XXX" ~ NA_character_,
      clean == "XX" ~ NA_character_,
      TRUE ~ clean
   )

   return(clean)
}

get_latest_pii <- function(data, pid_col, pii_cols) {
   for (col in pii_cols) {
      missing <- data %>%
         filter_at(.vars = vars(matches(col)), ~is.na(.))

      if (nrow(missing) > 0) {
         col_name <- as.name(col)
         pids     <- stri_c(collapse = "','", missing[[pid_col]])
         not_null <- case_when(
            str_detect(col, "_PSGC_") ~ stri_c(str_extract(col, "(.*_PSGC_).*", 1), "MUNC"),
            col == "SELF_IDENT_OTHER" ~ "SELF_IDENT",
            TRUE ~ col
         )

         lw_conn  <- ohasis$conn("lw")
         data_cid <- dbTable(
            lw_conn,
            "ohasis_lake",
            "px_pii",
            cols      = c("PATIENT_ID", "SNAPSHOT", col),
            join      = list(
               "ohasis_warehouse.id_registry" = list(
                  by   = c("PATIENT_ID" = "PATIENT_ID"),
                  type = "INNER",
                  cols = "CENTRAL_ID"
               )
            ),
            raw_where = TRUE,
            where     = glue(r"(
         id_registry.PATIENT_ID IN (SELECT PATIENT_ID FROM ohasis_warehouse.id_registry WHERE CENTRAL_ID IN (SELECT CENTRAL_ID FROM ohasis_warehouse.id_registry WHERE PATIENT_ID IN ('{pids}')))
            AND px_pii.{not_null} IS NOT NULL
      )")
         )
         data_pid <- dbTable(lw_conn, "ohasis_lake", "px_pii", cols = c("PATIENT_ID", "SNAPSHOT", col), raw_where = TRUE, where = glue(r"(PATIENT_ID IN ('{pids}') AND px_pii.{not_null} IS NOT NULL)"))
         dbDisconnect(lw_conn)

         corr <- data_cid %>%
            select(-PATIENT_ID) %>%
            bind_rows(data_pid) %>%
            mutate(
               CENTRAL_ID = if_else(coalesce(CENTRAL_ID, "") == "", PATIENT_ID, CENTRAL_ID, CENTRAL_ID)
            ) %>%
            mutate_if(
               .predicate = is.character,
               ~clean_pii(.)
            ) %>%
            filter(!is.na({{col_name}})) %>%
            arrange(desc(SNAPSHOT)) %>%
            distinct(CENTRAL_ID, .keep_all = TRUE) %>%
            select(-PATIENT_ID, -SNAPSHOT)

         data %<>%
            left_join(
               y  = corr %>%
                  rename(CORRECT_PII = {{col_name}}),
               by = join_by(CENTRAL_ID)
            ) %>%
            mutate(
               {{col_name}} := coalesce({{col_name}}, CORRECT_PII)
            ) %>%
            select(-CORRECT_PII)
      }
   }

   return(data)
}
