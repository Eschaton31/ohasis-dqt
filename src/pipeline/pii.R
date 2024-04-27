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

get_latest_pii <- function(data, pid_col, pii_cols) {
   missing <- data %>%
      filter(if_any(any_of(pii_cols), ~is.na(.)))


   if (nrow(missing) > 0) {

      lw_conn <- ohasis$conn("lw")
      pids    <- QB$new(lw_conn)$
         select("PATIENT_ID")$
         from("ohasis_warehouse.id_registry")$
         whereIn("CENTRAL_ID", missing$CENTRAL_ID)$
         get()
      pids    <- unique(c(pids$PATIENT_ID, missing$PATIENT_ID))

      pii <- QB$new(lw_conn)
      for (col in pii_cols) {
         pii$selectRaw(stri_c("px_pii.", col))
      }
      pii$selectRaw("px_pii.SNAPSHOT")
      pii$selectRaw("COALESCE(id_registry.CENTRAL_ID, px_pii.PATIENT_ID) AS CENTRAL_ID")
      pii$from("ohasis_lake.px_pii")
      pii$leftJoin("ohasis_warehouse.id_registry", "px_pii.PATIENT_ID", "=", "id_registry.PATIENT_ID")
      pii$whereIn("PATIENT_ID", pids)
      pii$where(function(query = QB$new(lw_conn)) {
         for (col in pii_cols) {
            query$whereNotNull(col, boolean = "or")
         }
         query$whereNested
      })
      pii <- pii$get()
      dbDisconnect(lw_conn)

      pii %<>%
         mutate_all(~as.character(.))

      if ("PERM_PSGC_MUNC" %in% pii_cols) {
         pii_cols <- pii_cols[!(pii_cols %in% c("PERM_PSGC_REG", "PERM_PSGC_PROV", "PERM_PSGC_MUNC"))]
         pii_cols <- append(pii_cols, "PERM_PSGC")
         data %<>%
            unite(
               col    = "PERM_PSGC",
               sep    = "|",
               PERM_PSGC_REG,
               PERM_PSGC_PROV,
               PERM_PSGC_MUNC,
               na.rm  = TRUE,
               remove = TRUE
            )
         pii %<>%
            unite(
               col    = "PERM_PSGC",
               sep    = "|",
               PERM_PSGC_REG,
               PERM_PSGC_PROV,
               PERM_PSGC_MUNC,
               na.rm  = TRUE,
               remove = TRUE
            )
      }

      if ("CURR_PSGC_MUNC" %in% pii_cols) {
         pii_cols <- pii_cols[!(pii_cols %in% c("CURR_PSGC_REG", "CURR_PSGC_PROV", "CURR_PSGC_MUNC"))]
         pii_cols <- append(pii_cols, "CURR_PSGC")
         data %<>%
            unite(
               col    = "CURR_PSGC",
               sep    = "|",
               CURR_PSGC_REG,
               CURR_PSGC_PROV,
               CURR_PSGC_MUNC,
               na.rm  = TRUE,
               remove = TRUE
            )
         pii %<>%
            unite(
               col    = "CURR_PSGC",
               sep    = "|",
               CURR_PSGC_REG,
               CURR_PSGC_PROV,
               CURR_PSGC_MUNC,
               na.rm  = TRUE,
               remove = TRUE
            )
      }

      if ("BIRTH_PSGC_MUNC" %in% pii_cols) {
         pii_cols <- pii_cols[!(pii_cols %in% c("BIRTH_PSGC_REG", "BIRTH_PSGC_PROV", "BIRTH_PSGC_MUNC"))]
         pii_cols <- append(pii_cols, "BIRTH_PSGC")
         data %<>%
            unite(
               col    = "BIRTH_PSGC",
               sep    = "|",
               BIRTH_PSGC_REG,
               BIRTH_PSGC_PROV,
               BIRTH_PSGC_MUNC,
               na.rm  = TRUE,
               remove = TRUE
            )
         pii %<>%
            unite(
               col    = "BIRTH_PSGC",
               sep    = "|",
               BIRTH_PSGC_REG,
               BIRTH_PSGC_PROV,
               BIRTH_PSGC_MUNC,
               na.rm  = TRUE,
               remove = TRUE
            )
      }

      pivot_cols <- names(pii)
      pivot_cols <- pivot_cols[!(pivot_cols %in% c("CENTRAL_ID", "SNAPSHOT"))]

      pii %<>%
         pivot_longer(all_of(pivot_cols)) %>%
         mutate(
            value = na_if(value, ""),
            sort  = if_else(!is.na(value), 1, 9999, 9999)
         ) %>%
         filter(!is.na(value)) %>%
         arrange(sort, desc(SNAPSHOT)) %>%
         distinct(CENTRAL_ID, name, .keep_all = TRUE) %>%
         pivot_wider(
            id_cols     = CENTRAL_ID,
            names_from  = name,
            values_from = value
         )

      if ("BIRTHDATE" %in% pii_cols) {
         pii %<>%
            mutate(
               BIRTHDATE = as.Date(BIRTHDATE)
            )
      }

      for (col in pii_cols) {
         col_name <- as.name(col)
         data %<>%
            left_join(
               y  = pii %>%
                  select(CENTRAL_ID, CORRECT_PII = {{col_name}}),
               by = join_by(CENTRAL_ID)
            ) %>%
            mutate(
               {{col_name}} := coalesce({{col_name}}, CORRECT_PII)
            ) %>%
            select(-CORRECT_PII)
      }


      if ("PERM_PSGC" %in% pii_cols) {
         data %<>%
            separate_wider_delim(
               PERM_PSGC,
               "|",
               names   = c("PERM_PSGC_REG", "PERM_PSGC_PROV", "PERM_PSGC_MUNC"),
               too_few = "align_start"
            )
      }

      if ("CURR_PSGC" %in% pii_cols) {
         data %<>%
            separate_wider_delim(
               CURR_PSGC,
               "|",
               names   = c("CURR_PSGC_REG", "CURR_PSGC_PROV", "CURR_PSGC_MUNC"),
               too_few = "align_start"
            )
      }

      if ("BIRTH_PSGC" %in% pii_cols) {
         data %<>%
            separate_wider_delim(
               BIRTH_PSGC,
               "|",
               names   = c("BIRTH_PSGC_REG", "BIRTH_PSGC_PROV", "BIRTH_PSGC_MUNC"),
               too_few = "align_start"
            )
      }
   }

   return(data)
}
