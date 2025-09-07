remove_table_alias <- function(data) {
   return(
      data %>%
         rename_all(
            ~case_when(
               str_detect(., "\\.") ~ str_extract(., ".+\\.(.+)", 1),
               TRUE ~ .
            )
         )
   )
}

get_addr <- function(data, addr_set = NULL, type = "nhsss") {
   # addr_set format:
   # reg, prov, munc

   type <- tolower(type)
   if (type %in% c("nhsss", "code")) {
      get_reg  <- as.symbol("nhsss_reg")
      get_prov <- as.symbol("nhsss_prov")
      get_munc <- as.symbol("nhsss_munc")
   } else if (type == "label") {
      get_reg  <- as.symbol("label_reg")
      get_prov <- as.symbol("label_prov")
      get_munc <- as.symbol("label_munc")
   } else if (type == "name") {
      get_reg  <- as.symbol("name_reg")
      get_prov <- as.symbol("name_prov")
      get_munc <- as.symbol("name_munc")
   }

   coded_reg  <- addr_set[1] %>% as.symbol()
   coded_prov <- addr_set[2] %>% as.symbol()
   coded_munc <- addr_set[3] %>% as.symbol()

   named_reg  <- names(addr_set)[1]
   named_prov <- names(addr_set)[2]
   named_munc <- names(addr_set)[3]

   # rename columns
   data %<>%
      select(
         -any_of(
            c(
               "psgc_reg",
               "psgc_prov",
               "psgc_munc",
               "psgc"
            )
         )
      ) %>%
      mutate(
         psgc = coalesce(!!coded_munc, !!coded_prov, !!coded_reg)
      ) %>%
      select(
         -!!coded_reg,
         -!!coded_prov,
         -!!coded_munc
      ) %>%
      left_join(
         y  = ohasis$ref_addr %>%
            select(
               psgc,
               !!named_reg  := !!get_reg,
               !!named_prov := !!get_prov,
               !!named_munc := !!get_munc,
            ),
         by = join_by(psgc)
      ) %>%
      relocate(!!named_reg, !!named_prov, !!named_munc, .before = psgc)

   return(data)
}

flow_dta <- function(data, surv_name, type, yr, mo) {
   yr <- stri_pad_left(yr, 4, "0")
   mo <- stri_pad_left(mo, 2, "0")

   db     <- surv_name
   table  <- stri_c(type, "_", yr, mo)
   schema <- Id(schema = db, table = table)
   id_col <- switch(
      surv_name,
      harp_dx   = "idnum",
      harp_tx   = "art_id",
      harp_dead = "mort_id",
      harp_full = "idnum",
      prep      = "prep_id",
      tbhiv     = "art_id"
   )


   log_info("Uploading {green(table)}.")
   lw_conn <- connect(surv_name)
   if (dbExistsTable(lw_conn, table)) {
      dbExecute(lw_conn, glue(r"(TRUNCATE `{db}`.`{table}`;)"))
   }
   ohasis$upsert(
      lw_conn,
      db,
      table,
      data %>%
         mutate_if(
            .predicate = is.labelled,
            ~to_character(.)
         ),
      id_col
   )

   version <- tibble(
      type        = type,
      period      = stri_c(yr, ".", mo),
      last_update = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
   )
   ohasis$upsert(lw_conn, surv_name, "version", version, c("type", "period"))

   dbDisconnect(lw_conn)
}

get_latest_pii <- function(data, pid_col, pii_cols) {
   missing <- data %>%
      filter(if_any(any_of(pii_cols), ~is.na(.)))

   if (nrow(missing) > 0) {
      idreg <- read_rds(Sys.getenv("LOC_IDREG")) %>%
         filter(central_id %in% missing$central_id)
      pids  <- unique(c(idreg$patient_id, missing$patient_id))

      pii <- read_rds(Sys.getenv("DEDUP_PII")) %>%
         filter(patient_id %in% pids) %>%
         mutate_at(
            .vars = vars(
               first,
               middle,
               last,
               suffix,
               confirmatory_code,
               patient_code,
               uic,
               philhealth_no,
               philsys_id,
               client_mobile,
               client_email
            ),
            ~clean_pii(.)
         ) %>%
         mutate(
            client_mobile = str_replace_all(client_mobile, "[^[:digit:]]", ""),
            client_mobile = case_when(
               str_left(client_mobile, 1) == "9" ~ stri_c("0", client_mobile),
               str_left(client_mobile, 2) == "63" ~ str_replace(client_mobile, "^63", "0"),
               TRUE ~ client_mobile
            ),
            birthdate     = as.character(birthdate)
         ) %>%
         get_cid(idreg, patient_id) %>%
         mutate(
            snapshot = max(created_at, updated_at, deleted_at, na.rm = TRUE)
         ) %>%
         select(
            central_id,
            first,
            middle,
            last,
            suffix,
            uic,
            confirmatory_code,
            patient_code,
            birthdate,
            philsys_id,
            philhealth_no,
            client_email,
            client_mobile,
            sex,
            self_ident,
            self_ident_other,
            civil_status,
            nationality,
            educ_level,
            curr_munc,
            curr_prov,
            curr_reg,
            perm_munc,
            perm_prov,
            perm_reg,
            birth_munc,
            birth_prov,
            birth_reg,
            snapshot
         ) %>%
         pivot_longer(
            cols = c(
               first,
               middle,
               last,
               suffix,
               uic,
               confirmatory_code,
               patient_code,
               birthdate,
               philsys_id,
               philhealth_no,
               client_email,
               client_mobile,
               sex,
               self_ident,
               self_ident_other,
               civil_status,
               nationality,
               educ_level,
               curr_munc,
               curr_prov,
               curr_reg,
               perm_munc,
               perm_prov,
               perm_reg,
               birth_munc,
               birth_prov,
               birth_reg,
            )
         ) %>%
         mutate(
            sort = if_else(!is.na(value), 1, 9999, 9999)
         ) %>%
         arrange(sort, desc(snapshot)) %>%
         distinct(central_id, name, .keep_all = TRUE) %>%
         pivot_wider(
            id_cols     = central_id,
            names_from  = name,
            values_from = value
         )

      pii %<>%
         mutate_all(~as.character(.))

      if ("perm_munc" %in% pii_cols) {
         pii_cols <- pii_cols[!(pii_cols %in% c("perm_reg", "perm_prov", "perm_munc", "perm_brgy"))]
         pii_cols <- append(pii_cols, "perm_psgc")
         data %<>%
            unite(
               col    = "perm_psgc",
               sep    = "|",
               perm_munc,
               perm_prov,
               perm_reg,
               na.rm  = TRUE,
               remove = TRUE
            ) %>%
            mutate(perm_psgc = na_if(perm_psgc, ""))
         pii %<>%
            unite(
               col    = "perm_psgc",
               sep    = "|",
               perm_munc,
               perm_prov,
               perm_reg,
               na.rm  = TRUE,
               remove = TRUE
            ) %>%
            mutate(perm_psgc = na_if(perm_psgc, ""))
      }

      if ("curr_munc" %in% pii_cols) {
         pii_cols <- pii_cols[!(pii_cols %in% c("curr_reg", "curr_prov", "curr_munc", "curr_brgy"))]
         pii_cols <- append(pii_cols, "curr_psgc")
         data %<>%
            unite(
               col    = "curr_psgc",
               sep    = "|",
               curr_munc,
               curr_prov,
               curr_reg,
               na.rm  = TRUE,
               remove = TRUE
            ) %>%
            mutate(curr_psgc = na_if(curr_psgc, ""))
         pii %<>%
            unite(
               col    = "curr_psgc",
               sep    = "|",
               curr_munc,
               curr_prov,
               curr_reg,
               na.rm  = TRUE,
               remove = TRUE
            ) %>%
            mutate(curr_psgc = na_if(curr_psgc, ""))
      }

      if ("birth_munc" %in% pii_cols) {
         pii_cols <- pii_cols[!(pii_cols %in% c("birth_reg", "birth_prov", "birth_munc", "birth_brgy"))]
         pii_cols <- append(pii_cols, "birth_psgc")
         data %<>%
            unite(
               col    = "birth_psgc",
               sep    = "|",
               birth_munc,
               birth_prov,
               birth_reg,
               na.rm  = TRUE,
               remove = TRUE
            ) %>%
            mutate(birth_psgc = na_if(birth_psgc, ""))
         pii %<>%
            unite(
               col    = "birth_psgc",
               sep    = "|",
               birth_munc,
               birth_prov,
               birth_reg,
               na.rm  = TRUE,
               remove = TRUE
            ) %>%
            mutate(birth_psgc = na_if(birth_psgc, ""))
      }

      if ("birthdate" %in% pii_cols) {
         pii %<>%
            mutate(
               birthdate = as.Date(birthdate)
            )
      }

      for (col in pii_cols) {
         col_name <- as.name(col)
         data %<>%
            left_join(
               y  = pii %>%
                  select(central_id, correct_pii = {{col_name}}),
               by = join_by(central_id)
            ) %>%
            mutate(
               {{col_name}} := coalesce({{col_name}}, correct_pii)
            ) %>%
            select(-correct_pii)
      }


      if ("perm_psgc" %in% pii_cols) {
         data %<>%
            separate_wider_delim(
               perm_psgc,
               "|",
               names   = c("perm_reg", "perm_prov", "perm_munc"),
               too_few = "align_start"
            )
      }

      if ("curr_psgc" %in% pii_cols) {
         data %<>%
            separate_wider_delim(
               curr_psgc,
               "|",
               names   = c("curr_reg", "curr_prov", "curr_munc"),
               too_few = "align_start"
            )
      }

      if ("birth_psgc" %in% pii_cols) {
         data %<>%
            separate_wider_delim(
               birth_psgc,
               "|",
               names   = c("birth_reg", "birth_prov", "birth_munc"),
               too_few = "align_start"
            )
      }
   }

   return(data)
}
