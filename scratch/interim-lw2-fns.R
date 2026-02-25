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
      update_pii()
      conn  <- connect("local-sqlite")
      idreg <- QB$new(conn)$from('id_registry')$whereIn('central_id', missing$central_id, 'or')$whereIn('patient_id', missing$central_id, 'or')$get()
      pids  <- unique(c(idreg$patient_id, missing$patient_id, missing$central_id))

      pii <- QB$new(conn)$from('patients')$whereIn('patient_id', pids)$get() %>%
         get_cid(idreg, patient_id) %>%
         filter(is.na(deleted_at)) %>%
         mutate(
            snapshot = max(created_at, updated_at, na.rm = TRUE)
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
               perm_reg,
               perm_prov,
               perm_munc,
               na.rm  = TRUE,
               remove = TRUE
            ) %>%
            mutate(perm_psgc = na_if(perm_psgc, ""))
         pii %<>%
            unite(
               col    = "perm_psgc",
               sep    = "|",
               perm_reg,
               perm_prov,
               perm_munc,
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
               curr_reg,
               curr_prov,
               curr_munc,
               na.rm  = TRUE,
               remove = TRUE
            ) %>%
            mutate(curr_psgc = na_if(curr_psgc, ""))
         pii %<>%
            unite(
               col    = "curr_psgc",
               sep    = "|",
               curr_reg,
               curr_prov,
               curr_munc,
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
               birth_reg,
               birth_prov,
               birth_munc,
               na.rm  = TRUE,
               remove = TRUE
            ) %>%
            mutate(birth_psgc = na_if(birth_psgc, ""))
         pii %<>%
            unite(
               col    = "birth_psgc",
               sep    = "|",
               birth_reg,
               birth_prov,
               birth_munc,
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

deconstruct_art <- function(forms, dispense = NULL, discontinue = NULL) {
   tables <- c(
      "px_record",
      "px_pii",
      "px_profile",
      "px_service",
      "px_staging",
      "px_ob",
      "px_tb",
      "px_tb_ipt",
      "px_tb_active",
      "px_med_profile",
      "px_vaccine",
      "px_key_pop",
      "px_labs",
      "px_oi",
      "px_prophylaxis",
      "px_other_service",
      "px_remarks",
      "px_medicine",
      "px_medicine_disc"
   )

   forms %<>%
      select(-any_of(c('branch', 'Branch'))) %>%
      rename_all(tolower) %>%
      mutate_at(
         .vars = vars(any_of(c(
            'sex',
            'self_ident',
            'civil_status',
            'educ_level',
            'client_type',
            'is_pregnant',
            'who_class',
            'visit_type',
            'tb_screen',
            'tb_ipt_status',
            'tx_status',
            'client_type'
         ))),
         ~keep_code(.)
      ) %>%
      mutate(
         form_id = 'art2021'
      ) %>%
      mutate(
         client_mobile = str_replace_all(client_mobile, "[^[:digit:]]", ""),
         client_mobile = case_when(
            str_left(client_mobile, 1) == "9" ~ stri_c("0", client_mobile),
            str_left(client_mobile, 2) == "63" ~ str_replace(client_mobile, "^63", "0"),
            TRUE ~ client_mobile
         ),
         birthdate     = as.character(birthdate)
      )

   conn <- ohasis$conn("db")

   # primary keys
   log_info("Obtaining {green('Primary Keys')}.")
   pks        <- lapply(tables, function(table) dbGetQuery(conn, glue("show keys from ohasis.{table} where Key_name = 'primary'")))
   pks        <- lapply(pks, function(data) return(data$Column_name))
   names(pks) <- tables


   # columns
   log_info("Obtaining {green('Column Names')}.")
   cols        <- lapply(tables, function(table) dbGetQuery(conn, glue("show columns from ohasis.{table}")))
   cols        <- lapply(cols, function(data) return(data$Field))
   names(cols) <- tables

   products <- QB$new(conn)$from("ohasis.products")$get()

   dbDisconnect(conn)

   log_info("Creating tables using obtained schema.")
   data        <- lapply(tables, function(table, data, cols) {
      col_need      <- cols[[table]]
      col_not_found <- setdiff(col_need, names(data))

      schema <- data %>%
         mutate(
            !!!setNames(rep(NA_character_, length(col_not_found)), col_not_found)
         ) %>%
         select(any_of(col_need)) %>%
         distinct()

      return(schema)
   }, data = forms, cols = cols)
   names(data) <- tables

   log_info("Manually creating long tables.")
   if (is.null(dispense)) {
      data$px_medicine <- forms %>%
         separate_longer_delim(
            cols  = medicine_summary,
            delim = "+"
         ) %>%
         mutate(
            medicine_summary = str_replace_all(medicine_summary, 'LPV/R', 'LPV/r')
         ) %>%
         select(-matches('per_day')) %>%
         left_join(
            y  = products %>%
               mutate(
                  per_day = as.integer(typical_batch) / 30
               ) %>%
               select(
                  medicine_summary = short,
                  medicine         = product_id,
                  per_day
               ),
            by = join_by(medicine_summary)
         ) %>%
         mutate(
            service_faci = as.character(service_faci),
            unit_basis   = "2",
         ) %>%
         filter(!is.na(medicine)) %>%
         group_by(row_id) %>%
         mutate(
            disp_num = row_number(),
            # disp_total = typical_per_day * disp_total
         ) %>%
         ungroup() %>%
         select(
            rec_id,
            faci_id     = service_faci,
            sub_faci_id = service_sub_faci,
            medicine,
            disp_num,
            unit_basis,
            disp_total,
            medicine_left,
            medicine_missed,
            disp_date   = visit_date,
            next_date   = latest_next_date,
            created_at,
            created_by,
            updated_at,
            updated_by,
         ) %>%
         select(any_of(cols$px_medicine))

      # data$px_labs <- forms %>%
      #    select(
      #       rec_id,
      #       created_at,
      #       created_by,
      #       updated_at,
      #       updated_by,
      #       starts_with("lab"),
      #    ) %>%
      #    pivot_longer(
      #       cols      = starts_with("lab"),
      #       names_to  = "lab_data",
      #       values_to = "lab_value"
      #    ) %>%
      #    mutate(
      #       lab_test = substr(lab_data, 5, stri_locate_last_fixed(lab_data, "_") - 1),
      #       piece    = substr(lab_data, stri_locate_last_fixed(lab_data, "_") + 1, 1000),
      #    ) %>%
      #    mutate(
      #       lab_test = case_when(
      #          lab_test == "hbsag" ~ "1",
      #          lab_test == "crea" ~ "2",
      #          lab_test == "syph" ~ "3",
      #          lab_test == "vl" ~ "4",
      #          lab_test == "viral" ~ "4",
      #          lab_test == "cd4" ~ "5",
      #          lab_test == "xray" ~ "6",
      #          lab_test == "xpert" ~ "7",
      #          lab_test == "dssm" ~ "8",
      #          lab_test == "hivdr" ~ "9",
      #          lab_test == "hemo" ~ "10",
      #          lab_test == "hemog" ~ "10",
      #          TRUE ~ lab_test
      #       )
      #    ) %>%
      #    distinct(rec_id, created_at, created_by, lab_test, piece, .keep_all = TRUE) %>%
      #    pivot_wider(
      #       id_cols      = c(rec_id, created_at, created_by, lab_test),
      #       names_from   = piece,
      #       values_from  = lab_value,
      #       names_prefix = "lab_"
      #    ) %>%
      #    filter(!is.na(lab_date) | !is.na(lab_result)) %>%
      #    arrange(rec_id, lab_test) %>%
      #    mutate(
      #       lab_date = as.Date(parse_date_time(lab_date, c("Ymd", "mdY"))),
      #    )
   } else {
      # labs
      data$px_labs <- forms %>%
         select(
            rec_id,
            created_at,
            created_by,
            updated_at,
            updated_by,
            starts_with("lab"),
         ) %>%
         pivot_longer(
            cols      = starts_with("lab"),
            names_to  = "lab_data",
            values_to = "lab_value"
         ) %>%
         mutate(
            lab_test = substr(lab_data, 5, stri_locate_last_fixed(lab_data, "_") - 1),
            piece    = substr(lab_data, stri_locate_last_fixed(lab_data, "_") + 1, 1000),
         ) %>%
         mutate(
            lab_test = case_when(
               lab_test == "hbsag" ~ "1",
               lab_test == "crea" ~ "2",
               lab_test == "syph" ~ "3",
               lab_test == "vl" ~ "4",
               lab_test == "cd4" ~ "5",
               lab_test == "xray" ~ "6",
               lab_test == "xpert" ~ "7",
               lab_test == "dssm" ~ "8",
               lab_test == "hivdr" ~ "9",
               lab_test == "hemo" ~ "10",
               lab_test == "hemog" ~ "10",
               TRUE ~ lab_test
            )
         ) %>%
         distinct(rec_id, created_at, created_by, lab_test, piece, .keep_all = TRUE) %>%
         pivot_wider(
            id_cols      = c(rec_id, created_at, created_by, lab_test),
            names_from   = piece,
            values_from  = lab_value,
            names_prefix = "lab_"
         ) %>%
         filter(!is.na(lab_date) | !is.na(lab_result)) %>%
         arrange(rec_id, lab_test) %>%
         mutate(
            lab_date = as.Date(parse_date_time(lab_date, c("Ymd", "mdY"))),
         )

      data$px_key_pop <- forms %>%
         select(
            rec_id,
            created_at,
            created_by,
            updated_at,
            updated_by,
            contains("kp"),
         ) %>%
         rename_all(
            ~case_when(
               . == "kp_pdl" ~ "is_kp_1",
               . == "kp_tg" ~ "is_kp_2",
               . == "kp_pwid" ~ "is_kp_3",
               . == "kp_msm" ~ "is_kp_5",
               . == "kp_sw" ~ "is_kp_6",
               . == "kp_ofw" ~ "is_kp_7",
               . == "kp_partner" ~ "is_kp_8",
               . == "other_kp" ~ "is_kp_8888",
               TRUE ~ .
            )
         ) %>%
         pivot_longer(
            cols      = contains("kp"),
            names_to  = "kp",
            values_to = "is_kp"
         ) %>%
         mutate(
            kp       = stri_replace_all_fixed(kp, "is_kp_", ""),
            kp_other = if_else(
               condition = kp == "8888" & !is.na(is_kp),
               true      = is_kp,
               false     = NA_character_,
               missing   = NA_character_
            ),
            is_kp    = if_else(
               condition = kp == "8888" & !is.na(kp_other),
               true      = 1,
               false     = 0,
               missing   = 0
            ),
         ) %>%
         filter(is_kp == 1)

      data$px_oi <- forms %>%
         select(
            rec_id,
            created_at,
            created_by,
            updated_at,
            updated_by,
            contains("oi"),
         ) %>%
         rename_all(
            ~case_when(
               stri_replace_first_fixed(., "oi_", "") == "hiv" ~ "oi_101000",
               stri_replace_first_fixed(., "oi_", "") == "hepb" ~ "oi_102000",
               stri_replace_first_fixed(., "oi_", "") == "hepc" ~ "oi_103000",
               stri_replace_first_fixed(., "oi_", "") == "syph" ~ "oi_104000",
               stri_replace_first_fixed(., "oi_", "") == "pcp" ~ "oi_111000",
               stri_replace_first_fixed(., "oi_", "") == "cmv" ~ "oi_112000",
               stri_replace_first_fixed(., "oi_", "") == "orocand" ~ "oi_113000",
               stri_replace_first_fixed(., "oi_", "") == "herpes" ~ "oi_117000",
               stri_replace_first_fixed(., "oi_", "") == "tb" ~ "oi_202000",
               stri_replace_first_fixed(., "oi_", "") == "pcp" ~ "oi_111000",
               stri_replace_first_fixed(., "oi_", "") == "meningitis" ~ "oi_115000",
               stri_replace_first_fixed(., "oi_", "") == "oropharyngeal" ~ "oi_113000",
               stri_replace_first_fixed(., "oi_", "") == "toxoplasmosis" ~ "oi_116000",
               stri_replace_first_fixed(., "oi_", "") == "covid19" ~ "oi_201000",
               stri_replace_first_fixed(., "oi_", "") == "other" ~ "oi_8888",
               TRUE ~ .
            )
         ) %>%
         select(
            -oi_med_cotri,
            -oi_med_azithro,
            -oi_med_fluca,
         ) %>%
         pivot_longer(
            cols      = contains("oi"),
            names_to  = "oi",
            values_to = "is_oi"
         ) %>%
         mutate(
            oi       = stri_replace_all_fixed(oi, "oi_", ""),
            oi_other = if_else(
               condition = oi == "8888" & !is.na(is_oi),
               true      = is_oi,
               false     = NA_character_,
               missing   = NA_character_
            ),
            is_oi    = if_else(
               condition = oi == "8888" & !is.na(oi_other),
               true      = 1,
               false     = 0,
               missing   = 0
            ),
         ) %>%
         filter(is_oi == 1)


      # key_pop
      data$px_key_pop <- forms %>%
         select(
            rec_id,
            created_at,
            created_by,
            updated_at,
            updated_by,
            starts_with("kp_")
         ) %>%
         pivot_longer(
            cols      = starts_with("kp_"),
            names_to  = "kp",
            values_to = "is_kp"
         ) %>%
         mutate(
            kp = stri_replace_all_regex(kp, "^kp_", ""),
            kp = case_when(
               kp == "msm" ~ "5",
               kp == "tg" ~ "2",
               kp == "sw" ~ "6",
               kp == "pwid" ~ "3",
               kp == "pdl" ~ "1",
               kp == "ofw" ~ "7",
               kp == "partner" ~ "8",
               TRUE ~ kp
            )
         ) %>%
         filter(is_kp == 1) %>%
         bind_rows(
            forms %>%
               select(
                  rec_id,
                  created_at,
                  created_by,
                  updated_at,
                  updated_by,
                  kp_other = other_kp
               ) %>%
               filter(!is.na(kp_other)) %>%
               mutate(
                  kp    = "8888",
                  is_kp = "1"
               )
         )

      # other_service
      data$px_other_service <- forms %>%
         select(
            rec_id,
            created_at,
            created_by,
            updated_at,
            updated_by,
            starts_with("service_")
         ) %>%
         select(-service_type, -service_condom, -service_lube) %>%
         pivot_longer(
            cols      = starts_with("service_"),
            names_to  = "service",
            values_to = "given"
         ) %>%
         mutate(
            service = stri_replace_all_regex(service, "^service_", ""),
            service = case_when(
               service == "hiv_101" ~ "1013",
               service == "iec_mats" ~ "1004",
               service == "risk_counsel" ~ "1002",
               service == "prep_refer" ~ "5001",
               service == "ssnt_offer" ~ "5002",
               service == "ssnt_accept" ~ "5003",
               service == "given_condoms" ~ "2001",
               service == "given_lubes" ~ "2002",
               TRUE ~ service
            )
         ) %>%
         filter(given == 1) %>%
         bind_rows(
            forms %>%
               select(
                  rec_id,
                  created_at,
                  created_by,
                  updated_at,
                  updated_by,
                  other_service = service_condom
               ) %>%
               filter(!is.na(other_service)) %>%
               mutate(
                  service = "2001",
                  given   = "1"
               ),
            forms %>%
               select(
                  rec_id,
                  created_at,
                  created_by,
                  updated_at,
                  updated_by,
                  other_service = service_lube
               ) %>%
               filter(!is.na(other_service)) %>%
               mutate(
                  service = "2002",
                  given   = "1"
               )
         )

      data$px_vaccine <- forms %>%
         select(
            rec_id,
            created_at,
            created_by,
            updated_at,
            updated_by,
            starts_with("hepb"),
         ) %>%
         rename(hepb_date_first = hepb_date) %>%
         mutate(
            hepb_date_second = hepb_date_first,
            hepb_date_third  = hepb_date_first,
         ) %>%
         pivot_longer(
            cols      = starts_with("hepb"),
            names_to  = "vax_data",
            values_to = "vax_value"
         ) %>%
         mutate(
            vax_data    = stri_replace_all_fixed(vax_data, "hepb_", ""),
            piece       = substr(vax_data, 1, stri_locate_last_fixed(vax_data, "_") - 1),
            piece       = case_when(
               piece == "date" ~ "date",
               piece == "dose" ~ "result",
               TRUE ~ piece
            ),
            vax_num     = substr(vax_data, stri_locate_last_fixed(vax_data, "_") + 1, 1000),
            vax_num     = case_when(
               vax_num == "first" ~ "1",
               vax_num == "second" ~ "2",
               vax_num == "third" ~ "3",
               TRUE ~ vax_num
            ),
            disease_vax = "102000"
         ) %>%
         distinct(rec_id, created_at, created_by, disease_vax, vax_num, piece, .keep_all = TRUE) %>%
         pivot_wider(
            id_cols      = c(rec_id, created_at, created_by, disease_vax, vax_num),
            names_from   = piece,
            values_from  = vax_value,
            names_prefix = "vax_"
         ) %>%
         filter(vax_result == 1) %>%
         arrange(rec_id, disease_vax, vax_num) %>%
         mutate(
            vax_date = as.Date(parse_date_time(vax_date, c("Ymd", "mdY"))),
         )

      data$px_prophylaxis <- forms %>%
         select(
            rec_id,
            created_at,
            created_by,
            updated_at,
            updated_by,
            starts_with("oi_med"),
         ) %>%
         pivot_longer(
            cols      = starts_with("oi_med"),
            names_to  = "prophylaxis",
            values_to = "is_proph"
         ) %>%
         mutate(
            prophylaxis = case_when(
               prophylaxis == "oi_med_cotri" ~ "2",
               prophylaxis == "oi_med_azithro" ~ "3",
               prophylaxis == "oi_med_fluca" ~ "4",
               TRUE ~ prophylaxis
            )
         )

      data$px_remarks <- forms %>%
         select(
            rec_id,
            created_at,
            created_by,
            updated_at,
            updated_by,
            ends_with("_notes"),
         ) %>%
         pivot_longer(
            cols      = ends_with("_notes"),
            names_to  = "remark_type",
            values_to = "remarks"
         ) %>%
         mutate(
            remark_type = case_when(
               remark_type == "clinic_notes" ~ "1",
               remark_type == "counseling_notes" ~ "2",
               remark_type == "counselnotes" ~ "2",
               TRUE ~ remark_type
            )
         ) %>%
         filter(!is.na(remarks))

      data$px_medicine <- forms %>%
         select(
            rec_id,
            created_at,
            created_by,
            updated_at,
            updated_by,
            page_id,
            encoder,
            disp_date   = record_date,
            faci_id     = disp_faci,
            sub_faci_id = disp_sub_faci
         ) %>%
         full_join(
            y          = dispense %>%
               select(-NAME) %>%
               rename_all(tolower),
            by         = join_by(encoder, page_id, disp_date),
            na_matches = "never"
         ) %>%
         filter(!is.na(rec_id)) %>%
         mutate_at(
            .vars = vars(dose_per_day, total_dispensed_pills, pills_left),
            ~as.numeric(coalesce(., "0"))
         ) %>%
         mutate(
            unit_basis = "2",
            days       = floor((total_dispensed_pills + pills_left) / dose_per_day),
         )

      data$px_medicine %<>%
         mutate(
            next_pickup = coalesce(next_pickup, disp_date %m+% days(coalesce(days, 0)))
         ) %>%
         select(
            rec_id,
            faci_id,
            sub_faci_id,
            medicine,
            disp_num        = arv_num,
            batch_num,
            unit_basis,
            per_day         = dose_per_day,
            disp_total      = total_dispensed_pills,
            medicine_left   = pills_left,
            medicine_missed = doses_missed,
            disp_date,
            next_date       = next_pickup,
            created_at,
            created_by,
            updated_at,
            updated_by,
         )


      data$px_medicine_disc <- forms %>%
         select(
            rec_id,
            created_at,
            created_by,
            updated_at,
            updated_by,
            page_id,
            encoder,
            record_date,
            faci_id     = disp_faci,
            sub_faci_id = disp_sub_faci
         ) %>%
         inner_join(
            y  = discontinue %>%
               select(-NAME) %>%
               rename_all(tolower),
            by = join_by(encoder, page_id),
         ) %>%
         mutate(
            disc_date                = if_else(
               condition = is.na(disc_date),
               true      = record_date,
               false     = disc_date,
               missing   = disc_date
            ),
            reason_for_discontinuing = keep_code(reason_for_discontinuing)
         ) %>%
         select(
            rec_id,
            faci_id,
            sub_faci_id,
            medicine,
            disc_date,
            disc_reason       = reason_for_discontinuing,
            disc_reason_other = other_reasons,
            created_at,
            created_by,
            updated_at,
            updated_by,
         )
   }

   log_info("Finalizing upload schema.")
   schema <- list()
   for (table in tables) {
      schema[[table]] <- list(
         name = table,
         pk   = pks[[table]],
         data = data[[table]]
      )
   }

   log_success("Done!")
   return(schema)
}

deconstruct_vl <- function(forms, dispense = NULL, discontinue = NULL) {
   tables <- c(
      "px_record",
      "px_pii",
      "px_labs"
   )

   forms %<>%
      select(-any_of(c('branch', 'Branch'))) %>%
      rename_all(tolower) %>%
      mutate_at(
         .vars = vars(any_of(c(
            'sex',
            'self_ident',
            'civil_status',
            'educ_level',
            'client_type',
            'is_pregnant',
            'who_class',
            'visit_type',
            'tb_screen',
            'tb_ipt_status',
            'tx_status',
            'client_type'
         ))),
         ~keep_code(.)
      ) %>%
      mutate(
         client_mobile = str_replace_all(client_mobile, "[^[:digit:]]", ""),
         client_mobile = case_when(
            str_left(client_mobile, 1) == "9" ~ stri_c("0", client_mobile),
            str_left(client_mobile, 2) == "63" ~ str_replace(client_mobile, "^63", "0"),
            TRUE ~ client_mobile
         ),
         birthdate     = as.character(birthdate)
      )

   conn <- ohasis$conn("db")

   # primary keys
   log_info("Obtaining {green('Primary Keys')}.")
   pks        <- lapply(tables, function(table) dbGetQuery(conn, glue("show keys from ohasis.{table} where Key_name = 'primary'")))
   pks        <- lapply(pks, function(data) return(data$Column_name))
   names(pks) <- tables


   # columns
   log_info("Obtaining {green('Column Names')}.")
   cols        <- lapply(tables, function(table) dbGetQuery(conn, glue("show columns from ohasis.{table}")))
   cols        <- lapply(cols, function(data) return(data$Field))
   names(cols) <- tables

   dbDisconnect(conn)

   log_info("Creating tables using obtained schema.")
   data        <- lapply(tables, function(table, data, cols) {
      col_need      <- cols[[table]]
      col_not_found <- setdiff(col_need, names(data))

      schema <- data %>%
         mutate(
            !!!setNames(rep(NA_character_, length(col_not_found)), col_not_found)
         ) %>%
         select(any_of(col_need)) %>%
         distinct()

      return(schema)
   }, data = forms, cols = cols)
   names(data) <- tables

   log_info("Manually creating long tables.")
   data$px_labs <- forms %>%
      select(
         rec_id,
         created_at,
         created_by,
         updated_at,
         updated_by,
         starts_with("lab"),
      ) %>%
      mutate(
         lab_viral_date = as.character(lab_viral_date)
      ) %>%
      pivot_longer(
         cols      = starts_with("lab"),
         names_to  = "lab_data",
         values_to = "lab_value"
      ) %>%
      mutate(
         lab_test = substr(lab_data, 5, stri_locate_last_fixed(lab_data, "_") - 1),
         piece    = substr(lab_data, stri_locate_last_fixed(lab_data, "_") + 1, 1000),
      ) %>%
      mutate(
         lab_test = case_when(
            lab_test == "hbsag" ~ "1",
            lab_test == "crea" ~ "2",
            lab_test == "syph" ~ "3",
            lab_test == "vl" ~ "4",
            lab_test == "viral" ~ "4",
            lab_test == "cd4" ~ "5",
            lab_test == "xray" ~ "6",
            lab_test == "xpert" ~ "7",
            lab_test == "dssm" ~ "8",
            lab_test == "hivdr" ~ "9",
            lab_test == "hemo" ~ "10",
            lab_test == "hemog" ~ "10",
            TRUE ~ lab_test
         )
      ) %>%
      distinct(rec_id, created_at, created_by, lab_test, piece, .keep_all = TRUE) %>%
      pivot_wider(
         id_cols      = c(rec_id, created_at, created_by, lab_test),
         names_from   = piece,
         values_from  = lab_value,
         names_prefix = "lab_"
      ) %>%
      filter(!is.na(lab_date) | !is.na(lab_result)) %>%
      arrange(rec_id, lab_test) %>%
      mutate(
         lab_date = as.Date(parse_date_time(lab_date, c("Ymd", "mdY"))),
      )

   log_info("Finalizing upload schema.")
   schema <- list()
   for (table in tables) {
      schema[[table]] <- list(
         name = table,
         pk   = pks[[table]],
         data = data[[table]]
      )
   }

   log_success("Done!")
   return(schema)
}

deconstruct_prep <- function(forms) {
   tables <- c(
      "px_record",
      "px_pii",
      "px_profile",
      "px_service",
      "px_expose_hist",
      "px_expose_profile",
      "px_occupation",
      "px_key_pop",
      "px_remarks",
      "px_labs",
      "px_vitals",
      "px_ars_sx",
      "px_sti_sx",
      "px_prep",
      "px_prep_status",
      "px_prep_checklist",
      "px_prep_finance",
      "px_prep_refuse",
      "px_medicine"
   )

   forms %<>%
      select(-any_of(c('branch', 'Branch'))) %>%
      rename_all(tolower) %>%
      mutate_at(
         .vars = vars(any_of(c(
            'sex',
            'self_ident'
         ))),
         ~keep_code(.)
      ) %>%
      mutate(
         client_mobile = str_replace_all(client_mobile, "[^[:digit:]]", ""),
         client_mobile = case_when(
            str_left(client_mobile, 1) == "9" ~ stri_c("0", client_mobile),
            str_left(client_mobile, 2) == "63" ~ str_replace(client_mobile, "^63", "0"),
            TRUE ~ client_mobile
         ),
         birthdate     = as.character(birthdate)
      )

   conn <- ohasis$conn("db")

   # primary keys
   log_info("Obtaining {green('Primary Keys')}.")
   pks        <- lapply(tables, function(table) dbGetQuery(conn, glue("show keys from ohasis.{table} where Key_name = 'primary'")))
   pks        <- lapply(pks, function(data) return(data$Column_name))
   names(pks) <- tables


   # columns
   log_info("Obtaining {green('Column Names')}.")
   cols        <- lapply(tables, function(table) dbGetQuery(conn, glue("show columns from ohasis.{table}")))
   cols        <- lapply(cols, function(data) return(data$Field))
   names(cols) <- tables

   dbDisconnect(conn)

   log_info("Creating tables using obtained schema.")
   data        <- lapply(tables, function(table, data, cols) {
      col_need      <- cols[[table]]
      col_not_found <- setdiff(col_need, names(data))

      schema <- data %>%
         mutate(
            !!!setNames(rep(NA_character_, length(col_not_found)), col_not_found)
         ) %>%
         select(any_of(col_need)) %>%
         distinct()

      return(schema)
   }, data = forms, cols = cols)
   names(data) <- tables

   log_info("Manually creating long tables.")
   data$px_labs <- forms %>%
      select(
         rec_id,
         created_at,
         created_by,
         updated_at,
         updated_by,
         starts_with("lab"),
      ) %>%
      mutate_all(as.character) %>%
      pivot_longer(
         cols      = starts_with("lab"),
         names_to  = "lab_data",
         values_to = "lab_value"
      ) %>%
      mutate(
         lab_test = substr(lab_data, 5, stri_locate_last_fixed(lab_data, "_") - 1),
         piece    = substr(lab_data, stri_locate_last_fixed(lab_data, "_") + 1, 1000),
      ) %>%
      mutate(
         lab_test = case_when(
            lab_test == "hbsag" ~ "1",
            lab_test == "crea" ~ "2",
            lab_test == "syph" ~ "3",
            lab_test == "vl" ~ "4",
            lab_test == "viral" ~ "4",
            lab_test == "cd4" ~ "5",
            lab_test == "xray" ~ "6",
            lab_test == "xpert" ~ "7",
            lab_test == "dssm" ~ "8",
            lab_test == "hivdr" ~ "9",
            lab_test == "hemo" ~ "10",
            lab_test == "hemog" ~ "10",
            TRUE ~ lab_test
         )
      ) %>%
      distinct(rec_id, created_at, created_by, lab_test, piece, .keep_all = TRUE) %>%
      pivot_wider(
         id_cols      = c(rec_id, created_at, created_by, lab_test),
         names_from   = piece,
         values_from  = lab_value,
         names_prefix = "lab_"
      ) %>%
      filter(!is.na(lab_date) | !is.na(lab_result)) %>%
      arrange(rec_id, lab_test) %>%
      mutate(
         lab_date = as.Date(parse_date_time(lab_date, c("Ymd", "mdY"))),
      )

   data$px_key_pop <- forms %>%
      select(
         rec_id,
         created_at,
         created_by,
         updated_at,
         updated_by,
         starts_with("kp_"),
      ) %>%
      rename_all(
         ~case_when(
            . == "kp_pdl" ~ "is_kp_1",
            . == "kp_tg" ~ "is_kp_2",
            . == "kp_pwid" ~ "is_kp_3",
            . == "kp_msm" ~ "is_kp_5",
            . == "kp_sw" ~ "is_kp_6",
            . == "kp_ofw" ~ "is_kp_7",
            . == "kp_partner" ~ "is_kp_8",
            . == "kp_other" ~ "is_kp_8888",
            TRUE ~ .
         )
      ) %>%
      pivot_longer(
         cols      = contains("kp"),
         names_to  = "kp",
         values_to = "is_kp"
      ) %>%
      mutate(
         kp = stri_replace_all_fixed(kp, "is_kp_", ""),
      )

   data$px_expose_hist <- forms %>%
      select(
         rec_id,
         created_at,
         created_by,
         updated_by,
         updated_at,
         contains("risk"),
      ) %>%
      pivot_longer(
         cols      = starts_with("risk_"),
         names_to  = "exposure",
         values_to = "expose_value"
      ) %>%
      mutate(
         exposure         = str_replace(exposure, "^risk_", ""),
         exposure         = case_when(
            exposure == "condomless_anal" ~ "261200",
            exposure == "condomless_vaginal" ~ "262200",
            exposure == "drug_inject" ~ "311010",
            exposure == "drug_sex" ~ "330000",
            exposure == "transact_sex" ~ "200030",
            exposure == "hiv_vl_unknown" ~ "200001",
            exposure == "hiv_unknown" ~ "230000",
            exposure == "sti" ~ "400000",
            exposure == "pep" ~ "320002",
            TRUE ~ exposure
         ),
         is_exposed       = case_when(
            expose_value == "4_Yes, within the past 30 days" ~ "1",
            expose_value == "3_Yes, within the past 6 months" ~ "1",
            expose_value == "2_Yes" ~ "1",
            expose_value == "0_No" ~ "0",
         ),
         type_last_expose = case_when(
            expose_value == "4_Yes, within the past 30 days" ~ "1",
            expose_value == "3_Yes, within the past 6 months" ~ "3",
            expose_value == "2_Yes" ~ "0",
         )
      ) %>%
      select(-expose_value)

   data$px_vitals <- forms %>%
      mutate(
         body_temp = NA_character_
      ) %>%
      select(
         rec_id,
         created_by,
         created_at,
         updated_by,
         updated_at,
         weight,
         body_temp
      ) %>%
      pivot_longer(
         names_to  = "vital_sign",
         cols      = c(weight, body_temp),
         values_to = "vital_result"
      ) %>%
      mutate(
         vital_sign = case_when(
            vital_sign == "weight" ~ "2",
            vital_sign == "body_temp" ~ "3",
            TRUE ~ vital_sign
         )
      )

   data$px_ars_sx <- forms %>%
      select(
         rec_id,
         created_by,
         created_at,
         updated_by,
         updated_at,
         starts_with("ars_sx_")
      ) %>%
      distinct() %>%
      pivot_longer(
         cols      = starts_with("ars_sx_"),
         names_to  = "ars_symptom",
         values_to = "symptom_value"
      ) %>%
      mutate(
         symptom_data = if_else(str_detect(ars_symptom, "_text"), "symptom_other", "is_symptom"),
         ars_symptom  = str_replace(ars_symptom, "^expose_", ""),
         ars_symptom  = str_replace(ars_symptom, "_text$", ""),

         ars_symptom  = case_when(
            ars_symptom == "ars_sx_fever" ~ "1",
            ars_symptom == "ars_sx_sore_throat" ~ "2",
            ars_symptom == "ars_sx_diarrhea" ~ "3",
            ars_symptom == "ars_sx_swollen_lymph" ~ "4",
            ars_symptom == "ars_sx_swollen_tonsils" ~ "5",
            ars_symptom == "ars_sx_rash" ~ "6",
            ars_symptom == "ars_sx_muscle_pains" ~ "7",
            ars_symptom == "ars_sx_other" ~ "8888",
            ars_symptom == "ars_sx_other_text" ~ "8888",
            ars_symptom == "ars_sx_none" ~ "9999",
            TRUE ~ ars_symptom
         ),
      ) %>%
      distinct(rec_id, symptom_data, ars_symptom, .keep_all = TRUE) %>%
      pivot_wider(
         names_from  = symptom_data,
         values_from = symptom_value,
      ) %>%
      mutate(
         is_symptom = keep_code(is_symptom),
         is_symptom = coalesce(is_symptom, "0"),
      )

   data$px_sti_sx <- forms %>%
      select(
         rec_id,
         created_by,
         created_at,
         updated_by,
         updated_at,
         starts_with("sti_sx_")
      ) %>%
      distinct() %>%
      pivot_longer(
         cols      = starts_with("sti_sx_"),
         names_to  = "sti_symptom",
         values_to = "symptom_value"
      ) %>%
      mutate(
         symptom_data = if_else(str_detect(sti_symptom, "_text"), "symptom_other", "is_symptom"),
         sti_symptom  = str_replace(sti_symptom, "^expose_", ""),
         sti_symptom  = str_replace(sti_symptom, "_text$", ""),

         sti_symptom  = case_when(
            sti_symptom == "sti_sx_discharge_vaginal" ~ "1",
            sti_symptom == "sti_sx_discharge_anal" ~ "2",
            sti_symptom == "sti_sx_discharge_urethral" ~ "3",
            sti_symptom == "sti_sx_swollen_scrotum" ~ "4",
            sti_symptom == "sti_sx_pain_urine" ~ "5",
            sti_symptom == "sti_sx_ulcer_genital" ~ "6",
            sti_symptom == "sti_sx_ulcer_oral" ~ "7",
            sti_symptom == "sti_sx_warts_genital" ~ "8",
            sti_symptom == "sti_sx_pain_abdomen" ~ "9",
            sti_symptom == "sti_sx_other" ~ "8888",
            sti_symptom == "sti_sx_other_text" ~ "8888",
            sti_symptom == "sti_sx_none" ~ "9999",
            TRUE ~ sti_symptom
         ),
      ) %>%
      pivot_wider(
         names_from  = symptom_data,
         values_from = symptom_value,
      ) %>%
      mutate(
         is_symptom = keep_code(is_symptom),
         is_symptom = coalesce(is_symptom, "0"),
      )

   data$px_prep_checklist <- forms %>%
      select(
         rec_id,
         created_by,
         created_at,
         updated_by,
         updated_at,
         starts_with("pre_init_")
      ) %>%
      rename_all(
         ~case_when(
            . == "pre_init_hiv_nr" ~ "is_checked_1",
            . == "pre_init_weight" ~ "is_checked_2",
            . == "pre_init_no_ars" ~ "is_checked_3",
            . == "pre_init_crea_clear" ~ "is_checked_4",
            . == "pre_init_no_arv_allergy" ~ "is_checked_5",
            TRUE ~ .
         )
      ) %>%
      pivot_longer(
         cols      = contains("checked"),
         names_to  = "requirement",
         values_to = "is_checked"
      ) %>%
      mutate(
         requirement = stri_replace_all_fixed(requirement, "is_checked_", ""),
         is_checked  = keep_code(is_checked),
      )

   data$px_medicine <- forms %>%
      mutate(
         per_day         = 1,
         disp_num        = 1,
         medicine        = if_else(!is.na(medicine_summary), "2028", NA_character_),
         medicine_missed = NA_character_,
         next_date       = disp_date %m+% days(disp_total * 30),
         unit_basis      = 1,
      ) %>%
      filter(!is.na(medicine_summary)) %>%
      select(
         rec_id,
         faci_id,
         sub_faci_id,
         medicine,
         disp_num,
         unit_basis,
         per_day,
         disp_total,
         medicine_left,
         medicine_missed,
         disp_date,
         next_date
      )

   log_info("Finalizing upload schema.")
   schema <- list()
   for (table in tables) {
      schema[[table]] <- list(
         name = table,
         pk   = pks[[table]],
         data = data[[table]]
      )
   }

   log_success("Done!")
   return(schema)
}

psgc_aem <- function(ref_addr) {
   local_drive_quiet()

   # process data
   aem <- read_xlsx('C:/Users/Bene-G16/Downloads/PLHIV est 2023-2027_14Nov2025 (1).xlsx', col_types = "text", .name_repair = "unique_quiet") %>%
      select(1:10) %>%
      slice(3:nrow(.)) %>%
      rename(
         province = prov,
         est2023  = 6,
         est2024  = 7,
         est2025  = 8,
         est2026  = 9,
         est2027  = 10,
      ) %>%
      mutate_at(
         .vars = vars(region),
         ~str_replace(., "\\.0$", "")
      ) %>%
      mutate_at(
         .vars = vars(starts_with("est2")),
         ~as.integer(.)
      ) %>%
      mutate(
         est_type  = case_when(
            is.na(region) ~ "adjust",
            !is.na(region) & is.na(province) & is.na(muncity) ~ "region",
            TRUE ~ "estimate"
         ),
         region    = case_when(
            stri_detect_regex(region, "^Discrepancy") ~ "UNKNOWN",
            TRUE ~ region
         ),
         muncity   = case_when(
            region == "9" & province == "BASILAN" ~ "ISABELA",
            TRUE ~ muncity
         ),
         province  = case_when(
            region == "9" & province == "BASILAN" ~ "BASILAN-RO9",
            TRUE ~ province
         ),
         unknown   = if_else(is.na(muncity), 1, 0, 0),
         aem_class = if_else(unknown == 1, "non a", aem_class, aem_class),
         munc_alt  = if_else(muncity == "ROTP", "UNKNOWN", muncity, muncity)
      ) %>%
      mutate_at(
         .vars = vars(province, muncity, munc_alt),
         ~if_else(unknown == 1, "UNKNOWN", ., .)
      )

   est_reg_adjust <- aem %>%
      filter(est_type == "region") %>%
      select(
         region,
         starts_with("est2"),
      ) %>%
      left_join(
         y  = aem %>%
            filter(est_type == "estimate") %>%
            group_by(region) %>%
            summarise_at(
               .vars = vars(starts_with("est2")),
               ~sum(., na.rm = TRUE)
            ) %>%
            rename_all(
               ~case_when(
                  stri_detect_regex(., "^est2") ~ paste0("sub_", .),
                  TRUE ~ .
               )
            ),
         by = "region"
      )

   est_reg_adjust %<>%
      mutate(
         across(
            names(select(., starts_with("est2", ignore.case = FALSE))),
            ~as.integer(. - coalesce(pull(est_reg_adjust, str_c("sub_", cur_column())), 0))
         )
      ) %>%
      mutate(
         province  = "UNKNOWN",
         muncity   = "UNKNOWN",
         munc_alt  = "UNKNOWN",
         aem_class = "non a",
         est_type  = "adjust",
         unknown   = 1
      ) %>%
      select(
         region,
         province,
         muncity,
         munc_alt,
         aem_class,
         est_type,
         unknown,
         starts_with("est2"),
      )

   new_ref <- ohasis$ref_addr %>%
      select(
         region    = nhsss_reg,
         province  = nhsss_prov,
         muncity   = nhsss_munc,
         psgc_reg  = reg,
         psgc_prov = prov,
         psgc_munc = munc,
         name_reg,
         name_prov,
         name_munc
      ) %>%
      distinct() %>%
      mutate(
         province = case_when(
            province == 'MAGUINDANAO DEL NORTE' ~ 'MAGUINDANAO',
            province == 'MAGUINDANAO DEL SUR' ~ 'MAGUINDANAO',
            TRUE ~ province
         ),
         drop     = case_when(
            str_left(psgc_prov, 5) == "13806" & (psgc_munc != "1380600000" | coalesce(psgc_munc, '') == "") ~ 1,
            str_left(psgc_reg, 4) == "1300" & coalesce(psgc_munc, '') == "" ~ 1,
            stri_detect_fixed(toupper(name_prov), "CITY") & muncity == "UNKNOWN" ~ 1,
            TRUE ~ 0
         ),
      ) %>%
      filter(drop == 0)

   ref_aem <- aem %>%
      filter(est_type == "estimate") %>%
      bind_rows(est_reg_adjust) %>%
      left_join(
         y  = new_ref %>%
            select(
               region,
               province,
               muncity,
               psgc_reg,
               psgc_prov,
               psgc_munc
            ) %>%
            mutate(
               muncity = if_else(province != "UNKNOWN" & muncity == "UNKNOWN", "ROTP", muncity, muncity)
            ),
         by = join_by(region, province, muncity)
      ) %>%
      left_join(
         y  = new_ref %>%
            select(
               region,
               province,
               rotp_psgc_reg  = psgc_reg,
               rotp_psgc_prov = psgc_prov,
               name_prov,
            ) %>%
            mutate(
               drop = case_when(
                  region == "NCR" & province == "NCR" ~ 1,
                  rotp_psgc_prov == "129800000" ~ 1,
                  str_detect(toupper(name_prov), 'CITY') ~ 1,
                  TRUE ~ 0
               )
            ) %>%
            filter(drop == 0) %>%
            select(-name_prov) %>%
            distinct(),
         by = join_by(region, province)
      ) %>%
      mutate(
         psgc_reg  = coalesce(psgc_reg, rotp_psgc_reg),
         psgc_prov = coalesce(psgc_prov, rotp_psgc_prov),
         psgc_aem  = coalesce(psgc_munc, psgc_prov)
      ) %>%
      select(-rotp_psgc_reg, -rotp_psgc_prov)

   aem_sites <- ref_aem %>%
      filter(muncity != "ROTP", province != "BASILAN-RO9") %>%
      select(
         aem_class_sites = aem_class,
         psgc_reg,
         psgc_prov,
         psgc_munc
      )

   aem_rotp <- ref_aem %>%
      filter(muncity == "ROTP" | province == "BASILAN-RO9") %>%
      mutate(
         aem_class = "non a"
      ) %>%
      select(
         aem_class_rotp = aem_class,
         psgc_reg,
         psgc_prov
      )

   # rename columns
   final_ref <- new_ref %>%
      rename(
         nhsss_reg  = region,
         nhsss_prov = province,
         nhsss_munc = muncity
      ) %>%
      left_join(aem_sites, join_by(psgc_reg, psgc_prov, psgc_munc), na_matches = "never") %>%
      left_join(aem_rotp, join_by(psgc_reg, psgc_prov), na_matches = "never") %>%
      distinct() %>%
      mutate(
         aem_class = coalesce(aem_class_sites, aem_class_rotp, 'non a'),
         rotp      = if_else(is.na(aem_class_sites) & !is.na(aem_class_rotp), 1, 0, 0),
      ) %>%
      mutate(
         name_reg  = case_when(
            psgc_munc == "1908703000" ~ "Region XII (SOCCSKSARGEN)", # cotabato city temp under 12-cotabato
            psgc_prov == "1999900000" ~ "Region XII (SOCCSKSARGEN)", # special geo area temp under 12-cotabato
            TRUE ~ name_reg
         ),
         name_prov = case_when(
            stri_detect_fixed(name_prov, "NCR") ~ stri_replace_all_fixed(name_prov, " (Not a Province)", ""),
            psgc_munc == "1908703000" ~ "Cotabato", # cotabato city temp under 12-cotabato
            psgc_prov == "1999900000" ~ "Cotabato", # special geo area temp under 12-cotabato
            TRUE ~ name_prov
         ),
         name_munc = case_when(
            psgc_munc == "0301405000" ~ "Bulacan City",
            TRUE ~ name_munc
         ),
         name_aem  = case_when(
            psgc_munc == "0301405000" ~ "Bulacan City",
            psgc_prov == "0990100000" ~ "Basilan Province",
            psgc_munc == "1908703000" ~ "Cotabato Province", # cotabato city temp under 12-cotabato
            psgc_prov == "1999900000" ~ "Cotabato Province", # special geo area temp under 12-cotabato
            psgc_prov == "1908700000" ~ "Maguindanao Province",
            psgc_prov == "1908800000" ~ "Maguindanao Province",
            aem_class %in% c("a", "ncr", "cebu city", "cebu province") ~ name_munc,
            rotp == 1 & !grepl("Province", name_prov) ~ str_c(name_prov, " Province"),
            TRUE ~ name_prov
         ),
         nhsss_aem = case_when(
            psgc_munc == "0301405000" ~ "BULACAN",
            # PSGC_MUNC == "129804000" ~ "ROTP",
            aem_class %in% c("a", "ncr", "cebu city", "cebu province") ~ nhsss_munc,
            rotp == 1 & !grepl("Province", nhsss_prov) ~ "ROTP",
            TRUE ~ "ROTP"
         ),

         psgc_reg  = case_when(
            psgc_munc == "1908703000" ~ "1200000000", # cotabato city temp under 12-cotabato
            psgc_prov == "1999900000" ~ "1200000000", # special geo area temp under 12-cotabato
            TRUE ~ psgc_reg
         ),
         psgc_prov = case_when(
            psgc_munc == "1908703000" ~ "1204700000", # cotabato city temp under 12-cotabato
            psgc_prov == "1999900000" ~ "1204700000", # special geo area temp under 12-cotabato
            TRUE ~ psgc_prov
         ),
         # aem tagging
         psgc_aem  = case_when(
            psgc_munc == "1908703000" ~ "1204700000", # cotabato city temp under 12-cotabato
            psgc_prov == "1999900000" ~ "1204700000", # special geo area temp under 12-cotabato
            psgc_prov == "1908700000" ~ "1908700000",
            psgc_prov == "1908800000" ~ "1908700000",
            aem_class %in% c("a", "ncr", "cebu city", "cebu province") ~ psgc_munc,
            TRUE ~ psgc_prov
         ),
      ) %>%
      select(-aem_class_sites, -aem_class_rotp, -rotp, -drop) %>%
      filter(psgc_munc != '1999900000') %>%
      filter(psgc_munc != '1908800000') %>%
      mutate(
         # correction for HUCs and NCR provinces
         psgc_aem  = case_when(
            psgc_aem == "0990100000" ~ "0990101000",
            TRUE ~ psgc_aem
         ),
         psgc_prov = case_when(
            psgc_munc == '0330100000' ~ '0305400000',
            psgc_munc == '0331400000' ~ '0307100000',
            psgc_munc == '0431200000' ~ '0405600000',
            psgc_munc == '0631000000' ~ '0603000000',
            psgc_munc == '0730600000' ~ '0702200000',
            psgc_munc == '0731100000' ~ '0702200000',
            psgc_munc == '0731300000' ~ '0702200000',
            psgc_munc == '0831600000' ~ '0803700000',
            psgc_munc == '0931700000' ~ '0907300000',
            psgc_munc == '1030500000' ~ '1004300000',
            psgc_munc == '1030900000' ~ '1003500000',
            psgc_munc == '1130700000' ~ '1102400000',
            psgc_munc == '1230800000' ~ '1206300000',
            psgc_munc == '1380100000' ~ '1330000000',
            psgc_munc == '1380200000' ~ '1340000000',
            psgc_munc == '1380300000' ~ '1340000000',
            psgc_munc == '1380400000' ~ '1330000000',
            psgc_munc == '1380500000' ~ '1320000000',
            psgc_munc == '1380600000' ~ '1310000000',
            psgc_munc == '1380700000' ~ '1320000000',
            psgc_munc == '1380800000' ~ '1340000000',
            psgc_munc == '1380900000' ~ '1330000000',
            psgc_munc == '1381000000' ~ '1340000000',
            psgc_munc == '1381100000' ~ '1340000000',
            psgc_munc == '1381200000' ~ '1320000000',
            psgc_munc == '1381300000' ~ '1320000000',
            psgc_munc == '1381400000' ~ '1320000000',
            psgc_munc == '1381500000' ~ '1340000000',
            psgc_munc == '1381600000' ~ '1330000000',
            psgc_munc == '1430300000' ~ '1401100000',
            psgc_munc == '1630400000' ~ '1600200000',
            psgc_munc == '1731500000' ~ '1705300000',
            psgc_munc == '1830200000' ~ '1804500000',
            psgc_munc == '1381701000' ~ '1340000000',
            TRUE ~ psgc_prov
         ),
         name_prov = case_when(
            psgc_munc == '0330100000' ~ 'Pampanga',
            psgc_munc == '0331400000' ~ 'Zambales',
            psgc_munc == '0431200000' ~ 'Quezon',
            psgc_munc == '0631000000' ~ 'Iloilo',
            psgc_munc == '0730600000' ~ 'Cebu',
            psgc_munc == '0731100000' ~ 'Cebu',
            psgc_munc == '0731300000' ~ 'Cebu',
            psgc_munc == '0831600000' ~ 'Leyte',
            psgc_munc == '0931700000' ~ 'Zamboanga del Sur',
            psgc_munc == '0990100000' ~ 'Basilan',
            psgc_munc == '1030500000' ~ 'Misamis Oriental',
            psgc_munc == '1030900000' ~ 'Lanao del Norte',
            psgc_munc == '1130700000' ~ 'Davao del Sur',
            psgc_munc == '1230800000' ~ 'South Cotabato',
            psgc_munc == '1380100000' ~ 'Northern Manila District (Camanava) (3rd District)',
            psgc_munc == '1380200000' ~ 'Southern Manila District (4th District)',
            psgc_munc == '1380300000' ~ 'Southern Manila District (4th District)',
            psgc_munc == '1380400000' ~ 'Northern Manila District (Camanava) (3rd District)',
            psgc_munc == '1380500000' ~ 'Eastern Manila District (2nd District)',
            psgc_munc == '1380600000' ~ 'Capital District (1st District)',
            psgc_munc == '1380700000' ~ 'Eastern Manila District (2nd District)',
            psgc_munc == '1380800000' ~ 'Southern Manila District (4th District)',
            psgc_munc == '1380900000' ~ 'Northern Manila District (Camanava) (3rd District)',
            psgc_munc == '1381000000' ~ 'Southern Manila District (4th District)',
            psgc_munc == '1381100000' ~ 'Southern Manila District (4th District)',
            psgc_munc == '1381200000' ~ 'Eastern Manila District (2nd District)',
            psgc_munc == '1381300000' ~ 'Eastern Manila District (2nd District)',
            psgc_munc == '1381400000' ~ 'Eastern Manila District (2nd District)',
            psgc_munc == '1381500000' ~ 'Southern Manila District (4th District)',
            psgc_munc == '1381600000' ~ 'Northern Manila District (Camanava) (3rd District)',
            psgc_munc == '1430300000' ~ 'Benguet',
            psgc_munc == '1630400000' ~ 'Agusan del Norte',
            psgc_munc == '1731500000' ~ 'Palawan',
            psgc_munc == '1830200000' ~ 'Negros Occidental',
            psgc_munc == '1381701000' ~ 'Southern Manila District (4th District)',
            TRUE ~ name_prov
         )
      )

   final_aem <- ref_aem %>%
      left_join(
         y  = final_ref %>%
            select(
               psgc_munc,
               psgc_aem,
               name_reg,
               name_prov,
               name_aem
            ) %>%
            distinct_all(),
         by = join_by(psgc_aem, psgc_munc)
      ) %>%
      # filter(name_prov != 'City of Isabela (Not a Province)') %>%
      select(
         nhsss_reg  = region,
         nhsss_prov = province,
         nhsss_munc = muncity,
         name_reg,
         name_prov,
         name_aem,
         aem_class,
         starts_with("est2"),
         starts_with("psgc"),
      ) %>%
      pivot_longer(
         cols      = starts_with("est2"),
         names_to  = "report_yr",
         values_to = "est"
      ) %>%
      mutate(
         report_date = as.numeric(stri_replace_first_fixed(report_yr, "est", "")),
         report_date = stri_c(report_date, '-12-31')
      ) %>%
      select(-report_yr) %>%
      filter(!is.na(name_reg))

   unlist(tmpfile)
   refs <- list(
      addr = final_ref,
      aem  = final_aem
   )

   return(refs)
}


# conn <- connect('mariadb-lw')
# dbxInsert(conn, Id(schema = 'dashboard', table = 'estimates'), final_aem)
# dbxInsert(conn, Id(schema = 'dashboard', table = 'ref_aem'), final_ref %>% mutate(psgc = coalesce(psgc_munc, psgc_prov, psgc_reg)))
#
# data <- QB$new(conn)$from('harp_dx.corr_dxlab')$get()
#
# unique <- data %>% distinct(dx_region, dx_province, dx_muncity, dxlab_standard, .keep_all = TRUE)
# dbxInsert(conn, Id(schema = 'harp_dx', table = 'corr_dxlab'), unique)
#
# try <- dbGetQuery(conn, "select distinct cast(faci_id as String) as faci_id,
#                              coalesce(if(addr_psgc_prov = '1380600000', '1380600000', addr_psgc_munc), addr_psgc_prov,
#                                       addr_psgc_reg) as psgc,
#                              addr_name_munc,
#                              psgc_aem
#              from ohasis_lake.ref_faci
#                       left join dashboard.ref_aem on psgc = ref_aem.psgc
#              where deleted_at is null")
#
# try %>% get_dupes(faci_id)
