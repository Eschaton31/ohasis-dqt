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
      "patients",
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
         left_join(
            y  = products %>%
               select(
                  medicine_summary = short,
                  medicine         = product_id
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
            per_day,
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