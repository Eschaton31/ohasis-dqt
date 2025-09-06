##  Deduplication functions used in processing data ----------------------------

dedup_prep2 <- function(
   data = NULL,
   name_f = NULL,
   name_m = NULL,
   name_l = NULL,
   name_s = NULL,
   uic = NULL,
   birthdate = NULL,
   code_confirm = NULL,
   code_px = NULL,
   phic = NULL,
   philsys = NULL
) {
   upper_utc <- function(x) stri_trans_general(stri_trans_toupper(x), "latin-ascii")
   dedup_new <- as.data.table(data)

   # derpase if provided as name
   name_f       <- deparse(substitute(name_f))
   name_m       <- deparse(substitute(name_m))
   name_l       <- deparse(substitute(name_l))
   name_s       <- deparse(substitute(name_s))
   uic          <- deparse(substitute(uic))
   birthdate    <- deparse(substitute(birthdate))
   code_confirm <- deparse(substitute(code_confirm))
   code_px      <- deparse(substitute(code_px))
   phic         <- deparse(substitute(phic))
   philsys      <- deparse(substitute(philsys))

   dedup_new[, LAST := lapply(.SD, upper_utc), .SDcols = name_l]
   dedup_new[, MIDDLE := lapply(.SD, upper_utc), .SDcols = name_m]
   dedup_new[, FIRST := lapply(.SD, upper_utc), .SDcols = name_f]
   dedup_new[, SUFFIX := lapply(.SD, upper_utc), .SDcols = name_s]
   dedup_new[, UIC := lapply(.SD, upper_utc), .SDcols = uic]
   dedup_new[, CONFIRMATORY_CODE := lapply(.SD, upper_utc), .SDcols = code_confirm]
   dedup_new[, PATIENT_CODE := lapply(.SD, upper_utc), .SDcols = code_px]

   # get components of birthdate
   dedup_new[, c("BIRTH_YR", "BIRTH_MO", "BIRTH_DY") := tstrsplit(get(birthdate), "-", fixed = TRUE)]
   dedup_new[, c("BIRTH_YR", "BIRTH_MO", "BIRTH_DY") := lapply(.SD, as.numeric), .SDcols = c("BIRTH_YR", "BIRTH_MO", "BIRTH_DY")]

   # extract parent info from uic
   dedup_new[, UIC_MOM := fifelse(!is.na(UIC), substr(UIC, 1, 2), NA_character_)]
   dedup_new[, UIC_DAD := fifelse(!is.na(UIC), substr(UIC, 3, 4), NA_character_)]
   dedup_new[, UIC_ORDER := fifelse(!is.na(UIC), substr(UIC, 5, 6), NA_character_)]

   # variables for first 3 letters of names
   dedup_new[, FIRST_A := fifelse(!is.na(FIRST), substr(FIRST, 1, 3), NA_character_)]
   dedup_new[, MIDDLE_A := fifelse(!is.na(MIDDLE), substr(MIDDLE, 1, 3), NA_character_)]
   dedup_new[, LAST_A := fifelse(!is.na(LAST), substr(LAST, 1, 3), NA_character_)]

   dedup_new[, LAST := if_else(is.na(LAST), MIDDLE, LAST)]
   dedup_new[, MIDDLE := if_else(is.na(MIDDLE), LAST, MIDDLE)]

   # clean ids
   dedup_new[, CONFIRM_SIEVE := fifelse(!is.na(get(code_confirm)), str_replace_all(get(code_confirm), "[^[:alnum:]]", ""), NA_character_)]
   dedup_new[, PXCODE_SIEVE := fifelse(!is.na(get(code_px)), str_replace_all(get(code_px), "[^[:alnum:]]", ""), NA_character_)]
   dedup_new[, FIRST_S := fifelse(!is.na(FIRST), str_replace_all(FIRST, "[^[:alnum:]]", ""), NA_character_)]
   dedup_new[, MIDDLE_S := fifelse(!is.na(MIDDLE), str_replace_all(MIDDLE, "[^[:alnum:]]", ""), NA_character_)]
   dedup_new[, LAST_S := fifelse(!is.na(LAST), str_replace_all(LAST, "[^[:alnum:]]", ""), NA_character_)]
   dedup_new[, PHIC := fifelse(!is.na(get(phic)), str_replace_all(get(phic), "[^[:alnum:]]", ""), NA_character_)]
   dedup_new[, PHILSYS := fifelse(!is.na(get(philsys)), str_replace_all(get(philsys), "[^[:alnum:]]", ""), NA_character_)]

   # code standard names
   dedup_new[, FIRST_NY := suppress_warnings(fifelse(!is.na(FIRST_S), nysiis(FIRST_S, stri_length(FIRST_S)), NA_character_), "unknown characters")]
   dedup_new[, MIDDLE_NY := suppress_warnings(fifelse(!is.na(MIDDLE_S), nysiis(MIDDLE_S, stri_length(MIDDLE_S)), NA_character_), "unknown characters")]
   dedup_new[, LAST_NY := suppress_warnings(fifelse(!is.na(LAST_S), nysiis(LAST_S, stri_length(LAST_S)), NA_character_), "unknown characters")]


   # genearte UIC w/o 1 parent, 2 combinations
   dedup_new_uic <- dedup_new[!is.na(UIC), c("CENTRAL_ID", "UIC_MOM", "UIC_DAD"), with = FALSE]
   dedup_new_uic <- unique(dedup_new_uic, by = c("CENTRAL_ID", "UIC_MOM", "UIC_DAD"))
   dedup_new_uic <- melt(dedup_new_uic, id.vars = "CENTRAL_ID", measure.vars = c("UIC_MOM", "UIC_DAD"))
   dedup_new_uic <- dedup_new_uic[order(CENTRAL_ID, value)]
   dedup_new_uic[, FIRST_TWO := rowid(CENTRAL_ID)]
   dedup_new_uic <- dcast(dedup_new_uic, CENTRAL_ID ~ FIRST_TWO, value.var = "value")
   setnames(dedup_new_uic, "1", "UIC_1")
   setnames(dedup_new_uic, "2", "UIC_2")

   dedup_new <- dedup_new[dedup_new_uic, on = "CENTRAL_ID"]
   dedup_new[, UIC_SORT := fifelse(!is.na(UIC), stri_c(UIC_1, UIC_2, substr(UIC, 5, 14)), NA_character_)]

   return(as_tibble(dedup_new))
}

dedup_prep <- function(
   data = NULL,
   name_f = NULL,
   name_m = NULL,
   name_l = NULL,
   name_s = NULL,
   uic = NULL,
   birthdate = NULL,
   code_confirm = NULL,
   code_px = NULL,
   phic = NULL,
   philsys = NULL
) {
   log_info("Starting.")
   dedup_new <- data %>%
      mutate(
         last              = stri_trans_general(stri_trans_toupper({{name_l}}), "latin-ascii"),
         middle            = stri_trans_general(stri_trans_toupper({{name_m}}), "latin-ascii"),
         first             = stri_trans_general(stri_trans_toupper({{name_f}}), "latin-ascii"),
         suffix            = stri_trans_general(stri_trans_toupper({{name_s}}), "latin-ascii"),
         uic               = stri_trans_general(stri_trans_toupper({{uic}}), "latin-ascii"),
         confirmatory_code = stri_trans_general(stri_trans_toupper({{code_confirm}}), "latin-ascii"),
         patient_code      = stri_trans_general(stri_trans_toupper({{code_px}}), "latin-ascii"),
         philhealth_no     = stri_trans_general(stri_trans_toupper({{phic}}), "latin-ascii"),
         philsys_id        = stri_trans_general(stri_trans_toupper({{philsys}}), "latin-ascii"),
      ) %>%
      mutate_at(
         .vars = vars(last, middle, first, suffix, uic, confirmatory_code, patient_code, philhealth_no, philsys_id),
         ~clean_pii(.)
      ) %>%
      mutate(
         # get components of birthdate
         birth_yr      = year({{birthdate}}),
         birth_mo      = month({{birthdate}}),
         birth_dy      = day({{birthdate}}),

         # extract parent info from uic
         uic_mom       = substr(uic, 1, 2),
         uic_dad       = substr(uic, 3, 4),
         uic_order     = substr(uic, 5, 6),

         # variables for first 3 letters of names
         first_a       = substr(first, 1, 3),
         middle_a      = substr(middle, 1, 3),
         last_a        = substr(last, 1, 3),

         last          = coalesce(last, middle),
         middle        = coalesce(middle, last),

         # clean ids
         confirm_sieve = str_replace_all(confirmatory_code, "[^[:alnum:]]", ""),
         pxcode_sieve  = str_replace_all(patient_code, "[^[:alnum:]]", ""),
         first_sieve   = str_replace_all(first, "[^[:alnum:]]", ""),
         middle_sieve  = str_replace_all(middle, "[^[:alnum:]]", ""),
         last_sieve    = str_replace_all(last, "[^[:alnum:]]", ""),
         phic          = str_replace_all(philhealth_no, "[^[:alnum:]]", ""),
         philsys       = str_replace_all(philsys_id, "[^[:alnum:]]", ""),
      ) %>%
      mutate_at(
         .vars = vars(ends_with("_SIEVE", ignore.case = TRUE), phic, philsys),
         ~str_replace_all(., "[^[:alnum:]]", "")
      ) %>%
      mutate_at(
         .vars = vars(first_sieve, middle_sieve, last_sieve),
         ~str_replace_all(., "([[:alnum:]])\\1+", "\\1")
      ) %>%
      mutate(
         # code standard names
         first_ny  = suppress_warnings(nysiis(first_sieve, stri_length(first_sieve)), "unknown characters"),
         middle_ny = suppress_warnings(nysiis(middle_sieve, stri_length(middle_sieve)), "unknown characters"),
         last_ny   = suppress_warnings(nysiis(last_sieve, stri_length(last_sieve)), "unknown characters"),
      )

   log_info("Splitting uic.")
   # genearte uic w/o 1 parent, 2 combinations
   dedup_new_uic <- dedup_new %>%
      filter(!is.na(uic)) %>%
      select(
         central_id,
         uic_mom,
         uic_dad
      ) %>%
      pivot_longer(
         cols      = starts_with('uic'),
         names_to  = 'uic',
         values_to = 'first_two'
      ) %>%
      arrange(central_id, first_two) %>%
      group_by(central_id) %>%
      mutate(uic = row_number()) %>%
      ungroup() %>%
      pivot_wider(
         id_cols      = central_id,
         names_from   = uic,
         names_prefix = 'uic_',
         values_from  = first_two
      )

   log_info("Sorting uic.")
   dedup_new %<>%
      left_join(
         y  = dedup_new_uic,
         by = 'central_id'
      ) %>%
      mutate(
         uic_sort = stri_c(uic_1, uic_2, substr(uic, 5, 14))
      )

   log_info("Sorting Names.")
   dedup_new_names <- dedup_new %>%
      select(
         central_id,
         name_1 = first_sieve,
         name_2 = middle_sieve,
         name_3 = last_sieve
      ) %>%
      pivot_longer(
         cols = starts_with("name_")
      ) %>%
      mutate(
         value = clean_pii(value),
         value = if_else(nchar(value) == 1, NA_character_, value, value)
      ) %>%
      filter(!is.na(value)) %>%
      arrange(central_id, value) %>%
      group_by(central_id) %>%
      summarise(
         namesort_first = first(value),
         namesort_last  = last(value),
      ) %>%
      ungroup()

   dedup_new %<>%
      left_join(
         y  = dedup_new_names,
         by = 'central_id'
      )

   return(dedup_new)
}

upload_dupes <- function(data) {
   db_conn <- dbConnect(
      RMariaDB::MariaDB(),
      user     = 'ohasis',
      password = 't1rh0uGCyN2sz6zk',
      host     = '192.168.193.232',
      port     = '3307',
      timeout  = -1,
      'ohasis_interim'
   )

   pb <- progress_bar$new(format = ":current of :total PIDs | [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed", total = nrow(data), width = 100, clear = FALSE)
   pb$tick(0)
   for (i in seq_len(nrow(data))) {
      cid <- data[i, 1] %>% as.character()
      pid <- data[i, 2] %>% as.character()
      ts  <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

      num_pid  <- nrow(
         dbGetQuery(
            db_conn,
            "SELECT * FROM ohasis_interim.registry WHERE PATIENT_ID = ?",
            params = pid
         )
      )
      num_cid  <- nrow(
         dbGetQuery(
            db_conn,
            "SELECT * FROM ohasis_interim.registry WHERE CENTRAL_ID = ?",
            params = pid
         )
      )
      num_pcid <- nrow(
         dbGetQuery(
            db_conn,
            "SELECT * FROM ohasis_interim.registry WHERE PATIENT_ID = ?",
            params = cid
         )
      )

      if (num_pcid == 0) {
         dbExecute(
            db_conn,
            "INSERT IGNORE INTO ohasis_interim.registry (CENTRAL_ID, PATIENT_ID, CREATED_BY, CREATED_AT) VALUES (?, ?, ?, ?);",
            params = list(cid, cid, Sys.getenv("OH_USER_ID"), ts)
         )
      }

      if (num_pid == 0) {
         dbExecute(
            db_conn,
            "INSERT IGNORE INTO ohasis_interim.registry (CENTRAL_ID, PATIENT_ID, CREATED_BY, CREATED_AT) VALUES (?, ?, ?, ?);",
            params = list(cid, pid, Sys.getenv("OH_USER_ID"), ts)
         )
      } else {
         dbExecute(
            db_conn,
            "UPDATE ohasis_interim.registry SET CENTRAL_ID = ?, UPDATED_BY = ?, UPDATED_AT = ? WHERE PATIENT_ID = ?;",
            params = list(cid, Sys.getenv("OH_USER_ID"), ts, pid)
         )
      }

      if (num_cid > 0) {
         dbExecute(
            db_conn,
            "UPDATE ohasis_interim.registry SET CENTRAL_ID = ?, UPDATED_BY = ?, UPDATED_AT = ? WHERE CENTRAL_ID = ?;",
            params = list(cid, Sys.getenv("OH_USER_ID"), ts, pid)
         )
      }

      pb$tick(1)
   }

   dbDisconnect(db_conn)
}

# local duplicates handling
upload_dupes2 <- function(dedup_upload, id_reg, upload = FALSE, from = NULL) {
   ts <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

   # new data
   bind_pid <- dedup_upload %>%
      select(
         central_id = 1,
         patient_id = 2,
      ) %>%
      left_join(
         y  = id_reg %>%
            select(patient_id, created_by, created_at) %>%
            mutate_all(as.character),
         by = join_by(patient_id)
      ) %>%
      mutate(old = if_else(!is.na(created_at), 1, 0, 0)) %>%
      mutate(
         created_by  = if_else(old == 0, Sys.getenv("OH_USER_ID"), created_by, created_by),
         created_at  = if_else(old == 0, ts, created_at, created_at),
         updated_by  = if_else(old == 1, Sys.getenv("OH_USER_ID"), NA_character_, NA_character_),
         updated_at  = if_else(old == 1, ts, NA_character_, NA_character_),
         deleted_by  = NA_character_,
         deleted_at  = NA_character_
      ) %>%
      select(any_of(names(id_reg)))

   bind_cid <- dedup_upload %>%
      select(
         central_id = 1,
      ) %>%
      distinct_all() %>%
      mutate(
         patient_id = central_id
      ) %>%
      left_join(
         y  = id_reg %>%
            select(patient_id, created_by, created_at) %>%
            mutate_all(as.character),
         by = join_by(patient_id)
      ) %>%
      mutate(old = if_else(!is.na(created_at), 1, 0, 0)) %>%
      mutate(
         created_by  = if_else(old == 0, Sys.getenv("OH_USER_ID"), created_by, created_by),
         created_at  = if_else(old == 0, ts, created_at, created_at),
         updated_by  = if_else(old == 1, Sys.getenv("OH_USER_ID"), NA_character_, NA_character_),
         updated_at  = if_else(old == 1, ts, NA_character_, NA_character_),
         deleted_by  = NA_character_,
         deleted_at  = NA_character_
      ) %>%
      filter(old == 0) %>%
      select(any_of(names(id_reg)))

   # updated old cids
   new_reg <- id_reg %>%
      mutate_all(as.character) %>%
      left_join(
         y  = dedup_upload %>%
            select(new_cid = 1, central_id = 2),
         by = join_by(central_id)
      ) %>%
      mutate(
         central_id = coalesce(new_cid, central_id),
         updated_by = if_else(!is.na(new_cid), Sys.getenv("OH_USER_ID"), updated_by, updated_by),
         updated_at = if_else(!is.na(new_cid), ts, updated_at, updated_at),
      ) %>%
      select(-new_cid)

   # final new data
   new_reg <- bind_pid %>%
      bind_rows(bind_cid) %>%
      bind_rows(new_reg) %>%
      distinct(patient_id, .keep_all = TRUE)

   if (upload && !is.null(from)) {
      new_data <- new_reg %>%
         filter(
            created_at >= from | updated_at >= from,
         )

      db_conn     <- ohasis$conn("db")
      table_space <- Id(schema = "ohasis", table = "registry")
      # remove relevant records first
      dbxDelete(
         db_conn,
         table_space,
         select(new_data, patient_id),
         batch_size = 1000
      )

      # upload new data
      dbxUpsert(
         db_conn,
         table_space,
         new_data,
         c("central_id", "patient_id"),
         batch_size = 1000
      )
      dbDisconnect(db_conn)
   }

   return(new_reg)
}

upload_non_dupes <- function(data) {
   lw_conn <- ohasis$conn("lw")
   schema  <- Id(schema = "ohasis_warehouse", table = "non_dupes")

   upload <- data %>%
      select(
         PATIENT_ID  = 1,
         NON_PAIR_ID = 2
      )

   dbxUpsert(lw_conn, schema, upload, c("PATIENT_ID", "NON_PAIR_ID"))

   dbDisconnect(lw_conn)
}


quick_reclink <- function(data_match, data_ref, id_find, id_ref, match_cols, distance_col) {
   distance_x <- as.name(paste0(distance_col, ".x"))
   distance_y <- as.name(paste0(distance_col, ".y"))
   reclink_df <- fastLink(
      dfA              = data_match,
      dfB              = data_ref,
      varnames         = match_cols,
      stringdist.match = match_cols,
      partial.match    = match_cols,
      threshold.match  = 0.95,
      cut.a            = 0.90,
      cut.p            = 0.85,
      dedupe.matches   = FALSE,
      n.cores          = 4,
   )

   reclink_review <- data.frame()
   if (length(reclink_df$matches$inds.a) > 0) {
      reclink_matched <- getMatches(
         dfA         = data_match,
         dfB         = data_ref,
         fl.out      = reclink_df,
         combine.dfs = FALSE
      )

      reclink_review <- reclink_matched$dfA.match %>%
         mutate(
            MATCH_ID = row_number()
         ) %>%
         select(
            posterior,
            MATCH_ID,
            any_of(c(id_find, match_cols))
         ) %>%
         left_join(
            y  = reclink_matched$dfB.match %>%
               mutate(
                  MATCH_ID = row_number()
               ) %>%
               select(
                  posterior,
                  MATCH_ID,
                  any_of(c(id_ref, match_cols))
               ),
            by = "MATCH_ID"
         ) %>%
         select(-posterior.y) %>%
         rename(posterior = posterior.x) %>%
         arrange(desc(posterior)) %>%
         relocate(posterior, .before = MATCH_ID) %>%
         # Additional sift through of matches
         mutate(
            # levenshtein
            LV       = stringdist::stringsim(!!distance_x, !!distance_y, method = 'lv'),
            # jaro-winkler
            JW       = stringdist::stringsim(!!distance_x, !!distance_y, method = 'jw'),
            # qgram
            QGRAM    = stringdist::stringsim(!!distance_x, !!distance_y, method = 'qgram', q = 3),
            AVG_DIST = (LV + QGRAM + JW) / 3,
         ) %>%
         # choose 60% and above match
         filter(AVG_DIST >= 0.60, !is.na(posterior))
   }
   return(reclink_review)
}