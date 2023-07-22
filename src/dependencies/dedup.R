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
   dedup_new <- data %>%
      mutate(
         LAST              = stri_trans_general(stri_trans_toupper({{name_l}}), "latin-ascii"),
         MIDDLE            = stri_trans_general(stri_trans_toupper({{name_m}}), "latin-ascii"),
         FIRST             = stri_trans_general(stri_trans_toupper({{name_f}}), "latin-ascii"),
         SUFFIX            = stri_trans_general(stri_trans_toupper({{name_s}}), "latin-ascii"),
         UIC               = stri_trans_general(stri_trans_toupper({{uic}}), "latin-ascii"),
         CONFIRMATORY_CODE = stri_trans_general(stri_trans_toupper({{code_confirm}}), "latin-ascii"),
         PATIENT_CODE      = stri_trans_general(stri_trans_toupper({{code_px}}), "latin-ascii"),
         PHILHEALTH_NO     = stri_trans_general(stri_trans_toupper({{phic}}), "latin-ascii"),
         PHILSYS_ID        = stri_trans_general(stri_trans_toupper({{philsys}}), "latin-ascii"),
      ) %>%
      mutate(
         # get components of birthdate
         BIRTH_YR      = year({{birthdate}}),
         BIRTH_MO      = month({{birthdate}}),
         BIRTH_DY      = day({{birthdate}}),

         # extract parent info from uic
         UIC_MOM       = substr(UIC, 1, 2),
         UIC_DAD       = substr(UIC, 3, 4),
         UIC_ORDER     = substr(UIC, 5, 6),

         # variables for first 3 letters of names
         FIRST_A       = substr(FIRST, 1, 3),
         MIDDLE_A      = substr(MIDDLE, 1, 3),
         LAST_A        = substr(LAST, 1, 3),

         LAST          = coalesce(LAST, MIDDLE),
         MIDDLE        = coalesce(MIDDLE, LAST),

         # clean ids
         CONFIRM_SIEVE = str_replace_all(CONFIRMATORY_CODE, "[^[:alnum:]]", ""),
         PXCODE_SIEVE  = str_replace_all(PATIENT_CODE, "[^[:alnum:]]", ""),
         FIRST_SIEVE   = str_replace_all(FIRST, "[^[:alnum:]]", ""),
         MIDDLE_SIEVE  = str_replace_all(MIDDLE, "[^[:alnum:]]", ""),
         LAST_SIEVE    = str_replace_all(LAST, "[^[:alnum:]]", ""),
         PHIC          = str_replace_all(PHILHEALTH_NO, "[^[:alnum:]]", ""),
         PHILSYS       = str_replace_all(PHILSYS_ID, "[^[:alnum:]]", ""),
      ) %>%
      mutate_at(
         .vars = vars(ends_with("_SIEVE", ignore.case = TRUE), PHIC, PHILSYS),
         ~str_replace_all(., "([[:alnum:]])\\1+", "\\1")
      ) %>%
      mutate(
         # code standard names
         FIRST_NY  = suppress_warnings(nysiis(FIRST_SIEVE, stri_length(FIRST_SIEVE)), "unknown characters"),
         MIDDLE_NY = suppress_warnings(nysiis(MIDDLE_SIEVE, stri_length(MIDDLE_SIEVE)), "unknown characters"),
         LAST_NY   = suppress_warnings(nysiis(LAST_SIEVE, stri_length(LAST_SIEVE)), "unknown characters"),
      )

   # genearte UIC w/o 1 parent, 2 combinations
   dedup_new_uic <- dedup_new %>%
      filter(!is.na(UIC)) %>%
      select(
         CENTRAL_ID,
         UIC_MOM,
         UIC_DAD
      ) %>%
      pivot_longer(
         cols      = starts_with('UIC'),
         names_to  = 'UIC',
         values_to = 'FIRST_TWO'
      ) %>%
      arrange(CENTRAL_ID, FIRST_TWO) %>%
      group_by(CENTRAL_ID) %>%
      mutate(UIC = row_number()) %>%
      ungroup() %>%
      pivot_wider(
         id_cols      = CENTRAL_ID,
         names_from   = UIC,
         names_prefix = 'UIC_',
         values_from  = FIRST_TWO
      )

   dedup_new %<>%
      left_join(
         y  = dedup_new_uic,
         by = 'CENTRAL_ID'
      ) %>%
      mutate(
         UIC_SORT = stri_c(UIC_1, UIC_2, substr(UIC, 5, 14))
      )

   dedup_new_names <- dedup_new %>%
      rename(
         NAME_1 = FIRST,
         NAME_2 = MIDDLE,
         NAME_3 = LAST
      ) %>%
      pivot_longer(
         cols = starts_with("NAME_")
      ) %>%
      filter(!is.na(value)) %>%
      arrange(CENTRAL_ID, value) %>%
      group_by(CENTRAL_ID) %>%
      summarise(
         NAMESORT_FIRST = first(value),
         NAMESORT_LAST  = last(value),
      ) %>%
      ungroup()

   dedup_new %<>%
      left_join(
         y  = dedup_new_names,
         by = 'CENTRAL_ID'
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
         CENTRAL_ID = 1,
         PATIENT_ID = 2,
      ) %>%
      left_join(
         y  = id_reg %>%
            select(PATIENT_ID, CREATED_BY, CREATED_AT),
         by = join_by(PATIENT_ID)
      ) %>%
      mutate(old = if_else(!is.na(CREATED_AT), 1, 0, 0)) %>%
      mutate(
         REPORT_DATE = NA_Date_,
         IDNUM       = NA_character_,
         REMARKS     = NA_character_,
         PRIME       = NA_integer_,
         CREATED_BY  = if_else(old == 0, Sys.getenv("OH_USER_ID"), CREATED_BY, CREATED_BY),
         CREATED_AT  = if_else(old == 0, ts, CREATED_AT, CREATED_AT),
         UPDATED_BY  = if_else(old == 1, Sys.getenv("OH_USER_ID"), NA_character_, NA_character_),
         UPDATED_AT  = if_else(old == 1, ts, NA_character_, NA_character_),
         DELETED_BY  = NA_character_,
         DELETED_AT  = NA_character_
      ) %>%
      select(names(id_reg))

   bind_cid <- dedup_upload %>%
      select(
         CENTRAL_ID = 1,
      ) %>%
      distinct_all() %>%
      mutate(
         PATIENT_ID = CENTRAL_ID
      ) %>%
      left_join(
         y  = id_reg %>%
            select(PATIENT_ID, CREATED_BY, CREATED_AT),
         by = join_by(PATIENT_ID)
      ) %>%
      mutate(old = if_else(!is.na(CREATED_AT), 1, 0, 0)) %>%
      mutate(
         REPORT_DATE = NA_Date_,
         IDNUM       = NA_character_,
         REMARKS     = NA_character_,
         PRIME       = NA_integer_,
         CREATED_BY  = if_else(old == 0, Sys.getenv("OH_USER_ID"), CREATED_BY, CREATED_BY),
         CREATED_AT  = if_else(old == 0, ts, CREATED_AT, CREATED_AT),
         UPDATED_BY  = if_else(old == 1, Sys.getenv("OH_USER_ID"), NA_character_, NA_character_),
         UPDATED_AT  = if_else(old == 1, ts, NA_character_, NA_character_),
         DELETED_BY  = NA_character_,
         DELETED_AT  = NA_character_
      ) %>%
      filter(old == 0) %>%
      select(names(id_reg))

   # updated old cids
   new_reg <- id_reg %>%
      left_join(
         y  = dedup_upload %>%
            select(NEW_CID = 1, CENTRAL_ID = 2),
         by = join_by(CENTRAL_ID)
      ) %>%
      mutate(
         CENTRAL_ID = coalesce(NEW_CID, CENTRAL_ID),
         UPDATED_BY = if_else(!is.na(NEW_CID), Sys.getenv("OH_USER_ID"), UPDATED_BY, UPDATED_BY),
         UPDATED_AT = if_else(!is.na(NEW_CID), ts, UPDATED_AT, UPDATED_AT),
      ) %>%
      select(-NEW_CID)

   # final new data
   new_reg <- bind_pid %>%
      bind_rows(bind_cid) %>%
      bind_rows(new_reg) %>%
      distinct(PATIENT_ID, .keep_all = TRUE)

   if (upload && !is.null(from)) {
      new_data <- new_reg %>%
         filter(
            CREATED_AT >= from | UPDATED_AT >= from,
         )

      db_conn     <- ohasis$conn("db")
      table_space <- Id(schema = "ohasis_interim", table = "registry")
      # remove relevant records first
      dbxDelete(
         db_conn,
         table_space,
         select(new_data, PATIENT_ID),
         batch_size = 1000
      )

      # upload new data
      dbxUpsert(
         db_conn,
         table_space,
         new_data,
         c("CENTRAL_ID", "PATIENT_ID"),
         batch_size = 1000
      )
      dbDisconnect(db_conn)
   }

   return(new_reg)
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