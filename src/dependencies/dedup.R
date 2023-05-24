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

         # get components of birthdate
         BIRTH_YR          = year({{birthdate}}),
         BIRTH_MO          = month({{birthdate}}),
         BIRTH_DY          = day({{birthdate}}),

         # extract parent info from uic
         UIC_MOM           = if_else(!is.na(UIC), substr(UIC, 1, 2), NA_character_),
         UIC_DAD           = if_else(!is.na(UIC), substr(UIC, 3, 4), NA_character_),
         UIC_ORDER         = if_else(!is.na(UIC), substr(UIC, 5, 6), NA_character_),

         # variables for first 3 letters of names
         FIRST_A           = if_else(!is.na(FIRST), substr(FIRST, 1, 3), NA_character_),
         MIDDLE_A          = if_else(!is.na(MIDDLE), substr(MIDDLE, 1, 3), NA_character_),
         LAST_A            = if_else(!is.na(LAST), substr(LAST, 1, 3), NA_character_),

         LAST              = if_else(is.na(LAST), MIDDLE, LAST),
         MIDDLE            = if_else(is.na(MIDDLE), LAST, MIDDLE),

         # clean ids
         CONFIRM_SIEVE     = if_else(!is.na(CONFIRMATORY_CODE), str_replace_all(CONFIRMATORY_CODE, "[^[:alnum:]]", ""), NA_character_),
         PXCODE_SIEVE      = if_else(!is.na(PATIENT_CODE), str_replace_all(PATIENT_CODE, "[^[:alnum:]]", ""), NA_character_),
         FIRST_S           = if_else(!is.na(FIRST), str_replace_all(FIRST, "[^[:alnum:]]", ""), NA_character_),
         MIDDLE_S          = if_else(!is.na(MIDDLE), str_replace_all(MIDDLE, "[^[:alnum:]]", ""), NA_character_),
         LAST_S            = if_else(!is.na(LAST), str_replace_all(LAST, "[^[:alnum:]]", ""), NA_character_),
         PHIC              = if_else(!is.na({{phic}}), str_replace_all({{phic}}, "[^[:alnum:]]", ""), NA_character_),
         PHILSYS           = if_else(!is.na({{philsys}}), str_replace_all({{philsys}}, "[^[:alnum:]]", ""), NA_character_),

         # code standard names
         FIRST_NY          = suppress_warnings(if_else(!is.na(FIRST_S), nysiis(FIRST_S, stri_length(FIRST_S)), NA_character_), "unknown characters"),
         MIDDLE_NY         = suppress_warnings(if_else(!is.na(MIDDLE_S), nysiis(MIDDLE_S, stri_length(MIDDLE_S)), NA_character_), "unknown characters"),
         LAST_NY           = suppress_warnings(if_else(!is.na(LAST_S), nysiis(LAST_S, stri_length(LAST_S)), NA_character_), "unknown characters"),
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
         UIC_SORT = if_else(
            condition = !is.na(UIC),
            true      = paste0(UIC_1, UIC_2, substr(UIC, 5, 14)),
            false     = NA_character_
         )
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
            "SELECT * FROM registry WHERE PATIENT_ID = ?",
            params = pid
         )
      )
      num_cid  <- nrow(
         dbGetQuery(
            db_conn,
            "SELECT * FROM registry WHERE CENTRAL_ID = ?",
            params = pid
         )
      )
      num_pcid <- nrow(
         dbGetQuery(
            db_conn,
            "SELECT * FROM registry WHERE PATIENT_ID = ?",
            params = cid
         )
      )

      if (num_pcid == 0) {
         dbExecute(
            db_conn,
            "INSERT IGNORE INTO registry (CENTRAL_ID, PATIENT_ID, CREATED_BY, CREATED_AT) VALUES (?, ?, ?, ?);",
            params = list(cid, cid, Sys.getenv("OH_USER_ID"), ts)
         )
      }

      if (num_pid == 0) {
         dbExecute(
            db_conn,
            "INSERT IGNORE INTO registry (CENTRAL_ID, PATIENT_ID, CREATED_BY, CREATED_AT) VALUES (?, ?, ?, ?);",
            params = list(cid, pid, Sys.getenv("OH_USER_ID"), ts)
         )
      } else {
         dbExecute(
            db_conn,
            "UPDATE registry SET CENTRAL_ID = ?, UPDATED_BY = ?, UPDATED_AT = ? WHERE PATIENT_ID = ?;",
            params = list(cid, Sys.getenv("OH_USER_ID"), ts, pid)
         )
      }

      if (num_cid > 0) {
         dbExecute(
            db_conn,
            "UPDATE registry SET CENTRAL_ID = ?, UPDATED_BY = ?, UPDATED_AT = ? WHERE CENTRAL_ID = ?;",
            params = list(cid, Sys.getenv("OH_USER_ID"), ts, pid)
         )
      }

      pb$tick(1)
   }

   dbDisconnect(db_conn)
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