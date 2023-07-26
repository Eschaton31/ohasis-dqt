##  Prepare dataset for dx registry deduplication ------------------------------

prep_data <- function(tx, dx) {
   data    <- list()
   data$tx <- tx %>%
      filter(is.na(idnum)) %>%
      mutate(
         firstname = first,
         byr       = if_else(
            condition = !is.na(birthdate),
            true      = year(birthdate),
            false     = NA_integer_
         ) %>% as.numeric(),
         bmo       = if_else(
            condition = !is.na(birthdate),
            true      = year(birthdate),
            false     = NA_integer_
         ) %>% as.numeric(),
         bdy       = if_else(
            condition = !is.na(birthdate),
            true      = year(birthdate),
            false     = NA_integer_
         ) %>% as.numeric(),
      )

   data$dx <- dx %>%
      mutate(
         byr = if_else(
            condition = !is.na(bdate),
            true      = year(bdate),
            false     = NA_integer_
         ) %>% as.numeric(),
         bmo = if_else(
            condition = !is.na(bdate),
            true      = year(bdate),
            false     = NA_integer_
         ) %>% as.numeric(),
         bdy = if_else(
            condition = !is.na(bdate),
            true      = year(bdate),
            false     = NA_integer_
         ) %>% as.numeric(),
      )

   return(data)
}

##  Patient Record Linkage Algorithm -------------------------------------------

dedup_dx <- function(data, non_dupes) {
   dedup_dx   <- list()
   reclink_df <- fastLink(
      dfA              = data$tx,
      dfB              = data$dx,
      varnames         = c(
         "firstname",
         "middle",
         "last",
         "byr",
         "bmo",
         "bdy"
      ),
      stringdist.match = c(
         "firstname",
         "middle",
         "last"
      ),
      partial.match    = c(
         "firstname",
         "middle",
         "last"
      ),
      numeric.match    = c(
         "byr",
         "bmo",
         "bdy"
      ),
      threshold.match  = 0.95,
      cut.a            = 0.90,
      cut.p            = 0.85,
      dedupe.matches   = FALSE,
      n.cores          = 4
   )

   if (length(reclink_df$matches$inds.a) > 0) {
      reclink_matched <- getMatches(
         dfA         = data$tx,
         dfB         = data$dx,
         fl.out      = reclink_df,
         combine.dfs = FALSE
      )

      reclink_review <- reclink_matched$dfA.match %>%
         mutate(
            MATCH_ID = row_number()
         ) %>%
         select(
            MATCH_ID,
            MASTER_RID          = REC_ID,
            MASTER_PID          = PATIENT_ID,
            MASTER_CID          = CENTRAL_ID,
            MASTER_FIRST        = firstname,
            MASTER_MIDDLE       = middle,
            MASTER_LAST         = last,
            MASTER_SUFFIX       = suffix,
            MASTER_BIRTHDATE    = birthdate,
            MASTER_CONFIRMATORY = confirmatory_code,
            MASTER_UIC          = uic,
            MASTER_INITIALS     = initials,
            posterior
         ) %>%
         left_join(
            y  = reclink_matched$dfB.match %>%
               mutate(
                  MATCH_ID = row_number()
               ) %>%
               select(
                  MATCH_ID,
                  # USING_RID          = REC_ID,
                  USING_PID          = PATIENT_ID,
                  USING_CID          = CENTRAL_ID,
                  USING_IDNUM        = idnum,
                  USING_FIRST        = firstname,
                  USING_MIDDLE       = middle,
                  USING_LAST         = last,
                  USING_SUFFIX       = name_suffix,
                  USING_BIRTHDATE    = bdate,
                  USING_CONFIRMATORY = labcode2,
                  USING_UIC          = uic,
                  USING_INITIALS     = pxcode,
                  posterior
               ),
            by = "MATCH_ID"
         ) %>%
         select(-posterior.y) %>%
         rename(posterior = posterior.x) %>%
         unite(
            col   = "MASTER_FMS",
            sep   = " ",
            MASTER_FIRST,
            MASTER_MIDDLE,
            MASTER_SUFFIX,
            na.rm = TRUE
         ) %>%
         unite(
            col   = "MASTER_NAME",
            sep   = ", ",
            MASTER_LAST,
            MASTER_FMS,
            na.rm = TRUE
         ) %>%
         unite(
            col   = "USING_FMS",
            sep   = " ",
            USING_FIRST,
            USING_MIDDLE,
            USING_SUFFIX,
            na.rm = TRUE
         ) %>%
         unite(
            col   = "USING_NAME",
            sep   = ", ",
            USING_LAST,
            USING_FMS,
            na.rm = TRUE
         ) %>%
         arrange(desc(posterior)) %>%
         relocate(posterior, .before = MATCH_ID) %>%
         # Additional sift through of matches
         mutate(
            # levenshtein
            LV       = stringsim(MASTER_NAME, USING_NAME, method = 'lv'),
            # jaro-winkler
            JW       = stringsim(MASTER_NAME, USING_NAME, method = 'jw'),
            # qgram
            QGRAM    = stringsim(MASTER_NAME, USING_NAME, method = 'qgram', q = 3),
            AVG_DIST = (LV + QGRAM + JW) / 3,
         ) %>%
         # choose 60% and above match
         filter(AVG_DIST >= 0.60, !is.na(posterior))

      # assign to global env
      dedup_dx$reclink <- reclink_review %>%
         mutate(
            Bene  = NA_character_,
            Gab   = NA_character_,
            Lala  = NA_character_,
            Fayye = NA_character_,
         ) %>%
         anti_join(
            y  = non_dupes %>%
               select(USING_CID = PATIENT_ID, MASTER_CID = NON_PAIR_ID),
            by = join_by(USING_CID, MASTER_CID)
         )
   }
   return(dedup_dx)
}

##  Merge w/ registry using labcode --------------------------------------------

prep_merge <- function(tx, dx) {
   tx %<>%
      filter(is.na(idnum)) %>%
      dedup_prep(
         first,
         middle,
         last,
         suffix,
         uic,
         birthdate,
         confirmatory_code,
         px_code,
         philhealth_no,
         philsys_id
      )

   dx %<>%
      dedup_prep(
         firstname,
         middle,
         last,
         name_suffix,
         uic,
         bdate,
         labcode2,
         pxcode,
         philhealth,
         philsys_id
      )

   dedup_dx       <- list()
   dedup_dx$merge <- tx %>%
      select(
         MASTER_CID          = CENTRAL_ID,
         MASTER_FIRST        = first,
         MASTER_MIDDLE       = middle,
         MASTER_LAST         = last,
         MASTER_SUFFIX       = suffix,
         MASTER_BIRTHDATE    = birthdate,
         MASTER_CONFIRMATORY = confirmatory_code,
         MASTER_UIC          = uic,
         MASTER_INITIALS     = initials,
         CONFIRM_SIEVE
      ) %>%
      inner_join(
         y  = dx %>%
            select(
               USING_CID          = CENTRAL_ID,
               USING_IDNUM        = idnum,
               USING_FIRST        = firstname,
               USING_MIDDLE       = middle,
               USING_LAST         = last,
               USING_SUFFIX       = name_suffix,
               USING_BIRTHDATE    = bdate,
               USING_CONFIRMATORY = labcode2,
               USING_UIC          = uic,
               USING_INITIALS     = pxcode,
               CONFIRM_SIEVE
            ),
         by = "CONFIRM_SIEVE"
      ) %>%
      select(
         -CONFIRM_SIEVE
      )

   return(dedup_dx)
}

##  Actual flow ----------------------------------------------------------------

.init <- function(envir = parent.env(environment()), ...) {
   step <- parent.env(environment())
   p    <- envir
   vars <- as.list(list(...))

   dx <- hs_data("harp_dx", "reg", p$params$yr, p$params$mo) %>%
      read_dta(
         col_select = c(
            PATIENT_ID,
            labcode,
            labcode2,
            idnum,
            uic,
            firstname,
            middle,
            last,
            name_suffix,
            bdate,
            sex,
            pxcode,
            philhealth,
            confirm_date
         )
      ) %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      ) %>%
      get_cid(p$forms$id_registry, PATIENT_ID)

   tx <- p$official$new_reg %>%
      select(
         -REC_ID,
         -PATIENT_ID
      )

   reclink <- prep_data(tx, dx)
   check   <- dedup_dx(reclink, p$forms$non_dupes)
   check   <- append(check, prep_merge(tx, dx))

   step$check <- check
   flow_validation(p, "dedup_dx", p$params$ym, upload = vars$upload)
   log_success("Done.")
}
