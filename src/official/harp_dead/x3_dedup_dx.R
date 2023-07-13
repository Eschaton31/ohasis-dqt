##  Prepare dataset for dx registry deduplication ------------------------------

prep_data <- function(dead, dx) {
   data      <- list()
   data$dead <- dead %>%
      filter(is.na(idnum)) %>%
      mutate(
         firstname = fname,
         middle    = mname,
         last      = lname,
         suffix    = sname,
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

dedup_dx <- function(data) {
   dedup_dx   <- list()
   reclink_df <- fastLink(
      dfA              = data$dead,
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
         dfA         = data$dead,
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
            MASTER_CONFIRMATORY = saccl_lab_code,
            MASTER_UIC          = uic,
            MASTER_INITIALS     = pxcode,
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
         )
   }
   return(dedup_dx)
}

##  Merge w/ registry using labcode --------------------------------------------

prep_merge <- function(dead, dx) {
   dead <- dead %>%
      filter(is.na(idnum)) %>%
      mutate(
         CONFIRMATORY_CODE = stri_trans_toupper(saccl_lab_code),
         CONFIRM_SIEVE     = if_else(!is.na(CONFIRMATORY_CODE), str_replace_all(CONFIRMATORY_CODE, "[^[:alnum:]]", ""), NA_character_),
      )

   dx <- dx %>%
      mutate(
         CONFIRMATORY_CODE = stri_trans_toupper(labcode),
         CONFIRM_SIEVE     = if_else(!is.na(CONFIRMATORY_CODE), str_replace_all(CONFIRMATORY_CODE, "[^[:alnum:]]", ""), NA_character_),
      )

   dedup_dx       <- list()
   dedup_dx$merge <- dead %>%
      select(
         MASTER_RID          = REC_ID,
         MASTER_PID          = PATIENT_ID,
         MASTER_CID          = CENTRAL_ID,
         MASTER_FIRST        = fname,
         MASTER_MIDDLE       = mname,
         MASTER_LAST         = lname,
         MASTER_SUFFIX       = sname,
         MASTER_BIRTHDATE    = birthdate,
         MASTER_CONFIRMATORY = saccl_lab_code,
         MASTER_UIC          = uic,
         MASTER_INITIALS     = pxcode,
         CONFIRM_SIEVE
      ) %>%
      inner_join(
         y  = dx %>%
            select(
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
   vars <- match.call(expand.dots = FALSE)$`...`

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

   dead <- p$official$new

   reclink <- prep_data(dead, dx)
   check   <- dedup_dx(reclink)
   check   <- append(check, prep_merge(dead, dx))

   flow_validation(p, "dedup_dx", p$params$ym, upload = vars$upload)
   log_success("Done.")
}
