##  Prepare dataset for tx registry deduplication ------------------------------

prep_data <- function(prep, tx) {
   data      <- list()
   data$prep <- prep %>%
      filter(is.na(art_id)) %>%
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

   data$tx <- tx %>%
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

   return(data)
}

##  Patient Record Linkage Algorithm -------------------------------------------

dedup_tx <- function(data) {
   dedup_tx   <- list()
   reclink_df <- fastLink(
      dfA              = data$prep,
      dfB              = data$tx,
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
         dfA         = data$prep,
         dfB         = data$tx,
         fl.out      = reclink_df,
         combine.dfs = FALSE
      )

      reclink_review <- reclink_matched$dfA.match %>%
         mutate(
            MATCH_ID = row_number()
         ) %>%
         select(
            MATCH_ID,
            MASTER_RID       = REC_ID,
            MASTER_PID       = PATIENT_ID,
            MASTER_CID       = CENTRAL_ID,
            MASTER_FIRST     = firstname,
            MASTER_MIDDLE    = middle,
            MASTER_LAST      = last,
            MASTER_SUFFIX    = suffix,
            MASTER_BIRTHDATE = birthdate,
            # MASTER_CONFIRMATORY = confirmatory_code,
            MASTER_UIC       = uic,
            MASTER_INITIALS  = initials,
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
                  USING_ART_ID       = art_id,
                  USING_FIRST        = firstname,
                  USING_MIDDLE       = middle,
                  USING_LAST         = last,
                  USING_SUFFIX       = name_suffix,
                  USING_BIRTHDATE    = birthdate,
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
      dedup_tx$reclink <- reclink_review %>%
         mutate(
            Bene  = NA_character_,
            Gab   = NA_character_,
            Lala  = NA_character_,
            Angie = NA_character_,
         )
   }
   return(dedup_tx)
}

##  Merge w/ registry using labcode --------------------------------------------

prep_merge <- function(prep, tx) {
   prep <- prep %>%
      filter(is.na(art_id)) %>%
      mutate(
         # standardize PII
         LAST              = stri_trans_toupper(last),
         MIDDLE            = stri_trans_toupper(middle),
         FIRST             = stri_trans_toupper(first),
         SUFFIX            = stri_trans_toupper(suffix),
         UIC               = stri_trans_toupper(uic),
         CONFIRMATORY_CODE = stri_trans_toupper(confirmatory_code),

         # get components of birthdate
         BIRTH_YR          = year(birthdate),
         BIRTH_MO          = month(birthdate),
         BIRTH_DY          = day(birthdate),

         # extract parent info from uic
         UIC_MOM           = if_else(!is.na(UIC), substr(UIC, 1, 2), NA_character_),
         UIC_DAD           = if_else(!is.na(UIC), substr(UIC, 3, 4), NA_character_),

         # variables for firstname 3 letters of names
         FIRST_A           = if_else(!is.na(FIRST), substr(FIRST, 1, 3), NA_character_),
         MIDDLE_A          = if_else(!is.na(MIDDLE), substr(MIDDLE, 1, 3), NA_character_),
         LAST_A            = if_else(!is.na(LAST), substr(LAST, 1, 3), NA_character_),

         LAST              = if_else(is.na(LAST), MIDDLE, LAST),
         MIDDLE            = if_else(is.na(MIDDLE), LAST, MIDDLE),

         # clean ids
         CONFIRM_SIEVE     = if_else(!is.na(CONFIRMATORY_CODE), str_replace_all(CONFIRMATORY_CODE, "[^[:alnum:]]", ""), NA_character_),
         FIRST_S           = if_else(!is.na(FIRST), str_replace_all(FIRST, "[^[:alnum:]]", ""), NA_character_),
         MIDDLE_S          = if_else(!is.na(MIDDLE), str_replace_all(MIDDLE, "[^[:alnum:]]", ""), NA_character_),
         LAST_S            = if_else(!is.na(LAST), str_replace_all(LAST, "[^[:alnum:]]", ""), NA_character_),
         PHIC              = if_else(!is.na(philhealth_no), str_replace_all(philhealth_no, "[^[:alnum:]]", ""), NA_character_),

         # code standard names
         FIRST_NY          = if_else(!is.na(FIRST_S), nysiis(FIRST_S, stri_length(FIRST_S)), NA_character_),
         MIDDLE_NY         = if_else(!is.na(MIDDLE_S), nysiis(MIDDLE_S, stri_length(MIDDLE_S)), NA_character_),
         LAST_NY           = if_else(!is.na(LAST_S), nysiis(LAST_S, stri_length(LAST_S)), NA_character_),
      )

   tx <- tx %>%
      mutate(
         # standardize PII
         LAST              = stri_trans_toupper(last),
         MIDDLE            = stri_trans_toupper(middle),
         FIRST             = stri_trans_toupper(firstname),
         SUFFIX            = stri_trans_toupper(name_suffix),
         UIC               = stri_trans_toupper(uic),
         CONFIRMATORY_CODE = stri_trans_toupper(labcode),

         # get components of birthdate
         BIRTH_YR          = year(birthdate),
         BIRTH_MO          = month(birthdate),
         BIRTH_DY          = day(birthdate),

         # extract parent info from uic
         UIC_MOM           = if_else(!is.na(UIC), substr(UIC, 1, 2), NA_character_),
         UIC_DAD           = if_else(!is.na(UIC), substr(UIC, 3, 4), NA_character_),

         # variables for firstname 3 letters of names
         FIRST_A           = if_else(!is.na(FIRST), substr(FIRST, 1, 3), NA_character_),
         MIDDLE_A          = if_else(!is.na(MIDDLE), substr(MIDDLE, 1, 3), NA_character_),
         LAST_A            = if_else(!is.na(LAST), substr(LAST, 1, 3), NA_character_),

         LAST              = if_else(is.na(LAST), MIDDLE, LAST),
         MIDDLE            = if_else(is.na(MIDDLE), LAST, MIDDLE),

         # clean ids
         CONFIRM_SIEVE     = if_else(!is.na(CONFIRMATORY_CODE), str_replace_all(CONFIRMATORY_CODE, "[^[:alnum:]]", ""), NA_character_),
         FIRST_S           = if_else(!is.na(FIRST), str_replace_all(FIRST, "[^[:alnum:]]", ""), NA_character_),
         MIDDLE_S          = if_else(!is.na(MIDDLE), str_replace_all(MIDDLE, "[^[:alnum:]]", ""), NA_character_),
         LAST_S            = if_else(!is.na(LAST), str_replace_all(LAST, "[^[:alnum:]]", ""), NA_character_),
         PHIC              = if_else(!is.na(philhealth), str_replace_all(philhealth, "[^[:alnum:]]", ""), NA_character_),

         # code standard names
         FIRST_NY          = if_else(!is.na(FIRST_S), nysiis(FIRST_S, stri_length(FIRST_S)), NA_character_),
         MIDDLE_NY         = if_else(!is.na(MIDDLE_S), nysiis(MIDDLE_S, stri_length(MIDDLE_S)), NA_character_),
         LAST_NY           = if_else(!is.na(LAST_S), nysiis(LAST_S, stri_length(LAST_S)), NA_character_),
      )

   dedup_tx       <- list()
   dedup_tx$merge <- prep %>%
      select(
         MASTER_RID          = REC_ID,
         MASTER_PID          = PATIENT_ID,
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
         y  = tx %>%
            select(
               # USING_RID          = REC_ID,
               USING_PID          = PATIENT_ID,
               USING_CID          = CENTRAL_ID,
               USING_ART_ID       = art_id,
               USING_FIRST        = firstname,
               USING_MIDDLE       = middle,
               USING_LAST         = last,
               USING_SUFFIX       = name_suffix,
               USING_BIRTHDATE    = birthdate,
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

   return(dedup_tx)
}

##  Actual flow ----------------------------------------------------------------

.init <- function(envir = parent.env(environment()), ...) {
   step <- parent.env(environment())
   p    <- envir
   vars <- as.list(list(...))

   tx <- hs_data("harp_tx", "reg", p$params$yr, p$params$mo) %>%
      read_dta(
         col_select = c(
            PATIENT_ID,
            confirmatory_code,
            art_id,
            uic,
            first,
            middle,
            last,
            suffix,
            birthdate,
            sex,
            px_code,
            philhealth,
            artstart_date
         )
      ) %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      ) %>%
      get_cid(p$forms$id_registry, PATIENT_ID)

   prep <- p$official$new_reg %>%
      mutate(confirmatory_code = NA_character_)

   reclink <- prep_data(prep, tx)
   check   <- dedup_tx(reclink)
   check   <- append(check, prep_merge(tx, tx))

   step$check <- check
   flow_validation(p, "dedup_dx", p$params$ym, upload = vars$upload)
   log_success("Done.")
}
