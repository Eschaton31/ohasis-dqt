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

dedup_dx <- function(data) {
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
         )
   }
   return(dedup_dx)
}

##  Merge w/ registry using labcode --------------------------------------------

prep_merge <- function(tx, dx) {
   tx <- tx %>%
      filter(is.na(idnum)) %>%
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

   dx <- dx %>%
      mutate(
         # standardize PII
         LAST              = stri_trans_toupper(last),
         MIDDLE            = stri_trans_toupper(middle),
         FIRST             = stri_trans_toupper(firstname),
         SUFFIX            = stri_trans_toupper(name_suffix),
         UIC               = stri_trans_toupper(uic),
         CONFIRMATORY_CODE = stri_trans_toupper(labcode),

         # get components of birthdate
         BIRTH_YR          = year(bdate),
         BIRTH_MO          = month(bdate),
         BIRTH_DY          = day(bdate),

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

   dedup_dx       <- list()
   dedup_dx$merge <- tx %>%
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

.init <- function() {
   p <- parent.env(environment())
   local(envir = p, {
      dx <- ohasis$get_data("harp_dx", ohasis$yr, ohasis$mo) %>%
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
         left_join(
            y  = nhsss$harp_tx$forms$id_registry,
            by = "PATIENT_ID"
         ) %>%
         mutate(
            CENTRAL_ID = if_else(
               condition = is.na(CENTRAL_ID),
               true      = PATIENT_ID,
               false     = CENTRAL_ID
            ),
            labcode2   = if_else(is.na(labcode2), labcode, labcode2)
         )
      tx <- read_rds(file.path(wd, "reg.final.RDS"))

      reclink <- prep_data(tx, dx)
      check   <- dedup_dx(reclink)
      check   <- append(check, prep_merge(tx, dx))
      rm(dx, tx, reclink)
   })

   local(envir = .GlobalEnv, flow_validation(nhsss$harp_tx, "dedup_dx", ohasis$ym))
}
