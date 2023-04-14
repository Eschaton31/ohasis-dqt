##  Prepare dataset for registry deduplication ---------------------------------

prep_data <- function(old, new) {
   data     <- list()
   data$new <- new %>%
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

   data$old <- old %>%
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

dedup_old <- function(data) {
   dedup_old  <- list()
   reclink_df <- fastLink(
      dfA              = data$new,
      dfB              = data$old,
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
         dfA         = data$new,
         dfB         = data$old,
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
            MASTER_SUFFIX       = name_suffix,
            MASTER_BIRTHDATE    = bdate,
            MASTER_CONFIRMATORY = labcode,
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
                  USING_RID          = REC_ID,
                  USING_PID          = PATIENT_ID,
                  USING_CID          = CENTRAL_ID,
                  USING_IDNUM        = idnum,
                  USING_FIRST        = firstname,
                  USING_MIDDLE       = middle,
                  USING_LAST         = last,
                  USING_SUFFIX       = name_suffix,
                  USING_BIRTHDATE    = bdate,
                  USING_CONFIRMATORY = labcode,
                  USING_UIC          = uic,
                  USING_PATIENT_CODE = pxcode,
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
      dedup_old$reclink <- reclink_review
   }

   return(dedup_old)
}

dedup_group_ids <- function(data) {
   dedup_old <- list()
   group_pii <- list(
      "UIC.Base"           = "uic",
      "UIC.Fixed"          = "UIC_SORT",
      "PhilHealth.Fixed"   = "PHIC",
      "PhilSys.Fixed"      = "PHILSYS",
      "ConfirmCode.Base"   = "CONFIRMATORY_CODE",
      "ConfirmCode.Fixed"  = "CONFIRM_SIEVE",
      "PxConfirm.Base"     = c("PATIENT_CODE", "CONFIRMATORY_CODE"),
      "PxConfirm.Fixed"    = c("PXCODE_SIEVE", "CONFIRM_SIEVE"),
      "ConfirmUIC.Base"    = c("CONFIRMATORY_CODE", "UIC"),
      "ConfirmUIC.Fixed"   = c("CONFIRM_SIEVE", "UIC"),
      "PxUIC.Base"         = c("PATIENT_CODE", "UIC"),
      "PxUIC.Fixed"        = c("PXCODE_SIEVE", "UIC"),
      "FirstUIC.Base"      = c("FIRST", "UIC"),
      "FirstUIC.Fixed"     = c("FIRST_NY", "UIC"),
      "FirstUIC.Partial"   = c("FIRST_A", "UIC"),
      "PxBD.Base"          = c("PATIENT_CODE", "bdate"),
      "PxBD.Fixed"         = c("PXCODE_SIEVE", "bdate"),
      "Name.Base"          = c("FIRST", "LAST", "bdate"),
      "Name.Fixed"         = c("FIRST_NY", "LAST_NY", "bdate"),
      "Name.Partial"       = c("FIRST_A", "LAST_A", "bdate"),
      "YM.BD-Name.Base"    = c("FIRST", "LAST", "BIRTH_YR", "BIRTH_MO"),
      "YD.BD-Name.Base"    = c("FIRST", "LAST", "BIRTH_YR", "BIRTH_DY"),
      "MD.BD-Name.Base"    = c("FIRST", "LAST", "BIRTH_MO", "BIRTH_DY"),
      "YM.BD-Name.Fixed"   = c("FIRST_NY", "LAST_NY", "BIRTH_YR", "BIRTH_MO"),
      "YD.BD-Name.Fixed"   = c("FIRST_NY", "LAST_NY", "BIRTH_YR", "BIRTH_DY"),
      "MD.BD-Name.Fixed"   = c("FIRST_NY", "LAST_NY", "BIRTH_MO", "BIRTH_DY"),
      "YM.BD-Name.Partial" = c("FIRST_A", "LAST_A", "BIRTH_YR", "BIRTH_MO"),
      "YD.BD-Name.Partial" = c("FIRST_A", "LAST_A", "BIRTH_YR", "BIRTH_DY"),
      "MD.BD-Name.Partial" = c("FIRST_A", "LAST_A", "BIRTH_MO", "BIRTH_DY")
   )
   for (i in seq_len(length(group_pii))) {
      dedup_name <- names(group_pii)[[i]]
      dedup_id   <- group_pii[[i]]

      # tag duplicates based on grouping
      df <- data %>%
         filter(if_all(any_of(dedup_id), ~!is.na(.))) %>%
         get_dupes(all_of(dedup_id)) %>%
         filter(dupe_count > 0) %>%
         group_by(across(all_of(dedup_id))) %>%
         mutate(
            # generate a group id to identify groups of duplicates
            group_id = cur_group_id(),
         ) %>%
         ungroup() %>%
         mutate(DUP_IDS = paste(collapse = ', ', dedup_id))

      # if any found, include in list for review
      dedup_old[[dedup_name]] <- df
   }

   return(dedup_old)
}

##  Actual flow ----------------------------------------------------------------

.init <- function() {
   p <- parent.env(environment())
   local(envir = p, {
      old  <- .GlobalEnv$nhsss$harp_dx$official$old
      new  <- read_rds(file.path(wd, "converted.RDS"))
      full <- read_rds(file.path(wd, "final.RDS"))
      data <- dedup_prep(
         full %>%
            zap_label %>%
            zap_labels %>%
            zap_formats %>%
            mutate(philsys_id = NA_character_),
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

      reclink <- prep_data(old, new)
      check   <- dedup_old(reclink)
      check   <- append(check, dedup_group_ids(data))
      rm(old, new, data, reclink)
   })

   local(envir = .GlobalEnv, flow_validation(nhsss$harp_dx, "dedup_old", ohasis$ym))
}
