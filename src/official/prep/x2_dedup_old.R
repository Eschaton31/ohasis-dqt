##  Prepare dataset for art registry deduplication -----------------------------

prep_data <- function(old, new) {
   data     <- list()
   data$new <- new %>%
      mutate(
         byr = if_else(
            condition = !is.na(birthdate),
            true      = year(birthdate),
            false     = NA_integer_
         ) %>% as.numeric(),
         bmo = if_else(
            condition = !is.na(birthdate),
            true      = year(birthdate),
            false     = NA_integer_
         ) %>% as.numeric(),
         bdy = if_else(
            condition = !is.na(birthdate),
            true      = year(birthdate),
            false     = NA_integer_
         ) %>% as.numeric(),
      )

   data$old <- old %>%
      mutate(
         byr = if_else(
            condition = !is.na(birthdate),
            true      = year(birthdate),
            false     = NA_integer_
         ) %>% as.numeric(),
         bmo = if_else(
            condition = !is.na(birthdate),
            true      = year(birthdate),
            false     = NA_integer_
         ) %>% as.numeric(),
         bdy = if_else(
            condition = !is.na(birthdate),
            true      = year(birthdate),
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
         "first",
         "middle",
         "last",
         "byr",
         "bmo",
         "bdy"
      ),
      stringdist.match = c(
         "first",
         "middle",
         "last"
      ),
      partial.match    = c(
         "first",
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
            MASTER_RID       = REC_ID,
            MASTER_PID       = PATIENT_ID,
            MASTER_CID       = CENTRAL_ID,
            MASTER_FIRST     = first,
            MASTER_MIDDLE    = middle,
            MASTER_LAST      = last,
            MASTER_SUFFIX    = suffix,
            MASTER_BIRTHDATE = birthdate,
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
                  USING_RID       = REC_ID,
                  USING_PID       = PATIENT_ID,
                  USING_CID       = CENTRAL_ID,
                  USING_PREP_ID   = prep_id,
                  USING_FIRST     = first,
                  USING_MIDDLE    = middle,
                  USING_LAST      = last,
                  USING_SUFFIX    = suffix,
                  USING_BIRTHDATE = birthdate,
                  USING_UIC       = uic,
                  USING_INITIALS  = initials,
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
      dedup_old$reclink <- reclink_review %>%
         mutate(
            Bene  = NA_character_,
            Gab   = NA_character_,
            Lala  = NA_character_,
            Angie = NA_character_,
         )
   }
   return(dedup_old)
}

##  Grouped identifiers for deduplication --------------------------------------

dedup_group_ids <- function(data) {
   dedup_old <- list()
   group_pii <- list(
      "UIC.Base"           = "uic",
      "UIC.Fixed"          = "UIC_SORT",
      "PhilHealth.Fixed"   = "PHIC",
      "PhilSys.Fixed"      = "PHILSYS",
      "PxCode.Base"        = "PATIENT_CODE",
      "PxCode.Fixed"       = "PXCODE_SIEVE",
      "PxUIC.Base"         = c("PATIENT_CODE", "UIC"),
      "PxUIC.Fixed"        = c("PXCODE_SIEVE", "UIC_SORT"),
      "FirstUIC.Base"      = c("FIRST_SIEVE", "UIC"),
      "FirstUIC.Fixed"     = c("FIRST_NY", "UIC_SORT"),
      "FirstUIC.Partial"   = c("FIRST_A", "UIC_SORT"),
      "FirstUIC.Sort"      = c("NAMESORT_FIRST", "UIC_SORT"),
      "PxBD.Base"          = c("PATIENT_CODE", "birthdate"),
      "PxBD.Fixed"         = c("PXCODE_SIEVE", "birthdate"),
      "Name.Base"          = c("FIRST_SIEVE", "LAST_SIEVE", "birthdate"),
      "Name.Fixed"         = c("FIRST_NY", "LAST_NY", "birthdate"),
      "Name.Partial"       = c("FIRST_A", "LAST_A", "birthdate"),
      "Name.Sort"          = c("NAMESORT_FIRST", "NAMESORT_LAST", "birthdate"),
      "YM.BD-Name.Base"    = c("FIRST_SIEVE", "LAST_SIEVE", "BIRTH_YR", "BIRTH_MO"),
      "YD.BD-Name.Base"    = c("FIRST_SIEVE", "LAST_SIEVE", "BIRTH_YR", "BIRTH_DY"),
      "MD.BD-Name.Base"    = c("FIRST_SIEVE", "LAST_SIEVE", "BIRTH_MO", "BIRTH_DY"),
      "YM.BD-Name.Fixed"   = c("FIRST_NY", "LAST_NY", "BIRTH_YR", "BIRTH_MO"),
      "YD.BD-Name.Fixed"   = c("FIRST_NY", "LAST_NY", "BIRTH_YR", "BIRTH_DY"),
      "MD.BD-Name.Fixed"   = c("FIRST_NY", "LAST_NY", "BIRTH_MO", "BIRTH_DY"),
      "YM.BD-Name.Partial" = c("FIRST_A", "LAST_A", "BIRTH_YR", "BIRTH_MO"),
      "YD.BD-Name.Partial" = c("FIRST_A", "LAST_A", "BIRTH_YR", "BIRTH_DY"),
      "MD.BD-Name.Partial" = c("FIRST_A", "LAST_A", "BIRTH_MO", "BIRTH_DY"),
      "YM.BD-Name.Sort"    = c("NAMESORT_FIRST", "NAMESORT_LAST", "BIRTH_YR", "BIRTH_MO"),
      "YD.BD-Name.Sort"    = c("NAMESORT_FIRST", "NAMESORT_LAST", "BIRTH_YR", "BIRTH_DY"),
      "MD.BD-Name.Sort"    = c("NAMESORT_FIRST", "NAMESORT_LAST", "BIRTH_MO", "BIRTH_DY")
   )
   for (i in seq_len(length(group_pii))) {
      dedup_name <- names(group_pii)[[i]]
      dedup_id   <- group_pii[[i]]

      # tag duplicates based on grouping
      df <- data %>%
         filter(if_all(any_of(dedup_id), ~!is.na(.))) %>%
         get_dupes(all_of(dedup_id)) %>%
         filter(dupe_count > 0) %>%
         mutate(
            ym = stri_c(sep = "-", year, stri_pad_left(month, 2, "0"))
         ) %>%
         group_by(across(all_of(dedup_id))) %>%
         mutate(
            .before  = prep_id,
            grp_ym   = str_c(collapse = ", ", sort(ym)),
            grp_id   = str_c(collapse = ",", sort(prep_id)),
            grp_sort = case_when(
               any(year == params$yr & month == params$mo) ~ 1,
               TRUE ~ 9
            )
         ) %>%
         ungroup() %>%
         arrange(grp_sort, across(all_of(dedup_id))) %>%
         select(-grp_sort)

      # if any found, include in list for review
      dedup_old[[dedup_name]] <- df
   }
   all_dedup <- combine_validations(data, dedup_old, c("grp_id", "prep_id", "grp_ym")) %>%
      group_by(grp_id, grp_ym) %>%
      mutate(
         grp_sort = case_when(
            any(year == params$yr & month == params$mo) ~ 1,
            TRUE ~ 9
         ),
      ) %>%
      ungroup()

   adjust_score <- c(
      `issue_UIC.Base`           = 3,
      `issue_UIC.Fixed`          = 3,
      `issue_PhilHealth.Fixed`   = 1,
      `issue_PhilSys.Fixed`      = 1,
      `issue_ConfirmCode.Base`   = 3,
      `issue_ConfirmCode.Fixed`  = 3,
      `issue_PxCode.Base`        = 1,
      `issue_PxCode.Fixed`       = 1,
      `issue_PxConfirm.Base`     = 3,
      `issue_PxConfirm.Fixed`    = 3,
      `issue_ConfirmUIC.Base`    = 4,
      `issue_ConfirmUIC.Fixed`   = 4,
      `issue_PxUIC.Base`         = 3,
      `issue_PxUIC.Fixed`        = 3,
      `issue_FirstUIC.Base`      = 3,
      `issue_FirstUIC.Fixed`     = 3,
      `issue_FirstUIC.Partial`   = 1,
      `issue_FirstUIC.Sort`      = 3,
      `issue_PxBD.Base`          = 1,
      `issue_PxBD.Fixed`         = 1,
      `issue_Name.Base`          = 4,
      `issue_Name.Fixed`         = 4,
      `issue_Name.Partial`       = 1,
      `issue_Name.Sort`          = 3,
      `issue_YM.BD-Name.Base`    = 3,
      `issue_YD.BD-Name.Base`    = 3,
      `issue_MD.BD-Name.Base`    = 3,
      `issue_YM.BD-Name.Fixed`   = 2,
      `issue_YD.BD-Name.Fixed`   = 2,
      `issue_MD.BD-Name.Fixed`   = 2,
      `issue_YM.BD-Name.Partial` = 1,
      `issue_YD.BD-Name.Partial` = 1,
      `issue_MD.BD-Name.Partial` = 1,
      `issue_YM.BD-Name.Sort`    = 2,
      `issue_YD.BD-Name.Sort`    = 2,
      `issue_MD.BD-Name.Sort`    = 2
   )
   adjust_only  <- intersect(names(adjust_score), names(all_dedup))
   for (var in adjust_only) {
      all_dedup %<>%
         mutate_at(
            .vars = vars(matches(var)),
            ~if_else(. == 1, adjust_score[[var]], 0, 0)
         )
   }

   all_dedup %<>%
      mutate(score = rowMeans(select(., starts_with("issue")), na.rm = TRUE)) %>%
      arrange(desc(score), grp_sort, grp_id) %>%
      select(-grp_sort) %>%
      select(
         -any_of(c(
            'LAST',
            'MIDDLE',
            'FIRST',
            'SUFFIX',
            'UIC',
            'CONFIRMATORY_CODE',
            'PATIENT_CODE',
            'PHILHEALTH_NO',
            'PHILSYS_ID',
            'BIRTH_YR',
            'BIRTH_MO',
            'BIRTH_DY',
            'UIC_MOM',
            'UIC_DAD',
            'UIC_ORDER',
            'FIRST_A',
            'MIDDLE_A',
            'LAST_A',
            'CONFIRM_SIEVE',
            'PXCODE_SIEVE',
            'FIRST_SIEVE',
            'MIDDLE_SIEVE',
            'LAST_SIEVE',
            'PHIC',
            'PHILSYS',
            'FIRST_NY',
            'MIDDLE_NY',
            'LAST_NY',
            'UIC_1',
            'UIC_2',
            'UIC_SORT',
            'NAMESORT_FIRST',
            'NAMESORT_LAST'
         ))
      )
   dedup_old <- list(group_dedup = all_dedup)

   return(dedup_old)
}

##  Actual flow ----------------------------------------------------------------

.init <- function(envir = parent.env(environment()), ...) {
   step <- parent.env(environment())
   p    <- envir
   vars <- as.list(list(...))

   full <- p$official$new_reg %>%
      mutate(
         data_filter = if_else(year == p$params$yr & month == p$params$mo, "new", "old", "old")
      )
   old  <- full %>% filter(data_filter == "old")
   new  <- full %>% filter(data_filter == "new")

   data <- dedup_prep(
      full %>% mutate(confirmatory_code = NA_character_),
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

   reclink <- prep_data(old, new)
   check   <- dedup_old(reclink)
   check   <- append(check, dedup_group_ids(data))

   step$check <- check
   flow_validation(p, "dedup_old", p$params$ym, upload = vars$upload)
   log_success("Done.")
}
