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

dedup_old <- function(data, non_dupes) {
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
            MASTER_CID          = CENTRAL_ID,
            MASTER_FIRST        = firstname,
            MASTER_MIDDLE       = middle,
            MASTER_LAST         = last,
            MASTER_SUFFIX       = name_suffix,
            MASTER_BIRTHDATE    = bdate,
            MASTER_CONFIRMATORY = labcode2,
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
                  USING_CID          = CENTRAL_ID,
                  USING_IDNUM        = idnum,
                  USING_FIRST        = firstname,
                  USING_MIDDLE       = middle,
                  USING_LAST         = last,
                  USING_SUFFIX       = name_suffix,
                  USING_BIRTHDATE    = bdate,
                  USING_CONFIRMATORY = labcode2,
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
      dedup_old$reclink <- reclink_review %>%
         mutate(
            Bene  = NA_character_,
            Gab   = NA_character_,
            Lala  = NA_character_,
            Angie = NA_character_,
         ) %>%
         anti_join(
            y  = non_dupes %>%
               select(USING_CID = PATIENT_ID, MASTER_CID = NON_PAIR_ID),
            by = join_by(USING_CID, MASTER_CID)
         )
   }

   return(dedup_old)
}

dedup_group_ids <- function(data, params, non_dupes) {
   dedup_old <- list()
   group_pii <- list(
      "UIC.Base"           = "uic",
      "UIC.Fixed"          = "UIC_SORT",
      "PhilHealth.Fixed"   = "PHIC",
      "PhilSys.Fixed"      = "PHILSYS",
      "PxUIC.Base"         = c("PATIENT_CODE", "UIC"),
      "PxUIC.Fixed"        = c("PXCODE_SIEVE", "UIC_SORT"),
      "FirstUIC.Base"      = c("FIRST_SIEVE", "UIC_SORT"),
      "FirstUIC.Fixed"     = c("FIRST_NY", "UIC_SORT"),
      "FirstUIC.Partial"   = c("FIRST_A", "UIC_SORT"),
      "FirstUIC.Sort"      = c("NAMESORT_FIRST", "UIC_SORT"),
      "FirstBD.Base"       = c("FIRST_SIEVE", "bdate"),
      "FirstBD.Fixed"      = c("FIRST_NY", "bdate"),
      "FirstBD.Sort"       = c("NAMESORT_FIRST", "bdate"),
      "PxBD.Base"          = c("PATIENT_CODE", "bdate"),
      "PxBD.Fixed"         = c("PXCODE_SIEVE", "bdate"),
      "Email"              = "email",
      "Mobile"             = "mobile",
      "Email.Mobile"       = c("email", "mobile"),
      "Name.Base"          = c("FIRST_SIEVE", "LAST_SIEVE", "bdate"),
      "Name.Fixed"         = c("FIRST_NY", "LAST_NY", "bdate"),
      "Name.Partial"       = c("FIRST_A", "LAST_A", "bdate"),
      "Name.Sort"          = c("NAMESORT_FIRST", "NAMESORT_LAST", "bdate"),
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
            .before  = idnum,
            grp_ym   = str_c(collapse = ", ", sort(ym)),
            grp_id   = str_c(collapse = ",", sort(idnum)),
            grp_sort = if_else(any(year == params$yr & month == params$mo), 1, 8, 9),
         ) %>%
         ungroup() %>%
         arrange(grp_sort, across(all_of(dedup_id))) %>%
         select(-grp_sort)

      # if any found, include in list for review
      dedup_old[[dedup_name]] <- df %>%
         anti_join(non_dupes, join_by(grp_id))
   }
   all_dedup <- combine_validations(data, dedup_old, c("grp_id", "idnum", "grp_ym")) %>%
      group_by(grp_id, grp_ym) %>%
      mutate(
         grp_sort = case_when(
            any(year == params$yr & month == params$mo) ~ 1,
            any(is.na(idnum)) ~ 2,
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

   full <- p$official$new %>%
      mutate(
         data_filter = if_else(year == p$params$yr & month == p$params$mo, "new", "old", "old")
      ) %>%
      select(
         -labcode,
         -REC_ID,
         -PATIENT_ID,
         -form,
         -modality,
         -consent_test,
         -any_of(
            c(
               "ocw",
               "motherisi1",
               "pregnant",
               "tbpatient1",
               "sexwithf",
               "sexwithm",
               "sexwithpro",
               "regularlya",
               "injectdrug",
               "chemsex",
               "receivedbt",
               "sti",
               "needlepri1",
               "risk_motherhashiv",
               "risk_sexwithf",
               "risk_sexwithf_nocdm",
               "risk_sexwithm",
               "risk_sexwithm_nocdm",
               "risk_payingforsex",
               "risk_paymentforsex",
               "risk_sexwithhiv",
               "risk_injectdrug",
               "risk_needlestick",
               "risk_bloodtransfuse",
               "risk_illicitdrug",
               "risk_chemsex",
               "risk_tattoo",
               "risk_sti",
               "confirm_region",
               "confirm_province",
               "confirm_muncity",
               "confirm_result",
               "confirm_remarks",
               "age_sex",
               "age_inj",
               "howmanymse",
               "yrlastmsex",
               "howmanyfse",
               "yrlastfsex",
               "past12mo_injdrug",
               "past12mo_rcvbt",
               "past12mo_sti",
               "past12mo_sexfnocondom",
               "past12mo_sexmnocondom",
               "past12mo_sexprosti",
               "past12mo_acceptpayforsex",
               "past12mo_needle",
               "past12mo_hadtattoo",
               "history_sex_m",
               "date_lastsex_m",
               "date_lastsex_condomless_m",
               "history_sex_f",
               "date_lastsex_f",
               "date_lastsex_condomless_f",
               "recombyph1",
               "recomby_peer_ed",
               "insurance1",
               "recheckpr1",
               "no_test_reason",
               "possible_exposure",
               "emp_local",
               "emp_abroad",
               "other_reason_test",
               "hx_hepb",
               "hx_hepc",
               "hx_cbs",
               "hx_prep",
               "hx_pep",
               "hx_sti",
               "reach_clinical",
               "reach_online",
               "reach_it",
               "reach_ssnt",
               "reach_venue",
               "refer_art",
               "refer_confirm",
               "retest",
               "retest_in_mos",
               "retest_in_wks",
               "retest_date",
               "given_hiv101",
               "given_iec_mats",
               "given_risk_reduce",
               "given_prep_pep",
               "given_ssnt",
               "provider_type",
               "provider_type_other",
               "venue_region",
               "venue_province",
               "venue_muncity",
               "venue_text",
               "px_type",
               "referred_by",
               "hts_date",
               "t0_date",
               "t0_result",
               "test_done",
               "name",
               "t1_date",
               "t1_kit",
               "t1_result",
               "t2_date",
               "t2_kit",
               "t2_result",
               "t3_date",
               "t3_kit",
               "t3_result",
               "final_interpretation",
               "visit_date",
               "blood_extract_date",
               "specimen_receipt_date",
               "rhivda_done",
               "sample_source",
               "diff_source_v_form",
               "SOURCE_FACI",
               "HTS_FACI",
               "DUP_MUNC",
               "age_pregnant",
               "age_vertical",
               "age_unknown",
               "harpid",
               "first",
               "clinichosp",
               "othernat",
               "job",
               "local",
               "travel",
               "labtest",
               "reporttype",
               "deadaids",
               "whendead",
               "otherinfo",
               "bldunit",
               "permanenta",
               "placeofbir",
               "withmultip",
               "reasonstes",
               "sexpartne1",
               "sharednsw1",
               "employmen1",
               "employme21",
               "received21",
               "pregnant21",
               "hepatitis1",
               "noreason1",
               "wantsknow1",
               "addresstes",
               "shc",
               "hub_reg",
               "begda",
               "test_date",
               "CONFIRM_FACI",
               "TEST_FACI",
               "CD4_CONFIRM",
               "update_ocw"
            )
         )
      ) %>%
      mutate(
         philhealth = str_replace_all(philhealth, "[^[:alnum:]]", ""),
         philhealth = clean_pii(philhealth)
      )
   old  <- full %>% filter(data_filter == "old")
   new  <- full %>% filter(data_filter == "new")

   data <- dedup_prep(
      full,
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
   check   <- dedup_old(reclink, p$forms$non_dupes)
   check   <- append(check, dedup_group_ids(data, p$params, p$corr$non_dupes))

   step$check <- check
   flow_validation(p, "dedup_old", p$params$ym, upload = vars$upload)
   log_success("Done.")
}
